"""RSS feed scraper for geopolitical news sources."""
import re
import feedparser
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from dateutil import tz as dateutil_tz
from requests import get
from requests.exceptions import RequestException

from config import SCRAPE_USER_AGENT
from app.scrapers.sources import SOURCES
from app.models import init_db, upsert_article

# YouTube/Vimeo URL patterns for embed detection
_VIDEO_HOST_PATTERNS = [
    (re.compile(r"(?:youtube\.com/watch\?v=|youtu\.be/)([a-zA-Z0-9_-]{11})", re.I), "youtube"),
    (re.compile(r"vimeo\.com/(?:video/)?(\d+)", re.I), "vimeo"),
]


def _extract_media(entry) -> tuple[str | None, str | None]:
    """Extract (image_url, video_url) from a feed entry. Returns (None, None) if nothing found."""
    image_url = None
    video_url = None
    # Media RSS: media_content, media_thumbnail
    if getattr(entry, "media_content", None) and len(entry.media_content) > 0:
        m = entry.media_content[0]
        url = m.get("url") if isinstance(m, dict) else getattr(m, "url", None)
        mt = (m.get("type") or "") if isinstance(m, dict) else getattr(m, "type", "") or ""
        if url:
            if "video" in mt:
                video_url = url
            elif "image" in mt or not mt:
                image_url = url
    if not image_url and getattr(entry, "media_thumbnail", None) and len(entry.media_thumbnail) > 0:
        m = entry.media_thumbnail[0]
        url = m.get("url") if isinstance(m, dict) else getattr(m, "url", None)
        if url:
            image_url = url
    # Enclosures (RSS)
    if getattr(entry, "enclosures", None):
        for enc in entry.enclosures:
            href = enc.get("href") if isinstance(enc, dict) else getattr(enc, "href", None)
            typ = (enc.get("type") or "") if isinstance(enc, dict) else getattr(enc, "type", "") or ""
            if not href:
                continue
            if "video" in typ:
                if not video_url:
                    video_url = href
            elif "image" in typ or typ in ("", "application/octet-stream"):
                if not image_url:
                    image_url = href
    # First <img> in summary/description
    if not image_url:
        raw = entry.get("summary") or entry.get("description") or ""
        if raw:
            soup = BeautifulSoup(raw, "html.parser")
            img = soup.find("img")
            if img and img.get("src"):
                src = img["src"].strip()
                if src.startswith("http"):
                    image_url = src
    # Video URL from summary or link (YouTube/Vimeo)
    if not video_url:
        text = (entry.get("summary") or "") + " " + (entry.get("link") or "")
        for pat, kind in _VIDEO_HOST_PATTERNS:
            mo = pat.search(text)
            if mo:
                if kind == "youtube":
                    video_url = f"https://www.youtube.com/embed/{mo.group(1)}"
                else:
                    video_url = f"https://player.vimeo.com/video/{mo.group(1)}"
                break
    return (image_url, video_url)


def _strip_html(text: str, max_len: int = 2000) -> str:
    """Remove HTML tags and normalize whitespace."""
    if not text or not isinstance(text, str):
        return ""
    soup = BeautifulSoup(text, "html.parser")
    raw = soup.get_text(separator=" ")
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw[:max_len]


def _parse_date(entry) -> str | None:
    """Extract ISO UTC date string from feed entry."""
    for key in ("published", "updated", "created"):
        val = entry.get(key)
        if val:
            try:
                dt = date_parser.parse(val)
                if dt.tzinfo:
                    dt = dt.astimezone(dateutil_tz.tzutc())
                return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except (ValueError, TypeError):
                pass
    return None


def _fetch_feed(url: str) -> feedparser.FeedParserDict | None:
    """Fetch and parse an RSS/Atom feed with a polite User-Agent."""
    try:
        resp = get(url, timeout=15, headers={"User-Agent": SCRAPE_USER_AGENT})
        resp.raise_for_status()
        return feedparser.parse(resp.content)
    except RequestException:
        return None


def scrape_source(source: dict, db_path: str) -> int:
    """Scrape one source and upsert articles into the database. Returns count added."""
    init_db(db_path)
    feed = _fetch_feed(source["url"])
    if not feed or not feed.entries:
        return 0
    count = 0
    for entry in feed.entries:
        link = entry.get("link")
        if not link:
            continue
        title = entry.get("title") or "No title"
        summary = ""
        raw = entry.get("summary") or ""
        summary = _strip_html(raw if isinstance(raw, str) else str(raw))
        published = _parse_date(entry)
        image_url, video_url = _extract_media(entry)
        upsert_article(
            title=title,
            url=link,
            source_name=source["name"],
            source_url=source.get("homepage", ""),
            summary=summary,
            published_utc=published,
            image_url=image_url,
            video_url=video_url,
        )
        count += 1
    return count


def scrape_all_sources(db_path: str) -> dict[str, int]:
    """Scrape all configured sources and return per-source counts."""
    results = {}
    for source in SOURCES:
        name = source["name"]
        try:
            results[name] = scrape_source(source, db_path)
        except Exception:
            results[name] = 0
    return results
