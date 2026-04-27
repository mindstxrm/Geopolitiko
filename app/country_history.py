"""Country history: Wikipedia narrative and activity timeline."""
import time
import urllib.request
import urllib.error
import json
from datetime import datetime, timedelta
from typing import Optional

# Wikipedia page title overrides for "History of X" (country names that need special handling)
WIKIPEDIA_HISTORY_OVERRIDES = {
    "United States": "History of the United States",
    "United Kingdom": "History of the United Kingdom",
    "China": "History of China",
    "Czechia (Czech Republic)": "History of the Czech Republic",
    "Czech Republic": "History of the Czech Republic",
    "Congo (Congo-Brazzaville)": "History of the Republic of the Congo",
    "Republic of the Congo": "History of the Republic of the Congo",
    "Côte d'Ivoire": "History of Ivory Coast",
    "Ivory Coast": "History of Ivory Coast",
    "Democratic Republic of the Congo": "History of the Democratic Republic of the Congo",
    "South Korea": "History of South Korea",
    "North Korea": "History of North Korea",
    "Russia": "History of Russia",
    "Vietnam": "History of Vietnam",
    "South Africa": "History of South Africa",
    "Taiwan": "History of Taiwan",
    "Palestine": "History of Palestine (region)",
    "Iran": "History of Iran",
    "Syria": "History of Syria",
    "Myanmar": "History of Myanmar",
    "Vatican City": "History of Vatican City",
    "Holy See": "History of Vatican City",
    "Türkiye": "History of Turkey",
    "Turkey": "History of Turkey",
    "Eswatini": "History of Eswatini",
    "Bolivia": "History of Bolivia",
    "Venezuela": "History of Venezuela",
    "Tanzania": "History of Tanzania",
}

# In-memory cache: {country_name: (timestamp, extract, wiki_url)}
_wikipedia_cache: dict = {}
WIKIPEDIA_CACHE_TTL_SEC = 86400  # 24 hours


def _slug_for_wikipedia(name: str) -> str:
    """Convert country name to Wikipedia URL slug."""
    if not name or not name.strip():
        return ""
    s = name.strip()
    if s in WIKIPEDIA_HISTORY_OVERRIDES:
        return WIKIPEDIA_HISTORY_OVERRIDES[s].replace(" ", "_")
    return f"History of {s}".replace(" ", "_")


def get_country_history_from_wikipedia(country_name: str) -> dict:
    """
    Fetch country history intro from Wikipedia REST API.
    Returns dict with keys: extract, extract_html, url, title, error.
    """
    if not country_name or not country_name.strip():
        return {"error": "No country name", "extract": None, "url": None, "title": None}
    name = country_name.strip()
    cache_key = name.lower()
    now = time.time()
    if cache_key in _wikipedia_cache:
        ts, extract, url, title = _wikipedia_cache[cache_key]
        if now - ts < WIKIPEDIA_CACHE_TTL_SEC:
            return {"extract": extract, "url": url, "title": title, "error": None}
    slug = _slug_for_wikipedia(name)
    url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{slug}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "GeopoliticalNews/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        extract = (data.get("extract") or "").strip()
        extract_html = (data.get("extract_html") or "").strip()
        page_url = None
        if data.get("content_urls", {}).get("desktop", {}).get("page"):
            page_url = data["content_urls"]["desktop"]["page"]
        title = data.get("title") or slug.replace("_", " ")
        result = {
            "extract": extract,
            "extract_html": extract_html,
            "url": page_url,
            "title": title,
            "error": None,
        }
        _wikipedia_cache[cache_key] = (now, extract, page_url, title)
        return result
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return {"extract": None, "url": None, "title": None, "error": "Page not found"}
        return {"extract": None, "url": None, "title": None, "error": str(e)}
    except Exception as e:
        return {"extract": None, "url": None, "title": None, "error": str(e)}


def get_country_activity_timeline(
    country_code: str,
    country_name: str,
    days: int = 90,
    limit_per_source: int = 15,
) -> list:
    """
    Aggregate articles, sanctions, protests, conflict events, elections into
    a unified timeline sorted by date (newest first).
    Each item: { date, type, title, summary, source, url?, extra? }
    """
    from app.models import (
        get_articles,
        get_sanctions,
        get_protest_tracking,
        get_conflict_event_imports,
        get_election_calendar,
    )

    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    items = []

    # Articles (by country name in entities/topics)
    try:
        arts = get_articles(
            country=country_name,
            days=days,
            limit=limit_per_source,
        )
        for a in arts:
            pub = (a.get("published_utc") or a.get("scraped_at") or "")[:10]
            if pub and pub >= cutoff:
                items.append({
                    "date": pub,
                    "type": "article",
                    "title": (a.get("title") or "")[:120],
                    "summary": (a.get("summary") or "")[:200],
                    "source": a.get("source_name") or "Article",
                    "url": a.get("url"),
                    "extra": {"impact_score": a.get("impact_score")},
                })
    except Exception:
        pass

    # Sanctions (target = country name)
    try:
        sanc = get_sanctions(target=country_name, limit=limit_per_source)
        for s in sanc:
            start = (s.get("start_date") or "")[:10]
            if start and start >= cutoff:
                desc = (s.get("description") or s.get("measure_type") or "")[:150]
                items.append({
                    "date": start,
                    "type": "sanction",
                    "title": f"{s.get('imposing_country') or '—'} → {s.get('target_country') or '—'}",
                    "summary": desc,
                    "source": "Sanctions",
                    "url": s.get("source_url"),
                    "extra": {"measure_type": s.get("measure_type")},
                })
    except Exception:
        pass

    # Protests
    try:
        protests = get_protest_tracking(
            country_code=country_code,
            date_from=cutoff,
            limit=limit_per_source,
        )
        for p in protests:
            ed = (p.get("event_date") or "")[:10]
            if ed:
                items.append({
                    "date": ed,
                    "type": "protest",
                    "title": (p.get("summary") or p.get("trigger_topic") or "Protest")[:120],
                    "summary": (p.get("trigger_topic") or "")[:150],
                    "source": "Protest tracking",
                    "url": None,
                    "extra": {"estimated_size": p.get("estimated_size")},
                })
    except Exception:
        pass

    # Conflict events
    try:
        conflicts = get_conflict_event_imports(country_code=country_code, limit=limit_per_source)
        for c in conflicts:
            ed = (c.get("event_date") or "")[:10]
            if ed and ed >= cutoff:
                items.append({
                    "date": ed,
                    "type": "conflict",
                    "title": (c.get("event_type") or "Conflict event")[:80],
                    "summary": (c.get("summary") or "")[:200],
                    "source": c.get("source") or "Conflict data",
                    "url": None,
                    "extra": {"fatalities": c.get("fatalities")},
                })
    except Exception:
        pass

    # Elections
    try:
        elections = get_election_calendar(
            country_code=country_code,
            date_from=cutoff,
            limit=limit_per_source,
        )
        for e in elections:
            dp = (e.get("date_planned") or "")[:10]
            if dp:
                items.append({
                    "date": dp,
                    "type": "election",
                    "title": (e.get("election_type") or "Election")[:80],
                    "summary": (e.get("status") or "")[:100],
                    "source": "Election calendar",
                    "url": None,
                    "extra": {"status": e.get("status")},
                })
    except Exception:
        pass

    # Sort by date descending
    items.sort(key=lambda x: x["date"] or "", reverse=True)
    return items[:80]  # Cap total
