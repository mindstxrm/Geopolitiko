"""Daily and weekly digest generation."""
import json
import logging
import os
import smtplib
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from config import DATABASE_PATH
from app.models import (
    init_db,
    add_digest,
    get_digests,
    get_daily_digest_for_date,
    update_digest,
    get_articles,
    get_trending_topics,
)

logger = logging.getLogger(__name__)


def _generate_digest_with_openai(articles: list, digest_type: str) -> str:
    """Use OpenAI to write a professional digest. Returns JSON string with items (title, analysis, url, source)."""
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return _generate_digest_fallback(articles, digest_type)
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        # Pass enough context: title + summary/key_takeaways so the model can write insightfully
        story_lines = []
        for i, a in enumerate(articles[:12], 1):
            title = a.get("title", "")
            summary = (a.get("summary") or "")[:400]
            takeaways = (a.get("key_takeaways") or "")[:300]
            why = (a.get("why_it_matters") or "")[:200]
            story_lines.append(
                f"{i}. {title}\n   Summary: {summary or 'N/A'}\n   Key points: {takeaways or 'N/A'}\n   Why it matters: {why or 'N/A'}"
            )
        stories_text = "\n\n".join(story_lines)
        prompt = f"""You are writing a daily geopolitical briefing for senior analysts and decision-makers. Based on the following stories, produce a concise but insightful digest.

For each of the top 6–8 stories, write:
- "title": the headline (use the original headline)
- "analysis": 2–4 sentences that explain what happened, why it matters, and any implications for policy, markets, or regional stability. Be specific and analytical, not generic.

Stories (with summaries and key points):
{stories_text}

Reply with ONLY a valid JSON array of objects, each with keys "title" and "analysis". No markdown, no code fence, no other text. Use double quotes for strings."""
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=2000,
        )
        content = (response.choices[0].message.content or "").strip()
        content = content.strip().strip("`").strip()
        if content.startswith("json"):
            content = content[4:].strip()
        arr = json.loads(content)
        if not isinstance(arr, list):
            arr = []
        # Enrich with url and source from our articles (match by title)
        items = []
        for obj in arr[:8]:
            title = obj.get("title") or obj.get("headline") or ""
            analysis = obj.get("analysis") or ""
            item = {"title": title, "analysis": analysis}
            for a in articles:
                if a.get("title", "").strip() == title.strip() or title in (a.get("title") or ""):
                    item["url"] = a.get("url", "")
                    item["source"] = a.get("source_name", "")
                    break
            else:
                idx = min(len(items), len(articles) - 1)
                item["url"] = articles[idx].get("url", "") if articles else ""
                item["source"] = articles[idx].get("source_name", "") if articles else ""
            items.append(item)
        return json.dumps({"items": items})
    except Exception:
        return _generate_digest_fallback(articles, digest_type)


def _generate_digest_fallback(articles: list, digest_type: str) -> str:
    """Template-based digest when no API key: use key_takeaways, why_it_matters, summary for richer copy."""
    items = []
    for a in articles[:8]:
        parts = []
        if a.get("key_takeaways"):
            parts.append((a["key_takeaways"] or "").strip()[:350])
        if a.get("why_it_matters"):
            w = (a["why_it_matters"] or "").strip()
            for phrase in [" It may have implications for regional stability and policy.", "This story relates to:"]:
                w = w.replace(phrase, "").strip()
            if w:
                parts.append(w[:250])
        if a.get("summary") and not parts:
            parts.append((a["summary"] or "")[:350])
        analysis = " ".join(parts).strip() or (a.get("summary") or "")[:400]
        items.append({
            "title": a["title"],
            "url": a.get("url", ""),
            "source": a.get("source_name", ""),
            "analysis": analysis or "No summary available.",
        })
    return json.dumps({"items": items})


def generate_daily_digest() -> int:
    """Create or update the daily digest for today. Only one daily digest per calendar day; re-runs update that day's digest. Returns digest id."""
    init_db(DATABASE_PATH)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    articles = get_articles(limit=15)
    if not articles:
        content = _generate_digest_fallback([], "daily")
        title = f"Daily Digest — {today}"
    else:
        content = _generate_digest_with_openai(articles, "daily")
        title = f"Daily Digest — {today}"
    existing = get_daily_digest_for_date(today)
    if existing:
        update_digest(existing["id"], title, content)
        return existing["id"]
    return add_digest("daily", title, content)


def generate_weekly_digest() -> int:
    """Create a weekly digest. Returns digest id."""
    init_db(DATABASE_PATH)
    articles = get_articles(limit=25)
    topics = get_trending_topics(days=7, limit=10)
    content = _generate_digest_with_openai(articles, "weekly")
    # Enrich with trending topics
    try:
        data = json.loads(content)
        data["trending_topics"] = [t[0] for t in topics]
        content = json.dumps(data)
    except (json.JSONDecodeError, TypeError):
        pass
    title = f"Weekly Digest — week of {datetime.utcnow().strftime('%Y-%m-%d')}"
    return add_digest("weekly", title, content)


def send_digest_email_if_configured() -> bool:
    """If SMTP_* and DIGEST_EMAIL_RECIPIENTS are set, send latest daily digest. Returns True if sent."""
    smtp_host = os.environ.get("SMTP_HOST")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER")
    smtp_pass = os.environ.get("SMTP_PASS")
    recipients = os.environ.get("DIGEST_EMAIL_RECIPIENTS", "").strip().split()
    if not smtp_host or not recipients or not smtp_user or not smtp_pass:
        return False
    init_db(DATABASE_PATH)
    digests = get_digests(limit=1, digest_type="daily")
    if not digests:
        return False
    d = digests[0]
    try:
        content = json.loads(d["content"])
        items = content.get("items", [])
    except (json.JSONDecodeError, TypeError):
        items = []
    body_lines = [d["title"], "", "Stories:"]
    for it in items:
        body_lines.append(f"• {it.get('title', it.get('headline', '—'))}")
        if it.get("analysis"):
            body_lines.append(f"  {it['analysis'][:150]}")
    msg = MIMEMultipart("alternative")
    msg["Subject"] = d["title"]
    msg["From"] = smtp_user
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText("\n".join(body_lines), "plain"))
    with smtplib.SMTP(smtp_host, smtp_port) as s:
        s.starttls()
        s.login(smtp_user, smtp_pass)
        s.sendmail(smtp_user, recipients, msg.as_string())
    logger.info("Digest email sent to %s", len(recipients))
    return True
