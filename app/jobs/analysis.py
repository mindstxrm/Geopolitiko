"""Per-article analysis: key takeaways and why it matters (extractive + optional OpenAI)."""
import os
import re
from config import DATABASE_PATH
from app.models import (
    init_db,
    get_all_articles_for_processing,
    update_article_analysis,
)


def _extractive_takeaways(summary: str, max_sentences: int = 3) -> str:
    """First N sentences of summary as bullet-like takeaways."""
    if not summary or not summary.strip():
        return ""
    # Split on sentence boundaries
    sentences = re.split(r"(?<=[.!?])\s+", summary.strip())
    sentences = [s.strip() for s in sentences if s.strip()][:max_sentences]
    return "\n• " + "\n• ".join(sentences) if sentences else ""


def _extractive_why_it_matters(topics: list, summary: str, has_takeaways: bool = False) -> str:
    """Short 'why it matters': topic context only. No repetition of summary or generic boilerplate."""
    if not topics:
        return ""
    return f"This story relates to: {', '.join(topics[:3])}."


def _generate_with_openai(title: str, summary: str) -> tuple[str, str]:
    """Call OpenAI for key takeaways and why it matters. Returns (takeaways, why_it_matters)."""
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return "", ""
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        text = f"Title: {title}\n\nSummary: {summary or 'No summary.'}"
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a geopolitical analyst. For each news item provide:\n(1) KEY TAKEAWAYS: 2-3 short bullet points (one per line, starting with •).\n(2) WHY IT MATTERS: 1-2 sentences that add context or stakes—do not repeat the key takeaways, and do not use generic phrases like 'regional stability' or 'policy implications' unless specific. Be concise.\nReply in this exact format:\nKEY TAKEAWAYS:\n• ...\n• ...\nWHY IT MATTERS:\n..."
                },
                {"role": "user", "content": text[:3000]}
            ],
            max_tokens=400,
        )
        content = (response.choices[0].message.content or "").strip()
        takeaways = ""
        why = ""
        if "KEY TAKEAWAYS:" in content and "WHY IT MATTERS:" in content:
            a, b = content.split("WHY IT MATTERS:", 1)
            takeaways = a.replace("KEY TAKEAWAYS:", "").strip()[:1500]
            why = b.strip()[:800]
        elif "KEY TAKEAWAYS:" in content:
            takeaways = content.replace("KEY TAKEAWAYS:", "").strip()[:1500]
        else:
            takeaways = content[:1500]
        return takeaways, why
    except Exception:
        return "", ""


def generate_analysis_for_article(article_id: int, title: str, summary: str, topics: list) -> None:
    """Generate and save key_takeaways and why_it_matters for one article."""
    if os.environ.get("OPENAI_API_KEY"):
        takeaways, why = _generate_with_openai(title, summary)
    else:
        takeaways = _extractive_takeaways(summary)
        why = _extractive_why_it_matters(topics, summary, has_takeaways=bool(takeaways))
    if not takeaways and not why:
        takeaways = _extractive_takeaways(summary)
        why = _extractive_why_it_matters(topics, summary, has_takeaways=bool(takeaways))
    update_article_analysis(article_id, takeaways, why)


def generate_analysis_for_all(limit: int = 500) -> int:
    """Generate analysis for articles that don't have it yet. Returns count processed."""
    init_db(DATABASE_PATH)
    from app.models import _connection

    with _connection() as conn:
        cur = conn.execute(
            """
            SELECT id, title, summary, topics FROM articles
            WHERE (key_takeaways IS NULL OR key_takeaways = '')
            ORDER BY published_utc DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cur.fetchall()
    count = 0
    for row in rows:
        d = dict(row)
        topics = []
        if d.get("topics"):
            try:
                import json
                topics = json.loads(d["topics"]) if isinstance(d["topics"], str) else d["topics"]
            except (ValueError, TypeError):
                pass
        generate_analysis_for_article(
            d["id"],
            d["title"],
            d.get("summary") or "",
            topics,
        )
        count += 1
    return count
