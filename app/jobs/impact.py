"""Assign impact scores (0-10) and domains based on topics/entities. Uses LLM when OPENAI_API_KEY set."""
import json
import os
from config import DATABASE_PATH
from app.models import (
    init_db,
    update_article_impact,
    update_article_urgency,
    update_article_event_type,
    _connection,
)

# Impact scale: 0-10 (0=unscored, 1-3=low, 4-6=med, 7-10=high)
IMPACT_SCALE_MAX = 10

# Keywords for urgency (breaking / developing)
URGENCY_BREAKING = {"breaking", "urgent", "just in", "developing", "flash", "alert"}
URGENCY_DEVELOPING = {"developing", "update", "latest", "unfolding"}
# Map topics/keywords to event_type for taxonomy
EVENT_TYPE_RULES = {
    "Sanctions": ["sanctions", "sanction", "embargo"],
    "Military": ["military", "strike", "attack", "invasion", "nato", "defense", "troops"],
    "Diplomacy": ["diplomacy", "summit", "talks", "treaty", "negotiation"],
    "Election": ["election", "vote", "ballot", "campaign"],
    "Economy": ["trade", "economy", "gdp", "sanctions", "market"],
}


DOMAIN_RULES = {
    "Security": ["Russia-Ukraine", "NATO", "Defense", "Middle East"],
    "Energy": ["Climate & Energy", "Middle East"],
    "Economy": ["Trade & Economy", "Europe"],
    "Indo-Pacific": ["US-China", "Asia-Pacific"],
}

# Keywords that indicate trade/supply chain relevance (title/summary) for domain inference
TRADE_SUPPLY_CHAIN_KEYWORDS = [
    "supply chain", "trade route", "maritime", "shipping", "chokepoint", "strait",
    "suez", "hormuz", "malacca", "panama", "bab el-mandeb", "taiwan strait",
    "lng", "oil tanker", "container", "port closure", "canal", "red sea", "black sea",
]


def _infer_domains(topics: list[str], title: str = "", summary: str = "") -> list[str]:
    domains = set()
    for domain, topic_list in DOMAIN_RULES.items():
        if any(t in topics for t in topic_list):
            domains.add(domain)
    # Add Economy if title/summary mention trade/supply chain (even without topic)
    text = (title or "").lower() + " " + (summary or "").lower()
    if any(kw in text for kw in TRADE_SUPPLY_CHAIN_KEYWORDS):
        domains.add("Economy")
    return list(domains) or ["General"]


def _score_from_topics(topics: list[str]) -> int:
    """Rule-based impact 0-10. Returns 2, 5, or 8 for low, mid, high."""
    if not topics:
        return 2
    high = {"Russia-Ukraine", "US-China", "Middle East", "NATO"}
    if any(t in high for t in topics):
        return 8
    mid = {"Climate & Energy", "Trade & Economy", "Defense", "Asia-Pacific"}
    if any(t in mid for t in topics):
        return 5
    return 2


def _score_with_llm(title: str, summary: str, topics: list[str]) -> int | None:
    """Use OpenAI to score impact 0-10. Returns None on failure or missing key."""
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return None
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        topics_str = ", ".join(topics[:8]) if topics else "General"
        text = f"Title: {title}\n\nSummary: {summary or 'No summary.'}\n\nTopics: {topics_str}"
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a geopolitical analyst. Score each news item's IMPACT on geopolitics, markets, or policy (0-10). 0=none, 1-3=low (routine), 4-6=medium (notable), 7-10=high (major escalation, sanctions, conflict, systemic risk). Reply with ONLY a single integer 0-10, no explanation."
                },
                {"role": "user", "content": text[:2500]}
            ],
            max_tokens=10,
        )
        content = (response.choices[0].message.content or "").strip()
        try:
            first_word = content.split()[0] if content else ""
            val = int("".join(c for c in str(first_word) if c.isdigit()) or 0)
            if 0 <= val <= 10:
                return val
        except (ValueError, IndexError):
            pass
    except Exception:
        pass
    return None


def _infer_urgency(title: str, summary: str) -> str | None:
    """Return 'breaking', 'developing', or None."""
    text = (title or "").lower() + " " + (summary or "").lower()
    if any(k in text for k in URGENCY_BREAKING):
        return "breaking"
    if any(k in text for k in URGENCY_DEVELOPING):
        return "developing"
    return None


def _infer_event_type(title: str, summary: str, topics: list) -> str | None:
    text = (title or "").lower() + " " + (summary or "").lower()
    for event_type, keywords in EVENT_TYPE_RULES.items():
        if any(k in text for k in keywords):
            return event_type
    if topics:
        for event_type, keywords in EVENT_TYPE_RULES.items():
            if any(k in (t or "").lower() for t in topics for k in keywords):
                return event_type
    return None


def score_all(limit: int = 2000) -> int:
    """Score recent articles. Uses LLM when OPENAI_API_KEY set, else rule-based. Impact 0-10."""
    init_db(DATABASE_PATH)
    with _connection() as conn:
        cur = conn.execute(
            """
            SELECT id, title, summary, topics FROM articles
            ORDER BY published_utc DESC, id DESC
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
                topics = json.loads(d["topics"]) if isinstance(d["topics"], str) else d["topics"]
            except (json.JSONDecodeError, TypeError):
                pass
        title = d.get("title") or ""
        summary = d.get("summary") or ""
        score = _score_with_llm(title, summary, topics)
        if score is None:
            score = _score_from_topics(topics)
        domains = _infer_domains(topics, title, summary)
        update_article_impact(d["id"], score, domains)
        urgency = _infer_urgency(title, summary)
        update_article_urgency(d["id"], urgency)
        event_type = _infer_event_type(title, summary, topics)
        update_article_event_type(d["id"], event_type)
        count += 1
    return count

