"""Keyword-based topic and entity extraction for articles."""
import re
from config import DATABASE_PATH
from app.models import init_db, get_all_articles_for_processing, update_article_topics_entities

# Topic keywords: topic label -> list of keywords (case-insensitive match in title + summary)
TOPIC_KEYWORDS = {
    "US-China": ["china", "chinese", "xi jinping", "taiwan", "beijing", "us-china", "u.s.-china", "biden china", "trump china"],
    "Russia-Ukraine": ["ukraine", "russia", "zelensky", "putin", "kyiv", "moscow", "donbas", "nato ukraine", "russian invasion"],
    "Middle East": ["israel", "palestine", "gaza", "iran", "tehran", "saudi", "uae", "syria", "lebanon", "hezbollah", "hamas", "middle east"],
    "NATO": ["nato", "alliance", "article 5", "baltic", "eastern europe"],
    "Trade & Economy": ["trade", "tariff", "sanctions", "gdp", "recession", "imf", "world bank", "supply chain"],
    "Climate & Energy": ["climate", "cop26", "cop27", "emissions", "renewable", "oil", "gas", "energy crisis", "green transition"],
    "Asia-Pacific": ["north korea", "kim jong", "japan", "south korea", "asean", "indonesia", "philippines", "australia", "indo-pacific"],
    "Europe": ["eu", "european union", "brussels", "macron", "scholz", "europe"],
    "Defense": ["military", "defense", "nuclear", "arms", "pentagon", "nato"],
}

# Entity patterns: simple regex or keyword sets for countries/orgs
COUNTRIES = [
    "United States", "USA", "US", "China", "Russia", "Ukraine", "Israel", "Iran", "North Korea",
    "South Korea", "Japan", "India", "Pakistan", "France", "Germany", "UK", "Britain", "Turkey",
    "Saudi Arabia", "Egypt", "Syria", "Iraq", "Afghanistan", "Taiwan", "Australia", "Canada",
    "Brazil", "Mexico", "South Africa", "Nigeria", "Indonesia", "Vietnam", "Philippines",
]
# Normalize for matching: we'll look for these in text
ENTITY_KEYWORDS = set(COUNTRIES) | {
    "EU", "NATO", "UN", "UN Security Council", "WHO", "IMF", "WTO", "OPEC",
    "Hamas", "Hezbollah", "Taliban", "ISIS", "Al Qaeda",
}


def _extract_topics(text: str) -> list[str]:
    """Return topic labels that match the text."""
    text_lower = (text or "").lower()
    found = []
    for topic, keywords in TOPIC_KEYWORDS.items():
        if any(kw.lower() in text_lower for kw in keywords):
            found.append(topic)
    return found


def _extract_entities(text: str) -> list[str]:
    """Return entities (countries/orgs) mentioned in text."""
    if not text:
        return []
    text_lower = text.lower()
    found = []
    for entity in ENTITY_KEYWORDS:
        if entity.lower() in text_lower:
            found.append(entity)
    # Dedupe and limit
    seen = set()
    out = []
    for e in found:
        if e not in seen:
            seen.add(e)
            out.append(e)
    return out[:15]


def extract_topics_for_article(article_id: int, title: str, summary: str) -> None:
    """Extract and save topics and entities for one article."""
    text = f"{title} {summary or ''}"
    topics = _extract_topics(text)
    entities = _extract_entities(text)
    update_article_topics_entities(article_id, topics, entities)


def extract_topics_for_all(limit: int = 3000) -> int:
    """Run topic/entity extraction for recent articles. Returns count processed."""
    init_db(DATABASE_PATH)
    articles = get_all_articles_for_processing(limit=limit)
    for a in articles:
        extract_topics_for_article(a["id"], a["title"], a.get("summary") or "")
    return len(articles)
