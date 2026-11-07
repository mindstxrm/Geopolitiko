"""Geopolitical news sources: RSS feed URLs, homepage, and provenance tier.

Tiers (desk / eval provenance):
  A — wire services / major global outlets (prefer for FACT)
  B — strong regional or specialist outlets
  C — useful but corroborate before leading
"""

SOURCES = [
    # Tier A
    {"name": "Reuters - World", "url": "https://www.reuters.com/rssfeed/worldNews", "homepage": "https://www.reuters.com", "tier": "A", "regions": ["global"]},
    {"name": "Associated Press", "url": "https://apnews.com/hub/world-news/rss", "homepage": "https://apnews.com", "tier": "A", "regions": ["global"]},
    {"name": "Agence France-Presse", "url": "https://www.afp.com/en/rss", "homepage": "https://www.afp.com/en", "tier": "A", "regions": ["global"]},
    {"name": "BBC News - World", "url": "https://feeds.bbci.co.uk/news/world/rss.xml", "homepage": "https://www.bbc.com/news", "tier": "A", "regions": ["global"]},
    {"name": "Al Jazeera English", "url": "https://www.aljazeera.com/xml/rss/all.xml", "homepage": "https://www.aljazeera.com", "tier": "A", "regions": ["global", "middle_east"]},
    {"name": "Financial Times", "url": "https://www.ft.com/world?format=rss", "homepage": "https://www.ft.com", "tier": "A", "regions": ["global"]},
    {"name": "Bloomberg", "url": "https://feeds.bloomberg.com/news.rss", "homepage": "https://www.bloomberg.com", "tier": "A", "regions": ["global"]},
    {"name": "UN News", "url": "https://news.un.org/feed/subscribe/en/news/all/rss.xml", "homepage": "https://news.un.org", "tier": "A", "regions": ["global"]},
    # Tier B — regional / specialist
    {"name": "Foreign Policy", "url": "https://foreignpolicy.com/feed/", "homepage": "https://foreignpolicy.com", "tier": "B", "regions": ["global"]},
    {"name": "Foreign Affairs", "url": "https://www.foreignaffairs.com/rss.xml", "homepage": "https://www.foreignaffairs.com", "tier": "B", "regions": ["global"]},
    {"name": "Council on Foreign Relations", "url": "https://www.cfr.org/rss", "homepage": "https://www.cfr.org", "tier": "B", "regions": ["global"]},
    {"name": "The Diplomat", "url": "https://thediplomat.com/feed/", "homepage": "https://thediplomat.com", "tier": "B", "regions": ["northeast_asia", "asean", "south_asia", "oceania"]},
    {"name": "Nikkei Asia", "url": "https://asia.nikkei.com/rss/feed", "homepage": "https://asia.nikkei.com", "tier": "B", "regions": ["northeast_asia", "asean"]},
    {"name": "South China Morning Post", "url": "https://www.scmp.com/rss/91/feed", "homepage": "https://www.scmp.com", "tier": "B", "regions": ["northeast_asia", "asean"]},
    {"name": "The Straits Times", "url": "https://www.straitstimes.com/rss", "homepage": "https://www.straitstimes.com", "tier": "B", "regions": ["asean"]},
    {"name": "Channel NewsAsia", "url": "https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml", "homepage": "https://www.channelnewsasia.com", "tier": "B", "regions": ["asean"]},
    {"name": "The Jakarta Post", "url": "https://www.thejakartapost.com/rss", "homepage": "https://www.thejakartapost.com", "tier": "B", "regions": ["asean"]},
    {"name": "NHK World-Japan", "url": "https://www3.nhk.or.jp/nhkworld/en/news/rss.xml", "homepage": "https://www3.nhk.or.jp/nhkworld/en", "tier": "B", "regions": ["northeast_asia"]},
    {"name": "DW - World", "url": "https://rss.dw.com/xml/rss-en-world", "homepage": "https://www.dw.com", "tier": "B", "regions": ["global", "eastern_europe"]},
    {"name": "Deutsche Welle - Europe", "url": "https://rss.dw.com/xml/rss-en-europe", "homepage": "https://www.dw.com", "tier": "B", "regions": ["eastern_europe", "southern_europe", "northern_europe"]},
    {"name": "France 24", "url": "https://www.france24.com/en/rss", "homepage": "https://www.france24.com/en", "tier": "B", "regions": ["global", "middle_east", "southern_europe"]},
    {"name": "The Economist", "url": "https://www.economist.com/rss", "homepage": "https://www.economist.com", "tier": "B", "regions": ["global"]},
    {"name": "Radio Free Asia", "url": "https://www.rfa.org/english/rss", "homepage": "https://www.rfa.org/english", "tier": "B", "regions": ["northeast_asia", "asean", "central_asia"]},
    {"name": "RNZ - World", "url": "https://www.rnz.co.nz/rss/world.xml", "homepage": "https://www.rnz.co.nz", "tier": "B", "regions": ["oceania"]},
    {"name": "RNZ - Pacific", "url": "https://www.rnz.co.nz/rss/pacific.xml", "homepage": "https://www.rnz.co.nz", "tier": "B", "regions": ["oceania"]},
    {"name": "The Hindu - International", "url": "https://www.thehindu.com/news/international/feeder/default.rss", "homepage": "https://www.thehindu.com", "tier": "B", "regions": ["south_asia"]},
    {"name": "Dawn", "url": "https://www.dawn.com/feeds/home", "homepage": "https://www.dawn.com", "tier": "B", "regions": ["south_asia"]},
    {"name": "Kyiv Independent", "url": "https://kyivindependent.com/feed/", "homepage": "https://kyivindependent.com", "tier": "B", "regions": ["eastern_europe"]},
    {"name": "Defense One", "url": "https://www.defenseone.com/rss/all/", "homepage": "https://www.defenseone.com", "tier": "B", "regions": ["global"]},
]


def _norm(s: dict) -> dict:
    out = dict(s)
    out["tier"] = (out.get("tier") or "B").strip().upper()[:1] or "B"
    regions = out.get("regions") or ["global"]
    if isinstance(regions, str):
        regions = [regions]
    out["regions"] = [str(r).strip().lower() for r in regions if r]
    return out


SOURCES = [_norm(s) for s in SOURCES]


def list_sources() -> list[dict]:
    return list(SOURCES)


def source_by_name(name: str) -> dict | None:
    key = (name or "").strip().lower()
    for s in SOURCES:
        if s["name"].lower() == key:
            return s
    # partial: "Reuters" matches "Reuters - World"
    for s in SOURCES:
        if key and key in s["name"].lower():
            return s
    return None


def source_tier_for_name(name: str) -> str:
    s = source_by_name(name)
    return (s or {}).get("tier") or "C"


def sources_summary() -> dict:
    tiers: dict[str, int] = {"A": 0, "B": 0, "C": 0}
    for s in SOURCES:
        t = s.get("tier") or "C"
        tiers[t] = tiers.get(t, 0) + 1
    return {"count": len(SOURCES), "by_tier": tiers, "names": [s["name"] for s in SOURCES]}
