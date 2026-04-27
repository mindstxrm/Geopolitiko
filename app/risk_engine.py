"""
Real-Time Geopolitical Risk Engine — core layer.

• Global risk heat map (country-level risk scores)
• Sector-specific risk exposure (energy, tech, maritime, supply chain)
• Dynamic Forward Risk Probability Index (coup %, sanctions %, trade disruption %, escalation)
• Source categories (news wire, think tank, government, defense, social)
"""
import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta

from config import DATABASE_PATH
from app.models import (
    init_db,
    _connection,
    upsert_country_risk,
    upsert_risk_index,
    get_country_risk_snapshots,
    get_risk_index,
    set_source_category,
)

logger = logging.getLogger(__name__)

# Topic/domain -> sector mapping for sector-specific risk exposure
SECTOR_ENERGY = {"Climate & Energy", "Energy", "Oil", "Gas", "OPEC", "Middle East"}
SECTOR_TECH = {"Technology", "Semiconductors", "Cyber", "US-China"}
SECTOR_MARITIME = {"Maritime", "Shipping", "Suez", "Strait", "South China Sea", "Red Sea"}
SECTOR_SUPPLY_CHAIN = {"Trade & Economy", "Supply Chain", "Trade", "Export", "Import"}

# Region/country normalization: topic/entity names -> display + optional ISO for map
REGION_TO_CODE = {
    "Russia-Ukraine": "UA",
    "Ukraine": "UA",
    "Russia": "RU",
    "US-China": "CN",
    "China": "CN",
    "Middle East": "XME",  # generic
    "Israel": "IL",
    "Iran": "IR",
    "Gaza": "PS",
    "Europe": "EU",
    "NATO": "NATO",
    "Asia-Pacific": "XAP",
    "Asia": "XAP",
    "North Korea": "KP",
    "South Korea": "KR",
    "Taiwan": "TW",
    "India": "IN",
    "Pakistan": "PK",
    "Bangladesh": "BD",
    "Afghanistan": "AF",
    "Syria": "SY",
    "Iraq": "IQ",
    "Yemen": "YE",
    "Libya": "LY",
    "Sudan": "SD",
    "Egypt": "EG",
    "Turkey": "TR",
    "Saudi Arabia": "SA",
    "United States": "US",
    "US": "US",
    "UK": "GB",
    "United Kingdom": "GB",
    "France": "FR",
    "Germany": "DE",
    "Brazil": "BR",
    "Venezuela": "VE",
    "Mexico": "MX",
    "Canada": "CA",
    "Australia": "AU",
    "Indonesia": "ID",
    "Philippines": "PH",
    "Myanmar": "MM",
    "Ethiopia": "ET",
    "Nigeria": "NG",
    "South Africa": "ZA",
}


def _topics_to_sectors(topics: list, domains: list) -> dict:
    """Return sector exposure counts from topics and impact_domains."""
    combined = set(topics or []) | set(domains or [])
    return {
        "energy": sum(1 for x in combined if any(s in str(x) for s in SECTOR_ENERGY)),
        "tech": sum(1 for x in combined if any(s in str(x) for s in SECTOR_TECH)),
        "maritime": sum(1 for x in combined if any(s in str(x) for s in SECTOR_MARITIME)),
        "supply_chain": sum(1 for x in combined if any(s in str(x) for s in SECTOR_SUPPLY_CHAIN)),
    }


def _normalize_region(name: str) -> str:
    """Map topic/entity to a canonical region code for risk tables."""
    return REGION_TO_CODE.get(name, name.replace(" ", "_")[:20])


def compute_country_risk_from_articles(days: int = 7) -> int:
    """
    Aggregate articles by region (from topics/entities), compute risk score and sector exposure,
    write to country_risk_snapshots. Risk score = weighted sum of volume and impact (1–100 scale).
    Returns number of regions updated.
    """
    init_db(DATABASE_PATH)
    since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    with _connection() as conn:
        cur = conn.execute(
            """
            SELECT topics, impact_domains, impact_score
            FROM articles
            WHERE (COALESCE(NULLIF(trim(published_utc), ''), scraped_at) >= ?)
              AND (topics IS NOT NULL AND topics != '' OR impact_domains IS NOT NULL AND impact_domains != '')
            """,
            (since,),
        )
        rows = cur.fetchall()

    # Aggregate by region: list of (impact_score, sectors)
    by_region = defaultdict(lambda: {"impact_sum": 0, "count": 0, "sectors": defaultdict(int)})
    for row in rows:
        topics = []
        domains = []
        if row["topics"]:
            try:
                topics = json.loads(row["topics"]) if isinstance(row["topics"], str) else row["topics"]
            except (json.JSONDecodeError, TypeError):
                pass
        if row["impact_domains"]:
            try:
                domains = json.loads(row["impact_domains"]) if isinstance(row["impact_domains"], str) else row["impact_domains"]
            except (json.JSONDecodeError, TypeError):
                pass
        impact = row["impact_score"]
        if impact is None:
            impact = 2  # Default unscored to low
        impact = max(0, min(10, int(impact)))
        sectors = _topics_to_sectors(topics, domains)
        for label in set(topics + domains):
            if not label or len(str(label).strip()) < 2:
                continue
            code = _normalize_region(str(label).strip())
            by_region[code]["impact_sum"] += impact
            by_region[code]["count"] += 1
            for k, v in sectors.items():
                if v:
                    by_region[code]["sectors"][k] += v

    # Score 0–100: combine volume and impact
    count = 0
    for region_code, data in by_region.items():
        n = data["count"]
        impact_avg = data["impact_sum"] / n if n else 0
        # Simple formula: volume factor (log) + impact factor
        import math
        vol = min(100, math.log1p(n) * 25)
        imp = (impact_avg / 3.0) * 40
        risk_score = min(100.0, round(vol + imp, 1))
        # Sector exposure as 0–100 scale relative to article count
        scale = max(1, n)
        se = min(100, data["sectors"]["energy"] * 50 / scale)
        st = min(100, data["sectors"]["tech"] * 50 / scale)
        sm = min(100, data["sectors"]["maritime"] * 50 / scale)
        ss = min(100, data["sectors"]["supply_chain"] * 50 / scale)
        upsert_country_risk(
            country_code=region_code,
            risk_score=risk_score,
            sector_energy=round(se, 1),
            sector_tech=round(st, 1),
            sector_maritime=round(sm, 1),
            sector_supply_chain=round(ss, 1),
            article_count=n,
        )
        count += 1
    logger.info("Risk engine: updated %s country/region risk snapshots", count)
    return count


def compute_forward_risk_index(days: int = 7) -> int:
    """
    Compute or update Forward Risk Probability Index (coup %, sanctions %, trade disruption %)
    from article signals. Rule-based: event_type and topics drive placeholder probabilities.
    Returns number of regions updated.
    """
    init_db(DATABASE_PATH)
    since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    with _connection() as conn:
        cur = conn.execute(
            """
            SELECT topics, impact_domains, event_type, impact_score
            FROM articles
            WHERE (COALESCE(NULLIF(trim(published_utc), ''), scraped_at) >= ?)
            """,
            (since,),
        )
        rows = cur.fetchall()

    by_region = defaultdict(lambda: {"coup": 0, "sanctions": 0, "trade": 0, "escalation": []})
    for row in rows:
        topics = []
        if row["topics"]:
            try:
                topics = json.loads(row["topics"]) if isinstance(row["topics"], str) else row["topics"]
            except (json.JSONDecodeError, TypeError):
                pass
        event = (row["event_type"] or "").lower()
        impact = row["impact_score"]
        if impact is None:
            impact = 2
        impact = max(0, min(10, int(impact)))
        topic_str = " ".join(str(t).lower() for t in topics)
        regions = set()
        for t in topics:
            if t:
                regions.add(_normalize_region(str(t).strip()))
        if not regions:
            regions.add("GLOBAL")
        for code in regions:
            if "coup" in event or "election" in event or "military" in event:
                if "election" in topic_str or "vote" in topic_str or "coup" in topic_str:
                    by_region[code]["coup"] += impact
            if "sanctions" in event or "sanctions" in topic_str:
                by_region[code]["sanctions"] += impact
            if "economy" in event or "trade" in event or "trade" in topic_str or "supply" in topic_str:
                by_region[code]["trade"] += impact
            if impact >= 7 or "military" in event or "strike" in topic_str:
                by_region[code]["escalation"].append(impact)

    count = 0
    for region_code, data in by_region.items():
        # Scale to 0–100% (heuristic cap)
        coup_pct = min(99, min(100, data["coup"] * 8))
        sanctions_pct = min(99, min(100, data["sanctions"] * 10))
        trade_pct = min(99, min(100, data["trade"] * 8))
        escalation = data["escalation"]
        pathway = [{"signal": "high_impact_count", "value": len(escalation)}] if escalation else []
        upsert_risk_index(
            region_code=region_code,
            coup_likelihood_pct=round(coup_pct, 1),
            sanctions_probability_pct=round(sanctions_pct, 1),
            trade_disruption_pct=round(trade_pct, 1),
            escalation_pathway_json=json.dumps(pathway),
        )
        count += 1
    logger.info("Risk engine: updated %s forward risk index rows", count)
    return count


def get_risk_heat_map_data(days: int = 7):
    """Return list of {country_code, risk_score, sector_*, article_count} for live heat map."""
    snapshots = get_country_risk_snapshots()
    return snapshots


def get_sector_exposure_by_country():
    """Sector-specific risk exposure by country (from snapshots)."""
    return get_country_risk_snapshots()


def seed_source_categories() -> None:
    """Seed source_categories for news_wire, think_tank, etc. All current sources as news_wire by default."""
    defaults = [
        ("Reuters - World", "news_wire"),
        ("BBC News - World", "news_wire"),
        ("Al Jazeera English", "news_wire"),
        ("Foreign Policy", "think_tank"),
        ("The Diplomat", "think_tank"),
        ("DW - World", "news_wire"),
    ]
    for name, cat in defaults:
        set_source_category(name, cat)
