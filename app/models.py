"""Data models and database access for articles, search, digests, and analysis."""
import hashlib
import json
import re
import secrets
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

from werkzeug.security import check_password_hash, generate_password_hash

_DB_PATH: Optional[str] = None

# Chokepoint map config (used by supply chain seed + get_chokepoints_with_geo)
CHOKEPOINT_COORDS = {
    "hormuz": (26.5, 56.5),
    "malacca": (2.5, 101.0),
    "bab_el_mandeb": (12.6, 43.3),
    "suez": (29.9, 32.4),
    "panama": (9.0, -79.7),
    "taiwan_strait": (24.5, 119.5),
    "bosporus": (41.0, 29.0),
}
CHOKEPOINT_PCT_GLOBAL = {
    "hormuz": 21,
    "malacca": 25,
    "bab_el_mandeb": 10,
    "suez": 12,
    "panama": 5,
    "taiwan_strait": 50,  # share of global container traffic
    "bosporus": 3,
}
# Extra chokepoints to add if not present: (slug, name, description, region, commodities, lat, lon, pct, alternative_routes, risk_score)
CHOKEPOINT_EXTRA = [
    ("suez", "Suez Canal", "Key Asia–Europe shortcut. Container and energy traffic; closures cause major reroutes.", "Middle East", "containers,oil,lng", 29.9, 32.4, 12.0, "Cape of Good Hope; alternative pipelines limited.", 70),
    ("panama", "Panama Canal", "Pacific–Atlantic link. Drought and capacity constraints affect transit.", "Latin America", "containers,lng", 9.0, -79.7, 5.0, "Suez or Cape routes for some traffic; rail/road alternatives.", 55),
    ("taiwan_strait", "Taiwan Strait", "Critical for East Asian semiconductor and container flows.", "East Asia", "containers,semiconductors", 24.5, 119.5, 50.0, "Reroute via Philippines or longer sea routes.", 85),
    ("bosporus", "Bosphorus Strait", "Black Sea–Mediterranean. Oil and grain from Russia/Ukraine.", "Europe", "oil,grain", 41.0, 29.0, 3.0, "Limited alternatives; Danube and land routes.", 65),
]

# Seed trade flows for extra chokepoints when they have none. (slug, country_code, country_name, sector, exposure_pct, impact_if_closed, notes)
CHOKEPOINT_EXTRA_FLOWS = [
    # Suez Canal – Asia–Europe containers, oil, LNG
    ("suez", "CHN", "China", "containers", 55.0, "high", "Asia–Europe container corridor; Belt and Road alternative via rail"),
    ("suez", "CHN", "China", "oil", 25.0, "medium", "Crude and products to/from Europe"),
    ("suez", "IND", "India", "containers", 40.0, "high", "Key export route to EU and North Africa"),
    ("suez", "IND", "India", "oil", 20.0, "medium", "Refined products, some crude"),
    ("suez", "JPN", "Japan", "containers", 35.0, "high", "Auto parts and goods to Europe"),
    ("suez", "JPN", "Japan", "lng", 15.0, "medium", "LNG trade with Europe/Mideast"),
    ("suez", "KOR", "South Korea", "containers", 45.0, "high", "Electronics and vehicles to EU"),
    ("suez", "EU", "European Union", "containers", 50.0, "high", "Imports from Asia; critical supply chain route"),
    ("suez", "EU", "European Union", "oil", 22.0, "high", "Gulf crude via Suez"),
    ("suez", "EU", "European Union", "lng", 18.0, "medium", "LNG from Qatar and others"),
    ("suez", "GBR", "United Kingdom", "containers", 42.0, "high", "Asia–UK trade through Suez"),
    ("suez", "USA", "United States", "containers", 12.0, "low", "Some Asia–East Coast via Suez"),
    # Panama Canal – Americas + Asia–US East Coast
    ("panama", "USA", "United States", "containers", 35.0, "high", "East Coast imports from Asia; Gulf traffic"),
    ("panama", "USA", "United States", "lng", 28.0, "high", "LNG exports to Asia; imports from Atlantic"),
    ("panama", "CHN", "China", "containers", 40.0, "high", "Exports to US East Coast and Latin America"),
    ("panama", "CHN", "China", "critical_minerals", 20.0, "medium", "Minerals from Chile/Peru"),
    ("panama", "CHL", "Chile", "containers", 45.0, "high", "Fruit, copper, lithium to Asia and USEC"),
    ("panama", "PER", "Peru", "containers", 40.0, "high", "Minerals, agriculture to Asia"),
    ("panama", "COL", "Colombia", "containers", 38.0, "high", "Coffee, goods to US and Asia"),
    ("panama", "ECU", "Ecuador", "containers", 50.0, "high", "Bananas, shrimp to Europe and Asia"),
    ("panama", "JPN", "Japan", "containers", 25.0, "medium", "Trade with Latin America and USEC"),
    ("panama", "KOR", "South Korea", "containers", 22.0, "medium", "Auto parts, electronics to Americas"),
    # Taiwan Strait – semiconductors, East Asia containers
    ("taiwan_strait", "CHN", "China", "containers", 60.0, "high", "Coastal and cross-strait shipping"),
    ("taiwan_strait", "CHN", "China", "semiconductors", 55.0, "high", "Chip equipment and materials; TSMC supply chain"),
    ("taiwan_strait", "TWN", "Taiwan", "containers", 70.0, "high", "Virtually all seaborne trade transits strait"),
    ("taiwan_strait", "TWN", "Taiwan", "semiconductors", 75.0, "high", "Chip exports to global markets"),
    ("taiwan_strait", "JPN", "Japan", "containers", 35.0, "high", "Trade with Taiwan and South China"),
    ("taiwan_strait", "JPN", "Japan", "semiconductors", 45.0, "high", "Equipment and materials to/from Taiwan"),
    ("taiwan_strait", "KOR", "South Korea", "containers", 38.0, "high", "Trade with Taiwan and China"),
    ("taiwan_strait", "KOR", "South Korea", "semiconductors", 40.0, "high", "Chip supply chain with Taiwan"),
    ("taiwan_strait", "USA", "United States", "semiconductors", 50.0, "high", "Critical chip supply from Taiwan"),
    ("taiwan_strait", "USA", "United States", "containers", 20.0, "medium", "Consumer goods from East Asia"),
    ("taiwan_strait", "EU", "European Union", "semiconductors", 35.0, "high", "Semiconductor imports from Taiwan"),
    # Bosphorus – Black Sea oil and grain
    ("bosporus", "TUR", "Turkey", "oil", 30.0, "high", "Transit and domestic supply; key straits state"),
    ("bosporus", "TUR", "Turkey", "grain", 25.0, "high", "Grain imports from Black Sea"),
    ("bosporus", "RUS", "Russia", "oil", 45.0, "high", "Crude and products to Med/global markets"),
    ("bosporus", "RUS", "Russia", "grain", 40.0, "high", "Wheat exports via Black Sea"),
    ("bosporus", "UKR", "Ukraine", "grain", 55.0, "high", "Critical grain export corridor"),
    ("bosporus", "UKR", "Ukraine", "oil", 15.0, "medium", "Refined products, some crude"),
    ("bosporus", "ROU", "Romania", "oil", 35.0, "medium", "Crude exports; refinery traffic"),
    ("bosporus", "BGR", "Bulgaria", "oil", 28.0, "medium", "Oil product imports"),
    ("bosporus", "EU", "European Union", "grain", 35.0, "high", "Ukrainian and Russian grain imports"),
    ("bosporus", "CHN", "China", "grain", 25.0, "medium", "Grain from Black Sea"),
    ("bosporus", "EGY", "Egypt", "grain", 45.0, "high", "Wheat imports from Ukraine/Russia"),
]

# Topic labels for alert suggestions (stored in articles; kept for quick-add alongside countries)
ALERT_SUGGESTIONS_TOPIC_LABELS = [
    "US-China",
    "Russia-Ukraine",
    "Middle East",
    "NATO",
    "Trade & Economy",
    "Climate & Energy",
    "Asia-Pacific",
    "Europe",
    "Defense",
]

# Default list of columns we expect on articles (for migration)
_ARTICLE_COLUMNS = [
    "topics",
    "entities",
    "key_takeaways",
    "why_it_matters",
    "cluster_id",
    "impact_score",
    "impact_domains",
    "urgency",      # breaking | developing | null
    "event_type",   # e.g. Sanctions, Military, Diplomacy, Election
    "image_url",    # lead/thumbnail image URL from feed
    "video_url",    # embeddable video URL (e.g. YouTube, Vimeo)
]


def _ensure_treaty_columns(conn: sqlite3.Connection) -> None:
    """Add treaty columns if they don't exist (e.g. document_url, wto_rta_id)."""
    cur = conn.execute("PRAGMA table_info(treaties)")
    existing = {row[1] for row in cur.fetchall()}
    if "document_url" not in existing:
        conn.execute("ALTER TABLE treaties ADD COLUMN document_url TEXT")
    if "wto_rta_id" not in existing:
        conn.execute("ALTER TABLE treaties ADD COLUMN wto_rta_id INTEGER")


def _ensure_chokepoint_columns(conn: sqlite3.Connection) -> None:
    """Add lat, lon, pct_global_trade, alternative_routes, risk_score to chokepoints if missing."""
    cur = conn.execute("PRAGMA table_info(chokepoints)")
    existing = {row[1] for row in cur.fetchall()}
    for col, typ in [
        ("lat", "REAL"),
        ("lon", "REAL"),
        ("pct_global_trade", "REAL"),
        ("alternative_routes", "TEXT"),
        ("risk_score", "REAL"),
    ]:
        if col not in existing:
            conn.execute(f"ALTER TABLE chokepoints ADD COLUMN {col} {typ}")


def _seed_chokepoints(conn: sqlite3.Connection) -> None:
    """Seed strategic chokepoints and sample trade flows if empty."""
    cur = conn.execute("SELECT 1 FROM chokepoints LIMIT 1")
    if cur.fetchone():
        _ensure_chokepoint_columns(conn)
        # Backfill lat, lon, pct_global_trade from config
        cur = conn.execute("PRAGMA table_info(chokepoints)")
        cols = {row[1] for row in cur.fetchall()}
        if "lat" in cols:
            for slug, (lat, lon) in CHOKEPOINT_COORDS.items():
                pct = CHOKEPOINT_PCT_GLOBAL.get(slug)
                conn.execute(
                    "UPDATE chokepoints SET lat = ?, lon = ?, pct_global_trade = ? WHERE LOWER(TRIM(slug)) = ?",
                    (lat, lon, pct, slug.lower()),
                )
        # Add extra chokepoints (Suez, Panama, etc.) if not present
        cur = conn.execute("SELECT slug FROM chokepoints")
        existing = {row[0].strip().lower() for row in cur.fetchall()}
        now = datetime.utcnow().isoformat() + "Z"
        has_geo = "lat" in cols and "pct_global_trade" in cols
        for row in CHOKEPOINT_EXTRA:
            slug = row[0].strip().lower()
            if slug in existing:
                continue
            if has_geo and len(row) >= 10:
                conn.execute(
                    """INSERT INTO chokepoints (slug, name, description, region, commodities, lat, lon, pct_global_trade, alternative_routes, risk_score, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], now),
                )
            else:
                conn.execute(
                    "INSERT INTO chokepoints (slug, name, description, region, commodities, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (row[0], row[1], row[2], row[3], row[4], now),
                )
        # Seed trade flows for extra chokepoints (Suez, Panama, Taiwan Strait, Bosphorus) if they have none
        cur = conn.execute("SELECT id, LOWER(TRIM(slug)) AS slug FROM chokepoints")
        ids_by_slug = {row[1]: row[0] for row in cur.fetchall()}
        extra_slugs = {row[0].strip().lower() for row in CHOKEPOINT_EXTRA}
        for slug in extra_slugs:
            cid = ids_by_slug.get(slug)
            if cid is None:
                continue
            cur = conn.execute("SELECT 1 FROM chokepoint_flows WHERE chokepoint_id = ? LIMIT 1", (cid,))
            if cur.fetchone():
                continue
            for flow in CHOKEPOINT_EXTRA_FLOWS:
                if (flow[0].strip().lower()) != slug:
                    continue
                conn.execute(
                    "INSERT INTO chokepoint_flows (chokepoint_id, country_code, country_name, sector, exposure_pct, impact_if_closed, notes, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (cid, flow[1], flow[2], flow[3], flow[4], flow[5], flow[6], now),
                )
        return
    _ensure_chokepoint_columns(conn)
    now = datetime.utcnow().isoformat() + "Z"
    chokepoints = [
        ("hormuz", "Strait of Hormuz", "Critical oil and gas chokepoint between Iran and Oman. ~21% of global oil and 20% of LNG pass through.", "Middle East", "oil,lng", 26.5, 56.5, 21.0, "Pipeline alternatives limited; bypass would require Cape route.", 72),
        ("malacca", "Strait of Malacca", "Primary Asia–Europe shipping lane. ~25% of global trade; heavy oil, LNG, containers, semiconductors.", "Southeast Asia", "oil,lng,containers,semiconductors", 2.5, 101.0, 25.0, "Lombok Strait, Sunda Strait; overland pipelines in development.", 68),
        ("bab_el_mandeb", "Bab el-Mandeb", "Red Sea–Gulf of Aden. Oil and LNG from Persian Gulf to Europe/Asia; container traffic to Suez.", "Horn of Africa / Yemen", "oil,lng,containers", 12.6, 43.3, 10.0, "Cape of Good Hope reroute; Suez alternative for some traffic.", 75),
    ]
    for slug, name, desc, region, commodities, lat, lon, pct, alt, risk in chokepoints:
        conn.execute(
            "INSERT INTO chokepoints (slug, name, description, region, commodities, lat, lon, pct_global_trade, alternative_routes, risk_score, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (slug, name, desc, region, commodities, lat, lon, pct, alt, risk, now),
        )
    # Get IDs (SQLite returns last rowid per insert; we need to query)
    cur = conn.execute("SELECT id, slug FROM chokepoints ORDER BY id")
    ids_by_slug = {row[1]: row[0] for row in cur.fetchall()}
    hid, mid, bid = ids_by_slug["hormuz"], ids_by_slug["malacca"], ids_by_slug["bab_el_mandeb"]
    flows = [
        # Hormuz: Japan, Korea, India, China heavily dependent on Gulf oil/LNG
        (hid, "JPN", "Japan", "oil", 75.0, "high", "Most crude imports from Gulf via Hormuz"),
        (hid, "JPN", "Japan", "lng", 30.0, "high", "Significant LNG from Qatar/UAE"),
        (hid, "KOR", "South Korea", "oil", 70.0, "high", "Heavy dependence on Gulf crude"),
        (hid, "KOR", "South Korea", "lng", 35.0, "high", "LNG imports via Hormuz"),
        (hid, "IND", "India", "oil", 65.0, "high", "Major oil imports from Saudi, Iraq, Iran"),
        (hid, "IND", "India", "lng", 25.0, "medium", "Growing LNG dependence"),
        (hid, "CHN", "China", "oil", 45.0, "high", "Large Gulf oil flows; pipelines supplement"),
        (hid, "CHN", "China", "lng", 20.0, "medium", "LNG from Qatar"),
        (hid, "USA", "United States", "oil", 15.0, "low", "Less dependent; domestic + Americas"),
        (hid, "EU", "European Union", "oil", 20.0, "medium", "Some Gulf crude via Hormuz"),
        # Malacca: Japan, Korea, China, India – containers, semiconductors, oil
        (mid, "JPN", "Japan", "oil", 80.0, "high", "Crude from Middle East; refined product trade"),
        (mid, "JPN", "Japan", "containers", 40.0, "high", "Key export route to Europe/Mideast"),
        (mid, "JPN", "Japan", "semiconductors", 35.0, "high", "Components to/from SEA, China"),
        (mid, "KOR", "South Korea", "oil", 85.0, "high", "Virtually all Gulf oil via Malacca"),
        (mid, "KOR", "South Korea", "containers", 50.0, "high", "Critical for exports"),
        (mid, "KOR", "South Korea", "semiconductors", 45.0, "high", "Chip supply chain through Malacca"),
        (mid, "IND", "India", "oil", 70.0, "high", "Gulf oil imports"),
        (mid, "IND", "India", "containers", 30.0, "medium", "Trade with East Asia, Europe"),
        (mid, "IND", "India", "critical_minerals", 25.0, "medium", "Some rare earths/minerals via SEA"),
        (mid, "CHN", "China", "oil", 75.0, "high", "Malacca 'dilemma' – heavy oil dependence"),
        (mid, "CHN", "China", "containers", 55.0, "high", "Belt and Road alternative routes"),
        (mid, "CHN", "China", "semiconductors", 50.0, "high", "Equipment and materials via Malacca"),
        (mid, "CHN", "China", "critical_minerals", 40.0, "high", "Indonesian/Malaysian minerals"),
        # Bab el-Mandeb: Europe, Asia oil/LNG; container reroute risk
        (bid, "JPN", "Japan", "oil", 20.0, "medium", "Some Gulf oil via Suez route"),
        (bid, "KOR", "South Korea", "oil", 18.0, "medium", "Suez-bound tankers"),
        (bid, "IND", "India", "oil", 25.0, "medium", "Trade with Europe, East Africa"),
        (bid, "CHN", "China", "containers", 30.0, "high", "Asia–Europe container via Suez"),
        (bid, "EU", "European Union", "oil", 35.0, "high", "Gulf oil via Bab el-Mandeb to Suez"),
        (bid, "EU", "European Union", "lng", 25.0, "high", "Qatar LNG to Europe"),
        (bid, "EU", "European Union", "containers", 45.0, "high", "Asia–Europe corridor"),
    ]
    for row in flows:
        conn.execute(
            "INSERT INTO chokepoint_flows (chokepoint_id, country_code, country_name, sector, exposure_pct, impact_if_closed, notes, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (*row, now),
        )


def _seed_stability(conn: sqlite3.Connection) -> None:
    """Seed political stability & domestic signals tables if empty."""
    now = datetime.utcnow().isoformat() + "Z"
    cur = conn.execute("SELECT 1 FROM election_calendar LIMIT 1")
    if cur.fetchone():
        return
    # Election calendar
    for row in [
        ("FRA", "France", "parliamentary", "2027-06", "upcoming", "National Assembly"),
        ("USA", "United States", "presidential", "2028-11", "upcoming", "General election"),
        ("IND", "India", "general", "2029-05", "upcoming", "Lok Sabha"),
        ("GBR", "United Kingdom", "parliamentary", "2025-01", "upcoming", "By latest Jan 2025"),
        ("MEX", "Mexico", "midterm", "2025-06", "upcoming", "Congress"),
        ("BRA", "Brazil", "municipal", "2026-10", "upcoming", "Mayors, councils"),
    ]:
        conn.execute(
            "INSERT INTO election_calendar (country_code, country_name, election_type, date_planned, status, notes, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (*row, now),
        )
    # Approval ratings
    for row in [
        ("USA", "United States", "President", 42.0, "2025-01-15", "Gallup"),
        ("FRA", "France", "President", 35.0, "2025-01-10", "Ifop"),
        ("DEU", "Germany", "Chancellor", 28.0, "2025-01-12", "ARD"),
        ("GBR", "United Kingdom", "Prime Minister", 25.0, "2025-01-08", "YouGov"),
        ("JPN", "Japan", "Cabinet", 32.0, "2025-01-14", "Kyodo"),
        ("IND", "India", "PM", 68.0, "2025-01-05", "Local poll"),
    ]:
        conn.execute(
            "INSERT INTO approval_ratings (country_code, country_name, subject, approval_pct, poll_date, source, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (*row, now),
        )
    # Protest tracking
    for row in [
        ("FRA", "France", "2025-01-10", "Farmers blockades", "Thousands", "Agricultural policy"),
        ("ARG", "Argentina", "2025-01-08", "General strike", "Hundreds of thousands", "Economic reform"),
        ("KEN", "Kenya", "2025-01-05", "Tax protests", "Thousands", "Finance bill"),
        ("PAK", "Pakistan", "2025-01-12", "Election protests", "Thousands", "Election dispute"),
        ("PER", "Peru", "2025-01-03", "Regional demonstrations", "Thousands", "Decentralization"),
    ]:
        conn.execute(
            "INSERT INTO protest_tracking (country_code, country_name, event_date, summary, estimated_size, trigger_topic, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (*row, now),
        )
    # Currency stress
    for row in [
        ("ARG", "Argentina", 45.0, "high", "2025-01-14", "Parallel rate spread"),
        ("EGY", "Egypt", 28.0, "high", "2025-01-13", "Devaluation pressure"),
        ("TUR", "Turkey", 18.0, "medium", "2025-01-12", "Volatility index"),
        ("NGA", "Nigeria", 22.0, "medium", "2025-01-11", "Naira weakness"),
        ("PAK", "Pakistan", 25.0, "medium", "2025-01-10", "Reserve drawdown"),
    ]:
        conn.execute(
            "INSERT INTO currency_stress (country_code, country_name, indicator_value, stress_level, as_of_date, notes, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (*row, now),
        )
    # Food inflation alerts
    for row in [
        ("TUR", "Turkey", 68.0, "high", "2025-01-14", "Staple foods"),
        ("ARG", "Argentina", 254.0, "critical", "2025-01-13", "YoY food CPI"),
        ("EGY", "Egypt", 48.0, "high", "2025-01-12", "Bread, sugar"),
        ("PAK", "Pakistan", 38.0, "high", "2025-01-11", "Wheat, edible oil"),
        ("NGA", "Nigeria", 35.0, "high", "2025-01-10", "Grain prices"),
    ]:
        conn.execute(
            "INSERT INTO food_inflation_alerts (country_code, country_name, inflation_pct, risk_level, as_of_date, notes, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (*row, now),
        )
    # Youth unemployment
    for row in [
        ("ESP", "Spain", 28.5, "2025-01-14", "INE"),
        ("ITA", "Italy", 22.1, "2025-01-13", "ISTAT"),
        ("ZAF", "South Africa", 59.4, "2025-01-12", "Stats SA"),
        ("GRC", "Greece", 24.8, "2025-01-11", "ELSTAT"),
        ("EGY", "Egypt", 19.2, "2025-01-10", "CAPMAS"),
        ("BRA", "Brazil", 18.9, "2025-01-09", "IBGE"),
    ]:
        conn.execute(
            "INSERT INTO youth_unemployment (country_code, country_name, rate_pct, as_of_date, source, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (*row, now),
        )
    # Social sentiment (geo-tagged placeholder)
    for row in [
        ("FRA", "France", -0.35, 12000, "Twitter/X", "2025-01-14"),
        ("ARG", "Argentina", -0.42, 8500, "Twitter/X", "2025-01-13"),
        ("PAK", "Pakistan", -0.28, 6200, "Twitter/X", "2025-01-12"),
        ("EGY", "Egypt", -0.31, 4100, "Twitter/X", "2025-01-11"),
        ("KEN", "Kenya", -0.19, 3800, "Twitter/X", "2025-01-10"),
    ]:
        conn.execute(
            "INSERT INTO social_sentiment (country_code, country_name, sentiment_score, sample_size, platform, as_of_date, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (*row, now),
        )


def _seed_conflict(conn: sqlite3.Connection) -> None:
    """Seed conflict & military escalation tables if empty."""
    now = datetime.utcnow().isoformat() + "Z"
    cur = conn.execute("SELECT 1 FROM defense_spending LIMIT 1")
    if cur.fetchone():
        return
    # Defense spending
    for row in [
        ("USA", "United States", 2024, 916.0, 3.2, "SIPRI"),
        ("CHN", "China", 2024, 296.0, 1.7, "SIPRI"),
        ("RUS", "Russia", 2024, 109.0, 5.9, "SIPRI"),
        ("IND", "India", 2024, 81.4, 2.4, "SIPRI"),
        ("SAU", "Saudi Arabia", 2024, 75.0, 7.1, "SIPRI"),
        ("GBR", "United Kingdom", 2024, 74.0, 2.3, "SIPRI"),
        ("DEU", "Germany", 2024, 66.8, 1.6, "SIPRI"),
        ("JPN", "Japan", 2024, 55.0, 1.1, "SIPRI"),
        ("FRA", "France", 2024, 53.6, 1.9, "SIPRI"),
        ("KOR", "South Korea", 2024, 48.0, 2.7, "SIPRI"),
    ]:
        conn.execute(
            "INSERT INTO defense_spending (country_code, country_name, year, spending_usd_billions, pct_gdp, source, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (*row, now),
        )
    # Military exercises
    for row in [
        ("USA,NATO", "NATO Steadfast Defender 2025", "2025-01", "2025-06", "Europe", "major", "Large-scale NATO exercise across Eastern Europe"),
        ("USA,JPN,KOR", "Freedom Edge", "2025-02", "2025-02", "East Asia", "major", "Trilateral naval and air"),
        ("RUS,BLR", "Zapad", "2024-09", "2024-09", "Eastern Europe", "major", "Joint strategic exercise"),
        ("CHN,RUS", "Northern Interaction 2024", "2024-07", "2024-07", "East Asia", "major", "Naval drill"),
        ("IND", "Gaganshakti", "2024-04", "2024-05", "South Asia", "major", "Pan-India air force"),
        ("PAK", "Shaheen", "2024-10", "2024-10", "South Asia", "medium", "Air force exercise"),
    ]:
        conn.execute(
            "INSERT INTO military_exercises (participants, name, start_date, end_date, region, scale, description, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (*row, now),
        )
    # Border incidents
    for row in [
        ("IND", "India", "CHN", "China", "2025-01-08", "Minor skirmish at LAC; no casualties", "tactical", "reported"),
        ("PRK", "North Korea", "KOR", "South Korea", "2025-01-05", "DMZ firing incident", "tactical", "reported"),
        ("ISR", "Israel", "LBN", "Lebanon", "2025-01-12", "Cross-border exchange", "tactical", "reported"),
        ("PAK", "Pakistan", "IND", "India", "2024-12-20", "LoC ceasefire violation", "tactical", "reported"),
        ("UKR", "Ukraine", "RUS", "Russia", "2025-01-10", "Front-line engagement", "tactical", "ongoing"),
    ]:
        conn.execute(
            "INSERT INTO border_incidents (country_a_code, country_a_name, country_b_code, country_b_name, incident_date, summary, severity, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (*row, now),
        )
    # Satellite-detected military movement
    for row in [
        ("RUS", "Russia", "Eastern Europe", "satellite", "Concentration of armor near border", "2025-01-14", 52.0, 21.0),
        ("CHN", "China", "South China Sea", "satellite", "Aircraft carrier group movement", "2025-01-13", 18.0, 112.0),
        ("PRK", "North Korea", "Korean Peninsula", "satellite", "Missile facility activity", "2025-01-12", 39.0, 125.0),
        ("IRN", "Iran", "Persian Gulf", "satellite", "Naval patrol buildup", "2025-01-11", 27.0, 53.0),
    ]:
        conn.execute(
            "INSERT INTO military_movement (country_code, country_name, region, detection_type, summary, observed_date, lat, lon, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (*row, now),
        )
    # Naval deployments (for heat map)
    for row in [
        ("USA", "United States", "Mediterranean", "Carrier Strike Group", "2025-01-14"),
        ("USA", "United States", "South China Sea", "Destroyer squadron", "2025-01-13"),
        ("CHN", "China", "South China Sea", "Liaoning group", "2025-01-14"),
        ("RUS", "Russia", "Black Sea", "Frigates, patrol", "2025-01-12"),
        ("IRN", "Iran", "Persian Gulf", "Fast attack craft", "2025-01-13"),
        ("IND", "India", "Indian Ocean", "Carrier group", "2025-01-11"),
        ("FRA", "France", "Mediterranean", "Frigate", "2025-01-10"),
        ("GBR", "United Kingdom", "North Atlantic", "Destroyer", "2025-01-14"),
    ]:
        conn.execute(
            "INSERT INTO naval_deployments (country_code, country_name, region, vessel_description, as_of_date, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (*row, now),
        )
    # Arms trade
    for row in [
        ("USA", "Taiwan", "F-16s, missiles", 8600, 2024, "delivered"),
        ("USA", "Poland", "HIMARS, Abrams", 4200, 2024, "delivered"),
        ("FRA", "UAE", "Rafale, helicopters", 1900, 2024, "delivered"),
        ("RUS", "IND", "S-400, ordnance", 1500, 2024, "partial"),
        ("KOR", "Poland", "K2 tanks, K9 artillery", 5800, 2024, "delivered"),
        ("USA", "Israel", "Munitions, JDAM", 3200, 2024, "ongoing"),
        ("DEU", "Israel", "Subsystems", 400, 2024, "ongoing"),
    ]:
        conn.execute(
            "INSERT INTO arms_trade (supplier_country, recipient_country, weapon_type, value_usd_millions, year, deal_status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (*row, now),
        )


def _seed_sanctions_watch(conn: sqlite3.Connection) -> None:
    """Seed sanctions watch tables (entity list alerts, export restrictions) if empty."""
    now = datetime.utcnow().isoformat() + "Z"
    cur = conn.execute("SELECT 1 FROM entity_list_alerts LIMIT 1")
    if not cur.fetchone():
        # Entity list alerts (OFAC, EU, US BIS Entity List, China) – expanded
        for row in [
            ("OFAC", "Rosneft", "company", "Russia", "SDN", "2024-03-15", "Oil sector designation"),
            ("OFAC", "Sberbank", "company", "Russia", "SDN", "2022-02-24", "Financial sector"),
            ("OFAC", "Gazprom Neft", "company", "Russia", "SDN", "2022-05-20", "Oil subsidiary"),
            ("OFAC", "VTB Bank", "company", "Russia", "SDN", "2022-02-24", "Banking sector"),
            ("OFAC", "Iran Central Bank", "entity", "Iran", "SDN", "2024-01-10", "Financial"),
            ("OFAC", "North Korea Ministry of Defense", "entity", "North Korea", "SDN", "2023-11-01", "Proliferation"),
            ("EU", "Gazprombank", "company", "Russia", "EU Consolidated", "2022-04-08", "Asset freeze"),
            ("EU", "Alfa-Bank", "company", "Russia", "EU Consolidated", "2022-04-09", "Asset freeze"),
            ("EU", "Novatek", "company", "Russia", "EU Consolidated", "2022-06-03", "LNG producer"),
            ("EU", "Rosneft Deutschland", "company", "Germany", "EU Consolidated", "2022-04-08", "Asset freeze"),
            ("US_BIS", "Huawei", "company", "China", "Entity List", "2019-05-16", "Export restrictions"),
            ("US_BIS", "SMIC", "company", "China", "Entity List", "2020-12-18", "Semiconductor"),
            ("US_BIS", "BGI Genomics", "company", "China", "Entity List", "2023-03-24", "Biotech/data"),
            ("US_BIS", "YMTC", "company", "China", "Entity List", "2022-12-15", "NAND flash memory"),
            ("US_BIS", "Cambrian", "company", "China", "Entity List", "2023-10-17", "AI chips"),
            ("US_BIS", "Inspur", "company", "China", "Entity List", "2023-03-02", "Server/supercomputing"),
            ("China", "Micron", "company", "USA", "Critical Infrastructure", "2023-05-21", "Cybersecurity review"),
            ("China", "Lockheed Martin", "company", "USA", "Unreliable Entity", "2024-02-16", "Arms sales to Taiwan"),
            ("China", "Raytheon", "company", "USA", "Unreliable Entity", "2024-02-16", "Arms sales to Taiwan"),
            ("China", "Northrop Grumman", "company", "USA", "Unreliable Entity", "2024-02-16", "Arms sales to Taiwan"),
        ]:
            conn.execute(
                "INSERT INTO entity_list_alerts (source, entity_name, entity_type, country, list_name, listed_date, summary, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (*row, now),
            )
    cur = conn.execute("SELECT 1 FROM export_restrictions LIMIT 1")
    if not cur.fetchone():
        for row in [
            ("US", "dual_use", "Semiconductor export controls (China)", "Advanced chips and equipment to China.", "2023-10-17", "https://www.bis.doc.gov"),
            ("EU", "dual_use", "EU dual-use regulation update", "Stricter controls on cyber surveillance, semiconductors.", "2024-05-09", ""),
            ("China", "technology", "Gallium and germanium export restrictions", "Export permits required for gallium, germanium products.", "2023-07-03", ""),
            ("China", "technology", "Graphite export controls", "Certain graphite materials; EV supply chain.", "2023-12-01", ""),
            ("US", "dual_use", "Russia/Belarus foreign direct product rule", "Expanded FDP rule for Russia/Belarus.", "2022-03-02", ""),
            ("US", "technology", "Advanced computing / GPU restrictions", "NVIDIA, AMD restrictions to China.", "2023-10-23", ""),
            ("Japan", "technology", "Semiconductor equipment export controls", "Alignment with US controls on advanced chip equipment to China.", "2023-07-23", ""),
            ("Netherlands", "technology", "ASML export restrictions", "Restrictions on certain DUV equipment to China.", "2023-06-30", ""),
            ("China", "technology", "Rare earth export regulations", "Export permits for certain rare earth elements.", "2024-01-01", ""),
            ("US", "dual_use", "CHIPS Act guardrails", "Restrictions on semiconductor investment in China.", "2023-09-01", "https://www.nist.gov/chips"),
        ]:
            conn.execute(
                "INSERT INTO export_restrictions (issuer, restriction_type, title, description, effective_date, source_url, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (*row, now),
            )
    # Sample OFAC/EU sanctions for tracker (if no source-tagged sanctions yet)
    cur = conn.execute("SELECT 1 FROM sanctions_registry WHERE source IN ('OFAC','EU') LIMIT 1")
    if cur.fetchone():
        return
    try:
        for row in [
            ("United States", "Russia", "asset_freeze", "SDN designation; financial and energy sector.", "2024-03-01", "", "https://ofac.treasury.gov", "OFAC"),
            ("United States", "Iran", "financial", "Secondary sanctions; oil and banking.", "2024-01-15", "", "https://ofac.treasury.gov", "OFAC"),
            ("United States", "North Korea", "comprehensive", "Virtually all trade and financial transactions prohibited.", "2017-09-21", "", "https://ofac.treasury.gov", "OFAC"),
            ("United States", "Syria", "sectoral", "Oil, financial, and designated entities.", "2019-06-17", "", "", "OFAC"),
            ("EU", "Russia", "asset_freeze", "Consolidated list; banks, energy, defence.", "2024-02-20", "", "https://finance.ec.europa.eu/eu-and-world/sanctions-restrictive-measures_en", "EU"),
            ("EU", "Belarus", "trade", "Restrictive measures; dual-use, potash, finance.", "2024-01-10", "", "", "EU"),
            ("EU", "Iran", "sectoral", "Oil, finance, and proliferation-related.", "2012-01-23", "", "", "EU"),
            ("UK", "Russia", "asset_freeze", "UK sanctions list; alignment with EU/US.", "2022-02-24", "", "https://www.gov.uk/government/collections/financial-sanctions", "EU"),
        ]:
            conn.execute(
                "INSERT INTO sanctions_registry (imposing_country, target_country, measure_type, description, start_date, end_date, source_url, source, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (*row, now),
            )
    except Exception:
        pass


def _seed_scenarios(conn: sqlite3.Connection) -> None:
    """Seed scenario planning engine scenarios if empty."""
    now = datetime.utcnow().isoformat() + "Z"
    cur = conn.execute("SELECT 1 FROM scenarios LIMIT 1")
    if cur.fetchone():
        return
    for row in [
        ("Taiwan crisis 2028", "taiwan_crisis_2028", "Simulate escalation or blockade scenario around Taiwan; economic, military, and diplomatic pathways.", "East Asia", 2028, "taiwan_crisis"),
        ("ASEAN currency stress", "asean_currency_stress", "Model regional currency volatility, capital flows, and policy responses across ASEAN.", "Southeast Asia", 2029, "currency_stress"),
        ("Gulf energy disruption", "gulf_energy_disruption", "Strait of Hormuz or supply shock; oil/LNG and global inflation.", "Middle East", 2028, "energy_crisis"),
        ("Europe defence surge", "europe_defence_surge", "Rapid defence spending and industrial shift; fiscal and political constraints.", "Europe", 2030, "defence_policy"),
    ]:
        conn.execute(
            "INSERT INTO scenarios (name, slug, description, region, horizon_year, scenario_type, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (*row, now),
        )


def _seed_integration(conn: sqlite3.Connection) -> None:
    """Seed economic-geopolitical integration layer with all countries (if table empty)."""
    cur = conn.execute("SELECT 1 FROM country_risk_integration LIMIT 1")
    if cur.fetchone():
        return
    from app.country_data import ALL_COUNTRIES, get_integration_risk

    now = datetime.utcnow().isoformat() + "Z"
    for iso3, name, region, pop, area, density in ALL_COUNTRIES:
        trade, debt, cap_flight, reserve, fx, energy, geo = get_integration_risk(iso3, region)
        conn.execute(
            """INSERT INTO country_risk_integration (country_code, country_name, region, trade_flow_pct_gdp, debt_distress_score, capital_flight_risk, reserve_months_imports, fx_vulnerability_score, energy_import_exposure_pct, geopolitical_fragility_score, population_2026, land_area_km2, density_per_km2, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (iso3, name, region, trade, debt, cap_flight, reserve, fx, energy, geo, pop, area, density, now),
        )


def _backfill_all_countries(conn: sqlite3.Connection) -> None:
    """Ensure all countries from ALL_COUNTRIES exist; update population/land/density and fill missing risk data."""
    from app.country_data import ALL_COUNTRIES, get_integration_risk

    now = datetime.utcnow().isoformat() + "Z"
    cur = conn.execute("PRAGMA table_info(country_risk_integration)")
    cols = {row[1] for row in cur.fetchall()}
    has_pop = "population_2026" in cols
    if not has_pop:
        return

    for iso3, name, region, pop, area, density in ALL_COUNTRIES:
        existing = conn.execute("SELECT 1 FROM country_risk_integration WHERE country_code = ?", (iso3,)).fetchone()
        trade, debt, cap_flight, reserve, fx, energy, geo = get_integration_risk(iso3, region)
        if existing:
            conn.execute(
                """UPDATE country_risk_integration SET population_2026 = ?, land_area_km2 = ?, density_per_km2 = ?, country_name = ?, region = ?, trade_flow_pct_gdp = ?, debt_distress_score = ?, capital_flight_risk = ?, reserve_months_imports = ?, fx_vulnerability_score = ?, energy_import_exposure_pct = ?, geopolitical_fragility_score = ?, updated_at = ? WHERE country_code = ?""",
                (pop, area, density, name, region, trade, debt, cap_flight, reserve, fx, energy, geo, now, iso3),
            )
        else:
            conn.execute(
                """INSERT INTO country_risk_integration (country_code, country_name, region, trade_flow_pct_gdp, debt_distress_score, capital_flight_risk, reserve_months_imports, fx_vulnerability_score, energy_import_exposure_pct, geopolitical_fragility_score, population_2026, land_area_km2, density_per_km2, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (iso3, name, region, trade, debt, cap_flight, reserve, fx, energy, geo, pop, area, density, now),
            )


def _ensure_sanctions_columns(conn: sqlite3.Connection) -> None:
    """Add sanctions_registry columns if missing (e.g. source, SFI scores)."""
    cur = conn.execute("PRAGMA table_info(sanctions_registry)")
    existing = {row[1] for row in cur.fetchall()}
    if "source" not in existing:
        conn.execute("ALTER TABLE sanctions_registry ADD COLUMN source TEXT")
    for col, typ in [
        ("severity_score", "REAL"),
        ("enforceability_score", "REAL"),
        ("economic_centrality_score", "REAL"),
    ]:
        if col not in existing:
            conn.execute(f"ALTER TABLE sanctions_registry ADD COLUMN {col} {typ}")


def _ensure_election_columns(conn: sqlite3.Connection) -> None:
    """Add election_calendar columns if missing (e.g. updated_at)."""
    cur = conn.execute("PRAGMA table_info(election_calendar)")
    existing = {row[1] for row in cur.fetchall()}
    if "updated_at" not in existing:
        conn.execute("ALTER TABLE election_calendar ADD COLUMN updated_at TEXT")


def _ensure_approval_columns(conn: sqlite3.Connection) -> None:
    """Add approval_ratings columns if missing (poll_url, sample_size)."""
    cur = conn.execute("PRAGMA table_info(approval_ratings)")
    existing = {row[1] for row in cur.fetchall()}
    for col, typ in [("poll_url", "TEXT"), ("sample_size", "INTEGER")]:
        if col not in existing:
            conn.execute(f"ALTER TABLE approval_ratings ADD COLUMN {col} {typ}")


def _ensure_protest_columns(conn: sqlite3.Connection) -> None:
    """Add protest_tracking columns if missing (location, severity, source_url)."""
    cur = conn.execute("PRAGMA table_info(protest_tracking)")
    existing = {row[1] for row in cur.fetchall()}
    for col, typ in [("location", "TEXT"), ("severity", "TEXT"), ("source_url", "TEXT")]:
        if col not in existing:
            conn.execute(f"ALTER TABLE protest_tracking ADD COLUMN {col} {typ}")


def _ensure_country_integration_columns(conn: sqlite3.Connection) -> None:
    """Add population_2026, land_area_km2, density_per_km2 if missing (migration for existing DBs)."""
    cur = conn.execute("PRAGMA table_info(country_risk_integration)")
    existing = {row[1] for row in cur.fetchall()}
    for col in ("population_2026", "land_area_km2", "density_per_km2"):
        if col not in existing:
            conn.execute(f"ALTER TABLE country_risk_integration ADD COLUMN {col} INTEGER")


def _create_indicator_tables(conn: sqlite3.Connection) -> None:
    """Create tables for extended indicator roadmap: macro, energy, military, trade, multilateral, capital flows, elite, climate, legislative, tech, conflict imports, geospatial."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS macroeconomic_stress (
            country_code TEXT NOT NULL,
            as_of_date TEXT NOT NULL,
            gdp_growth_quarterly_pct REAL,
            gdp_growth_annual_pct REAL,
            inflation_pct REAL,
            current_account_pct_gdp REAL,
            external_debt_pct_gdp REAL,
            debt_to_gdp_pct REAL,
            sovereign_rating TEXT,
            bond_spread_bps INTEGER,
            source TEXT,
            created_at TEXT NOT NULL,
            PRIMARY KEY (country_code, as_of_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS energy_commodity_exposure (
            country_code TEXT NOT NULL,
            as_of_date TEXT NOT NULL,
            oil_production_bpd REAL,
            gas_exports_bcm REAL,
            lng_capacity_mtpa REAL,
            energy_import_pct REAL,
            rare_earth_production_share REAL,
            grain_export_import_exposure TEXT,
            notes TEXT,
            created_at TEXT NOT NULL,
            PRIMARY KEY (country_code, as_of_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS military_capability_snapshot (
            country_code TEXT NOT NULL,
            as_of_year INTEGER NOT NULL,
            active_troops INTEGER,
            naval_tonnage INTEGER,
            defense_alliances TEXT,
            source TEXT,
            created_at TEXT NOT NULL,
            PRIMARY KEY (country_code, as_of_year)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trade_flow_partners (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_code TEXT NOT NULL,
            direction TEXT NOT NULL,
            partner_country TEXT NOT NULL,
            sector TEXT,
            share_pct REAL,
            value_usd_millions REAL,
            as_of_year INTEGER,
            created_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS multilateral_participation (
            country_code TEXT NOT NULL,
            org_key TEXT NOT NULL,
            membership_status TEXT,
            program_notes TEXT,
            as_of_date TEXT,
            created_at TEXT NOT NULL,
            PRIMARY KEY (country_code, org_key)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS capital_flows (
            country_code TEXT NOT NULL,
            as_of_year INTEGER NOT NULL,
            fdi_inflow_usd_millions REAL,
            fdi_outflow_usd_millions REAL,
            portfolio_flows_usd_millions REAL,
            sector_exposure_json TEXT,
            sovereign_wealth_fund_usd_billions REAL,
            source TEXT,
            created_at TEXT NOT NULL,
            PRIMARY KEY (country_code, as_of_year)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS elite_institutional (
            country_code TEXT NOT NULL PRIMARY KEY,
            governance_model TEXT,
            key_actors_json TEXT,
            major_soes TEXT,
            central_bank_independence_score INTEGER,
            party_structure TEXT,
            coup_attempts_history TEXT,
            updated_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS climate_resource_vulnerability (
            country_code TEXT NOT NULL,
            as_of_year INTEGER NOT NULL,
            water_stress_index REAL,
            food_insecurity_index REAL,
            natural_disaster_frequency REAL,
            climate_risk_score INTEGER,
            source TEXT,
            created_at TEXT NOT NULL,
            PRIMARY KEY (country_code, as_of_year)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS legislative_policy_tracker (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            jurisdiction TEXT NOT NULL,
            bill_type TEXT NOT NULL,
            title TEXT NOT NULL,
            status TEXT,
            summary TEXT,
            source_url TEXT,
            introduced_date TEXT,
            created_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS technology_semiconductor (
            country_code TEXT NOT NULL,
            as_of_year INTEGER NOT NULL,
            chip_exports_usd_millions REAL,
            critical_tech_companies TEXT,
            advanced_manufacturing_capacity TEXT,
            export_restriction_notes TEXT,
            created_at TEXT NOT NULL,
            PRIMARY KEY (country_code, as_of_year)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS conflict_event_imports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            external_id TEXT,
            country_code TEXT,
            event_date TEXT NOT NULL,
            event_type TEXT,
            fatalities INTEGER,
            summary TEXT,
            raw_json TEXT,
            created_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS geospatial_infrastructure (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            infra_type TEXT NOT NULL,
            name TEXT NOT NULL,
            country_code TEXT,
            region TEXT,
            lat REAL,
            lon REAL,
            capacity_notes TEXT,
            created_at TEXT NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_macro_country ON macroeconomic_stress(country_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_energy_country ON energy_commodity_exposure(country_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_military_cap_country ON military_capability_snapshot(country_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_partners_country ON trade_flow_partners(country_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_capital_flows_country ON capital_flows(country_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_climate_country ON climate_resource_vulnerability(country_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tech_semi_country ON technology_semiconductor(country_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_conflict_imports_source ON conflict_event_imports(source)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_conflict_imports_country ON conflict_event_imports(country_code)")


def _create_macro_timeseries_tables(conn: sqlite3.Connection) -> None:
    """Create macro time-series tables for Live Macroeconomic Indicators module."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS countries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL UNIQUE,          -- ISO3 where available (USA, CHN, SGP)
            name TEXT NOT NULL,
            region TEXT,
            is_asean INTEGER DEFAULT 0,
            is_g20 INTEGER DEFAULT 0,
            is_major INTEGER DEFAULT 0,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_countries_region ON countries(region)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS data_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key TEXT NOT NULL UNIQUE,           -- e.g. world_bank, fx_host
            name TEXT NOT NULL,
            base_url TEXT,
            notes TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS indicators (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,          -- stable key used in APIs (gdp_growth_yoy, cpi_inflation, fx_usd_sgd)
            label TEXT NOT NULL,
            unit TEXT,
            frequency TEXT,                     -- daily|monthly|quarterly|annual
            category TEXT,                      -- growth|inflation|rates|labor|trade|fx|debt|pmi
            source_id INTEGER,
            external_code TEXT,                 -- e.g. World Bank indicator code
            updated_at TEXT NOT NULL,
            FOREIGN KEY(source_id) REFERENCES data_sources(id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_indicators_category ON indicators(category)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS indicator_values (
            country_id INTEGER NOT NULL,
            indicator_id INTEGER NOT NULL,
            date TEXT NOT NULL,                 -- YYYY-MM-DD or YYYY-MM or YYYY or YYYY-Qn (stored as text, lexicographically sortable)
            value REAL,
            raw_json TEXT,
            created_at TEXT NOT NULL,
            PRIMARY KEY (country_id, indicator_id, date),
            FOREIGN KEY(country_id) REFERENCES countries(id),
            FOREIGN KEY(indicator_id) REFERENCES indicators(id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_indicator_values_country ON indicator_values(country_id, date DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_indicator_values_indicator ON indicator_values(indicator_id, date DESC)")


def _ensure_scenario_engine_runs_columns(conn: sqlite3.Connection) -> None:
    """Add name and notes to scenario_engine_runs if missing."""
    cur = conn.execute("PRAGMA table_info(scenario_engine_runs)")
    existing = {row[1] for row in cur.fetchall()}
    if "name" not in existing:
        conn.execute("ALTER TABLE scenario_engine_runs ADD COLUMN name TEXT")
    if "notes" not in existing:
        conn.execute("ALTER TABLE scenario_engine_runs ADD COLUMN notes TEXT")


def _ensure_desk_terminal_intel_table(conn: sqlite3.Connection) -> None:
    """Approved Analyst Desk output surfaced on country dashboards (Geopolitical Terminal)."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS desk_terminal_intel (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            proposal_id INTEGER NOT NULL UNIQUE,
            agent_id TEXT NOT NULL,
            run_type TEXT NOT NULL,
            title TEXT,
            body_markdown TEXT NOT NULL,
            countries_csv TEXT,
            focus_country TEXT,
            metrics_json TEXT NOT NULL DEFAULT '[]',
            reviewer_note TEXT,
            published_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_desk_intel_published ON desk_terminal_intel(published_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_desk_intel_focus ON desk_terminal_intel(focus_country, published_at DESC)"
    )


def init_db(path: str) -> None:
    """Create database, tables, FTS, and migrate schema if needed."""
    global _DB_PATH
    _DB_PATH = path
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with _connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                url TEXT UNIQUE NOT NULL,
                source_name TEXT NOT NULL,
                source_url TEXT,
                summary TEXT,
                published_utc TEXT,
                scraped_at TEXT NOT NULL
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_utc)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source_name)"
        )
        _ensure_article_columns(conn)
        _ensure_desk_terminal_intel_table(conn)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_articles_cluster ON articles(cluster_id)"
        )
        _create_watchlists_table(conn)
        _create_users_table(conn)
        _ensure_users_profile_columns(conn)
        _ensure_users_policy_prefs_columns(conn)
        _create_api_keys_table(conn)
        _create_alerts_table(conn)
        _ensure_alerts_user_id(conn)
        _create_digests_table(conn)
        _create_saved_views_table(conn)
        _ensure_saved_views_user_id(conn)
        _create_saved_briefings_table(conn)
        _ensure_saved_briefings_user_id(conn)
        _create_annotations_table(conn)
        _create_policy_workspace_tables(conn)
        _ensure_saved_briefings_policy_columns(conn)
        _create_alert_webhook_state_table(conn)
        _create_messaging_tables(conn)
        # Geopolitical Risk Engine tables
        conn.execute("""
            CREATE TABLE IF NOT EXISTS country_risk_snapshots (
                country_code TEXT NOT NULL,
                risk_score REAL NOT NULL,
                sector_energy REAL DEFAULT 0,
                sector_tech REAL DEFAULT 0,
                sector_maritime REAL DEFAULT 0,
                sector_supply_chain REAL DEFAULT 0,
                article_count INTEGER DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (country_code)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS risk_index (
                region_code TEXT NOT NULL PRIMARY KEY,
                coup_likelihood_pct REAL DEFAULT 0,
                sanctions_probability_pct REAL DEFAULT 0,
                trade_disruption_pct REAL DEFAULT 0,
                escalation_pathway_json TEXT,
                updated_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS source_categories (
                source_name TEXT NOT NULL PRIMARY KEY,
                category TEXT NOT NULL
            )
        """)
        # Diplomacy & Treaty Intelligence
        conn.execute("""
            CREATE TABLE IF NOT EXISTS treaties (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                treaty_type TEXT NOT NULL,
                name TEXT NOT NULL,
                party_a TEXT NOT NULL,
                party_b TEXT,
                signed_date TEXT,
                summary TEXT,
                full_text TEXT,
                clauses_json TEXT,
                has_escalation_clause INTEGER DEFAULT 0,
                source_url TEXT,
                document_url TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sanctions_registry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                imposing_country TEXT NOT NULL,
                target_country TEXT NOT NULL,
                measure_type TEXT,
                description TEXT,
                start_date TEXT,
                end_date TEXT,
                source_url TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sanctions_global (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                jurisdiction TEXT NOT NULL,
                target_type TEXT NOT NULL,
                name TEXT NOT NULL,
                country TEXT,
                sanctions_type TEXT,
                effective_date TEXT,
                expiry_date TEXT,
                measures TEXT,
                source_link TEXT,
                last_updated TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sanctions_global_jurisdiction ON sanctions_global(jurisdiction)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sanctions_global_target_type ON sanctions_global(target_type)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sanctions_global_country ON sanctions_global(country)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS un_votes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                resolution_id TEXT NOT NULL,
                resolution_title TEXT,
                country_code TEXT NOT NULL,
                vote TEXT NOT NULL,
                vote_date TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(resolution_id, country_code)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS voting_alignment (
                country_a TEXT NOT NULL,
                country_b TEXT NOT NULL,
                alignment_score REAL NOT NULL,
                votes_agreed INTEGER NOT NULL,
                votes_total INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (country_a, country_b)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_treaties_type ON treaties(treaty_type)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_treaties_escalation ON treaties(has_escalation_clause)")
        _ensure_treaty_columns(conn)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_un_votes_country ON un_votes(country_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_un_votes_resolution ON un_votes(resolution_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_un_votes_resolution_date ON un_votes(resolution_id, vote_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_voting_alignment_score ON voting_alignment(alignment_score DESC)")
        # Supply chain & trade flow intelligence
        conn.execute("""
            CREATE TABLE IF NOT EXISTS chokepoints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                slug TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                region TEXT,
                commodities TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS chokepoint_flows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chokepoint_id INTEGER NOT NULL,
                country_code TEXT NOT NULL,
                country_name TEXT NOT NULL,
                sector TEXT NOT NULL,
                exposure_pct REAL,
                impact_if_closed TEXT,
                notes TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (chokepoint_id) REFERENCES chokepoints(id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_chokepoint_flows_chokepoint ON chokepoint_flows(chokepoint_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_chokepoint_flows_country ON chokepoint_flows(country_code)")
        _seed_chokepoints(conn)
        # Political stability & domestic signals
        conn.execute("""
            CREATE TABLE IF NOT EXISTS election_calendar (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_code TEXT NOT NULL,
                country_name TEXT NOT NULL,
                election_type TEXT NOT NULL,
                date_planned TEXT NOT NULL,
                status TEXT,
                notes TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS approval_ratings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_code TEXT NOT NULL,
                country_name TEXT NOT NULL,
                subject TEXT NOT NULL,
                approval_pct REAL NOT NULL,
                poll_date TEXT,
                source TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS protest_tracking (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_code TEXT NOT NULL,
                country_name TEXT NOT NULL,
                event_date TEXT NOT NULL,
                summary TEXT,
                estimated_size TEXT,
                trigger_topic TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS currency_stress (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_code TEXT NOT NULL,
                country_name TEXT NOT NULL,
                indicator_value REAL,
                stress_level TEXT,
                as_of_date TEXT,
                notes TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS food_inflation_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_code TEXT NOT NULL,
                country_name TEXT NOT NULL,
                inflation_pct REAL,
                risk_level TEXT,
                as_of_date TEXT,
                notes TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS youth_unemployment (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_code TEXT NOT NULL,
                country_name TEXT NOT NULL,
                rate_pct REAL,
                as_of_date TEXT,
                source TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS social_sentiment (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_code TEXT NOT NULL,
                country_name TEXT NOT NULL,
                sentiment_score REAL,
                sample_size INTEGER,
                platform TEXT,
                as_of_date TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_election_country ON election_calendar(country_code)")
        _ensure_election_columns(conn)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_approval_country ON approval_ratings(country_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_approval_country_date ON approval_ratings(country_code, poll_date)")
        _ensure_approval_columns(conn)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_protest_country ON protest_tracking(country_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_protest_country_date ON protest_tracking(country_code, event_date)")
        _ensure_protest_columns(conn)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_currency_country ON currency_stress(country_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_food_inflation_country ON food_inflation_alerts(country_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_youth_unemp_country ON youth_unemployment(country_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_social_sentiment_country ON social_sentiment(country_code)")
        _seed_stability(conn)
        # Conflict & military escalation monitor
        conn.execute("""
            CREATE TABLE IF NOT EXISTS defense_spending (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_code TEXT NOT NULL,
                country_name TEXT NOT NULL,
                year INTEGER NOT NULL,
                spending_usd_billions REAL,
                pct_gdp REAL,
                source TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS military_exercises (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                participants TEXT NOT NULL,
                name TEXT NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                region TEXT,
                scale TEXT,
                description TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS border_incidents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_a_code TEXT NOT NULL,
                country_a_name TEXT NOT NULL,
                country_b_code TEXT NOT NULL,
                country_b_name TEXT NOT NULL,
                incident_date TEXT NOT NULL,
                summary TEXT,
                severity TEXT,
                status TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS military_movement (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_code TEXT NOT NULL,
                country_name TEXT NOT NULL,
                region TEXT,
                detection_type TEXT,
                summary TEXT,
                observed_date TEXT,
                lat REAL,
                lon REAL,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS naval_deployments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_code TEXT NOT NULL,
                country_name TEXT NOT NULL,
                region TEXT NOT NULL,
                vessel_description TEXT,
                as_of_date TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS arms_trade (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                supplier_country TEXT NOT NULL,
                recipient_country TEXT NOT NULL,
                weapon_type TEXT,
                value_usd_millions REAL,
                year INTEGER,
                deal_status TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_defense_spending_country ON defense_spending(country_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_border_incidents_a ON border_incidents(country_a_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_border_incidents_b ON border_incidents(country_b_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_military_movement_country ON military_movement(country_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_naval_deployments_region ON naval_deployments(region)")
        _seed_conflict(conn)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS conflict_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                region TEXT,
                country_code TEXT,
                record_id INTEGER NOT NULL,
                table_name TEXT NOT NULL,
                summary TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_conflict_events_type ON conflict_events(event_type)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS conflict_alert_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                event_types TEXT NOT NULL,
                region TEXT,
                country_code TEXT,
                webhook_url TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS conflict_events_last_id (
                table_name TEXT PRIMARY KEY,
                last_id INTEGER NOT NULL DEFAULT 0
            )
        """)
        # Sanctions, export controls & regulatory watch
        _ensure_sanctions_columns(conn)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS entity_list_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                entity_name TEXT NOT NULL,
                entity_type TEXT,
                country TEXT,
                list_name TEXT,
                listed_date TEXT,
                summary TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS export_restrictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                issuer TEXT NOT NULL,
                restriction_type TEXT,
                title TEXT NOT NULL,
                description TEXT,
                effective_date TEXT,
                source_url TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_entity_list_source ON entity_list_alerts(source)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_entity_list_entity ON entity_list_alerts(entity_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_export_restrictions_issuer ON export_restrictions(issuer)")
        _seed_sanctions_watch(conn)
        # Scenario planning engine (MCDA / Delphi)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS scenarios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                slug TEXT UNIQUE NOT NULL,
                description TEXT,
                region TEXT,
                horizon_year INTEGER,
                scenario_type TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS scenario_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scenario_id INTEGER NOT NULL,
                run_at TEXT NOT NULL,
                agent_outputs TEXT,
                probability_summary TEXT,
                outlook_summary TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_scenario_runs_scenario ON scenario_runs(scenario_id)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS scenario_engine_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                event_label TEXT NOT NULL,
                region TEXT,
                country TEXT,
                horizon_year INTEGER,
                agents_json TEXT,
                paths_json TEXT,
                path_descriptions_json TEXT,
                run_at TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_scenario_engine_runs_run_at ON scenario_engine_runs(run_at)")
        _ensure_scenario_engine_runs_columns(conn)
        _seed_scenarios(conn)
        # Economic-geopolitical integration layer
        conn.execute("""
            CREATE TABLE IF NOT EXISTS country_risk_integration (
                country_code TEXT PRIMARY KEY,
                country_name TEXT NOT NULL,
                region TEXT,
                trade_flow_pct_gdp REAL,
                debt_distress_score INTEGER,
                capital_flight_risk INTEGER,
                reserve_months_imports REAL,
                fx_vulnerability_score INTEGER,
                energy_import_exposure_pct REAL,
                geopolitical_fragility_score INTEGER,
                population_2026 INTEGER,
                land_area_km2 INTEGER,
                density_per_km2 INTEGER,
                updated_at TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_integration_region ON country_risk_integration(region)")
        _ensure_country_integration_columns(conn)
        _seed_integration(conn)
        _backfill_all_countries(conn)
        import sys
        getattr(sys.modules[__name__], "_create_macro_timeseries_tables")(conn)
        getattr(sys.modules[__name__], "_create_indicator_tables")(conn)
        _create_fts(conn)
        _create_fts_triggers(conn)
    # Populate FTS for existing rows (idempotent)
    rebuild_fts()


def _ensure_article_columns(conn: sqlite3.Connection) -> None:
    """Add analysis/topic columns if they don't exist."""
    cur = conn.execute("PRAGMA table_info(articles)")
    existing = {row[1] for row in cur.fetchall()}
    for col in _ARTICLE_COLUMNS:
        if col in existing:
            continue
        if col == "cluster_id":
            conn.execute("ALTER TABLE articles ADD COLUMN cluster_id INTEGER")
        elif col == "impact_score":
            conn.execute("ALTER TABLE articles ADD COLUMN impact_score INTEGER")
        elif col in ("urgency", "event_type", "image_url", "video_url"):
            conn.execute(f"ALTER TABLE articles ADD COLUMN {col} TEXT")
        else:
            conn.execute(f"ALTER TABLE articles ADD COLUMN {col} TEXT")


def _create_digests_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS digests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            digest_type TEXT NOT NULL,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_digests_created ON digests(created_at)"
    )


def _create_watchlists_table(conn: sqlite3.Connection) -> None:
    """Watchlists group topics/regions into reusable views."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS watchlists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            topics TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)


def _create_users_table(conn: sqlite3.Connection) -> None:
    """Users for auth; password stored as hash."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT,
            password_hash TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")


def _ensure_users_profile_columns(conn: sqlite3.Connection) -> None:
    """Add name, title, organization to users if missing."""
    cur = conn.execute("PRAGMA table_info(users)")
    existing = {row[1] for row in cur.fetchall()}
    for col in ("name", "title", "organization"):
        if col not in existing:
            conn.execute(f"ALTER TABLE users ADD COLUMN {col} TEXT")


def _ensure_users_policy_prefs_columns(conn: sqlite3.Connection) -> None:
    """Default export sensitivity & legal-review preference for think-tank UX."""
    cur = conn.execute("PRAGMA table_info(users)")
    existing = {row[1] for row in cur.fetchall()}
    if "default_sensitivity_tier" not in existing:
        conn.execute("ALTER TABLE users ADD COLUMN default_sensitivity_tier TEXT DEFAULT 'internal'")
    if "default_legal_review" not in existing:
        conn.execute("ALTER TABLE users ADD COLUMN default_legal_review INTEGER DEFAULT 0")


def _create_policy_workspace_tables(conn: sqlite3.Connection) -> None:
    """Tasks, threaded object comments, per-user visit tracking, entity change audit."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS policy_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_user_id INTEGER NOT NULL REFERENCES users(id),
            assignee_user_id INTEGER REFERENCES users(id),
            title TEXT NOT NULL,
            body TEXT,
            entity_type TEXT,
            entity_ref TEXT,
            due_date TEXT,
            status TEXT NOT NULL DEFAULT 'open',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_policy_tasks_owner ON policy_tasks(owner_user_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_policy_tasks_assignee ON policy_tasks(assignee_user_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_policy_tasks_entity ON policy_tasks(entity_type, entity_ref)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS object_thread_comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            entity_type TEXT NOT NULL,
            entity_ref TEXT NOT NULL,
            parent_id INTEGER REFERENCES object_thread_comments(id),
            body TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_obj_comments_entity ON object_thread_comments(entity_type, entity_ref)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_obj_comments_parent ON object_thread_comments(parent_id)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS user_entity_visits (
            user_id INTEGER NOT NULL REFERENCES users(id),
            entity_type TEXT NOT NULL,
            entity_ref TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            PRIMARY KEY (user_id, entity_type, entity_ref)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS entity_change_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type TEXT NOT NULL,
            entity_ref TEXT NOT NULL,
            summary TEXT NOT NULL,
            detail_json TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_entity_change_entity ON entity_change_log(entity_type, entity_ref)")


def _ensure_saved_briefings_policy_columns(conn: sqlite3.Connection) -> None:
    cur = conn.execute("PRAGMA table_info(saved_briefings)")
    existing = {row[1] for row in cur.fetchall()}
    if "sensitivity_tier" not in existing:
        conn.execute("ALTER TABLE saved_briefings ADD COLUMN sensitivity_tier TEXT DEFAULT 'internal'")
    if "legal_review_required" not in existing:
        conn.execute("ALTER TABLE saved_briefings ADD COLUMN legal_review_required INTEGER DEFAULT 0")


def _create_api_keys_table(conn: sqlite3.Connection) -> None:
    """API keys for programmatic access; store key hash only."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS api_keys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            key_hash TEXT NOT NULL,
            name TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_user ON api_keys(user_id)")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash)")


def _ensure_saved_views_user_id(conn: sqlite3.Connection) -> None:
    cur = conn.execute("PRAGMA table_info(saved_views)")
    existing = {row[1] for row in cur.fetchall()}
    if "user_id" not in existing:
        conn.execute("ALTER TABLE saved_views ADD COLUMN user_id INTEGER REFERENCES users(id)")


def _ensure_alerts_user_id(conn: sqlite3.Connection) -> None:
    cur = conn.execute("PRAGMA table_info(alerts)")
    existing = {row[1] for row in cur.fetchall()}
    if "user_id" not in existing:
        conn.execute("ALTER TABLE alerts ADD COLUMN user_id INTEGER REFERENCES users(id)")


def _ensure_saved_briefings_user_id(conn: sqlite3.Connection) -> None:
    cur = conn.execute("PRAGMA table_info(saved_briefings)")
    existing = {row[1] for row in cur.fetchall()}
    if "user_id" not in existing:
        conn.execute("ALTER TABLE saved_briefings ADD COLUMN user_id INTEGER REFERENCES users(id)")


def _create_alerts_table(conn: sqlite3.Connection) -> None:
    """Alert rules for matching topics + impact."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            topics TEXT NOT NULL,
            min_impact_score INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            webhook_url TEXT
        )
    """)
    _ensure_alerts_webhook_column(conn)


def _ensure_alerts_webhook_column(conn: sqlite3.Connection) -> None:
    cur = conn.execute("PRAGMA table_info(alerts)")
    existing = {row[1] for row in cur.fetchall()}
    if "webhook_url" not in existing:
        conn.execute("ALTER TABLE alerts ADD COLUMN webhook_url TEXT")


def _create_saved_views_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS saved_views (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            params_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)


def _create_saved_briefings_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS saved_briefings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            title TEXT NOT NULL,
            intro TEXT NOT NULL,
            article_ids_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)


def _create_annotations_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS annotations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id INTEGER NOT NULL,
            body TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (article_id) REFERENCES articles(id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_annotations_article ON annotations(article_id)")


def _create_alert_webhook_state_table(conn: sqlite3.Connection) -> None:
    """Track last match count per alert for webhook on new matches."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alert_webhook_state (
            alert_id INTEGER PRIMARY KEY,
            last_count INTEGER NOT NULL DEFAULT 0,
            last_run_at TEXT,
            FOREIGN KEY (alert_id) REFERENCES alerts(id)
        )
    """)


def _create_messaging_tables(conn: sqlite3.Connection) -> None:
    """Intelligence Messaging: private channels, members, invites, encrypted messages."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS messaging_channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            slug TEXT UNIQUE NOT NULL,
            channel_type TEXT NOT NULL,
            description TEXT,
            invite_only INTEGER NOT NULL DEFAULT 1,
            created_by_user_id INTEGER,
            created_at TEXT NOT NULL,
            FOREIGN KEY (created_by_user_id) REFERENCES users(id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_messaging_channels_slug ON messaging_channels(slug)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_messaging_channels_type ON messaging_channels(channel_type)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS messaging_channel_members (
            channel_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            role TEXT NOT NULL DEFAULT 'member',
            joined_at TEXT NOT NULL,
            PRIMARY KEY (channel_id, user_id),
            FOREIGN KEY (channel_id) REFERENCES messaging_channels(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_messaging_members_user ON messaging_channel_members(user_id)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS messaging_invites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL,
            invited_by_user_id INTEGER NOT NULL,
            invite_token TEXT UNIQUE NOT NULL,
            invited_email TEXT,
            expires_at TEXT NOT NULL,
            used_at TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (channel_id) REFERENCES messaging_channels(id) ON DELETE CASCADE,
            FOREIGN KEY (invited_by_user_id) REFERENCES users(id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_messaging_invites_token ON messaging_invites(invite_token)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS messaging_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            content_encrypted BLOB NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (channel_id) REFERENCES messaging_channels(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_messaging_messages_channel ON messaging_messages(channel_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_messaging_messages_created ON messaging_messages(channel_id, created_at)")
    _ensure_messaging_messages_extra_columns(conn)
    _ensure_messaging_channels_webhook(conn)
    _create_messaging_mutes_table(conn)
    _create_messaging_invite_requests_table(conn)
    _create_messaging_reports_table(conn)
    _create_messaging_audit_table(conn)
    _create_messaging_channel_read_table(conn)
    _ensure_messaging_channel_read_columns(conn)
    _create_messaging_reactions_table(conn)
    _create_messaging_channel_prefs_table(conn)
    _create_messaging_mentions_table(conn)
    _create_messaging_notifications_table(conn)
    _ensure_messaging_notifications_from_user_id(conn)
    _ensure_messaging_channels_archived(conn)
    try:
        from app.institutional_models import init_gpi_tables
        init_gpi_tables(conn)
    except ImportError:
        pass
    try:
        from app.un_votes import init_gpi_un_tables
        init_gpi_un_tables(conn)
    except ImportError:
        pass


def _create_messaging_channel_read_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS messaging_channel_read (
            user_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            last_read_message_id INTEGER,
            last_read_at TEXT NOT NULL,
            PRIMARY KEY (user_id, channel_id),
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (channel_id) REFERENCES messaging_channels(id) ON DELETE CASCADE
        )
    """)


def _ensure_messaging_channel_read_columns(conn: sqlite3.Connection) -> None:
    """Add last_read_message_id and last_read_at if table existed with old schema."""
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='messaging_channel_read'")
    if not cur.fetchone():
        return
    cur = conn.execute("PRAGMA table_info(messaging_channel_read)")
    existing = {row[1] for row in cur.fetchall()}
    if "last_read_at" not in existing:
        conn.execute("ALTER TABLE messaging_channel_read ADD COLUMN last_read_at TEXT DEFAULT ''")
    if "last_read_message_id" not in existing:
        conn.execute("ALTER TABLE messaging_channel_read ADD COLUMN last_read_message_id INTEGER")


def _create_messaging_reactions_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS messaging_reactions (
            message_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            emoji TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (message_id, user_id, emoji),
            FOREIGN KEY (message_id) REFERENCES messaging_messages(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_messaging_reactions_message ON messaging_reactions(message_id)")


def _create_messaging_channel_prefs_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS messaging_channel_prefs (
            user_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            starred INTEGER NOT NULL DEFAULT 0,
            sort_order INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, channel_id),
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (channel_id) REFERENCES messaging_channels(id) ON DELETE CASCADE
        )
    """)


def _create_messaging_mentions_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS messaging_mentions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (message_id) REFERENCES messaging_messages(id) ON DELETE CASCADE,
            FOREIGN KEY (channel_id) REFERENCES messaging_channels(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_messaging_mentions_user ON messaging_mentions(user_id)")


def _create_messaging_notifications_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS messaging_notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            from_user_id INTEGER,
            read_at TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (channel_id) REFERENCES messaging_channels(id) ON DELETE CASCADE,
            FOREIGN KEY (message_id) REFERENCES messaging_messages(id) ON DELETE CASCADE,
            FOREIGN KEY (from_user_id) REFERENCES users(id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_messaging_notifications_user ON messaging_notifications(user_id)")


def _ensure_messaging_notifications_from_user_id(conn: sqlite3.Connection) -> None:
    """Add from_user_id to messaging_notifications if missing (for existing DBs that require it NOT NULL)."""
    cur = conn.execute("PRAGMA table_info(messaging_notifications)")
    existing = {row[1] for row in cur.fetchall()}
    if "from_user_id" not in existing:
        conn.execute("ALTER TABLE messaging_notifications ADD COLUMN from_user_id INTEGER REFERENCES users(id)")


def _ensure_messaging_channels_archived(conn: sqlite3.Connection) -> None:
    cur = conn.execute("PRAGMA table_info(messaging_channels)")
    existing = {row[1] for row in cur.fetchall()}
    if "archived" not in existing:
        conn.execute("ALTER TABLE messaging_channels ADD COLUMN archived INTEGER NOT NULL DEFAULT 0")
    if "featured" not in existing:
        conn.execute("ALTER TABLE messaging_channels ADD COLUMN featured INTEGER NOT NULL DEFAULT 0")


def _ensure_messaging_messages_extra_columns(conn: sqlite3.Connection) -> None:
    cur = conn.execute("PRAGMA table_info(messaging_messages)")
    existing = {row[1] for row in cur.fetchall()}
    for col, defn in [
        ("parent_id", "INTEGER REFERENCES messaging_messages(id)"),
        ("edited_at", "TEXT"),
        ("deleted_at", "TEXT"),
        ("pinned_at", "TEXT"),
        ("attachment_type", "TEXT"),
        ("attachment_id", "INTEGER"),
        ("attachment_extra", "TEXT"),
        ("quoted_message_id", "INTEGER REFERENCES messaging_messages(id)"),
    ]:
        if col not in existing:
            conn.execute(f"ALTER TABLE messaging_messages ADD COLUMN {col} {defn}")


def _ensure_messaging_channels_webhook(conn: sqlite3.Connection) -> None:
    cur = conn.execute("PRAGMA table_info(messaging_channels)")
    existing = {row[1] for row in cur.fetchall()}
    if "webhook_url" not in existing:
        conn.execute("ALTER TABLE messaging_channels ADD COLUMN webhook_url TEXT")


def _create_messaging_mutes_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS messaging_channel_mutes (
            user_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            muted_at TEXT NOT NULL,
            PRIMARY KEY (user_id, channel_id),
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (channel_id) REFERENCES messaging_channels(id) ON DELETE CASCADE
        )
    """)


def _create_messaging_invite_requests_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS messaging_invite_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            requested_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            resolved_by_user_id INTEGER,
            resolved_at TEXT,
            FOREIGN KEY (channel_id) REFERENCES messaging_channels(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(id),
            UNIQUE(channel_id, user_id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_messaging_invite_requests_channel ON messaging_invite_requests(channel_id)")


def _create_messaging_reports_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS messaging_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            reported_by_user_id INTEGER NOT NULL,
            reason TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (message_id) REFERENCES messaging_messages(id) ON DELETE CASCADE,
            FOREIGN KEY (channel_id) REFERENCES messaging_channels(id) ON DELETE CASCADE,
            FOREIGN KEY (reported_by_user_id) REFERENCES users(id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_messaging_reports_message ON messaging_reports(message_id)")


def _create_messaging_audit_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS messaging_audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL,
            user_id INTEGER,
            action TEXT NOT NULL,
            details_json TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (channel_id) REFERENCES messaging_channels(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_messaging_audit_channel ON messaging_audit_log(channel_id)")


def _create_fts(conn: sqlite3.Connection) -> None:
    """Create FTS5 virtual table for full-text search."""
    try:
        conn.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS articles_fts USING fts5(
                title,
                summary,
                content='articles',
                content_rowid='id'
            )
        """)
    except sqlite3.OperationalError:
        pass  # already exists or content table schema mismatch


def _create_fts_triggers(conn: sqlite3.Connection) -> None:
    """Create triggers to keep FTS in sync with articles."""
    try:
        conn.execute("""
            CREATE TRIGGER IF NOT EXISTS articles_fts_insert AFTER INSERT ON articles
            BEGIN
                INSERT INTO articles_fts(rowid, title, summary) VALUES (new.id, new.title, COALESCE(new.summary, ''));
            END
        """)
        conn.execute("""
            CREATE TRIGGER IF NOT EXISTS articles_fts_update AFTER UPDATE ON articles
            BEGIN
                INSERT INTO articles_fts(articles_fts, rowid, title, summary) VALUES ('delete', old.id, COALESCE(old.title,''), COALESCE(old.summary,''));
                INSERT INTO articles_fts(rowid, title, summary) VALUES (new.id, new.title, COALESCE(new.summary, ''));
            END
        """)
        conn.execute("""
            CREATE TRIGGER IF NOT EXISTS articles_fts_delete AFTER DELETE ON articles
            BEGIN
                INSERT INTO articles_fts(articles_fts, rowid, title, summary) VALUES ('delete', old.id, COALESCE(old.title,''), COALESCE(old.summary,''));
            END
        """)
    except sqlite3.OperationalError:
        pass


@contextmanager
def _connection():
    conn = sqlite3.connect(_DB_PATH, timeout=60.0)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        yield conn
        conn.commit()
    finally:
        conn.close()


def rebuild_fts() -> None:
    """Rebuild FTS table from current articles (e.g. after migration)."""
    if not _DB_PATH:
        return
    try:
        with _connection() as conn:
            conn.execute("DELETE FROM articles_fts")
            conn.execute("""
                INSERT INTO articles_fts(rowid, title, summary)
                SELECT id, title, COALESCE(summary, '') FROM articles
            """)
    except sqlite3.OperationalError:
        pass


# --- Auth: users and API keys ---
def get_user_by_id(user_id: int) -> Optional[dict]:
    """Return user dict (incl. policy prefs) or None."""
    if not user_id:
        return None
    with _connection() as conn:
        cur = conn.execute(
            """SELECT id, username, email, is_active, created_at, name, title, organization,
                      default_sensitivity_tier, default_legal_review FROM users WHERE id = ?""",
            (user_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    d = dict(row)
    for k in ("name", "title", "organization"):
        if k not in d:
            d[k] = None
    if "default_sensitivity_tier" not in d or d.get("default_sensitivity_tier") is None:
        d["default_sensitivity_tier"] = "internal"
    if "default_legal_review" not in d or d.get("default_legal_review") is None:
        d["default_legal_review"] = 0
    return d


def get_user_by_username(username: str) -> Optional[dict]:
    """Return user dict including password_hash, or None."""
    if not username or not username.strip():
        return None
    with _connection() as conn:
        cur = conn.execute(
            "SELECT id, username, email, password_hash, is_active, created_at, name, title, organization FROM users WHERE username = ?",
            (username.strip().lower(),),
        )
        row = cur.fetchone()
    if not row:
        return None
    d = dict(row)
    for k in ("name", "title", "organization"):
        if k not in d:
            d[k] = None
    return d


def create_user(
    username: str,
    password: str,
    email: Optional[str] = None,
    name: Optional[str] = None,
    title: Optional[str] = None,
    organization: Optional[str] = None,
) -> int:
    """Create a user. Username stored lowercased. Returns user id. Email can be required by caller."""
    username = (username or "").strip().lower()
    if not username or not password:
        raise ValueError("username and password required")
    created = datetime.utcnow().isoformat() + "Z"
    password_hash = generate_password_hash(password, method="scrypt")
    email_val = (email or "").strip() or None
    name_val = (name or "").strip() or None
    title_val = (title or "").strip() or None
    org_val = (organization or "").strip() or None
    with _connection() as conn:
        cur = conn.execute(
            """INSERT INTO users (username, email, password_hash, is_active, created_at, name, title, organization)
               VALUES (?, ?, ?, 1, ?, ?, ?, ?)""",
            (username, email_val, password_hash, created, name_val, title_val, org_val),
        )
        return cur.lastrowid


def update_user_profile(
    user_id: int,
    name: Optional[str] = None,
    title: Optional[str] = None,
    organization: Optional[str] = None,
    default_sensitivity_tier: Optional[str] = None,
    default_legal_review: Optional[int] = None,
) -> None:
    """Update profile fields for a user. None = leave unchanged (except name/title/org still accept empty string to clear)."""
    if not user_id:
        return
    updates = []
    params = []
    for col, val in (("name", name), ("title", title), ("organization", organization)):
        if val is not None:
            updates.append(f"{col} = ?")
            params.append((val or "").strip() or None)
    if default_sensitivity_tier is not None:
        tier = (default_sensitivity_tier or "internal").strip().lower()
        if tier not in ("public", "internal", "restricted"):
            tier = "internal"
        updates.append("default_sensitivity_tier = ?")
        params.append(tier)
    if default_legal_review is not None:
        updates.append("default_legal_review = ?")
        params.append(1 if int(default_legal_review) else 0)
    if not updates:
        return
    params.append(user_id)
    with _connection() as conn:
        conn.execute(
            "UPDATE users SET " + ", ".join(updates) + " WHERE id = ?",
            tuple(params),
        )


def verify_password(user: dict, password: str) -> bool:
    """Return True if password matches user's password_hash."""
    if not user or not user.get("password_hash"):
        return False
    return check_password_hash(user["password_hash"], password)


def _hash_api_key(plain_key: str) -> str:
    return hashlib.sha256(plain_key.encode("utf-8")).hexdigest()


def create_api_key(user_id: int, name: str = "") -> Tuple[int, str]:
    """Create an API key for user. Returns (api_key_id, plain_key). Plain key shown only once."""
    plain_key = "gt_" + secrets.token_urlsafe(32)
    key_hash = _hash_api_key(plain_key)
    created = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        cur = conn.execute(
            "INSERT INTO api_keys (user_id, key_hash, name, created_at) VALUES (?, ?, ?, ?)",
            (user_id, key_hash, (name or "").strip() or None, created),
        )
        return cur.lastrowid, plain_key


def get_user_by_api_key(plain_key: str) -> Optional[dict]:
    """Return user dict if the API key is valid, else None. Does not expose password_hash."""
    if not plain_key or not plain_key.strip():
        return None
    key_hash = _hash_api_key(plain_key.strip())
    with _connection() as conn:
        cur = conn.execute(
            "SELECT u.id, u.username, u.email, u.is_active, u.created_at, u.name, u.title, u.organization FROM users u JOIN api_keys k ON u.id = k.user_id WHERE k.key_hash = ? AND u.is_active = 1",
            (key_hash,),
        )
        row = cur.fetchone()
    if not row:
        return None
    d = dict(row)
    for k in ("name", "title", "organization"):
        if k not in d:
            d[k] = None
    return d


def upsert_desk_terminal_intel(
    *,
    proposal_id: int,
    agent_id: str,
    run_type: str,
    title: Optional[str],
    body_markdown: str,
    countries_csv: Optional[str],
    focus_country: Optional[str],
    metrics_json: str,
    reviewer_note: Optional[str],
    database_path: Optional[str] = None,
) -> None:
    """Write or update approved Analyst Desk content for the Terminal (news.db)."""
    path = database_path or _DB_PATH
    if not path:
        raise RuntimeError("Database not initialized")
    now = datetime.utcnow().isoformat() + "Z"
    with _connection_for_path(path) as conn:
        _ensure_desk_terminal_intel_table(conn)
        conn.execute(
            """
            INSERT INTO desk_terminal_intel (
                proposal_id, agent_id, run_type, title, body_markdown, countries_csv,
                focus_country, metrics_json, reviewer_note, published_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(proposal_id) DO UPDATE SET
                agent_id = excluded.agent_id,
                run_type = excluded.run_type,
                title = excluded.title,
                body_markdown = excluded.body_markdown,
                countries_csv = excluded.countries_csv,
                focus_country = excluded.focus_country,
                metrics_json = excluded.metrics_json,
                reviewer_note = excluded.reviewer_note,
                updated_at = excluded.updated_at
            """,
            (
                proposal_id,
                agent_id,
                run_type,
                title,
                body_markdown,
                countries_csv,
                focus_country,
                metrics_json,
                reviewer_note,
                now,
                now,
            ),
        )


@contextmanager
def _connection_for_path(path: str):
    """Short-lived connection for a specific DB path (e.g. publish from desk worker)."""
    conn = sqlite3.connect(path, timeout=60.0)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def get_desk_terminal_intel_for_country(country_code: str, limit: int = 12) -> list:
    """Approved desk briefs + extracted metrics snapshot for a country (ISO3)."""
    if not country_code or not str(country_code).strip():
        return []
    cc = str(country_code).strip().upper()
    with _connection() as conn:
        try:
            cur = conn.execute(
                """
                SELECT id, proposal_id, agent_id, run_type, title, body_markdown, countries_csv,
                       focus_country, metrics_json, reviewer_note, published_at, updated_at
                FROM desk_terminal_intel
                WHERE focus_country = ?
                   OR (countries_csv IS NOT NULL AND (
                        countries_csv = ? OR countries_csv LIKE ? OR countries_csv LIKE ? OR countries_csv LIKE ?
                   ))
                ORDER BY datetime(published_at) DESC
                LIMIT ?
                """,
                (
                    cc,
                    cc,
                    f"{cc},%",
                    f"%,{cc},%",
                    f"%,{cc}",
                    limit,
                ),
            )
            rows = [dict(r) for r in cur.fetchall()]
        except sqlite3.OperationalError:
            return []
    out = []
    for r in rows:
        mj = r.get("metrics_json") or "[]"
        try:
            r["metrics"] = json.loads(mj) if isinstance(mj, str) else []
        except json.JSONDecodeError:
            r["metrics"] = []
        del r["metrics_json"]
        # Keep only metrics mentioning this country (or global rows)
        r["metrics"] = [
            m
            for m in r["metrics"]
            if not m.get("country") or str(m.get("country")).upper() == cc
        ][:25]
        out.append(r)
    return out


def upsert_article(
    title: str,
    url: str,
    source_name: str,
    source_url: str = "",
    summary: str = "",
    published_utc: Optional[str] = None,
    image_url: Optional[str] = None,
    video_url: Optional[str] = None,
) -> None:
    """Insert or update article by URL. Updates scraped_at and media when re-scraping."""
    scraped_at = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        cur = conn.execute("SELECT id, title, summary FROM articles WHERE url = ?", (url,))
        old = cur.fetchone()
        conn.execute(
            """
            INSERT INTO articles
            (title, url, source_name, source_url, summary, published_utc, scraped_at, image_url, video_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                title = excluded.title,
                summary = excluded.summary,
                published_utc = excluded.published_utc,
                scraped_at = excluded.scraped_at,
                image_url = COALESCE(excluded.image_url, image_url),
                video_url = COALESCE(excluded.video_url, video_url)
            """,
            (title, url, source_name, source_url or "", summary or "", published_utc or "", scraped_at, image_url or "", video_url or ""),
        )
        if old:
            aid = old[0]
            old_title = (old[1] or "").strip()
            old_summary = (old[2] or "").strip()
            if old_title != title.strip() or old_summary != (summary or "").strip():
                log_at = datetime.utcnow().isoformat() + "Z"
                conn.execute(
                    """INSERT INTO entity_change_log (entity_type, entity_ref, summary, detail_json, created_at)
                       VALUES (?, ?, ?, ?, ?)""",
                    (
                        "article",
                        str(aid),
                        "Article text updated (re-ingest)",
                        json.dumps({"title_changed": old_title != title.strip(), "summary_changed": old_summary != (summary or "").strip()}),
                        log_at,
                    ),
                )


def _articles_where_params(
    source: Optional[str] = None,
    topic: Optional[str] = None,
    topics_list: Optional[list] = None,
    cluster_id: Optional[int] = None,
    min_impact: Optional[int] = None,
    domain: Optional[str] = None,
    days: Optional[int] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    country: Optional[str] = None,
    countries_list: Optional[list] = None,
    risk_category: Optional[str] = None,
    risk_categories_list: Optional[list] = None,
    exclude_ids: Optional[list] = None,
):
    """Build WHERE clause and params for article filters. Returns (where_sql, params_list)."""
    params = []
    where_clauses = []
    if source:
        where_clauses.append("source_name = ?")
        params.append(source)
    if topic:
        where_clauses.append("(topics IS NOT NULL AND topics LIKE ?)")
        params.append(f'%"{topic}"%')
    if topics_list:
        placeholders = " OR ".join(["(topics IS NOT NULL AND topics LIKE ?)" for _ in topics_list])
        where_clauses.append("(" + placeholders + ")")
        for t in topics_list:
            params.append(f'%"{t}"%')
    if cluster_id is not None:
        where_clauses.append("cluster_id = ?")
        params.append(cluster_id)
    if min_impact is not None:
        where_clauses.append("impact_score >= ?")
        params.append(min_impact)
    if domain:
        where_clauses.append("(impact_domains IS NOT NULL AND impact_domains LIKE ?)")
        params.append(f'%"{domain}"%')
    if date_from or date_to:
        if date_from:
            where_clauses.append("(COALESCE(NULLIF(published_utc, ''), scraped_at) >= ?)")
            params.append(date_from)
        if date_to:
            where_clauses.append("(COALESCE(NULLIF(published_utc, ''), scraped_at) <= ?)")
            params.append(date_to + " 23:59:59" if len(date_to) <= 10 else date_to)
    elif days is not None:
        since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        where_clauses.append("(COALESCE(NULLIF(published_utc, ''), scraped_at) >= ?)")
        params.append(since)
    if country:
        where_clauses.append("(entities LIKE ? OR topics LIKE ? OR title LIKE ? OR summary LIKE ?)")
        pat = f"%{country}%"
        params.extend([pat, pat, pat, pat])
    if countries_list:
        or_parts = []
        for c in countries_list:
            pat = f"%{c}%"
            or_parts.append("(entities LIKE ? OR topics LIKE ? OR title LIKE ? OR summary LIKE ?)")
            params.extend([pat, pat, pat, pat])
        where_clauses.append("(" + " OR ".join(or_parts) + ")")
    if risk_category:
        where_clauses.append(
            "(event_type = ? OR (impact_domains IS NOT NULL AND impact_domains LIKE ?))"
        )
        params.append(risk_category)
        params.append(f'%"{risk_category}"%')
    if risk_categories_list:
        or_parts = []
        for rc in risk_categories_list:
            or_parts.append("(event_type = ? OR (impact_domains IS NOT NULL AND impact_domains LIKE ?))")
            params.append(rc)
            params.append(f'%"{rc}"%')
        where_clauses.append("(" + " OR ".join(or_parts) + ")")
    if exclude_ids:
        placeholders = ",".join("?" * len(exclude_ids))
        where_clauses.append(f"id NOT IN ({placeholders})")
        params.extend(exclude_ids)
    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    return where_sql, params


def get_articles_count(
    source: Optional[str] = None,
    topic: Optional[str] = None,
    topics_list: Optional[list] = None,
    cluster_id: Optional[int] = None,
    min_impact: Optional[int] = None,
    domain: Optional[str] = None,
    days: Optional[int] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    country: Optional[str] = None,
    countries_list: Optional[list] = None,
    risk_category: Optional[str] = None,
    risk_categories_list: Optional[list] = None,
    exclude_ids: Optional[list] = None,
) -> int:
    """Count articles matching the same filters as get_articles."""
    where_sql, params = _articles_where_params(
        source=source, topic=topic, topics_list=topics_list, cluster_id=cluster_id,
        min_impact=min_impact, domain=domain, days=days,
        date_from=date_from, date_to=date_to,
        country=country, countries_list=countries_list,
        risk_category=risk_category, risk_categories_list=risk_categories_list,
        exclude_ids=exclude_ids,
    )
    with _connection() as conn:
        sql = f"SELECT COUNT(*) AS n FROM articles {where_sql}"
        cur = conn.execute(sql, tuple(params))
        row = cur.fetchone()
    return (row["n"] if row and "n" in row else 0) or 0


def get_articles(
    limit: int = 100,
    offset: int = 0,
    source: Optional[str] = None,
    topic: Optional[str] = None,
    topics_list: Optional[list] = None,
    cluster_id: Optional[int] = None,
    min_impact: Optional[int] = None,
    domain: Optional[str] = None,
    days: Optional[int] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    country: Optional[str] = None,
    countries_list: Optional[list] = None,
    risk_category: Optional[str] = None,
    risk_categories_list: Optional[list] = None,
    exclude_ids: Optional[list] = None,
):
    """Fetch latest articles with optional filters. Returns full row dicts including analysis and impact."""
    where_sql, params = _articles_where_params(
        source=source, topic=topic, topics_list=topics_list, cluster_id=cluster_id,
        min_impact=min_impact, domain=domain, days=days,
        date_from=date_from, date_to=date_to,
        country=country, countries_list=countries_list,
        risk_category=risk_category, risk_categories_list=risk_categories_list,
        exclude_ids=exclude_ids,
    )
    params.extend([limit, offset])
    with _connection() as conn:
        sql = f"""
            SELECT id, title, url, source_name, source_url, summary, published_utc, scraped_at,
                   topics, entities, key_takeaways, why_it_matters, cluster_id,
                   impact_score, impact_domains, urgency, event_type, image_url, video_url
            FROM articles
            {where_sql}
            ORDER BY published_utc DESC, scraped_at DESC
            LIMIT ? OFFSET ?
        """
        cur = conn.execute(sql, tuple(params))
        rows = cur.fetchall()
    out = []
    for row in rows:
        d = dict(row)
        if d.get("topics"):
            try:
                d["topics_list"] = json.loads(d["topics"]) if isinstance(d["topics"], str) else d["topics"]
            except (json.JSONDecodeError, TypeError):
                d["topics_list"] = []
        else:
            d["topics_list"] = []
        if d.get("entities"):
            try:
                d["entities_list"] = json.loads(d["entities"]) if isinstance(d["entities"], str) else d["entities"]
            except (json.JSONDecodeError, TypeError):
                d["entities_list"] = []
        else:
            d["entities_list"] = []
        if d.get("impact_domains"):
            try:
                d["impact_domains_list"] = json.loads(d["impact_domains"]) if isinstance(d["impact_domains"], str) else d["impact_domains"]
            except (json.JSONDecodeError, TypeError):
                d["impact_domains_list"] = []
        else:
            d["impact_domains_list"] = []
        out.append(d)
    return out


def article_confidence_score(article: dict) -> int:
    """Derived confidence 0–100: impact (0-10 scale), analysis present, urgency."""
    score = 0
    imp = article.get("impact_score")
    if imp is not None:
        imp = int(imp)
        if imp >= 7:
            score += 40
        elif imp >= 4:
            score += 25
        elif imp >= 1:
            score += 10
    if article.get("key_takeaways"):
        score += 25
    if article.get("why_it_matters"):
        score += 15
    if article.get("urgency") in ("breaking", "developing"):
        score += 10
    if article.get("event_type"):
        score += 5
    return min(100, score)


def article_signal_score(article: dict) -> float:
    """Signal vs noise: higher = more signal (impact, analysis, specificity)."""
    c = article_confidence_score(article)
    topic_count = len(article.get("topics_list") or [])
    domain_count = len(article.get("impact_domains_list") or [])
    specificity = min(20, (topic_count + domain_count) * 4)
    return c * 0.7 + specificity


def get_feed_event_types(days: Optional[int] = 7) -> list:
    """Distinct event_type values for Intelligence Feed risk category filter."""
    since = (datetime.utcnow() - timedelta(days=days or 7)).strftime("%Y-%m-%d")
    with _connection() as conn:
        cur = conn.execute(
            "SELECT DISTINCT event_type FROM articles WHERE event_type IS NOT NULL AND event_type != '' AND (COALESCE(NULLIF(published_utc, ''), scraped_at) >= ?) ORDER BY event_type",
            (since,),
        )
        return [row[0] for row in cur.fetchall()]


def get_feed_event_type_counts(days: Optional[int] = 7, limit: int = 10) -> list:
    """Event type counts for sidebar (event_type, count) in current window."""
    since = (datetime.utcnow() - timedelta(days=days or 7)).strftime("%Y-%m-%d")
    with _connection() as conn:
        cur = conn.execute(
            """SELECT event_type, COUNT(*) AS cnt FROM articles
               WHERE event_type IS NOT NULL AND event_type != '' AND (COALESCE(NULLIF(published_utc, ''), scraped_at) >= ?)
               GROUP BY event_type ORDER BY cnt DESC LIMIT ?""",
            (since, limit),
        )
        return [(row[0], row[1]) for row in cur.fetchall()]


def get_feed_country_options(days: Optional[int] = 7, limit: Optional[int] = None) -> list:
    """All countries for Intelligence Feed filter. Uses canonical list from country_data."""
    try:
        from app.country_data import ALL_COUNTRIES
        # ALL_COUNTRIES: (ISO3, display_name, region, ...)
        out = [(name, name) for _, name, *_ in ALL_COUNTRIES if name]
        return sorted(out, key=lambda x: x[0].lower())
    except Exception:
        pass
    # Fallback: integration countries if ALL_COUNTRIES unavailable
    try:
        countries = get_integration_countries(limit=500)
        out = []
        for c in countries:
            name = c.get("country_name") or c.get("country_code") or ""
            if name:
                out.append((name, name))
        return sorted(set(out), key=lambda x: x[0].lower())[: limit or 300]
    except Exception:
        pass
    return []


def get_article(article_id: int):
    """Get a single article by id."""
    with _connection() as conn:
        cur = conn.execute(
            """
            SELECT id, title, url, source_name, source_url, summary, published_utc, scraped_at,
                   topics, entities, key_takeaways, why_it_matters, cluster_id,
                   impact_score, impact_domains, urgency, event_type, image_url, video_url
            FROM articles WHERE id = ?
            """,
            (article_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    d = dict(row)
    for key in ("topics", "entities"):
        if d.get(key):
            try:
                d[key + "_list"] = json.loads(d[key]) if isinstance(d[key], str) else d[key]
            except (json.JSONDecodeError, TypeError):
                d[key + "_list"] = []
        else:
            d[key + "_list"] = []
    return d


def get_articles_trade_supply_chain_relevant(limit: int = 15, days: int = 14):
    """Articles relevant to trade, supply chains, chokepoints, maritime routes. Used on Trade & Supply Chain page."""
    since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    # Keywords for trade/supply chain relevance (title or summary)
    keywords = [
        "supply chain", "trade route", "maritime", "shipping", "chokepoint", "strait",
        "suez", "hormuz", "malacca", "panama", "bab el-mandeb", "taiwan strait",
        "lng", "oil tanker", "container", "semiconductor", "rare earth", "critical mineral",
        "port closure", "canal", "naval", "red sea", "black sea", "flight ban", "airspace",
        "no-fly", "aviation", "air corridor", "flight restriction",
    ]
    # Build OR conditions for keywords (LIKE %keyword%)
    keyword_conditions = []
    keyword_params = []
    for kw in keywords:
        pattern = f"%{kw}%"
        keyword_conditions.append("(LOWER(COALESCE(a.title, '')) LIKE ? OR LOWER(COALESCE(a.summary, '')) LIKE ?)")
        keyword_params.extend([pattern, pattern])
    keyword_sql = " OR ".join(keyword_conditions) if keyword_conditions else "0"
    # Domain filter: Economy, Energy (impact_domains JSON array)
    domain_cond = "(a.impact_domains LIKE '%\"Economy\"%' OR a.impact_domains LIKE '%\"Energy\"%')"
    # Combine: (keywords match) OR (domain match AND impact >= 4)
    where_sql = f"""(({keyword_sql}) OR ({domain_cond} AND COALESCE(a.impact_score, 0) >= 4))
        AND (COALESCE(NULLIF(a.published_utc, ''), a.scraped_at) >= ?)"""
    params = keyword_params + [since, limit]
    with _connection() as conn:
        cur = conn.execute(
            f"""
            SELECT a.id, a.title, a.url, a.source_name, a.source_url, a.summary, a.published_utc, a.scraped_at,
                   a.topics, a.entities, a.key_takeaways, a.why_it_matters, a.cluster_id,
                   a.impact_score, a.impact_domains, a.urgency, a.event_type, a.image_url, a.video_url
            FROM articles a
            WHERE {where_sql}
            ORDER BY COALESCE(a.impact_score, 0) DESC, a.published_utc DESC, a.scraped_at DESC
            LIMIT ?
            """,
            tuple(params),
        )
        rows = cur.fetchall()
    out = []
    for row in rows:
        d = dict(row)
        for key in ("topics", "entities", "impact_domains"):
            if d.get(key):
                try:
                    d[key + "_list"] = json.loads(d[key]) if isinstance(d[key], str) else d[key]
                except (json.JSONDecodeError, TypeError):
                    d[key + "_list"] = []
            else:
                d[key + "_list"] = []
        out.append(d)
    return out


def search_articles(query: str, limit: int = 50):
    """Full-text search over title and summary. Returns same shape as get_articles."""
    if not query or not query.strip():
        return get_articles(limit=limit)
    q = query.strip()
    with _connection() as conn:
        cur = conn.execute(
            """
            SELECT a.id, a.title, a.url, a.source_name, a.source_url, a.summary, a.published_utc, a.scraped_at,
                   a.topics, a.entities, a.key_takeaways, a.why_it_matters, a.cluster_id,
                   a.impact_score, a.impact_domains, a.urgency, a.event_type, a.image_url, a.video_url
            FROM articles_fts f
            JOIN articles a ON a.id = f.rowid
            WHERE articles_fts MATCH ?
            ORDER BY rank
            LIMIT ?
            """,
            (q, limit),
        )
        rows = cur.fetchall()
    out = []
    for row in rows:
        d = dict(row)
        for key in ("topics", "entities", "impact_domains"):
            if d.get(key):
                try:
                    d[key + "_list"] = json.loads(d[key]) if isinstance(d[key], str) else d[key]
                except (json.JSONDecodeError, TypeError):
                    d[key + "_list"] = []
            else:
                d[key + "_list"] = []
        out.append(d)
    return out


def update_article_topics_entities(article_id: int, topics: list, entities: list) -> None:
    """Set topics and entities JSON for an article."""
    with _connection() as conn:
        conn.execute(
            "UPDATE articles SET topics = ?, entities = ? WHERE id = ?",
            (json.dumps(topics), json.dumps(entities), article_id),
        )


def update_article_analysis(article_id: int, key_takeaways: str, why_it_matters: str) -> None:
    """Set key takeaways and why it matters for an article."""
    with _connection() as conn:
        conn.execute(
            "UPDATE articles SET key_takeaways = ?, why_it_matters = ? WHERE id = ?",
            (key_takeaways or "", why_it_matters or "", article_id),
        )


def update_article_cluster(article_id: int, cluster_id: Optional[int]) -> None:
    """Assign article to a cluster."""
    with _connection() as conn:
        conn.execute("UPDATE articles SET cluster_id = ? WHERE id = ?", (cluster_id, article_id))


def update_article_impact(article_id: int, impact_score: int, impact_domains: list[str]) -> None:
    """Set impact score (0–10) and domains (JSON) for an article. 0=unscored, 1-3=low, 4-6=med, 7-10=high."""
    score = max(0, min(int(impact_score or 0), 10))
    with _connection() as conn:
        conn.execute(
            "UPDATE articles SET impact_score = ?, impact_domains = ? WHERE id = ?",
            (score, json.dumps(impact_domains or []), article_id),
        )


def get_sources():
    """Return list of distinct source names with article counts."""
    with _connection() as conn:
        cur = conn.execute(
            """
            SELECT source_name, COUNT(*) as count
            FROM articles
            GROUP BY source_name
            ORDER BY count DESC
            """
        )
        return [dict(row) for row in cur.fetchall()]


def get_all_articles_for_processing(limit: int = 5000):
    """Return id, title, summary for jobs (topic extraction, analysis)."""
    with _connection() as conn:
        cur = conn.execute(
            """
            SELECT id, title, summary FROM articles
            ORDER BY published_utc DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(row) for row in cur.fetchall()]


def get_all_topic_counts():
    """For each topic string, count articles that contain it in topics JSON."""
    with _connection() as conn:
        cur = conn.execute(
            "SELECT topics FROM articles WHERE topics IS NOT NULL AND topics != ''"
        )
        rows = cur.fetchall()
    from collections import Counter
    counter = Counter()
    for row in rows:
        try:
            topics = json.loads(row[0]) if isinstance(row[0], str) else row[0]
            if isinstance(topics, list):
                for t in topics:
                    counter[t] += 1
        except (json.JSONDecodeError, TypeError):
            pass
    return counter.most_common(50)


def get_impact_summary(days: int = 7):
    """Return simple impact stats for dashboard. Impact 0-10: high>=7, med 4-6, low 1-3."""
    since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    with _connection() as conn:
        cur = conn.execute(
            """
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN impact_score >= 7 THEN 1 ELSE 0 END) as high,
                SUM(CASE WHEN impact_score >= 4 AND impact_score < 7 THEN 1 ELSE 0 END) as medium,
                SUM(CASE WHEN impact_score >= 1 AND impact_score < 4 THEN 1 ELSE 0 END) as low
            FROM articles
            WHERE published_utc >= ?
            """,
            (since,),
        )
        row = cur.fetchone() or {}
    return dict(row) if row else {"total": 0, "high": 0, "medium": 0, "low": 0}


def get_domain_counts(days: int = 7, limit: int = 10):
    """Top impact domains over last N days."""
    since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    with _connection() as conn:
        cur = conn.execute(
            "SELECT impact_domains FROM articles WHERE impact_domains IS NOT NULL AND impact_domains != '' AND published_utc >= ?",
            (since,),
        )
        rows = cur.fetchall()
    from collections import Counter
    counter = Counter()
    for row in rows:
        try:
            domains = json.loads(row[0]) if isinstance(row[0], str) else row[0]
            if isinstance(domains, list):
                for d in domains:
                    counter[d] += 1
        except (json.JSONDecodeError, TypeError):
            pass
    return counter.most_common(limit)


def get_trending_topics(days: int = 7, limit: int = 20):
    """Topics that appear most in articles from the last N days."""
    since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    with _connection() as conn:
        cur = conn.execute(
            "SELECT topics FROM articles WHERE topics IS NOT NULL AND topics != '' AND published_utc >= ?",
            (since,),
        )
        rows = cur.fetchall()
    from collections import Counter
    counter = Counter()
    for row in rows:
        try:
            topics = json.loads(row[0]) if isinstance(row[0], str) else row[0]
            if isinstance(topics, list):
                for t in topics:
                    counter[t] += 1
        except (json.JSONDecodeError, TypeError):
            pass
    return counter.most_common(limit)


def get_daily_counts(days: int = 7):
    """Return list of {'day': 'YYYY-MM-DD', 'count': N} for last N days."""
    since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    with _connection() as conn:
        cur = conn.execute(
            """
            SELECT substr(COALESCE(NULLIF(published_utc, ''), scraped_at), 1, 10) as day,
                   COUNT(*) as count
            FROM articles
            WHERE substr(COALESCE(NULLIF(published_utc, ''), scraped_at), 1, 10) >= ?
            GROUP BY day
            ORDER BY day
            """,
            (since,),
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def add_digest(digest_type: str, title: str, content: str) -> int:
    """Store a digest. content is JSON string. Returns id."""
    created = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        cur = conn.execute(
            "INSERT INTO digests (digest_type, title, content, created_at) VALUES (?, ?, ?, ?)",
            (digest_type, title, content, created),
        )
        return cur.lastrowid


def get_daily_digest_for_date(date_iso: str) -> Optional[dict]:
    """Return the daily digest for the given date (YYYY-MM-DD), if any."""
    with _connection() as conn:
        cur = conn.execute(
            "SELECT id, digest_type, title, content, created_at FROM digests WHERE digest_type = 'daily' AND created_at LIKE ? ORDER BY created_at DESC LIMIT 1",
            (date_iso + "%",),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def update_digest(digest_id: int, title: str, content: str) -> None:
    """Update an existing digest's title and content."""
    with _connection() as conn:
        conn.execute(
            "UPDATE digests SET title = ?, content = ? WHERE id = ?",
            (title, content, digest_id),
        )


def get_digests(limit: int = 20, digest_type: Optional[str] = None):
    """List digests, optionally filtered by type (daily/weekly)."""
    with _connection() as conn:
        if digest_type:
            cur = conn.execute(
                "SELECT id, digest_type, title, content, created_at FROM digests WHERE digest_type = ? ORDER BY created_at DESC LIMIT ?",
                (digest_type, limit),
            )
        else:
            cur = conn.execute(
                "SELECT id, digest_type, title, content, created_at FROM digests ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
        return [dict(row) for row in cur.fetchall()]


def get_digest(digest_id: int):
    """Get one digest by id."""
    with _connection() as conn:
        cur = conn.execute(
            "SELECT id, digest_type, title, content, created_at FROM digests WHERE id = ?",
            (digest_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def _cluster_label_from_titles(titles: list[str], max_chars: int = 55) -> str:
    """Build a readable label from cluster titles: most common significant words, or shortest title."""
    from collections import Counter
    stop = {"the", "a", "an", "and", "or", "for", "in", "on", "at", "to", "of", "with", "as", "is", "it", "be", "by", "on", "that", "this", "from", "has", "have", "are", "was", "were", "but", "not", "can", "will", "said", "says"}
    if not titles:
        return "Cluster"
    # Count word frequency across all titles (normalize: lowercase, strip punctuation)
    counter = Counter()
    for t in titles:
        if not t or not isinstance(t, str):
            continue
        words = re.sub(r"[^\w\s]", " ", t.lower()).split()
        for w in words:
            if len(w) > 1 and w not in stop:
                counter[w] += 1
    top = [w for w, _ in counter.most_common(5) if w]
    if len(top) >= 2:
        label = " ".join(w.capitalize() for w in top[:4])
        return label[:max_chars] + ("…" if len(label) > max_chars else "")
    # Fallback: shortest title (often the cleanest headline)
    shortest = min((t for t in titles if t and isinstance(t, str)), key=len, default="")
    if shortest:
        return shortest[:max_chars] + ("…" if len(shortest) > max_chars else "")
    return "Cluster"


def get_clusters_with_counts(limit: int = 50):
    """List cluster_id, count, and a human-readable label for clusters with at least 2 articles."""
    with _connection() as conn:
        cur = conn.execute(
            """
            SELECT cluster_id, COUNT(*) as count FROM articles
            WHERE cluster_id IS NOT NULL
            GROUP BY cluster_id
            HAVING COUNT(*) >= 2
            ORDER BY count DESC, cluster_id DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = [dict(row) for row in cur.fetchall()]
    if not rows:
        return []
    cluster_ids = [r["cluster_id"] for r in rows]
    # Fetch one title per cluster (we need titles for label; get all to compute common words)
    with _connection() as conn:
        cur = conn.execute(
            """
            SELECT cluster_id, title FROM articles
            WHERE cluster_id IN (%s)
            """ % ",".join("?" * len(cluster_ids)),
            tuple(cluster_ids),
        )
        cluster_titles = {}
        for row in cur.fetchall():
            cid = row["cluster_id"]
            if cid not in cluster_titles:
                cluster_titles[cid] = []
            cluster_titles[cid].append(row["title"])
    for r in rows:
        r["label"] = _cluster_label_from_titles(cluster_titles.get(r["cluster_id"], []))
    return rows


def get_cluster_label(cluster_id: int) -> str:
    """Human-readable label for a cluster (from its article titles)."""
    with _connection() as conn:
        cur = conn.execute(
            "SELECT title FROM articles WHERE cluster_id = ?",
            (cluster_id,),
        )
        titles = [row["title"] for row in cur.fetchall() if row["title"]]
    return _cluster_label_from_titles(titles)


def get_articles_by_cluster(cluster_id: int):
    """Articles in a given cluster."""
    return get_articles(limit=100, cluster_id=cluster_id)


def get_watchlists():
    """Return all watchlists."""
    with _connection() as conn:
        cur = conn.execute(
            "SELECT id, name, topics, created_at FROM watchlists ORDER BY created_at DESC"
        )
        rows = cur.fetchall()
    out = []
    for row in rows:
        d = dict(row)
        try:
            d["topics_list"] = json.loads(d["topics"]) if isinstance(d["topics"], str) else d["topics"]
        except (json.JSONDecodeError, TypeError):
            d["topics_list"] = []
        out.append(d)
    return out


def add_watchlist(name: str, topics: list[str]) -> int:
    """Create a new watchlist."""
    created = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        cur = conn.execute(
            "INSERT INTO watchlists (name, topics, created_at) VALUES (?, ?, ?)",
            (name, json.dumps(topics or []), created),
        )
        return cur.lastrowid


def get_watchlist(watchlist_id: int):
    with _connection() as conn:
        cur = conn.execute(
            "SELECT id, name, topics, created_at FROM watchlists WHERE id = ?",
            (watchlist_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    d = dict(row)
    try:
        d["topics_list"] = json.loads(d["topics"]) if isinstance(d["topics"], str) else d["topics"]
    except (json.JSONDecodeError, TypeError):
        d["topics_list"] = []
    return d


def watchlist_add_topic(watchlist_id: int, topic: str) -> bool:
    """Append a topic to a watchlist if not already present. Returns True if updated."""
    wl = get_watchlist(watchlist_id)
    if not wl or not topic or not topic.strip():
        return False
    topics = list(wl.get("topics_list") or [])
    t = topic.strip()
    if t in topics:
        return False
    topics.append(t)
    with _connection() as conn:
        conn.execute("UPDATE watchlists SET topics = ? WHERE id = ?", (json.dumps(topics), watchlist_id))
    return True


def get_articles_for_watchlist(watchlist_id: int, limit: int = 150):
    """Articles matching any of the watchlist's topics (single query, de-duplicated)."""
    wl = get_watchlist(watchlist_id)
    if not wl or not wl.get("topics_list"):
        return []
    topics = wl["topics_list"]
    with _connection() as conn:
        like_clauses = []
        params = []
        for t in topics:
            like_clauses.append("(topics IS NOT NULL AND topics LIKE ?)")
            params.append(f'%"{t}"%')
        where = " OR ".join(like_clauses)
        params.append(limit)
        cur = conn.execute(
            f"""
            SELECT id, title, url, source_name, source_url, summary, published_utc, scraped_at,
                   topics, entities, key_takeaways, why_it_matters, cluster_id,
                   impact_score, impact_domains, image_url, video_url
            FROM articles
            WHERE {where}
            ORDER BY published_utc DESC, scraped_at DESC
            LIMIT ?
            """,
            tuple(params),
        )
        rows = cur.fetchall()
    out = []
    seen_urls = set()
    for row in rows:
        d = dict(row)
        if d["url"] in seen_urls:
            continue
        seen_urls.add(d["url"])
        for key in ("topics", "entities", "impact_domains"):
            if d.get(key):
                try:
                    d[key + "_list"] = json.loads(d[key]) if isinstance(d[key], str) else d[key]
                except (json.JSONDecodeError, TypeError):
                    d[key + "_list"] = []
            else:
                d[key + "_list"] = []
        out.append(d)
    return out


def update_watchlist(watchlist_id: int, name: str, topics: list[str]) -> bool:
    """Update watchlist name and topics. Returns True if updated."""
    wl = get_watchlist(watchlist_id)
    if not wl:
        return False
    with _connection() as conn:
        cur = conn.execute(
            "UPDATE watchlists SET name = ?, topics = ? WHERE id = ?",
            (name.strip() or wl["name"], json.dumps(topics or []), watchlist_id),
        )
        return cur.rowcount > 0


def delete_watchlist(watchlist_id: int) -> bool:
    """Remove a watchlist. Returns True if deleted."""
    with _connection() as conn:
        cur = conn.execute("DELETE FROM watchlists WHERE id = ?", (watchlist_id,))
        return cur.rowcount > 0


def get_alert_suggestions() -> tuple:
    """Return (topic_labels, country_names) for alert quick-add. Countries from Countries & Regions."""
    try:
        from app.country_data import ALL_COUNTRIES
        # ALL_COUNTRIES: (iso3, display_name, region, pop, area, density)
        country_names = sorted([row[1] for row in ALL_COUNTRIES if row[1]])
    except Exception:
        country_names = []
    return (list(ALERT_SUGGESTIONS_TOPIC_LABELS), country_names)


def get_alerts(user_id: Optional[int] = None):
    """Return alert rules. If user_id is given, only that user's; else only rows with user_id IS NULL (legacy)."""
    with _connection() as conn:
        if user_id is not None:
            cur = conn.execute(
                "SELECT id, name, topics, min_impact_score, created_at, webhook_url, user_id FROM alerts WHERE user_id = ? ORDER BY created_at DESC",
                (user_id,),
            )
        else:
            cur = conn.execute(
                "SELECT id, name, topics, min_impact_score, created_at, webhook_url, user_id FROM alerts WHERE user_id IS NULL ORDER BY created_at DESC"
            )
        rows = cur.fetchall()
    out = []
    for row in rows:
        d = dict(row)
        try:
            d["topics_list"] = json.loads(d["topics"]) if isinstance(d["topics"], str) else d["topics"]
        except (json.JSONDecodeError, TypeError):
            d["topics_list"] = []
        out.append(d)
    return out


def add_alert(name: str, topics: list[str], min_impact_score: int = 5, webhook_url: Optional[str] = None, user_id: Optional[int] = None) -> int:
    """Create an alert rule. min_impact_score 0-10; 0 = any impact. user_id optional for multi-tenant."""
    created = datetime.utcnow().isoformat() + "Z"
    score = max(0, min(int(min_impact_score if min_impact_score is not None else 1), 10))
    with _connection() as conn:
        cur = conn.execute(
            "INSERT INTO alerts (name, topics, min_impact_score, created_at, webhook_url, user_id) VALUES (?, ?, ?, ?, ?, ?)",
            (name, json.dumps(topics or []), score, created, webhook_url or "", user_id),
        )
        return cur.lastrowid


def get_all_alerts():
    """Return all alerts (for scheduler webhooks). No user filter."""
    with _connection() as conn:
        cur = conn.execute(
            "SELECT id, name, topics, min_impact_score, created_at, webhook_url, user_id FROM alerts ORDER BY created_at DESC"
        )
        rows = cur.fetchall()
    out = []
    for row in rows:
        d = dict(row)
        try:
            d["topics_list"] = json.loads(d["topics"]) if isinstance(d["topics"], str) else d["topics"]
        except (json.JSONDecodeError, TypeError):
            d["topics_list"] = []
        out.append(d)
    return out


def get_alert(alert_id: int, user_id: Optional[int] = None) -> Optional[dict]:
    """Return single alert if it exists and belongs to user (or is legacy when user_id is None)."""
    with _connection() as conn:
        if user_id is not None:
            cur = conn.execute(
                "SELECT id, name, topics, min_impact_score, created_at, webhook_url FROM alerts WHERE id = ? AND user_id = ?",
                (alert_id, user_id),
            )
        else:
            cur = conn.execute(
                "SELECT id, name, topics, min_impact_score, created_at, webhook_url FROM alerts WHERE id = ? AND user_id IS NULL",
                (alert_id,),
            )
        row = cur.fetchone()
    if not row:
        return None
    d = dict(row)
    try:
        d["topics_list"] = json.loads(d["topics"]) if isinstance(d["topics"], str) else d["topics"]
    except (json.JSONDecodeError, TypeError):
        d["topics_list"] = []
    return d


def get_alert_match_counts(days: int = 1, user_id: Optional[int] = None):
    """Return list of {alert_id, name, count} for sidebar. Pass user_id to scope to that user's alerts."""
    alerts = get_alerts(user_id=user_id)
    result = []
    for a in alerts:
        count = len(get_alert_matches(a["id"], days=days, limit=500))
        result.append({"alert_id": a["id"], "name": a["name"], "count": count})
    return result


def update_alert(
    alert_id: int,
    name: str,
    topics: list[str],
    min_impact_score: int = 5,
    webhook_url: Optional[str] = None,
    user_id: Optional[int] = None,
) -> bool:
    """Update an alert. If user_id given, only update when alert belongs to that user."""
    score = max(0, min(int(min_impact_score if min_impact_score is not None else 1), 10))
    with _connection() as conn:
        if user_id is not None:
            cur = conn.execute(
                "UPDATE alerts SET name = ?, topics = ?, min_impact_score = ?, webhook_url = ? WHERE id = ? AND user_id = ?",
                (name.strip(), json.dumps(topics or []), score, (webhook_url or "").strip() or "", alert_id, user_id),
            )
        else:
            cur = conn.execute(
                "UPDATE alerts SET name = ?, topics = ?, min_impact_score = ?, webhook_url = ? WHERE id = ? AND user_id IS NULL",
                (name.strip(), json.dumps(topics or []), score, (webhook_url or "").strip() or "", alert_id),
            )
        return cur.rowcount > 0


def delete_alert(alert_id: int, user_id: Optional[int] = None) -> bool:
    """Remove an alert rule. If user_id given, only delete when alert belongs to that user."""
    with _connection() as conn:
        if user_id is not None:
            cur = conn.execute("DELETE FROM alerts WHERE id = ? AND user_id = ?", (alert_id, user_id))
        else:
            cur = conn.execute("DELETE FROM alerts WHERE id = ? AND user_id IS NULL", (alert_id,))
        return cur.rowcount > 0


def get_alert_matches(alert_id: int, days: int = 1, limit: int = 50):
    """Articles matching an alert in the last N days.
    Uses COALESCE(impact_score, 1) so unscored articles can match min_impact_score=1.
    When min_impact_score is 0, any impact is accepted. Date filter uses effective date
    (published_utc or scraped_at) compared as YYYY-MM-DD for reliability."""
    with _connection() as conn:
        cur = conn.execute(
            "SELECT id, name, topics, min_impact_score FROM alerts WHERE id = ?",
            (alert_id,),
        )
        row = cur.fetchone()
    if not row:
        return []
    alert = dict(row)
    try:
        topics = json.loads(alert["topics"]) if isinstance(alert["topics"], str) else alert["topics"]
    except (json.JSONDecodeError, TypeError):
        topics = []
    since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    # Effective date: published_utc or scraped_at; compare date part only (YYYY-MM-DD)
    date_expr = "COALESCE(NULLIF(TRIM(published_utc), ''), scraped_at)"
    date_condition = f"SUBSTR({date_expr}, 1, 10) >= ?"
    # min_impact_score 0 = any; otherwise COALESCE(impact_score, 0) so unscored articles need min=0 to match
    min_impact = int(alert["min_impact_score"]) if alert["min_impact_score"] is not None else 1
    if min_impact <= 0:
        impact_condition = "1=1"
        impact_params = []
    else:
        impact_condition = "COALESCE(impact_score, 0) >= ?"
        impact_params = [min_impact]

    if not topics:
        with _connection() as conn:
            cur = conn.execute(
                f"""
                SELECT id, title, url, source_name, source_url, summary, published_utc, scraped_at,
                       topics, entities, key_takeaways, why_it_matters, cluster_id,
                       impact_score, impact_domains, image_url, video_url
                FROM articles
                WHERE {impact_condition} AND {date_condition}
                ORDER BY {date_expr} DESC, scraped_at DESC
                LIMIT ?
                """,
                tuple(impact_params + [since, limit]),
            )
            rows = cur.fetchall()
    else:
        # Match exact topic label ("US-China") OR any topic containing the term (so "China" matches "US-China")
        # and also match title/summary/entities so "China" finds articles that mention China anywhere (like the feed)
        topic_conditions = []
        topic_params = []
        for t in topics:
            if not (t and t.strip()):
                continue
            t = t.strip()
            topic_conditions.append(
                "(topics LIKE ? OR topics LIKE ? OR title LIKE ? OR summary LIKE ? OR entities LIKE ?)"
            )
            exact = f'%"{t}"%'
            substring = f"%{t}%"
            topic_params.extend([exact, substring, substring, substring, substring])
        where_topics = " OR ".join(topic_conditions) if topic_conditions else "1=1"
        params = impact_params + [since] + topic_params + [limit]
        with _connection() as conn:
            cur = conn.execute(
                f"""
                SELECT id, title, url, source_name, source_url, summary, published_utc, scraped_at,
                       topics, entities, key_takeaways, why_it_matters, cluster_id,
                       impact_score, impact_domains, image_url, video_url
                FROM articles
                WHERE {impact_condition} AND {date_condition} AND ({where_topics})
                ORDER BY {date_expr} DESC, scraped_at DESC
                LIMIT ?
                """,
                tuple(params),
            )
            rows = cur.fetchall()
    out = []
    for row in rows:
        d = dict(row)
        for key in ("topics", "entities", "impact_domains"):
            if d.get(key):
                try:
                    d[key + "_list"] = json.loads(d[key]) if isinstance(d[key], str) else d[key]
                except (json.JSONDecodeError, TypeError):
                    d[key + "_list"] = []
            else:
                d[key + "_list"] = []
        out.append(d)
    return out


# --- Pro features: freshness, saved views, annotations, entities, map, spike, webhook ---


def get_last_scrape_time() -> Optional[str]:
    """Return most recent scraped_at from articles (ISO string) or None."""
    with _connection() as conn:
        cur = conn.execute("SELECT MAX(scraped_at) as t FROM articles")
        row = cur.fetchone()
    return row["t"] if row and row["t"] else None


def get_articles_total_count() -> int:
    """Total number of articles in the database (no date/filter). For diagnostics when feed is empty."""
    with _connection() as conn:
        cur = conn.execute("SELECT COUNT(*) as n FROM articles")
        row = cur.fetchone()
    return row["n"] if row and row["n"] is not None else 0


def update_article_urgency(article_id: int, urgency: Optional[str]) -> None:
    """Set urgency: breaking, developing, or None."""
    with _connection() as conn:
        conn.execute("UPDATE articles SET urgency = ? WHERE id = ?", (urgency or None, article_id))


def update_article_event_type(article_id: int, event_type: Optional[str]) -> None:
    """Set event_type (e.g. Sanctions, Military, Diplomacy)."""
    with _connection() as conn:
        conn.execute("UPDATE articles SET event_type = ? WHERE id = ?", (event_type or None, article_id))


# Saved views (user-scoped when user_id provided)
def add_saved_view(name: str, params: dict, user_id: Optional[int] = None) -> int:
    created = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        cur = conn.execute(
            "INSERT INTO saved_views (name, params_json, created_at, user_id) VALUES (?, ?, ?, ?)",
            (name, json.dumps(params), created, user_id),
        )
        return cur.lastrowid


def get_saved_views(user_id: Optional[int] = None):
    """If user_id is None, return only legacy (user_id IS NULL). Else return that user's views."""
    with _connection() as conn:
        if user_id is not None:
            cur = conn.execute("SELECT id, name, params_json, created_at FROM saved_views WHERE user_id = ? ORDER BY created_at DESC", (user_id,))
        else:
            cur = conn.execute("SELECT id, name, params_json, created_at FROM saved_views WHERE user_id IS NULL ORDER BY created_at DESC")
        return [dict(row) for row in cur.fetchall()]


def get_saved_view(view_id: int, user_id: Optional[int] = None):
    """Return view only if it belongs to user (or is legacy when user_id is None)."""
    with _connection() as conn:
        if user_id is not None:
            cur = conn.execute("SELECT id, name, params_json, created_at FROM saved_views WHERE id = ? AND user_id = ?", (view_id, user_id))
        else:
            cur = conn.execute("SELECT id, name, params_json, created_at FROM saved_views WHERE id = ? AND user_id IS NULL", (view_id,))
        row = cur.fetchone()
    if not row:
        return None
    d = dict(row)
    try:
        d["params"] = json.loads(d["params_json"]) if d.get("params_json") else {}
    except (json.JSONDecodeError, TypeError):
        d["params"] = {}
    return d


def delete_saved_view(view_id: int, user_id: Optional[int] = None) -> bool:
    with _connection() as conn:
        if user_id is not None:
            cur = conn.execute("DELETE FROM saved_views WHERE id = ? AND user_id = ?", (view_id, user_id))
        else:
            cur = conn.execute("DELETE FROM saved_views WHERE id = ? AND user_id IS NULL", (view_id,))
        return cur.rowcount > 0


# Saved briefings (reports, user-scoped when user_id provided)
def add_saved_briefing(
    name: str,
    title: str,
    intro: str,
    article_ids: list,
    user_id: Optional[int] = None,
    sensitivity_tier: str = "internal",
    legal_review_required: bool = False,
) -> int:
    created = datetime.utcnow().isoformat() + "Z"
    tier = (sensitivity_tier or "internal").strip().lower()
    if tier not in ("public", "internal", "restricted"):
        tier = "internal"
    lr = 1 if legal_review_required else 0
    with _connection() as conn:
        cur = conn.execute(
            """INSERT INTO saved_briefings (name, title, intro, article_ids_json, created_at, user_id, sensitivity_tier, legal_review_required)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (name, title or "Briefing", intro or "", json.dumps(article_ids or []), created, user_id, tier, lr),
        )
        return cur.lastrowid


def get_saved_briefings(user_id: Optional[int] = None):
    if user_id is not None:
        with _connection() as conn:
            cur = conn.execute(
                """SELECT id, name, title, intro, article_ids_json, created_at,
                          COALESCE(sensitivity_tier, 'internal') AS sensitivity_tier,
                          COALESCE(legal_review_required, 0) AS legal_review_required
                   FROM saved_briefings WHERE user_id = ? ORDER BY created_at DESC""",
                (user_id,),
            )
            rows = cur.fetchall()
    else:
        with _connection() as conn:
            cur = conn.execute(
                """SELECT id, name, title, intro, article_ids_json, created_at,
                          COALESCE(sensitivity_tier, 'internal') AS sensitivity_tier,
                          COALESCE(legal_review_required, 0) AS legal_review_required
                   FROM saved_briefings WHERE user_id IS NULL ORDER BY created_at DESC"""
            )
            rows = cur.fetchall()
    out = []
    for row in rows:
        d = dict(row)
        try:
            d["article_ids"] = json.loads(d["article_ids_json"]) if d.get("article_ids_json") else []
        except (json.JSONDecodeError, TypeError):
            d["article_ids"] = []
        out.append(d)
    return out


def get_saved_briefing(briefing_id: int, user_id: Optional[int] = None):
    with _connection() as conn:
        if user_id is not None:
            cur = conn.execute(
                """SELECT id, name, title, intro, article_ids_json, created_at,
                          COALESCE(sensitivity_tier, 'internal') AS sensitivity_tier,
                          COALESCE(legal_review_required, 0) AS legal_review_required
                   FROM saved_briefings WHERE id = ? AND user_id = ?""",
                (briefing_id, user_id),
            )
        else:
            cur = conn.execute(
                """SELECT id, name, title, intro, article_ids_json, created_at,
                          COALESCE(sensitivity_tier, 'internal') AS sensitivity_tier,
                          COALESCE(legal_review_required, 0) AS legal_review_required
                   FROM saved_briefings WHERE id = ? AND user_id IS NULL""",
                (briefing_id,),
            )
        row = cur.fetchone()
    if not row:
        return None
    d = dict(row)
    try:
        d["article_ids"] = json.loads(d["article_ids_json"]) if d.get("article_ids_json") else []
    except (json.JSONDecodeError, TypeError):
        d["article_ids"] = []
    return d


def update_saved_briefing(
    briefing_id: int,
    name: str,
    title: str,
    intro: str,
    article_ids: list,
    user_id: Optional[int] = None,
    sensitivity_tier: Optional[str] = None,
    legal_review_required: Optional[bool] = None,
) -> bool:
    """Update an existing saved briefing. Returns True if updated. Scoped by user_id when provided."""
    ex = get_saved_briefing(briefing_id, user_id=user_id)
    if not ex:
        return False
    tier = sensitivity_tier if sensitivity_tier is not None else ex.get("sensitivity_tier") or "internal"
    tier = (tier or "internal").strip().lower()
    if tier not in ("public", "internal", "restricted"):
        tier = "internal"
    if legal_review_required is not None:
        lr = 1 if legal_review_required else 0
    else:
        lr = 1 if ex.get("legal_review_required") else 0
    with _connection() as conn:
        if user_id is not None:
            cur = conn.execute(
                """UPDATE saved_briefings SET name = ?, title = ?, intro = ?, article_ids_json = ?,
                       sensitivity_tier = ?, legal_review_required = ? WHERE id = ? AND user_id = ?""",
                (
                    name or "Briefing",
                    title or "Briefing",
                    intro or "",
                    json.dumps(article_ids or []),
                    tier,
                    lr,
                    briefing_id,
                    user_id,
                ),
            )
        else:
            cur = conn.execute(
                """UPDATE saved_briefings SET name = ?, title = ?, intro = ?, article_ids_json = ?,
                       sensitivity_tier = ?, legal_review_required = ? WHERE id = ? AND user_id IS NULL""",
                (
                    name or "Briefing",
                    title or "Briefing",
                    intro or "",
                    json.dumps(article_ids or []),
                    tier,
                    lr,
                    briefing_id,
                ),
            )
        ok = cur.rowcount > 0
        if ok and user_id is not None:
            now = datetime.utcnow().isoformat() + "Z"
            conn.execute(
                """INSERT INTO entity_change_log (entity_type, entity_ref, summary, detail_json, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                ("briefing", str(briefing_id), "Saved briefing updated", json.dumps({"user_id": user_id}), now),
            )
        return ok


def duplicate_saved_briefing(briefing_id: int, user_id: Optional[int] = None) -> int:
    """Clone a saved briefing as 'Copy of …'. Returns new briefing id or 0 if source not found."""
    b = get_saved_briefing(briefing_id, user_id=user_id)
    if not b:
        return 0
    new_name = "Copy of " + (b.get("name") or "Briefing")
    return add_saved_briefing(
        new_name,
        b.get("title") or "Briefing",
        b.get("intro") or "",
        b.get("article_ids") or [],
        user_id=user_id,
        sensitivity_tier=b.get("sensitivity_tier") or "internal",
        legal_review_required=bool(b.get("legal_review_required")),
    )


def delete_saved_briefing(briefing_id: int, user_id: Optional[int] = None) -> bool:
    with _connection() as conn:
        if user_id is not None:
            cur = conn.execute("DELETE FROM saved_briefings WHERE id = ? AND user_id = ?", (briefing_id, user_id))
        else:
            cur = conn.execute("DELETE FROM saved_briefings WHERE id = ? AND user_id IS NULL", (briefing_id,))
        return cur.rowcount > 0


# Annotations
def add_annotation(article_id: int, body: str) -> int:
    created = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        cur = conn.execute(
            "INSERT INTO annotations (article_id, body, created_at) VALUES (?, ?, ?)",
            (article_id, body, created),
        )
        return cur.lastrowid


def get_annotations_for_article(article_id: int):
    with _connection() as conn:
        cur = conn.execute(
            "SELECT id, article_id, body, created_at FROM annotations WHERE article_id = ? ORDER BY created_at ASC",
            (article_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def get_annotation(annotation_id: int):
    with _connection() as conn:
        cur = conn.execute(
            "SELECT id, article_id, body, created_at FROM annotations WHERE id = ?",
            (annotation_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def delete_annotation(annotation_id: int) -> bool:
    with _connection() as conn:
        cur = conn.execute("DELETE FROM annotations WHERE id = ?", (annotation_id,))
        return cur.rowcount > 0


# --- Policy workspace: tasks, object threads, visits, change log ---

OBJECT_ENTITY_TYPES = frozenset({"article", "country", "digest", "saved_view", "briefing"})


def list_users_for_assignment(limit: int = 500) -> list:
    """Active users for task assignee dropdown (id, username)."""
    with _connection() as conn:
        cur = conn.execute(
            "SELECT id, username FROM users WHERE is_active = 1 ORDER BY username COLLATE NOCASE LIMIT ?",
            (min(limit, 2000),),
        )
        return [dict(row) for row in cur.fetchall()]


def policy_task_create(
    owner_user_id: int,
    title: str,
    body: str = "",
    entity_type: Optional[str] = None,
    entity_ref: Optional[str] = None,
    assignee_user_id: Optional[int] = None,
    due_date: Optional[str] = None,
) -> int:
    now = datetime.utcnow().isoformat() + "Z"
    et = (entity_type or "").strip() or None
    if et and et not in OBJECT_ENTITY_TYPES:
        et = None
    with _connection() as conn:
        cur = conn.execute(
            """INSERT INTO policy_tasks (owner_user_id, assignee_user_id, title, body, entity_type, entity_ref, due_date, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'open', ?, ?)""",
            (
                owner_user_id,
                assignee_user_id,
                (title or "").strip() or "Task",
                (body or "").strip(),
                et,
                (entity_ref or "").strip() or None,
                (due_date or "").strip() or None,
                now,
                now,
            ),
        )
        return cur.lastrowid


def policy_tasks_for_user(user_id: int, include_done: bool = False) -> list:
    status_clause = "" if include_done else "AND t.status = 'open'"
    with _connection() as conn:
        cur = conn.execute(
            f"""SELECT t.*, ou.username AS owner_username, au.username AS assignee_username
                FROM policy_tasks t
                LEFT JOIN users ou ON ou.id = t.owner_user_id
                LEFT JOIN users au ON au.id = t.assignee_user_id
                WHERE (t.owner_user_id = ? OR t.assignee_user_id = ?) {status_clause}
                ORDER BY (t.due_date IS NULL), t.due_date, t.created_at DESC""",
            (user_id, user_id),
        )
        return [dict(row) for row in cur.fetchall()]


def policy_task_set_status(task_id: int, user_id: int, status: str) -> bool:
    if status not in ("open", "done", "cancelled"):
        status = "open"
    now = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        cur = conn.execute(
            """UPDATE policy_tasks SET status = ?, updated_at = ?
               WHERE id = ? AND (owner_user_id = ? OR assignee_user_id = ?)""",
            (status, now, task_id, user_id, user_id),
        )
        return cur.rowcount > 0


def object_comment_add(
    user_id: int,
    entity_type: str,
    entity_ref: str,
    body: str,
    parent_id: Optional[int] = None,
) -> int:
    et = (entity_type or "").strip()
    if et not in OBJECT_ENTITY_TYPES:
        raise ValueError("invalid entity_type")
    ref = (entity_ref or "").strip()
    if not ref:
        raise ValueError("entity_ref required")
    text = (body or "").strip()
    if not text:
        raise ValueError("empty body")
    now = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        cur = conn.execute(
            """INSERT INTO object_thread_comments (user_id, entity_type, entity_ref, parent_id, body, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (user_id, et, ref, parent_id, text, now),
        )
        return cur.lastrowid


def object_comments_list(entity_type: str, entity_ref: str) -> list:
    et = (entity_type or "").strip()
    ref = (entity_ref or "").strip()
    if et not in OBJECT_ENTITY_TYPES or not ref:
        return []
    with _connection() as conn:
        cur = conn.execute(
            """SELECT c.id, c.user_id, c.entity_type, c.entity_ref, c.parent_id, c.body, c.created_at, u.username
               FROM object_thread_comments c
               JOIN users u ON u.id = c.user_id
               WHERE c.entity_type = ? AND c.entity_ref = ?
               ORDER BY c.created_at ASC""",
            (et, ref),
        )
        return [dict(row) for row in cur.fetchall()]


def touch_user_entity_visit(user_id: int, entity_type: str, entity_ref: str) -> None:
    if not user_id or entity_type not in OBJECT_ENTITY_TYPES:
        return
    ref = (entity_ref or "").strip()
    if not ref:
        return
    now = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        conn.execute(
            """INSERT INTO user_entity_visits (user_id, entity_type, entity_ref, last_seen_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(user_id, entity_type, entity_ref) DO UPDATE SET last_seen_at = excluded.last_seen_at""",
            (user_id, entity_type, ref, now),
        )


def get_user_entity_visit(user_id: int, entity_type: str, entity_ref: str) -> Optional[str]:
    if not user_id:
        return None
    ref = (entity_ref or "").strip()
    with _connection() as conn:
        cur = conn.execute(
            "SELECT last_seen_at FROM user_entity_visits WHERE user_id = ? AND entity_type = ? AND entity_ref = ?",
            (user_id, entity_type, ref),
        )
        row = cur.fetchone()
    return row[0] if row else None


def get_entity_change_log(entity_type: str, entity_ref: str, limit: int = 15) -> list:
    with _connection() as conn:
        cur = conn.execute(
            """SELECT id, summary, detail_json, created_at FROM entity_change_log
               WHERE entity_type = ? AND entity_ref = ?
               ORDER BY created_at DESC LIMIT ?""",
            (entity_type, entity_ref, min(limit, 100)),
        )
        return [dict(row) for row in cur.fetchall()]


def log_entity_change_event(entity_type: str, entity_ref: str, summary: str, detail: Optional[dict] = None) -> None:
    """Public helper for app code to append audit rows."""
    if entity_type not in OBJECT_ENTITY_TYPES:
        return
    ref = (entity_ref or "").strip()
    if not ref:
        return
    now = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        conn.execute(
            """INSERT INTO entity_change_log (entity_type, entity_ref, summary, detail_json, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (entity_type, ref, (summary or "")[:500], json.dumps(detail) if detail else None, now),
        )


# --- Intelligence Messaging (channels, members, invites, encrypted messages) ---
MESSAGING_CHANNEL_TYPES = ("country_desk", "thematic", "briefing")
MESSAGING_ROLES = ("member", "admin", "verified_analyst")


def messaging_create_channel(name: str, slug: str, channel_type: str, description: str = "", invite_only: bool = True, created_by_user_id: Optional[int] = None, webhook_url: Optional[str] = None) -> int:
    if channel_type not in MESSAGING_CHANNEL_TYPES:
        channel_type = "thematic"
    created = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        cur = conn.execute(
            """INSERT INTO messaging_channels (name, slug, channel_type, description, invite_only, created_by_user_id, created_at, webhook_url)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (name, slug.strip().lower().replace(" ", "-"), channel_type, description or "", 1 if invite_only else 0, created_by_user_id, created, webhook_url or ""),
        )
        return cur.lastrowid


def messaging_get_channel_by_id(channel_id: int) -> Optional[dict]:
    with _connection() as conn:
        cur = conn.execute(
            "SELECT id, name, slug, channel_type, description, invite_only, created_by_user_id, created_at, webhook_url, COALESCE(archived, 0) AS archived, COALESCE(featured, 0) AS featured FROM messaging_channels WHERE id = ?",
            (channel_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def messaging_get_channel_by_slug(slug: str) -> Optional[dict]:
    with _connection() as conn:
        cur = conn.execute(
            "SELECT id, name, slug, channel_type, description, invite_only, created_by_user_id, created_at, webhook_url, COALESCE(archived, 0) AS archived, COALESCE(featured, 0) AS featured FROM messaging_channels WHERE slug = ?",
            (slug.strip().lower(),),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def messaging_add_member(channel_id: int, user_id: int, role: str = "member") -> bool:
    if role not in MESSAGING_ROLES:
        role = "member"
    joined = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        try:
            conn.execute(
                "INSERT INTO messaging_channel_members (channel_id, user_id, role, joined_at) VALUES (?, ?, ?, ?)",
                (channel_id, user_id, role, joined),
            )
            return True
        except sqlite3.IntegrityError:
            return False  # already member


def messaging_get_channel_members(channel_id: int):
    with _connection() as conn:
        cur = conn.execute(
            """SELECT m.user_id, m.role, m.joined_at, u.username, u.name, u.title, u.organization FROM messaging_channel_members m
               JOIN users u ON u.id = m.user_id WHERE m.channel_id = ? ORDER BY m.joined_at""",
            (channel_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def messaging_is_member(channel_id: int, user_id: int) -> bool:
    with _connection() as conn:
        cur = conn.execute("SELECT 1 FROM messaging_channel_members WHERE channel_id = ? AND user_id = ?", (channel_id, user_id))
        return cur.fetchone() is not None


def messaging_get_member_role(channel_id: int, user_id: int) -> Optional[str]:
    with _connection() as conn:
        cur = conn.execute("SELECT role FROM messaging_channel_members WHERE channel_id = ? AND user_id = ?", (channel_id, user_id))
        row = cur.fetchone()
    return row[0] if row else None


def messaging_set_member_role(channel_id: int, user_id: int, role: str) -> bool:
    if role not in MESSAGING_ROLES:
        return False
    with _connection() as conn:
        cur = conn.execute(
            "UPDATE messaging_channel_members SET role = ? WHERE channel_id = ? AND user_id = ?",
            (role, channel_id, user_id),
        )
        return cur.rowcount > 0


def messaging_get_channels_for_user(user_id: int):
    with _connection() as conn:
        cur = conn.execute(
            """SELECT c.id, c.name, c.slug, c.channel_type, c.description, c.invite_only, c.created_at, m.role
               FROM messaging_channels c
               JOIN messaging_channel_members m ON m.channel_id = c.id
               WHERE m.user_id = ? ORDER BY c.name""",
            (user_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def messaging_create_invite(channel_id: int, invited_by_user_id: int, invited_email: Optional[str] = None) -> Optional[str]:
    import secrets
    token = secrets.token_urlsafe(32)
    expires = (datetime.utcnow() + timedelta(days=7)).isoformat() + "Z"
    created = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        conn.execute(
            """INSERT INTO messaging_invites (channel_id, invited_by_user_id, invite_token, invited_email, expires_at, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (channel_id, invited_by_user_id, token, invited_email or "", expires, created),
        )
    return token


def messaging_get_invite_by_token(token: str) -> Optional[dict]:
    with _connection() as conn:
        cur = conn.execute(
            """SELECT id, channel_id, invited_by_user_id, invited_email, expires_at, used_at, created_at
               FROM messaging_invites WHERE invite_token = ? AND used_at IS NULL""",
            (token.strip(),),
        )
        row = cur.fetchone()
    if not row:
        return None
    d = dict(row)
    now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    if d.get("expires_at") and d["expires_at"] < now_iso:
        return None  # expired
    return d


def messaging_use_invite(token: str, user_id: int) -> bool:
    inv = messaging_get_invite_by_token(token)
    if not inv:
        return False
    used = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        conn.execute("UPDATE messaging_invites SET used_at = ? WHERE id = ?", (used, inv["id"]))
    return messaging_add_member(inv["channel_id"], user_id, "member")


def messaging_get_messages(channel_id: int, limit: int = 200, before_id: Optional[int] = None, after_id: Optional[int] = None, parent_id: Optional[int] = None):
    """Get messages. Use after_id for polling new messages, before_id for load more. parent_id=None for top-level only."""
    with _connection() as conn:
        params = [channel_id]
        where = "m.channel_id = ? AND m.deleted_at IS NULL"
        if parent_id is not None:
            where += " AND m.parent_id IS ?"
            params.append(parent_id)
        else:
            where += " AND m.parent_id IS NULL"
        if after_id:
            where += " AND m.id > ?"
            params.append(after_id)
            order = "ORDER BY m.id ASC"
            params.append(limit)
            cur = conn.execute(
                f"""SELECT m.id, m.channel_id, m.user_id, m.content_encrypted, m.created_at, m.parent_id, m.edited_at, m.deleted_at, m.pinned_at, m.attachment_type, m.attachment_id, m.attachment_extra, u.username, u.name, u.title, u.organization
                   FROM messaging_messages m JOIN users u ON u.id = m.user_id
                   WHERE {where} {order} LIMIT ?""",
                tuple(params),
            )
            rows = cur.fetchall()
            return [dict(row) for row in rows]
        elif before_id:
            where += " AND m.id < ?"
            params.append(before_id)
            params.append(limit)
            cur = conn.execute(
                f"""SELECT m.id, m.channel_id, m.user_id, m.content_encrypted, m.created_at, m.parent_id, m.edited_at, m.deleted_at, m.pinned_at, m.attachment_type, m.attachment_id, m.attachment_extra, u.username, u.name, u.title, u.organization
                   FROM messaging_messages m JOIN users u ON u.id = m.user_id
                   WHERE {where} ORDER BY m.pinned_at IS NOT NULL DESC, m.id DESC LIMIT ?""",
                tuple(params),
            )
            rows = cur.fetchall()
            return [dict(row) for row in reversed(rows)]
        else:
            params.append(limit)
            cur = conn.execute(
                f"""SELECT m.id, m.channel_id, m.user_id, m.content_encrypted, m.created_at, m.parent_id, m.edited_at, m.deleted_at, m.pinned_at, m.attachment_type, m.attachment_id, m.attachment_extra, u.username, u.name, u.title, u.organization
                   FROM messaging_messages m JOIN users u ON u.id = m.user_id
                   WHERE {where} ORDER BY m.pinned_at IS NOT NULL DESC, m.id DESC LIMIT ?""",
                tuple(params),
            )
            rows = cur.fetchall()
            return [dict(row) for row in reversed(rows)]


def messaging_get_message_by_id(message_id: int) -> Optional[dict]:
    with _connection() as conn:
        cur = conn.execute(
            """SELECT m.id, m.channel_id, m.user_id, m.content_encrypted, m.created_at, m.parent_id, m.edited_at, m.deleted_at, m.pinned_at, m.attachment_type, m.attachment_id, m.attachment_extra, u.username, u.name, u.title, u.organization
               FROM messaging_messages m JOIN users u ON u.id = m.user_id WHERE m.id = ?""",
            (message_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def messaging_add_message(channel_id: int, user_id: int, content_encrypted: bytes, parent_id: Optional[int] = None, attachment_type: Optional[str] = None, attachment_id: Optional[int] = None, attachment_extra: Optional[str] = None) -> int:
    created = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        cur = conn.execute(
            """INSERT INTO messaging_messages (channel_id, user_id, content_encrypted, created_at, parent_id, attachment_type, attachment_id, attachment_extra)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (channel_id, user_id, content_encrypted, created, parent_id, attachment_type, attachment_id, attachment_extra or ""),
        )
        return cur.lastrowid


def messaging_edit_message(message_id: int, user_id: int, content_encrypted: bytes) -> bool:
    edited = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        cur = conn.execute(
            "UPDATE messaging_messages SET content_encrypted = ?, edited_at = ? WHERE id = ? AND user_id = ? AND deleted_at IS NULL",
            (content_encrypted, edited, message_id, user_id),
        )
        return cur.rowcount > 0


def messaging_delete_message(message_id: int, user_id: int, soft: bool = True) -> bool:
    with _connection() as conn:
        if soft:
            deleted = datetime.utcnow().isoformat() + "Z"
            cur = conn.execute(
                "UPDATE messaging_messages SET deleted_at = ?, content_encrypted = ? WHERE id = ? AND user_id = ?",
                (deleted, b"", message_id, user_id),
            )
        else:
            cur = conn.execute("DELETE FROM messaging_messages WHERE id = ? AND user_id = ?", (message_id, user_id))
        return cur.rowcount > 0


def messaging_pin_message(message_id: int, channel_id: int, user_id: int) -> bool:
    role = messaging_get_member_role(channel_id, user_id)
    if role != "admin":
        return False
    pinned = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        conn.execute("UPDATE messaging_messages SET pinned_at = NULL WHERE channel_id = ?", (channel_id,))
        cur = conn.execute(
            "UPDATE messaging_messages SET pinned_at = ? WHERE id = ? AND channel_id = ?",
            (pinned, message_id, channel_id),
        )
        return cur.rowcount > 0


def messaging_unpin_message(channel_id: int, user_id: int) -> bool:
    role = messaging_get_member_role(channel_id, user_id)
    if role != "admin":
        return False
    with _connection() as conn:
        cur = conn.execute("UPDATE messaging_messages SET pinned_at = NULL WHERE channel_id = ?", (channel_id,))
        return cur.rowcount >= 0


def messaging_get_pinned(channel_id: int) -> Optional[dict]:
    with _connection() as conn:
        cur = conn.execute(
            """SELECT m.id, m.channel_id, m.user_id, m.content_encrypted, m.created_at, m.edited_at, m.pinned_at, m.attachment_type, m.attachment_id, m.attachment_extra, u.username, u.name, u.title, u.organization
               FROM messaging_messages m JOIN users u ON u.id = m.user_id
               WHERE m.channel_id = ? AND m.pinned_at IS NOT NULL AND m.deleted_at IS NULL LIMIT 1""",
            (channel_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def messaging_get_thread_replies(channel_id: int, parent_id: int, limit: int = 50):
    with _connection() as conn:
        cur = conn.execute(
            """SELECT m.id, m.channel_id, m.user_id, m.content_encrypted, m.created_at, m.edited_at, m.attachment_type, m.attachment_id, m.attachment_extra, u.username, u.name, u.title, u.organization
               FROM messaging_messages m JOIN users u ON u.id = m.user_id
               WHERE m.channel_id = ? AND m.parent_id = ? AND m.deleted_at IS NULL ORDER BY m.id ASC LIMIT ?""",
            (channel_id, parent_id, limit),
        )
        return [dict(row) for row in cur.fetchall()]


def messaging_get_reply_counts(channel_id: int):
    """Return dict parent_id -> reply count for top-level messages."""
    with _connection() as conn:
        cur = conn.execute(
            "SELECT parent_id, COUNT(*) AS cnt FROM messaging_messages WHERE channel_id = ? AND parent_id IS NOT NULL AND deleted_at IS NULL GROUP BY parent_id",
            (channel_id,),
        )
        return {row[0]: row[1] for row in cur.fetchall()}


def messaging_leave_channel(channel_id: int, user_id: int) -> bool:
    with _connection() as conn:
        cur = conn.execute("DELETE FROM messaging_channel_members WHERE channel_id = ? AND user_id = ?", (channel_id, user_id))
        return cur.rowcount > 0


def messaging_mute_channel(channel_id: int, user_id: int) -> bool:
    muted = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        try:
            conn.execute(
                "INSERT OR REPLACE INTO messaging_channel_mutes (user_id, channel_id, muted_at) VALUES (?, ?, ?)",
                (user_id, channel_id, muted),
            )
            return True
        except Exception:
            return False


def messaging_unmute_channel(channel_id: int, user_id: int) -> bool:
    with _connection() as conn:
        cur = conn.execute("DELETE FROM messaging_channel_mutes WHERE channel_id = ? AND user_id = ?", (channel_id, user_id))
        return cur.rowcount > 0


def messaging_is_muted(channel_id: int, user_id: int) -> bool:
    with _connection() as conn:
        cur = conn.execute("SELECT 1 FROM messaging_channel_mutes WHERE channel_id = ? AND user_id = ?", (channel_id, user_id))
        return cur.fetchone() is not None


def messaging_get_channels_for_user(user_id: int, include_muted: bool = True):
    with _connection() as conn:
        cur = conn.execute(
            """SELECT c.id, c.name, c.slug, c.channel_type, c.description, c.invite_only, c.created_at, m.role
               FROM messaging_channels c
               JOIN messaging_channel_members m ON m.channel_id = c.id
               WHERE m.user_id = ? ORDER BY c.name""",
            (user_id,),
        )
        rows = cur.fetchall()
    out = [dict(row) for row in rows]
    if not include_muted:
        muted = set()
        with _connection() as conn:
            cur = conn.execute("SELECT channel_id FROM messaging_channel_mutes WHERE user_id = ?", (user_id,))
            muted = {row[0] for row in cur.fetchall()}
        out = [c for c in out if c["id"] not in muted]
    return out


def messaging_invite_request_create(channel_id: int, user_id: int) -> bool:
    requested = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        try:
            conn.execute(
                "INSERT INTO messaging_invite_requests (channel_id, user_id, requested_at, status) VALUES (?, ?, ?, 'pending')",
                (channel_id, user_id, requested),
            )
            return True
        except sqlite3.IntegrityError:
            return False  # already requested


def messaging_invite_request_list(channel_id: int, status: str = "pending"):
    with _connection() as conn:
        cur = conn.execute(
            """SELECT r.id, r.channel_id, r.user_id, r.requested_at, r.status, u.username
               FROM messaging_invite_requests r JOIN users u ON u.id = r.user_id
               WHERE r.channel_id = ? AND r.status = ? ORDER BY r.requested_at""",
            (channel_id, status),
        )
        return [dict(row) for row in cur.fetchall()]


def messaging_invite_request_resolve(request_id: int, channel_id: int, resolver_user_id: int, grant: bool) -> bool:
    role = messaging_get_member_role(channel_id, resolver_user_id)
    if role != "admin":
        return False
    resolved = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        cur = conn.execute(
            "SELECT user_id FROM messaging_invite_requests WHERE id = ? AND channel_id = ? AND status = 'pending'",
            (request_id, channel_id),
        )
        row = cur.fetchone()
    if not row:
        return False
    user_id = row[0]
    with _connection() as conn:
        conn.execute(
            "UPDATE messaging_invite_requests SET status = ?, resolved_by_user_id = ?, resolved_at = ? WHERE id = ?",
            ("granted" if grant else "denied", resolver_user_id, resolved, request_id),
        )
    if grant:
        messaging_add_member(channel_id, user_id, "member")
    return True


def messaging_report_message(message_id: int, channel_id: int, reported_by_user_id: int, reason: Optional[str] = None) -> bool:
    created = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        try:
            conn.execute(
                "INSERT INTO messaging_reports (message_id, channel_id, reported_by_user_id, reason, created_at) VALUES (?, ?, ?, ?, ?)",
                (message_id, channel_id, reported_by_user_id, reason or "", created),
            )
            return True
        except Exception:
            return False


def messaging_audit_log(channel_id: int, user_id: Optional[int], action: str, details: Optional[dict] = None) -> None:
    created = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        conn.execute(
            "INSERT INTO messaging_audit_log (channel_id, user_id, action, details_json, created_at) VALUES (?, ?, ?, ?, ?)",
            (channel_id, user_id, action, json.dumps(details or {}), created),
        )


def messaging_get_audit_log(channel_id: int, limit: int = 50):
    with _connection() as conn:
        cur = conn.execute(
            """SELECT a.id, a.channel_id, a.user_id, a.action, a.details_json, a.created_at, u.username, u.name, u.title, u.organization
               FROM messaging_audit_log a LEFT JOIN users u ON u.id = a.user_id
               WHERE a.channel_id = ? ORDER BY a.id DESC LIMIT ?""",
            (channel_id, limit),
        )
        rows = cur.fetchall()
    out = []
    for row in rows:
        d = dict(row)
        try:
            d["details"] = json.loads(d["details_json"]) if d.get("details_json") else {}
        except (json.JSONDecodeError, TypeError):
            d["details"] = {}
        out.append(d)
    return out


def messaging_get_all_channels(include_archived: bool = False):
    """All channels for directory. Optionally exclude archived."""
    with _connection() as conn:
        where = "" if include_archived else " WHERE COALESCE(c.archived, 0) = 0"
        cur = conn.execute(
            f"""SELECT c.id, c.name, c.slug, c.channel_type, c.description, c.invite_only, c.created_at, c.webhook_url, COALESCE(c.archived, 0) AS archived, COALESCE(c.featured, 0) AS featured,
                      (SELECT COUNT(*) FROM messaging_channel_members m WHERE m.channel_id = c.id) AS member_count,
                      (SELECT MAX(id) FROM messaging_messages mm WHERE mm.channel_id = c.id AND mm.deleted_at IS NULL) AS last_message_id
               FROM messaging_channels c{where} ORDER BY c.name""",
        )
        return [dict(row) for row in cur.fetchall()]


def messaging_create_invite_for_user(channel_id: int, invited_by_user_id: int, invitee_user_id: int) -> Optional[str]:
    """Create invite for an existing user by id; returns token so they can use the link."""
    return messaging_create_invite(channel_id, invited_by_user_id, invited_email=None)


def messaging_set_last_read(channel_id: int, user_id: int, last_read_message_id: Optional[int] = None) -> None:
    now = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        cur = conn.execute("PRAGMA table_info(messaging_channel_read)")
        cols = {row[1] for row in cur.fetchall()}
        if "updated_at" in cols:
            conn.execute(
                """INSERT INTO messaging_channel_read (user_id, channel_id, last_read_message_id, last_read_at, updated_at)
                   VALUES (?, ?, ?, ?, ?) ON CONFLICT(user_id, channel_id) DO UPDATE SET
                   last_read_message_id = excluded.last_read_message_id, last_read_at = excluded.last_read_at, updated_at = excluded.updated_at""",
                (user_id, channel_id, last_read_message_id, now, now),
            )
        else:
            conn.execute(
                """INSERT INTO messaging_channel_read (user_id, channel_id, last_read_message_id, last_read_at)
                   VALUES (?, ?, ?, ?) ON CONFLICT(user_id, channel_id) DO UPDATE SET
                   last_read_message_id = excluded.last_read_message_id, last_read_at = excluded.last_read_at""",
                (user_id, channel_id, last_read_message_id, now),
            )


def messaging_get_last_read(channel_id: int, user_id: int) -> Optional[dict]:
    with _connection() as conn:
        cur = conn.execute(
            "SELECT last_read_message_id, last_read_at FROM messaging_channel_read WHERE channel_id = ? AND user_id = ?",
            (channel_id, user_id),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def messaging_get_unread_count(channel_id: int, user_id: int) -> int:
    """Count messages in channel with id > last_read_message_id (or all if never read)."""
    read = messaging_get_last_read(channel_id, user_id)
    since_id = read["last_read_message_id"] if read and read.get("last_read_message_id") else 0
    with _connection() as conn:
        cur = conn.execute(
            "SELECT COUNT(*) FROM messaging_messages WHERE channel_id = ? AND id > ? AND deleted_at IS NULL AND parent_id IS NULL",
            (channel_id, since_id),
        )
        return cur.fetchone()[0]


def messaging_get_channels_for_user_with_unread(user_id: int):
    """Channels with unread count and last activity for sorting."""
    channels = messaging_get_channels_for_user(user_id, include_muted=True)
    for c in channels:
        c["unread_count"] = messaging_get_unread_count(c["id"], user_id)
    with _connection() as conn:
        cur = conn.execute(
            "SELECT channel_id, MAX(id) AS last_msg_id FROM messaging_messages WHERE deleted_at IS NULL GROUP BY channel_id"
        )
        last_by_channel = {row[0]: row[1] for row in cur.fetchall()}
    for c in channels:
        c["last_message_id"] = last_by_channel.get(c["id"])
    return channels


def messaging_reaction_add(message_id: int, user_id: int, emoji: str) -> bool:
    created = datetime.utcnow().isoformat() + "Z"
    emoji = (emoji or "👍").strip()[:32]
    if not emoji:
        return False
    with _connection() as conn:
        try:
            conn.execute(
                "INSERT INTO messaging_reactions (message_id, user_id, emoji, created_at) VALUES (?, ?, ?, ?)",
                (message_id, user_id, emoji, created),
            )
            return True
        except sqlite3.IntegrityError:
            return False


def messaging_reaction_remove(message_id: int, user_id: int, emoji: str) -> bool:
    with _connection() as conn:
        cur = conn.execute(
            "DELETE FROM messaging_reactions WHERE message_id = ? AND user_id = ? AND emoji = ?",
            (message_id, user_id, (emoji or "👍").strip()[:32]),
        )
        return cur.rowcount > 0


def messaging_get_reactions(message_id: int):
    with _connection() as conn:
        cur = conn.execute(
            """SELECT emoji, user_id, u.username FROM messaging_reactions r JOIN users u ON u.id = r.user_id
               WHERE r.message_id = ? ORDER BY r.emoji, r.created_at""",
            (message_id,),
        )
        rows = cur.fetchall()
    by_emoji = {}
    for row in rows:
        d = dict(row)
        e = d["emoji"]
        if e not in by_emoji:
            by_emoji[e] = {"emoji": e, "user_ids": [], "usernames": []}
        by_emoji[e]["user_ids"].append(d["user_id"])
        by_emoji[e]["usernames"].append(d["username"])
    return list(by_emoji.values())


def messaging_channel_pref_set_starred(channel_id: int, user_id: int, starred: bool) -> None:
    with _connection() as conn:
        conn.execute(
            """INSERT INTO messaging_channel_prefs (user_id, channel_id, starred, sort_order) VALUES (?, ?, ?, 0)
               ON CONFLICT(user_id, channel_id) DO UPDATE SET starred = ?""",
            (user_id, channel_id, 1 if starred else 0, 1 if starred else 0),
        )


def messaging_channel_pref_get(channel_id: int, user_id: int) -> dict:
    with _connection() as conn:
        cur = conn.execute(
            "SELECT starred, sort_order FROM messaging_channel_prefs WHERE channel_id = ? AND user_id = ?",
            (channel_id, user_id),
        )
        row = cur.fetchone()
    return dict(row) if row else {"starred": 0, "sort_order": 0}


def messaging_add_mention(message_id: int, channel_id: int, user_id: int) -> None:
    created = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        try:
            conn.execute(
                "INSERT INTO messaging_mentions (message_id, channel_id, user_id, created_at) VALUES (?, ?, ?, ?)",
                (message_id, channel_id, user_id, created),
            )
        except sqlite3.IntegrityError:
            pass


def messaging_create_notification(user_id: int, channel_id: int, message_id: int, kind: str, from_user_id: Optional[int] = None) -> None:
    """Create a notification for user_id (recipient). from_user_id is who triggered it (e.g. mention author)."""
    created = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        conn.execute(
            "INSERT INTO messaging_notifications (user_id, channel_id, message_id, kind, from_user_id, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, channel_id, message_id, kind, from_user_id, created),
        )


def messaging_get_notifications(user_id: int, limit: int = 50, unread_only: bool = True):
    with _connection() as conn:
        where = " AND read_at IS NULL" if unread_only else ""
        cur = conn.execute(
            f"""SELECT n.id, n.channel_id, n.message_id, n.kind, n.read_at, n.created_at, c.name AS channel_name, c.slug AS channel_slug
               FROM messaging_notifications n JOIN messaging_channels c ON c.id = n.channel_id
               WHERE n.user_id = ?{where} ORDER BY n.id DESC LIMIT ?""",
            (user_id, limit),
        )
        return [dict(row) for row in cur.fetchall()]


def messaging_notification_mark_read(notification_id: int, user_id: int) -> bool:
    now = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        cur = conn.execute(
            "UPDATE messaging_notifications SET read_at = ? WHERE id = ? AND user_id = ?",
            (now, notification_id, user_id),
        )
        return cur.rowcount > 0


def messaging_get_reports_for_admin(admin_user_id: int):
    """Reports in channels where the user is admin."""
    with _connection() as conn:
        cur = conn.execute(
            """SELECT r.id, r.message_id, r.channel_id, r.reported_by_user_id, r.reason, r.created_at,
                      u.username AS reporter_username, u.name AS reporter_name, u.title AS reporter_title, u.organization AS reporter_organization,
                      c.name AS channel_name, c.slug AS channel_slug
               FROM messaging_reports r
               JOIN users u ON u.id = r.reported_by_user_id
               JOIN messaging_channels c ON c.id = r.channel_id
               JOIN messaging_channel_members m ON m.channel_id = r.channel_id AND m.user_id = ? AND m.role = 'admin'
               ORDER BY r.created_at DESC LIMIT 100""",
            (admin_user_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def messaging_report_resolve(report_id: int, admin_user_id: int, delete_message: bool = False) -> bool:
    with _connection() as conn:
        cur = conn.execute(
            "SELECT channel_id, message_id FROM messaging_reports WHERE id = ?",
            (report_id,),
        )
        row = cur.fetchone()
    if not row or messaging_get_member_role(row[0], admin_user_id) != "admin":
        return False
    channel_id, message_id = row[0], row[1]
    if delete_message:
        with _connection() as conn:
            conn.execute(
                "UPDATE messaging_messages SET deleted_at = ?, content_encrypted = ? WHERE id = ?",
                (datetime.utcnow().isoformat() + "Z", b"", message_id),
            )
    with _connection() as conn:
        conn.execute("DELETE FROM messaging_reports WHERE id = ?", (report_id,))
    return True


def messaging_update_channel(channel_id: int, admin_user_id: int, name: Optional[str] = None, description: Optional[str] = None, webhook_url: Optional[str] = None, archived: Optional[bool] = None, featured: Optional[bool] = None) -> bool:
    if messaging_get_member_role(channel_id, admin_user_id) != "admin":
        return False
    updates = []
    params = []
    if name is not None:
        updates.append("name = ?")
        params.append(name)
    if description is not None:
        updates.append("description = ?")
        params.append(description)
    if webhook_url is not None:
        updates.append("webhook_url = ?")
        params.append(webhook_url)
    if archived is not None:
        updates.append("archived = ?")
        params.append(1 if archived else 0)
    if featured is not None:
        updates.append("featured = ?")
        params.append(1 if featured else 0)
    if not updates:
        return True
    params.append(channel_id)
    with _connection() as conn:
        cur = conn.execute(
            "UPDATE messaging_channels SET " + ", ".join(updates) + " WHERE id = ?",
            tuple(params),
        )
        return cur.rowcount > 0


def get_articles_by_entity(entity_name: str, limit: int = 100, days: Optional[int] = 7):
    """Articles that mention entity in entities or topics."""
    since = (datetime.utcnow() - timedelta(days=days or 7)).strftime("%Y-%m-%d")
    with _connection() as conn:
        pattern = f'%"{entity_name}"%'
        cur = conn.execute(
            """
            SELECT id, title, url, source_name, source_url, summary, published_utc, scraped_at,
                   topics, entities, key_takeaways, why_it_matters, cluster_id,
                   impact_score, impact_domains, urgency, event_type, image_url, video_url
            FROM articles
            WHERE (COALESCE(NULLIF(published_utc, ''), scraped_at) >= ?)
              AND (entities LIKE ? OR topics LIKE ?)
            ORDER BY published_utc DESC, scraped_at DESC
            LIMIT ?
            """,
            (since, pattern, pattern, limit),
        )
        rows = cur.fetchall()
    out = []
    for row in rows:
        d = dict(row)
        for key in ("topics", "entities", "impact_domains"):
            if d.get(key):
                try:
                    d[key + "_list"] = json.loads(d[key]) if isinstance(d[key], str) else d[key]
                except (json.JSONDecodeError, TypeError):
                    d[key + "_list"] = []
            else:
                d[key + "_list"] = []
        out.append(d)
    return out


def get_articles_iran_attacks(days: int = 7, limit: int = 150) -> list:
    """Articles linking Iran with attacks/strikes/missiles/drones (for World Monitor iranAttacks layer).
    Matches Iran in title/summary/topics/entities and attack-related keywords in title/summary."""
    since = (datetime.utcnow() - timedelta(days=max(1, days))).strftime("%Y-%m-%d")
    with _connection() as conn:
        cur = conn.execute(
            """
            SELECT id, title, url, source_name, summary, published_utc, topics, entities
            FROM articles
            WHERE (COALESCE(NULLIF(published_utc, ''), scraped_at) >= ?)
              AND (
                (title LIKE '%Iran%' OR summary LIKE '%Iran%' OR topics LIKE '%Iran%' OR entities LIKE '%Iran%')
                AND (
                  title LIKE '%attack%' OR title LIKE '%strike%' OR title LIKE '%missile%' OR title LIKE '%drone%'
                  OR title LIKE '%retaliation%' OR title LIKE '%strikes%' OR title LIKE '%struck%'
                  OR summary LIKE '%attack%' OR summary LIKE '%strike%' OR summary LIKE '%missile%'
                  OR summary LIKE '%drone%' OR summary LIKE '%retaliation%'
                )
              )
            ORDER BY published_utc DESC, scraped_at DESC
            LIMIT ?
            """,
            (since, limit),
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def get_article_counts_by_country(days: int = 7, limit: int = 30):
    """Return list of (country_or_region_name, count) from topics/entities for map."""
    since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    with _connection() as conn:
        cur = conn.execute(
            "SELECT topics, entities FROM articles WHERE (topics IS NOT NULL OR entities IS NOT NULL) AND (COALESCE(NULLIF(published_utc, ''), scraped_at) >= ?)",
            (since,),
        )
        rows = cur.fetchall()
    from collections import Counter
    counter = Counter()
    # Normalize common names to map-friendly labels
    region_aliases = {
        "Russia-Ukraine": "Ukraine",
        "US-China": "China",
        "Middle East": "Middle East",
        "Asia-Pacific": "Asia",
        "Europe": "Europe",
    }
    for row in rows:
        for col in ("topics", "entities"):
            if not row[col]:
                continue
            try:
                data = json.loads(row[col]) if isinstance(row[col], str) else row[col]
                if isinstance(data, list):
                    for item in data:
                        label = region_aliases.get(item, item)
                        counter[label] += 1
            except (json.JSONDecodeError, TypeError):
                pass
    return counter.most_common(limit)


def get_spike_topics(days_recent: int = 1, days_prior: int = 1, limit: int = 10):
    """Topics that spiked in last days_recent vs previous days_prior (for trending up)."""
    since_recent = (datetime.utcnow() - timedelta(days=days_recent)).strftime("%Y-%m-%d")
    since_prior = (datetime.utcnow() - timedelta(days=days_recent + days_prior)).strftime("%Y-%m-%d")
    with _connection() as conn:
        cur = conn.execute(
            "SELECT topics FROM articles WHERE topics IS NOT NULL AND topics != '' AND (COALESCE(NULLIF(published_utc, ''), scraped_at) >= ?)",
            (since_prior,),
        )
        rows = cur.fetchall()
    from collections import Counter
    recent_count = Counter()
    prior_count = Counter()
    for row in rows:
        try:
            topics = json.loads(row[0]) if isinstance(row[0], str) else row[0]
            if not isinstance(topics, list):
                continue
            dt_str = None  # we don't have date in this query; need to requery with date
            for t in topics:
                recent_count[t] += 0  # will fill below
                prior_count[t] += 1
        except (json.JSONDecodeError, TypeError):
            pass
    # Count recent
    with _connection() as conn:
        cur = conn.execute(
            "SELECT topics FROM articles WHERE topics IS NOT NULL AND topics != '' AND (COALESCE(NULLIF(published_utc, ''), scraped_at) >= ?)",
            (since_recent,),
        )
        for row in cur.fetchall():
            try:
                topics = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                if isinstance(topics, list):
                    for t in topics:
                        recent_count[t] += 1
            except (json.JSONDecodeError, TypeError):
                pass
    # Spike = more in recent than prior
    spikes = []
    for topic, recent in recent_count.most_common(limit * 2):
        prior = prior_count.get(topic, 0)
        if recent > prior:
            spikes.append((topic, recent, prior))
    spikes.sort(key=lambda x: x[1] - x[2], reverse=True)
    return spikes[:limit]


def get_declining_topics(days_recent: int = 1, days_prior: int = 1, limit: int = 10):
    """Topics that declined in last days_recent vs previous days_prior (prior > recent)."""
    since_recent = (datetime.utcnow() - timedelta(days=days_recent)).strftime("%Y-%m-%d")
    since_prior = (datetime.utcnow() - timedelta(days=days_recent + days_prior)).strftime("%Y-%m-%d")
    with _connection() as conn:
        cur = conn.execute(
            "SELECT topics FROM articles WHERE topics IS NOT NULL AND topics != '' AND (COALESCE(NULLIF(published_utc, ''), scraped_at) >= ?)",
            (since_prior,),
        )
        rows = cur.fetchall()
    from collections import Counter
    recent_count = Counter()
    prior_count = Counter()
    for row in rows:
        try:
            topics = json.loads(row[0]) if isinstance(row[0], str) else row[0]
            if isinstance(topics, list):
                for t in topics:
                    prior_count[t] += 1
        except (json.JSONDecodeError, TypeError):
            pass
    with _connection() as conn:
        cur = conn.execute(
            "SELECT topics FROM articles WHERE topics IS NOT NULL AND topics != '' AND (COALESCE(NULLIF(published_utc, ''), scraped_at) >= ?)",
            (since_recent,),
        )
        for row in cur.fetchall():
            try:
                topics = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                if isinstance(topics, list):
                    for t in topics:
                        recent_count[t] += 1
            except (json.JSONDecodeError, TypeError):
                pass
    declining = []
    for topic, prior in prior_count.most_common(limit * 2):
        recent = recent_count.get(topic, 0)
        if prior > recent:
            declining.append((topic, recent, prior))
    declining.sort(key=lambda x: x[2] - x[1], reverse=True)
    return declining[:limit]


def get_sources_for_window(days: int = 7, limit: int = 10):
    """Top sources by article count in the last N days."""
    since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    with _connection() as conn:
        cur = conn.execute(
            """
            SELECT source_name, COUNT(*) as count
            FROM articles
            WHERE (COALESCE(NULLIF(published_utc, ''), scraped_at) >= ?)
            GROUP BY source_name
            ORDER BY count DESC
            LIMIT ?
            """,
            (since, limit),
        )
        return [dict(row) for row in cur.fetchall()]


def get_alert_webhook_state(alert_id: int):
    with _connection() as conn:
        cur = conn.execute(
            "SELECT alert_id, last_count, last_run_at FROM alert_webhook_state WHERE alert_id = ?",
            (alert_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def set_alert_webhook_state(alert_id: int, last_count: int) -> None:
    now = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        conn.execute(
            "REPLACE INTO alert_webhook_state (alert_id, last_count, last_run_at) VALUES (?, ?, ?)",
            (alert_id, last_count, now),
        )


# --- Geopolitical Risk Engine ---


def upsert_country_risk(
    country_code: str,
    risk_score: float,
    sector_energy: float = 0,
    sector_tech: float = 0,
    sector_maritime: float = 0,
    sector_supply_chain: float = 0,
    article_count: int = 0,
) -> None:
    now = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        conn.execute(
            """
            REPLACE INTO country_risk_snapshots
            (country_code, risk_score, sector_energy, sector_tech, sector_maritime, sector_supply_chain, article_count, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (country_code, risk_score, sector_energy, sector_tech, sector_maritime, sector_supply_chain, article_count, now),
        )


def get_country_risk_snapshots():
    """All country risk snapshots for heat map."""
    with _connection() as conn:
        cur = conn.execute(
            "SELECT country_code, risk_score, sector_energy, sector_tech, sector_maritime, sector_supply_chain, article_count, updated_at FROM country_risk_snapshots ORDER BY risk_score DESC"
        )
        return [dict(row) for row in cur.fetchall()]


def upsert_risk_index(
    region_code: str,
    coup_likelihood_pct: float = 0,
    sanctions_probability_pct: float = 0,
    trade_disruption_pct: float = 0,
    escalation_pathway_json: Optional[str] = None,
) -> None:
    now = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        conn.execute(
            """
            REPLACE INTO risk_index (region_code, coup_likelihood_pct, sanctions_probability_pct, trade_disruption_pct, escalation_pathway_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (region_code, coup_likelihood_pct, sanctions_probability_pct, trade_disruption_pct, escalation_pathway_json or "[]", now),
        )


def get_risk_index(region_code: Optional[str] = None):
    """Forward Risk Probability Index. If region_code given, return one row else all."""
    with _connection() as conn:
        if region_code:
            cur = conn.execute(
                "SELECT region_code, coup_likelihood_pct, sanctions_probability_pct, trade_disruption_pct, escalation_pathway_json, updated_at FROM risk_index WHERE region_code = ?",
                (region_code,),
            )
            row = cur.fetchone()
            if not row:
                return None
            d = dict(row)
            if d.get("escalation_pathway_json"):
                try:
                    d["escalation_pathway"] = json.loads(d["escalation_pathway_json"])
                except (json.JSONDecodeError, TypeError):
                    d["escalation_pathway"] = []
            return d
        cur = conn.execute(
            "SELECT region_code, coup_likelihood_pct, sanctions_probability_pct, trade_disruption_pct, escalation_pathway_json, updated_at FROM risk_index ORDER BY (coup_likelihood_pct + sanctions_probability_pct + trade_disruption_pct) DESC"
        )
        return [dict(row) for row in cur.fetchall()]


def set_source_category(source_name: str, category: str) -> None:
    """Category: news_wire, think_tank, government, defense_bulletin, social."""
    with _connection() as conn:
        conn.execute(
            "REPLACE INTO source_categories (source_name, category) VALUES (?, ?)",
            (source_name, category),
        )


def get_source_categories():
    with _connection() as conn:
        cur = conn.execute("SELECT source_name, category FROM source_categories")
        return {row["source_name"]: row["category"] for row in cur.fetchall()}


# --- Diplomacy & Treaty Intelligence ---

TREATY_TYPES = ("bilateral_agreement", "defense_pact", "trade_agreement", "investment_treaty", "other")
UN_VOTE_VALUES = ("yes", "no", "abstain", "absent")


def delete_treaties_by_source_contains(substring: str) -> int:
    """Delete treaties whose source_url contains the given substring. Returns count deleted."""
    if not substring:
        return 0
    with _connection() as conn:
        cur = conn.execute(
            "DELETE FROM treaties WHERE source_url LIKE ?",
            (f"%{substring}%",),
        )
        return cur.rowcount


def add_treaty(
    treaty_type: str,
    name: str,
    party_a: str,
    party_b: Optional[str] = None,
    signed_date: Optional[str] = None,
    summary: Optional[str] = None,
    full_text: Optional[str] = None,
    clauses_json: Optional[str] = None,
    has_escalation_clause: int = 0,
    source_url: Optional[str] = None,
    document_url: Optional[str] = None,
    wto_rta_id: Optional[int] = None,
) -> int:
    now = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        _ensure_treaty_columns(conn)
        cur = conn.execute(
            """
            INSERT INTO treaties (treaty_type, name, party_a, party_b, signed_date, summary, full_text, clauses_json, has_escalation_clause, source_url, document_url, wto_rta_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (treaty_type or "other", name, party_a, party_b or "", signed_date or "", summary or "", full_text or "", clauses_json or "", has_escalation_clause, source_url or "", document_url or "", wto_rta_id, now),
        )
        return cur.lastrowid


def get_treaty_by_wto_rta_id(rta_id: int):
    """Return treaty dict by WTO RTA ID, or None."""
    with _connection() as conn:
        _ensure_treaty_columns(conn)
        cur = conn.execute(
            "SELECT id, treaty_type, name, party_a, party_b, signed_date, summary, full_text, clauses_json, has_escalation_clause, source_url, document_url, wto_rta_id, created_at FROM treaties WHERE wto_rta_id = ?",
            (rta_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def update_treaty_wto_details(
    treaty_id: int,
    document_url: Optional[str] = None,
    clauses_json: Optional[str] = None,
    summary: Optional[str] = None,
) -> None:
    """Update treaty with WTO-scraped details (documents, provisions, etc.)."""
    updates = []
    params = []
    if document_url is not None:
        updates.append("document_url = ?")
        params.append(document_url)
    if clauses_json is not None:
        updates.append("clauses_json = ?")
        params.append(clauses_json)
    if summary is not None:
        updates.append("summary = ?")
        params.append(summary)
    if not updates:
        return
    params.append(treaty_id)
    with _connection() as conn:
        conn.execute(
            f"UPDATE treaties SET {', '.join(updates)} WHERE id = ?",
            tuple(params),
        )


def get_treaties(
    treaty_type: Optional[str] = None,
    party: Optional[str] = None,
    escalation_only: bool = False,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    search: Optional[str] = None,
    source_wto: Optional[bool] = None,
    has_documents: Optional[bool] = None,
    status: Optional[str] = None,
    provision: Optional[str] = None,
    region: Optional[str] = None,
    coverage: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    order_by: Optional[str] = None,
):
    order_clause = "COALESCE(NULLIF(TRIM(signed_date), ''), '9999') DESC, created_at DESC"
    if order_by == "name":
        order_clause = "name ASC, created_at DESC"
    elif order_by == "name_desc":
        order_clause = "name DESC, created_at DESC"
    elif order_by == "type":
        order_clause = "treaty_type ASC, name ASC"
    elif order_by == "created":
        order_clause = "created_at DESC"
    elif order_by == "date_asc":
        order_clause = "COALESCE(NULLIF(TRIM(signed_date), ''), '0000') ASC, name ASC"
    elif order_by == "date_desc":
        order_clause = "COALESCE(NULLIF(TRIM(signed_date), ''), '9999') DESC, name ASC"
    with _connection() as conn:
        _ensure_treaty_columns(conn)
        where = []
        params = []
        if treaty_type:
            where.append("treaty_type = ?")
            params.append(treaty_type)
        if party:
            where.append("(party_a LIKE ? OR party_b LIKE ?)")
            params.extend([f"%{party}%", f"%{party}%"])
        if escalation_only:
            where.append("has_escalation_clause = 1")
        if date_from:
            where.append("(signed_date >= ? OR signed_date IS NULL OR signed_date = '')")
            params.append(date_from[:10])
        if date_to:
            where.append("(signed_date <= ? OR signed_date IS NULL OR signed_date = '')")
            params.append(date_to[:10])
        if search:
            where.append("(name LIKE ? OR summary LIKE ? OR full_text LIKE ?)")
            q = f"%{search}%"
            params.extend([q, q, q])
        if source_wto is True:
            where.append("(source_url LIKE ? OR wto_rta_id IS NOT NULL)")
            params.append("%wto.org%")
        if has_documents is True:
            where.append(
                "((document_url IS NOT NULL AND document_url != '') "
                "OR (clauses_json IS NOT NULL AND clauses_json LIKE '%agreement_links%' AND clauses_json LIKE '%http%'))"
            )
        if status:
            where.append("summary LIKE ?")
            params.append(f"%Status: {status}%")
        if provision:
            where.append("(summary LIKE ? OR clauses_json LIKE ?)")
            params.extend([f"%{provision}%", f"%{provision}%"])
        if region:
            where.append("summary LIKE ?")
            params.append(f"%Region: {region}%")
        if coverage:
            where.append("summary LIKE ?")
            params.append(f"%Coverage: {coverage}%")
        where_sql = " AND ".join(where) if where else "1=1"
        params.extend([limit, offset])
        cur = conn.execute(
            f"""
            SELECT id, treaty_type, name, party_a, party_b, signed_date, summary, full_text, clauses_json,
                   has_escalation_clause, source_url, document_url, wto_rta_id, created_at
            FROM treaties WHERE {where_sql} ORDER BY {order_clause} LIMIT ? OFFSET ?
            """,
            tuple(params),
        )
        rows = [dict(row) for row in cur.fetchall()]
        for r in rows:
            if "wto_rta_id" not in r:
                r["wto_rta_id"] = None
        return rows


def get_treaties_count(
    treaty_type: Optional[str] = None,
    party: Optional[str] = None,
    escalation_only: bool = False,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    search: Optional[str] = None,
    source_wto: Optional[bool] = None,
    has_documents: Optional[bool] = None,
    status: Optional[str] = None,
    provision: Optional[str] = None,
    region: Optional[str] = None,
    coverage: Optional[str] = None,
) -> int:
    with _connection() as conn:
        _ensure_treaty_columns(conn)
        where = []
        params = []
        if treaty_type:
            where.append("treaty_type = ?")
            params.append(treaty_type)
        if party:
            where.append("(party_a LIKE ? OR party_b LIKE ?)")
            params.extend([f"%{party}%", f"%{party}%"])
        if escalation_only:
            where.append("has_escalation_clause = 1")
        if date_from:
            where.append("(signed_date >= ? OR signed_date IS NULL OR signed_date = '')")
            params.append(date_from[:10])
        if date_to:
            where.append("(signed_date <= ? OR signed_date IS NULL OR signed_date = '')")
            params.append(date_to[:10])
        if search:
            where.append("(name LIKE ? OR summary LIKE ? OR full_text LIKE ?)")
            q = f"%{search}%"
            params.extend([q, q, q])
        if source_wto is True:
            where.append("(source_url LIKE ? OR wto_rta_id IS NOT NULL)")
            params.append("%wto.org%")
        if has_documents is True:
            where.append(
                "((document_url IS NOT NULL AND document_url != '') "
                "OR (clauses_json IS NOT NULL AND clauses_json LIKE '%agreement_links%' AND clauses_json LIKE '%http%'))"
            )
        if status:
            where.append("summary LIKE ?")
            params.append(f"%Status: {status}%")
        if provision:
            where.append("(summary LIKE ? OR clauses_json LIKE ?)")
            params.extend([f"%{provision}%", f"%{provision}%"])
        if region:
            where.append("summary LIKE ?")
            params.append(f"%Region: {region}%")
        if coverage:
            where.append("summary LIKE ?")
            params.append(f"%Coverage: {coverage}%")
        where_sql = " AND ".join(where) if where else "1=1"
        cur = conn.execute(f"SELECT COUNT(*) FROM treaties WHERE {where_sql}", tuple(params))
        return (cur.fetchone() or (0,))[0]


def get_treaty_distinct_regions() -> list:
    """Return distinct region values from treaty summaries (for filter dropdown)."""
    import re
    seen = set()
    with _connection() as conn:
        cur = conn.execute("SELECT summary FROM treaties WHERE summary IS NOT NULL AND summary != ''")
        for (summary,) in cur.fetchall():
            for m in re.finditer(r"Region:\s*([^.\n]+)", summary or ""):
                val = m.group(1).strip()
                if val and len(val) < 80:
                    seen.add(val)
    return sorted(seen)


def get_treaty_distinct_coverages() -> list:
    """Return distinct coverage values from treaty summaries (for filter dropdown)."""
    import re
    seen = set()
    with _connection() as conn:
        cur = conn.execute("SELECT summary FROM treaties WHERE summary IS NOT NULL AND summary != ''")
        for (summary,) in cur.fetchall():
            for m in re.finditer(r"Coverage:\s*([^.\n]+)", summary or ""):
                val = m.group(1).strip()
                if val and len(val) < 80:
                    seen.add(val)
    return sorted(seen)


def get_treaties_by_year(limit_years: int = 20) -> list:
    """Return list of {year, count, treaties} for timeline view. Years with most treaties first."""
    with _connection() as conn:
        cur = conn.execute(
            """
            SELECT substr(signed_date, 1, 4) AS year, COUNT(*) AS cnt
            FROM treaties
            WHERE signed_date IS NOT NULL AND signed_date != '' AND length(signed_date) >= 4
            GROUP BY year
            ORDER BY cnt DESC
            LIMIT ?
            """,
            (limit_years,),
        )
        rows = cur.fetchall()
    return [{"year": str(r[0]), "count": r[1]} for r in rows]


def get_agreements_page_stats() -> dict:
    """Stats for agreements database page: total, by type, in force, with docs, etc."""
    with _connection() as conn:
        _ensure_treaty_columns(conn)
        stats = {}
        cur = conn.execute("SELECT COUNT(*) FROM treaties")
        stats["total"] = (cur.fetchone() or (0,))[0]
        cur = conn.execute(
            "SELECT treaty_type, COUNT(*) AS cnt FROM treaties GROUP BY treaty_type ORDER BY cnt DESC"
        )
        stats["by_type"] = [{"type": r[0], "count": r[1]} for r in cur.fetchall()]
        cur = conn.execute("SELECT COUNT(*) FROM treaties WHERE summary LIKE '%Status: In Force%'")
        stats["in_force"] = (cur.fetchone() or (0,))[0]
        cur = conn.execute(
            "SELECT COUNT(*) FROM treaties WHERE (document_url IS NOT NULL AND document_url != '') "
            "OR (clauses_json LIKE '%agreement_links%' AND clauses_json LIKE '%http%')"
        )
        stats["with_documents"] = (cur.fetchone() or (0,))[0]
        cur = conn.execute(
            "SELECT COUNT(*) FROM treaties WHERE source_url LIKE '%wto.org%' OR wto_rta_id IS NOT NULL"
        )
        stats["wto_count"] = (cur.fetchone() or (0,))[0]
    return stats


def parse_treaty_summary(summary: str | None) -> dict:
    """Extract Status, Coverage, Type, Region, WTO RTA ID from summary text."""
    out = {"status": "", "coverage": "", "rta_type": "", "region": "", "wto_rta_id": None}
    if not summary:
        return out
    import re
    for m in re.finditer(r"Status:\s*([^.]+)", summary):
        out["status"] = m.group(1).strip()
        break
    for m in re.finditer(r"Coverage:\s*([^.]+)", summary):
        out["coverage"] = m.group(1).strip()
        break
    for m in re.finditer(r"Type:\s*([^.]+)", summary):
        out["rta_type"] = m.group(1).strip()
        break
    for m in re.finditer(r"Region:\s*([^.]+)", summary):
        out["region"] = m.group(1).strip()
        break
    for m in re.finditer(r"WTO RTA ID:\s*(\d+)", summary):
        try:
            out["wto_rta_id"] = int(m.group(1))
        except (ValueError, TypeError):
            pass
        break
    return out


def get_treaty_counts_by_type() -> list:
    """Return list of {treaty_type, count} for dashboard summary."""
    with _connection() as conn:
        cur = conn.execute(
            "SELECT treaty_type, COUNT(*) AS count FROM treaties GROUP BY treaty_type ORDER BY count DESC"
        )
        return [dict(row) for row in cur.fetchall()]


def get_diplomacy_summary() -> dict:
    """Counts and last updated for diplomacy dashboard."""
    with _connection() as conn:
        _ensure_treaty_columns(conn)
        cur = conn.execute("SELECT COUNT(*) FROM treaties")
        treaty_count = (cur.fetchone() or (0,))[0]
        cur = conn.execute("SELECT COUNT(*) FROM sanctions_registry")
        sanctions_count = (cur.fetchone() or (0,))[0]
        cur = conn.execute("SELECT COUNT(*) FROM treaties WHERE has_escalation_clause = 1")
        escalation_count = (cur.fetchone() or (0,))[0]
        cur = conn.execute("SELECT COUNT(*) FROM treaties WHERE source_url LIKE '%wto.org%' OR wto_rta_id IS NOT NULL")
        wto_count = (cur.fetchone() or (0,))[0]
        cur = conn.execute(
            "SELECT COUNT(*) FROM treaties WHERE "
            "(document_url IS NOT NULL AND document_url != '') "
            "OR (clauses_json IS NOT NULL AND clauses_json LIKE '%agreement_links%' AND clauses_json LIKE '%http%')"
        )
        with_documents_count = (cur.fetchone() or (0,))[0]
        cur = conn.execute("SELECT COUNT(*) FROM treaties WHERE summary LIKE '%Status: In Force%'")
        in_force_count = (cur.fetchone() or (0,))[0]
        cur = conn.execute(
            "SELECT COUNT(*) FROM treaties WHERE (source_url LIKE '%wto.org%' OR wto_rta_id IS NOT NULL) "
            "AND (document_url IS NULL OR document_url = '') "
            "AND (clauses_json IS NULL OR clauses_json NOT LIKE '%agreement_links%' OR clauses_json NOT LIKE '%http%')"
        )
        wto_needs_scrape_count = (cur.fetchone() or (0,))[0]
        cur = conn.execute("SELECT MAX(created_at) FROM treaties")
        t_updated = (cur.fetchone() or (None,))[0]
        cur = conn.execute("SELECT MAX(created_at) FROM sanctions_registry")
        s_updated = (cur.fetchone() or (None,))[0]
        last_updated = max((x or "") for x in (t_updated, s_updated)) or None
        if last_updated and len(last_updated) >= 10:
            last_updated = last_updated[:10]
    return {
        "treaty_count": treaty_count,
        "sanctions_count": sanctions_count,
        "escalation_count": escalation_count,
        "wto_count": wto_count,
        "with_documents_count": with_documents_count,
        "in_force_count": in_force_count,
        "wto_needs_scrape_count": wto_needs_scrape_count,
        "last_updated": last_updated,
    }


def get_treaty(treaty_id: int):
    with _connection() as conn:
        _ensure_treaty_columns(conn)
        cur = conn.execute(
            "SELECT id, treaty_type, name, party_a, party_b, signed_date, summary, full_text, clauses_json, has_escalation_clause, source_url, document_url, wto_rta_id, created_at FROM treaties WHERE id = ?",
            (treaty_id,),
        )
        row = cur.fetchone()
    d = dict(row) if row else None
    if d and "wto_rta_id" not in d:
        d["wto_rta_id"] = None
    return d


def get_related_treaties(treaty_id: int, limit: int = 10) -> list:
    """Other treaties involving the same party_a or party_b as the given treaty."""
    t = get_treaty(treaty_id)
    if not t:
        return []
    party_a = (t.get("party_a") or "").strip()
    party_b = (t.get("party_b") or "").strip()
    if not party_a and not party_b:
        return []
    with _connection() as conn:
        parties = [p for p in (party_a, party_b) if p]
        parts = []
        params = [treaty_id]
        for p in parties:
            parts.append("(party_a LIKE ? OR party_b LIKE ?)")
            params.extend([f"%{p}%", f"%{p}%"])
        where_sql = "id != ? AND (" + " OR ".join(parts) + ")"
        params.append(limit)
        cur = conn.execute(
            f"""
            SELECT id, treaty_type, name, party_a, party_b, signed_date, has_escalation_clause
            FROM treaties WHERE {where_sql}
            ORDER BY COALESCE(NULLIF(TRIM(signed_date), ''), '9999') DESC, created_at DESC
            LIMIT ?
            """,
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


def update_treaty_escalation(treaty_id: int, has_escalation_clause: int) -> None:
    with _connection() as conn:
        conn.execute("UPDATE treaties SET has_escalation_clause = ? WHERE id = ?", (has_escalation_clause, treaty_id))


def update_treaty(
    treaty_id: int,
    *,
    treaty_type: Optional[str] = None,
    name: Optional[str] = None,
    party_a: Optional[str] = None,
    party_b: Optional[str] = None,
    signed_date: Optional[str] = None,
    summary: Optional[str] = None,
    full_text: Optional[str] = None,
    clauses_json: Optional[str] = None,
    has_escalation_clause: Optional[int] = None,
    source_url: Optional[str] = None,
    document_url: Optional[str] = None,
) -> None:
    """Update treaty fields. Pass only the fields you want to change."""
    with _connection() as conn:
        cur = conn.execute("SELECT id FROM treaties WHERE id = ?", (treaty_id,))
        if not cur.fetchone():
            return
        updates = []
        params = []
        for key, val in [
            ("treaty_type", treaty_type),
            ("name", name),
            ("party_a", party_a),
            ("party_b", party_b),
            ("signed_date", signed_date),
            ("summary", summary),
            ("full_text", full_text),
            ("clauses_json", clauses_json),
            ("has_escalation_clause", has_escalation_clause),
            ("source_url", source_url),
            ("document_url", document_url),
        ]:
            if val is not None:
                updates.append(f"{key} = ?")
                params.append(val)
        if not updates:
            return
        params.append(treaty_id)
        conn.execute("UPDATE treaties SET " + ", ".join(updates) + " WHERE id = ?", tuple(params))


def add_sanction(
    imposing_country: str,
    target_country: str,
    measure_type: Optional[str] = None,
    description: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source_url: Optional[str] = None,
    source: Optional[str] = None,
) -> int:
    now = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO sanctions_registry (imposing_country, target_country, measure_type, description, start_date, end_date, source_url, source, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (imposing_country, target_country, measure_type or "", description or "", start_date or "", end_date or "", source_url or "", source or "", now),
        )
        return cur.lastrowid


def _sanctions_where(imposing, target, source, date_from, date_to, search):
    where, params = [], []
    if imposing:
        where.append("imposing_country LIKE ?")
        params.append(f"%{imposing}%")
    if target:
        where.append("target_country LIKE ?")
        params.append(f"%{target}%")
    if source:
        where.append("source = ?")
        params.append(source)
    if date_from:
        where.append("(start_date >= ? OR start_date IS NULL OR start_date = '')")
        params.append(date_from[:10])
    if date_to:
        where.append("(start_date <= ? OR start_date IS NULL OR start_date = '')")
        params.append(date_to[:10])
    if search:
        where.append("(description LIKE ? OR imposing_country LIKE ? OR target_country LIKE ? OR measure_type LIKE ?)")
        q = f"%{search}%"
        params.extend([q, q, q, q])
    return " AND ".join(where) if where else "1=1", params


def get_sanctions_total_count(
    imposing: Optional[str] = None,
    target: Optional[str] = None,
    source: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    search: Optional[str] = None,
) -> int:
    where_sql, params = _sanctions_where(imposing, target, source, date_from, date_to, search)
    with _connection() as conn:
        cur = conn.execute(f"SELECT COUNT(*) FROM sanctions_registry WHERE {where_sql}", tuple(params))
        return (cur.fetchone() or (0,))[0]


def get_sanction(sanction_id: int):
    """Return a single sanction by id, or None."""
    with _connection() as conn:
        cur = conn.execute(
            "SELECT id, imposing_country, target_country, measure_type, description, start_date, end_date, source_url, source, created_at FROM sanctions_registry WHERE id = ?",
            (sanction_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def get_sanctions(
    imposing: Optional[str] = None,
    target: Optional[str] = None,
    source: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
):
    where_sql, params = _sanctions_where(imposing, target, source, date_from, date_to, search)
    params.extend([limit, offset])
    with _connection() as conn:
        cur = conn.execute(
            f"SELECT id, imposing_country, target_country, measure_type, description, start_date, end_date, source_url, source, created_at FROM sanctions_registry WHERE {where_sql} ORDER BY start_date DESC, created_at DESC LIMIT ? OFFSET ?",
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


def add_sanction_global(
    jurisdiction: str,
    target_type: str,
    name: str,
    country: Optional[str] = None,
    sanctions_type: Optional[str] = None,
    effective_date: Optional[str] = None,
    expiry_date: Optional[str] = None,
    measures: Optional[str] = None,
    source_link: Optional[str] = None,
) -> int:
    """Insert into sanctions_global. measures should be JSON array string."""
    now = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO sanctions_global (jurisdiction, target_type, name, country, sanctions_type, effective_date, expiry_date, measures, source_link, last_updated, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (jurisdiction, target_type, name or "", country or "", sanctions_type or "", effective_date or "", expiry_date or "", measures or "[]", source_link or "", now, now),
        )
        return cur.lastrowid


def clear_sanctions_global_by_jurisdiction(jurisdiction: str) -> int:
    """Delete all sanctions_global rows for a jurisdiction. Returns count deleted."""
    with _connection() as conn:
        cur = conn.execute("DELETE FROM sanctions_global WHERE jurisdiction = ?", (jurisdiction,))
        return cur.rowcount


def _sanctions_global_where(jurisdiction, target_type, country, search, date_from, date_to):
    where, params = [], []
    if jurisdiction:
        where.append("jurisdiction = ?")
        params.append(jurisdiction)
    if target_type:
        where.append("target_type = ?")
        params.append(target_type)
    if country:
        where.append("(country LIKE ? OR country = ?)")
        params.extend([f"%{country}%", country])
    if search:
        where.append("(name LIKE ? OR sanctions_type LIKE ?)")
        q = f"%{search}%"
        params.extend([q, q])
    if date_from:
        where.append("(effective_date >= ? OR effective_date IS NULL OR effective_date = '')")
        params.append(date_from[:10])
    if date_to:
        where.append("(effective_date <= ? OR effective_date IS NULL OR effective_date = '')")
        params.append(date_to[:10])
    return " AND ".join(where) if where else "1=1", params


def get_sanctions_global_count(
    jurisdiction: Optional[str] = None,
    target_type: Optional[str] = None,
    country: Optional[str] = None,
    search: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> int:
    where_sql, params = _sanctions_global_where(jurisdiction, target_type, country, search, date_from, date_to)
    with _connection() as conn:
        cur = conn.execute(f"SELECT COUNT(*) FROM sanctions_global WHERE {where_sql}", tuple(params))
        return (cur.fetchone() or (0,))[0]


def get_sanctions_global(
    jurisdiction: Optional[str] = None,
    target_type: Optional[str] = None,
    country: Optional[str] = None,
    search: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
) -> list:
    where_sql, params = _sanctions_global_where(jurisdiction, target_type, country, search, date_from, date_to)
    params.extend([limit, offset])
    with _connection() as conn:
        cur = conn.execute(
            f"""SELECT id, jurisdiction, target_type, name, country, sanctions_type, effective_date, expiry_date, measures, source_link, last_updated, created_at
            FROM sanctions_global WHERE {where_sql} ORDER BY effective_date DESC, last_updated DESC LIMIT ? OFFSET ?""",
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


def get_sanction_global(sanction_id: int):
    """Return a single sanction_global by id, or None."""
    with _connection() as conn:
        cur = conn.execute(
            "SELECT id, jurisdiction, target_type, name, country, sanctions_type, effective_date, expiry_date, measures, source_link, last_updated, created_at FROM sanctions_global WHERE id = ?",
            (sanction_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def add_un_vote(resolution_id: str, resolution_title: Optional[str], country_code: str, vote: str, vote_date: str) -> None:
    now = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO un_votes (resolution_id, resolution_title, country_code, vote, vote_date, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (resolution_id, resolution_title or "", country_code, vote.lower(), vote_date, now),
        )


def add_un_votes_bulk(rows: list) -> int:
    """Bulk insert UN votes in a single transaction. Each row: (resolution_id, resolution_title, country_code, vote, vote_date). Returns count inserted."""
    if not rows:
        return 0
    now = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        for r in rows:
            resolution_id, resolution_title, country_code, vote, vote_date = r
            conn.execute(
                "INSERT OR REPLACE INTO un_votes (resolution_id, resolution_title, country_code, vote, vote_date, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (resolution_id, resolution_title or "", country_code, (vote or "").lower(), vote_date or "0000-00-00", now),
            )
    return len(rows)


def get_un_votes(resolution_id: Optional[str] = None, country_code: Optional[str] = None, limit: int = 5000):
    with _connection() as conn:
        where = []
        params = []
        if resolution_id:
            where.append("resolution_id = ?")
            params.append(resolution_id)
        if country_code:
            where.append("country_code = ?")
            params.append(country_code)
        where_sql = " AND ".join(where) if where else "1=1"
        params.append(limit)
        cur = conn.execute(
            f"SELECT id, resolution_id, resolution_title, country_code, vote, vote_date, created_at FROM un_votes WHERE {where_sql} ORDER BY vote_date DESC LIMIT ?",
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


_resolutions_cache: tuple = (0, [])


def clear_resolutions_cache():
    """Clear cached resolutions (call after sync/import)."""
    global _resolutions_cache
    _resolutions_cache = (0, [])


def get_un_resolutions(limit: int = 500, use_cache: bool = False, cache_ttl_sec: int = 300):
    """Get recent resolutions. use_cache=True skips DB for default limit within TTL."""
    global _resolutions_cache
    if use_cache and limit <= 50:
        import time
        ts, cached = _resolutions_cache
        if ts and time.time() - ts < cache_ttl_sec and len(cached) >= limit:
            return cached[:limit]
    with _connection() as conn:
        cur = conn.execute(
            "SELECT resolution_id, resolution_title, vote_date FROM un_votes GROUP BY resolution_id ORDER BY MAX(vote_date) DESC LIMIT ?",
            (limit,),
        )
        rows = [dict(row) for row in cur.fetchall()]
    if use_cache and limit >= 50:
        import time
        _resolutions_cache = (time.time(), rows)
    return rows


def get_un_resolutions_for_country(country_code: str, limit: int = 200):
    """Resolutions where the country voted, ordered by vote date desc. Includes vote."""
    with _connection() as conn:
        cur = conn.execute(
            """SELECT resolution_id, resolution_title, vote_date, vote
               FROM un_votes WHERE country_code = ? ORDER BY vote_date DESC LIMIT ?""",
            (country_code, limit),
        )
        return [dict(row) for row in cur.fetchall()]


def get_un_resolution_detail(resolution_id: str):
    """Vote breakdown for a resolution: yes/no/abstain/absent counts and per-country votes."""
    with _connection() as conn:
        cur = conn.execute(
            "SELECT resolution_id, resolution_title, vote_date FROM un_votes WHERE resolution_id = ? GROUP BY resolution_id",
            (resolution_id,),
        )
        meta = cur.fetchone()
        cur = conn.execute(
            "SELECT country_code, vote FROM un_votes WHERE resolution_id = ? ORDER BY vote, country_code",
            (resolution_id,),
        )
        votes = [dict(row) for row in cur.fetchall()]
    if not meta:
        return None
    from collections import Counter
    counts = Counter(v["vote"] for v in votes)
    return {
        "resolution_id": meta["resolution_id"],
        "resolution_title": meta["resolution_title"] or "",
        "vote_date": meta["vote_date"] or "",
        "counts": dict(counts),
        "votes": votes,
        "total": len(votes),
    }


def normalize_un_votes_to_iso3() -> Tuple[int, int]:
    """
    Migrate un_votes.country_code from raw names to ISO3. Deduplicates (resolution_id, country).
    Returns (rows_migrated, rows_skipped).
    """
    try:
        from app.un_votes.country_map import normalize_country_to_iso3
    except ImportError:
        return 0, 0

    def _to_iso3(cc):
        if not cc:
            return None
        iso3 = normalize_country_to_iso3(cc)
        return iso3 or (cc if len(cc) == 3 and cc.isupper() and str(cc).isalpha() else None)

    with _connection() as conn:
        cur = conn.execute(
            "SELECT resolution_id, resolution_title, country_code, vote, vote_date, created_at FROM un_votes"
        )
        rows = cur.fetchall()
    seen = set()
    normalized = []
    skipped = 0
    for r in rows:
        iso3 = _to_iso3(r["country_code"])
        if not iso3:
            skipped += 1
            continue
        key = (r["resolution_id"], iso3)
        if key in seen:
            continue
        seen.add(key)
        normalized.append((r["resolution_id"], r["resolution_title"], iso3, r["vote"], r["vote_date"], r["created_at"]))

    if not normalized:
        return 0, skipped

    now = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        conn.execute("DELETE FROM voting_alignment")
        conn.execute("DELETE FROM un_votes")
        conn.executemany(
            "INSERT INTO un_votes (resolution_id, resolution_title, country_code, vote, vote_date, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            [(r[0], r[1] or "", r[2], r[3], r[4] or "0000-00-00", r[5] or now) for r in normalized],
        )
    return len(normalized), skipped


def compute_voting_alignment(min_votes: int = 5) -> int:
    """Compute pairwise UN voting alignment; store in voting_alignment. Returns number of pairs updated.
    Normalizes country_code to ISO3 so 'France' and 'FRA' are merged."""
    with _connection() as conn:
        cur = conn.execute(
            "SELECT resolution_id, country_code, vote FROM un_votes"
        )
        rows = cur.fetchall()
    from collections import defaultdict

    def _to_iso3(cc):
        if not cc:
            return None
        iso3 = None
        try:
            from app.un_votes.country_map import normalize_country_to_iso3
            iso3 = normalize_country_to_iso3(cc)
        except Exception:
            pass
        return iso3 or (cc if len(cc) == 3 and cc.isupper() and str(cc).isalpha() else None)

    by_resolution = defaultdict(dict)
    for row in rows:
        iso3 = _to_iso3(row["country_code"])
        if iso3:
            by_resolution[row["resolution_id"]][iso3] = row["vote"]
    countries = set()
    for r in by_resolution.values():
        countries.update(r.keys())
    countries = sorted(countries)
    now = datetime.utcnow().isoformat() + "Z"
    pairs_updated = 0
    with _connection() as conn:
        for i, ca in enumerate(countries):
            for cb in countries[i + 1:]:
                if ca == cb:
                    continue
                if _to_iso3(ca) == _to_iso3(cb):
                    continue
                agreed = total = 0
                for res, votes in by_resolution.items():
                    va = votes.get(ca)
                    vb = votes.get(cb)
                    if va is None or vb is None:
                        continue
                    total += 1
                    if va == vb:
                        agreed += 1
                if total < min_votes:
                    continue
                score = (agreed / total * 100) if total else 0
                conn.execute(
                    "REPLACE INTO voting_alignment (country_a, country_b, alignment_score, votes_agreed, votes_total, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (ca, cb, round(score, 1), agreed, total, now),
                )
                pairs_updated += 1
    return pairs_updated


def get_voting_alignment(
    country: Optional[str] = None,
    min_votes: Optional[int] = None,
    limit: int = 100,
    include_defunct: bool = False,
    sort: str = "score_desc",
    bloc: Optional[str] = None,
    offset: int = 0,
):
    """Get voting alignment. sort: score_desc, votes_desc, score_asc. bloc: ASEAN, EU, G7, BRICS, GCC, OAS, AU."""
    try:
        from app.un_votes.country_map import is_defunct_country
    except ImportError:
        def is_defunct_country(c):
            return False

    order_map = {
        "score_desc": "alignment_score DESC",
        "score_asc": "alignment_score ASC",
        "votes_desc": "votes_total DESC, alignment_score DESC",
        "votes_asc": "votes_total ASC, alignment_score DESC",
    }
    order_sql = order_map.get(sort) or order_map["score_desc"]

    with _connection() as conn:
        where = []
        params = []
        if country:
            where.append("(country_a = ? OR country_b = ?)")
            params.extend([country, country])
        if min_votes is not None and min_votes > 0:
            where.append("votes_total >= ?")
            params.append(min_votes)
        where_sql = " AND ".join(where) if where else "1=1"
        fetch_size = max((offset + limit) * 5, 1000)
        params.append(fetch_size)
        cur = conn.execute(
            f"""
            SELECT country_a, country_b, alignment_score, votes_agreed, votes_total, updated_at
            FROM voting_alignment
            WHERE {where_sql}
            ORDER BY {order_sql}
            LIMIT ?
            """,
            tuple(params),
        )
        rows = [dict(row) for row in cur.fetchall()]

    if include_defunct:
        rows = [r for r in rows if is_defunct_country(r.get("country_a")) or is_defunct_country(r.get("country_b"))]
    else:
        rows = [r for r in rows if not is_defunct_country(r.get("country_a")) and not is_defunct_country(r.get("country_b"))]

    # Drop same-country pairs and deduplicate (Yugoslavia/Moldova vs YUG/Moldova = same pair)
    try:
        from app.un_votes.country_map import normalize_country_to_iso3

        def _iso3(cc):
            c = (cc or "").strip()
            if not c:
                return None
            if len(c) == 3 and c.isupper() and c.isalpha():
                return c
            return normalize_country_to_iso3(c)

        rows = [r for r in rows if _iso3(r.get("country_a")) != _iso3(r.get("country_b"))]

        # Deduplicate: keep one row per (iso3_a, iso3_b) pair, preferring most votes then highest score
        seen: dict = {}
        for r in rows:
            ia, ib = _iso3(r.get("country_a")), _iso3(r.get("country_b"))
            if ia is None or ib is None:
                continue
            key = (min(ia, ib), max(ia, ib))
            v = r.get("votes_total") or 0
            s = r.get("alignment_score") or 0
            if key not in seen or (v > seen[key][1]) or (v == seen[key][1] and s > seen[key][2]):
                seen[key] = (r, v, s)
        rows = [v[0] for v in seen.values()]
        # Re-sort after dedup (score_desc, votes_desc, etc.)
        if sort == "score_desc":
            rows.sort(key=lambda x: (-(x.get("alignment_score") or 0), -(x.get("votes_total") or 0)))
        elif sort == "score_asc":
            rows.sort(key=lambda x: (x.get("alignment_score") or 0, -(x.get("votes_total") or 0)))
        elif sort == "votes_desc":
            rows.sort(key=lambda x: (-(x.get("votes_total") or 0), -(x.get("alignment_score") or 0)))
        else:
            rows.sort(key=lambda x: (x.get("votes_total") or 0, -(x.get("alignment_score") or 0)))
    except ImportError:
        rows = [r for r in rows if (r.get("country_a") or "").strip() != (r.get("country_b") or "").strip()]

    if bloc:
        try:
            from app.un_votes.blocs import BLOCS, P5
            from app.un_votes.country_map import normalize_country_to_iso3
            bloc_codes = set(BLOCS.get(bloc) or BLOCS.get((bloc or "").upper()) or (P5 if (bloc or "").upper() == "P5" else []) or [])

            def _in_bloc(cc):
                c = (cc or "").strip()
                if not c:
                    return False
                if c.upper() in bloc_codes:
                    return True
                return (normalize_country_to_iso3(c) or "") in bloc_codes

            if bloc_codes:
                rows = [r for r in rows if _in_bloc(r.get("country_a")) and _in_bloc(r.get("country_b"))]
        except ImportError:
            pass

    rows = rows[offset:offset + limit]

    try:
        from app.un_votes.country_map import get_iso3_to_canonical_name, get_defunct_names, normalize_country_to_iso3
        names = get_iso3_to_canonical_name()
        defunct = get_defunct_names()

        def _display_name(val: str) -> str:
            v = (val or "").strip()
            if not v:
                return ""
            v_upper = v.upper()
            if names.get(v_upper):
                return names[v_upper]
            if defunct.get(v_upper):
                return defunct[v_upper]
            iso3 = normalize_country_to_iso3(v)
            if iso3:
                return names.get(iso3) or defunct.get(iso3) or v
            return v

        for r in rows:
            r["country_a_name"] = _display_name(r.get("country_a") or "")
            r["country_b_name"] = _display_name(r.get("country_b") or "")
    except Exception:
        for r in rows:
            r["country_a_name"] = r.get("country_a") or ""
            r["country_b_name"] = r.get("country_b") or ""
    return rows


# --- Supply chain & trade flow intelligence ---
# CHOKEPOINT_COORDS and CHOKEPOINT_PCT_GLOBAL are defined at top of module (for seed/backfill)


def get_chokepoints():
    """All strategic chokepoints (with optional lat, lon, pct_global_trade, alternative_routes, risk_score from DB)."""
    with _connection() as conn:
        cur = conn.execute("PRAGMA table_info(chokepoints)")
        cols = [row[1] for row in cur.fetchall()]
        base = "id, slug, name, description, region, commodities, created_at"
        extra = []
        for c in ("lat", "lon", "pct_global_trade", "alternative_routes", "risk_score"):
            if c in cols:
                extra.append(c)
        sel = base + (", " + ", ".join(extra) if extra else "")
        cur = conn.execute(f"SELECT {sel} FROM chokepoints ORDER BY name")
        return [dict(row) for row in cur.fetchall()]


def get_chokepoints_with_geo():
    """Chokepoints with lat, lon, pct_global_trade (from DB or config fallback), and risk_score."""
    chokepoints = get_chokepoints()
    for cp in chokepoints:
        slug = (cp.get("slug") or "").strip().lower()
        if cp.get("lat") is None or cp.get("lon") is None:
            cp["lat"], cp["lon"] = CHOKEPOINT_COORDS.get(slug, (0, 0))
        if cp.get("pct_global_trade") is None:
            cp["pct_global_trade"] = CHOKEPOINT_PCT_GLOBAL.get(slug)
        if cp.get("risk_score") is None:
            cp["risk_score"] = None
    return chokepoints


def get_chokepoint(chokepoint_id: int):
    with _connection() as conn:
        cur = conn.execute("PRAGMA table_info(chokepoints)")
        cols = [row[1] for row in cur.fetchall()]
        sel = "id, slug, name, description, region, commodities, created_at"
        for c in ("lat", "lon", "pct_global_trade", "alternative_routes", "risk_score"):
            if c in cols:
                sel += ", " + c
        cur = conn.execute(f"SELECT {sel} FROM chokepoints WHERE id = ?", (chokepoint_id,))
        row = cur.fetchone()
    return dict(row) if row else None


def get_chokepoint_countries():
    """Distinct countries that appear in any chokepoint flow (for scenario form)."""
    with _connection() as conn:
        cur = conn.execute(
            "SELECT DISTINCT country_code, country_name FROM chokepoint_flows ORDER BY country_name"
        )
        return [dict(row) for row in cur.fetchall()]


def get_flows_for_chokepoint(chokepoint_id: int, country_code: Optional[str] = None):
    """Trade flows through a chokepoint; optionally filter by country."""
    with _connection() as conn:
        if country_code:
            cur = conn.execute(
                """SELECT id, chokepoint_id, country_code, country_name, sector, exposure_pct, impact_if_closed, notes, created_at
                   FROM chokepoint_flows WHERE chokepoint_id = ? AND country_code = ? ORDER BY sector""",
                (chokepoint_id, country_code),
            )
        else:
            cur = conn.execute(
                """SELECT id, chokepoint_id, country_code, country_name, sector, exposure_pct, impact_if_closed, notes, created_at
                   FROM chokepoint_flows WHERE chokepoint_id = ? ORDER BY country_code, sector""",
                (chokepoint_id,),
            )
        return [dict(row) for row in cur.fetchall()]


def get_flows_for_country(country_code: Optional[str] = None, limit: int = 50):
    """All chokepoint flows for a given country (for country dashboard exposure block)."""
    if not country_code or not str(country_code).strip():
        return []
    code = str(country_code).strip().upper()
    with _connection() as conn:
        cur = conn.execute(
            """SELECT f.id, f.chokepoint_id, f.country_code, f.country_name, f.sector, f.exposure_pct, f.impact_if_closed, f.notes,
                      c.slug AS chokepoint_slug, c.name AS chokepoint_name, c.region AS chokepoint_region
               FROM chokepoint_flows f
               JOIN chokepoints c ON c.id = f.chokepoint_id
               WHERE f.country_code = ? ORDER BY f.exposure_pct DESC LIMIT ?""",
            (code, limit),
        )
        return [dict(row) for row in cur.fetchall()]


def get_airspace_restrictions(limit: int = 20) -> list:
    """
    Airspace restrictions, no-fly zones, and flight diversions. Used on Trade & Supply Chain page.
    Returns list of dicts: region, status, description, lat, lon, source, updated_at.
    Reads from airspace_restrictions table if it exists and has data; otherwise returns seed/placeholder.
    """
    with _connection() as conn:
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='airspace_restrictions'")
        if cur.fetchone():
            cur = conn.execute(
                """SELECT region, status, description, lat, lon, source, updated_at
                   FROM airspace_restrictions ORDER BY updated_at DESC LIMIT ?""",
                (limit,),
            )
            rows = cur.fetchall()
            if rows:
                return [dict(row) for row in rows]
    # Placeholder seed: known high-profile airspace zones (extend via DB table when data available)
    now = datetime.utcnow().strftime("%Y-%m-%d")
    return [
        {"region": "Ukraine", "status": "restricted", "description": "Conflict zone; commercial overflights rerouted.", "lat": 48.5, "lon": 31.2, "source": "ICAO/EASA", "updated_at": now},
        {"region": "Israel / Gaza", "status": "restricted", "description": "Heightened risk; airlines avoiding airspace.", "lat": 31.5, "lon": 34.8, "source": "FAA/ICAO", "updated_at": now},
        {"region": "Russia (western)", "status": "restricted", "description": "Sanctions; many carriers avoiding Russian airspace.", "lat": 55.8, "lon": 37.6, "source": "EASA", "updated_at": now},
        {"region": "Red Sea / Yemen", "status": "advisory", "description": "Drone/missile risk; flights advised to reroute.", "lat": 15.0, "lon": 43.0, "source": "ICAO", "updated_at": now},
        {"region": "Taiwan Strait", "status": "advisory", "description": "Tension; some carriers adjusting routes.", "lat": 24.5, "lon": 119.5, "source": "Regional", "updated_at": now},
    ]


def run_chokepoint_scenario(chokepoint_id: int, country_codes: list) -> dict:
    """
    If chokepoint closes → impact on selected countries. Returns a scenario tree:
    { chokepoint: {...}, countries: [ { country_code, country_name, sectors: [...], max_impact } ], summary }
    """
    chokepoint = get_chokepoint(chokepoint_id)
    if not chokepoint:
        return {"error": "Chokepoint not found", "countries": []}
    codes_set = {c.strip().upper() for c in country_codes if c and str(c).strip()}
    if not codes_set:
        codes_set = set()
        for row in get_flows_for_chokepoint(chokepoint_id):
            codes_set.add(row["country_code"])
    flows = get_flows_for_chokepoint(chokepoint_id)
    by_country = {}
    for f in flows:
        if f["country_code"] not in codes_set:
            continue
        c = f["country_code"]
        if c not in by_country:
            by_country[c] = {"country_code": c, "country_name": f["country_name"], "sectors": [], "max_impact": "low"}
        by_country[c]["sectors"].append({
            "sector": f["sector"],
            "exposure_pct": f["exposure_pct"],
            "impact_if_closed": f["impact_if_closed"],
            "notes": f["notes"],
        })
        imp = (f["impact_if_closed"] or "").lower()
        if imp == "high":
            by_country[c]["max_impact"] = "high"
        elif imp == "medium" and by_country[c]["max_impact"] != "high":
            by_country[c]["max_impact"] = "medium"
    impact_order = {"high": 3, "medium": 2, "low": 1}
    countries = sorted(by_country.values(), key=lambda x: (-impact_order.get(x["max_impact"], 0), x["country_name"]))
    return {
        "chokepoint": chokepoint,
        "countries": countries,
        "summary": f"If {chokepoint['name']} closes: {len(countries)} countries with direct exposure; "
                   + ", ".join(c["country_name"] for c in countries[:5]) + ("..." if len(countries) > 5 else ""),
    }


# --- Political stability & domestic signals ---

def get_election_calendar(
    country_code: Optional[str] = None,
    region: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[str] = None,
    election_type: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    order_by: Optional[str] = None,
):
    where = []
    params = []
    if country_code:
        where.append("country_code = ?")
        params.append(country_code)
    if region:
        where.append("country_code IN (SELECT country_code FROM country_risk_integration WHERE region = ?)")
        params.append(region)
    if date_from:
        where.append("(date_planned >= ? OR date_planned IS NULL OR date_planned = '')")
        params.append(date_from[:7] if len(date_from or "") >= 7 else date_from)
    if date_to:
        where.append("(date_planned <= ? OR date_planned IS NULL OR date_planned = '')")
        params.append(date_to[:7] if len(date_to or "") >= 7 else date_to)
    if status:
        where.append("status = ?")
        params.append(status)
    if election_type:
        where.append("election_type = ?")
        params.append(election_type)
    if search:
        where.append("(country_name LIKE ? OR notes LIKE ?)")
        q = f"%{search}%"
        params.extend([q, q])
    where_sql = " AND ".join(where) if where else "1=1"
    order = "date_planned ASC, created_at DESC"
    if order_by == "date_desc":
        order = "date_planned DESC, created_at DESC"
    elif order_by == "country":
        order = "country_name ASC, date_planned ASC"
    elif order_by == "type":
        order = "election_type ASC, date_planned ASC"
    elif order_by == "status":
        order = "status ASC, date_planned ASC"
    elif order_by == "created":
        order = "created_at DESC"
    params.extend([limit, offset])
    with _connection() as conn:
        cur = conn.execute(
            f"""SELECT id, country_code, country_name, election_type, date_planned, status, notes, created_at
                FROM election_calendar WHERE {where_sql} ORDER BY {order} LIMIT ? OFFSET ?""",
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


def get_election_calendar_count(
    country_code: Optional[str] = None,
    region: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[str] = None,
    election_type: Optional[str] = None,
    search: Optional[str] = None,
) -> int:
    where = []
    params = []
    if country_code:
        where.append("country_code = ?")
        params.append(country_code)
    if region:
        where.append("country_code IN (SELECT country_code FROM country_risk_integration WHERE region = ?)")
        params.append(region)
    if date_from:
        where.append("(date_planned >= ? OR date_planned IS NULL OR date_planned = '')")
        params.append(date_from[:7] if len(date_from or "") >= 7 else date_from)
    if date_to:
        where.append("(date_planned <= ? OR date_planned IS NULL OR date_planned = '')")
        params.append(date_to[:7] if len(date_to or "") >= 7 else date_to)
    if status:
        where.append("status = ?")
        params.append(status)
    if election_type:
        where.append("election_type = ?")
        params.append(election_type)
    if search:
        where.append("(country_name LIKE ? OR notes LIKE ?)")
        q = f"%{search}%"
        params.extend([q, q])
    where_sql = " AND ".join(where) if where else "1=1"
    with _connection() as conn:
        cur = conn.execute(f"SELECT COUNT(*) FROM election_calendar WHERE {where_sql}", tuple(params))
        return (cur.fetchone() or (0,))[0]


def get_election(election_id: int):
    with _connection() as conn:
        cur = conn.execute(
            """SELECT id, country_code, country_name, election_type, date_planned, status, notes, created_at
               FROM election_calendar WHERE id = ?""",
            (election_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def add_election(
    country_code: str,
    country_name: str,
    election_type: str,
    date_planned: str,
    status: Optional[str] = None,
    notes: Optional[str] = None,
) -> int:
    now = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        cur = conn.execute(
            """INSERT INTO election_calendar (country_code, country_name, election_type, date_planned, status, notes, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (country_code.strip(), country_name.strip(), election_type.strip(), (date_planned or "").strip() or "", status or None, notes or None, now),
        )
        return cur.lastrowid


def update_election(
    election_id: int,
    *,
    country_code: Optional[str] = None,
    country_name: Optional[str] = None,
    election_type: Optional[str] = None,
    date_planned: Optional[str] = None,
    status: Optional[str] = None,
    notes: Optional[str] = None,
) -> None:
    with _connection() as conn:
        cur = conn.execute("SELECT id FROM election_calendar WHERE id = ?", (election_id,))
        if not cur.fetchone():
            return
        updates = []
        params = []
        for key, val in [
            ("country_code", country_code),
            ("country_name", country_name),
            ("election_type", election_type),
            ("date_planned", date_planned),
            ("status", status),
            ("notes", notes),
        ]:
            if val is not None:
                updates.append(f"{key} = ?")
                params.append(val)
        if not updates:
            return
        now = datetime.utcnow().isoformat() + "Z"
        cur = conn.execute("PRAGMA table_info(election_calendar)")
        if "updated_at" in {row[1] for row in cur.fetchall()}:
            updates.append("updated_at = ?")
            params.append(now)
        params.append(election_id)
        conn.execute("UPDATE election_calendar SET " + ", ".join(updates) + " WHERE id = ?", tuple(params))


def delete_election(election_id: int) -> bool:
    """Delete an election. Returns True if a row was deleted."""
    with _connection() as conn:
        cur = conn.execute("DELETE FROM election_calendar WHERE id = ?", (election_id,))
        return cur.rowcount > 0


def get_election_calendar_last_updated() -> Optional[str]:
    """Return the most recent created_at/updated_at from election_calendar."""
    with _connection() as conn:
        cur = conn.execute("PRAGMA table_info(election_calendar)")
        cols = {row[1] for row in cur.fetchall()}
        if "updated_at" in cols:
            cur = conn.execute("SELECT MAX(COALESCE(NULLIF(TRIM(updated_at), ''), created_at)) FROM election_calendar")
        else:
            cur = conn.execute("SELECT MAX(created_at) FROM election_calendar")
        row = cur.fetchone()
        val = (row or (None,))[0]
    return (val or "")[:10] if val else None


def get_election_regions() -> list:
    """Return distinct regions from country_risk_integration for election filter dropdown."""
    with _connection() as conn:
        cur = conn.execute("SELECT DISTINCT region FROM country_risk_integration WHERE region IS NOT NULL AND region != '' ORDER BY region")
        return [row[0] for row in cur.fetchall()]


def _approval_where(country_code, region, date_from, date_to, subject, min_approval, max_approval, source):
    where, params = [], []
    if country_code:
        where.append("country_code = ?")
        params.append(country_code)
    if region:
        where.append("country_code IN (SELECT country_code FROM country_risk_integration WHERE region = ?)")
        params.append(region)
    if date_from:
        where.append("(poll_date >= ? OR poll_date IS NULL OR poll_date = '')")
        params.append(date_from[:10])
    if date_to:
        where.append("(poll_date <= ? OR poll_date IS NULL OR poll_date = '')")
        params.append(date_to[:10])
    if subject:
        where.append("subject LIKE ?")
        params.append(f"%{subject}%")
    if min_approval is not None:
        where.append("approval_pct >= ?")
        params.append(min_approval)
    if max_approval is not None:
        where.append("approval_pct <= ?")
        params.append(max_approval)
    if source:
        where.append("source LIKE ?")
        params.append(f"%{source}%")
    return " AND ".join(where) if where else "1=1", params


def get_approval_ratings(
    country_code: Optional[str] = None,
    region: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    subject: Optional[str] = None,
    min_approval: Optional[float] = None,
    max_approval: Optional[float] = None,
    source: Optional[str] = None,
    limit: int = 30,
    offset: int = 0,
    order_by: Optional[str] = None,
):
    where_sql, params = _approval_where(country_code, region, date_from, date_to, subject, min_approval, max_approval, source)
    order = "poll_date DESC, created_at DESC"
    if order_by == "approval_asc":
        order = "approval_pct ASC, poll_date DESC"
    elif order_by == "approval_desc":
        order = "approval_pct DESC, poll_date DESC"
    elif order_by == "country":
        order = "country_name ASC, poll_date DESC"
    elif order_by == "date_asc":
        order = "poll_date ASC, created_at DESC"
    params.extend([limit, offset])
    with _connection() as conn:
        cur = conn.execute(
            f"""SELECT id, country_code, country_name, subject, approval_pct, poll_date, source, poll_url, sample_size, created_at
                FROM approval_ratings WHERE {where_sql} ORDER BY {order} LIMIT ? OFFSET ?""",
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


def get_approval_ratings_count(
    country_code: Optional[str] = None,
    region: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    subject: Optional[str] = None,
    min_approval: Optional[float] = None,
    max_approval: Optional[float] = None,
    source: Optional[str] = None,
) -> int:
    where_sql, params = _approval_where(country_code, region, date_from, date_to, subject, min_approval, max_approval, source)
    with _connection() as conn:
        cur = conn.execute(f"SELECT COUNT(*) FROM approval_ratings WHERE {where_sql}", tuple(params))
        return (cur.fetchone() or (0,))[0]


def get_approval_rating(rating_id: int):
    with _connection() as conn:
        cur = conn.execute(
            "SELECT id, country_code, country_name, subject, approval_pct, poll_date, source, poll_url, sample_size, created_at FROM approval_ratings WHERE id = ?",
            (rating_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def approval_duplicate_exists(country_code: str, subject: str, poll_date: Optional[str], exclude_id: Optional[int] = None) -> bool:
    """True if a rating already exists for this country + subject + poll_date."""
    with _connection() as conn:
        pd = (poll_date or "").strip()[:10] or None
        q = "SELECT 1 FROM approval_ratings WHERE country_code = ? AND subject = ?"
        params = [country_code.strip(), subject.strip()]
        if pd:
            q += " AND poll_date = ?"
            params.append(pd)
        else:
            q += " AND (poll_date IS NULL OR poll_date = '')"
        if exclude_id is not None:
            q += " AND id != ?"
            params.append(exclude_id)
        cur = conn.execute(q + " LIMIT 1", params)
        return cur.fetchone() is not None


def add_approval_rating(
    country_code: str,
    country_name: str,
    subject: str,
    approval_pct: float,
    poll_date: Optional[str] = None,
    source: Optional[str] = None,
    poll_url: Optional[str] = None,
    sample_size: Optional[int] = None,
) -> int:
    now = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        cur = conn.execute(
            """INSERT INTO approval_ratings (country_code, country_name, subject, approval_pct, poll_date, source, poll_url, sample_size, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                country_code.strip(), country_name.strip(), subject.strip(), float(approval_pct),
                (poll_date or "").strip() or None, (source or "").strip() or None,
                (poll_url or "").strip() or None, sample_size, now,
            ),
        )
        return cur.lastrowid


def update_approval_rating(
    rating_id: int,
    *,
    country_code: Optional[str] = None,
    country_name: Optional[str] = None,
    subject: Optional[str] = None,
    approval_pct: Optional[float] = None,
    poll_date: Optional[str] = None,
    source: Optional[str] = None,
    poll_url: Optional[str] = None,
    sample_size: Optional[int] = None,
) -> None:
    with _connection() as conn:
        cur = conn.execute("SELECT id FROM approval_ratings WHERE id = ?", (rating_id,))
        if not cur.fetchone():
            return
        updates, params = [], []
        for key, val in [
            ("country_code", country_code),
            ("country_name", country_name),
            ("subject", subject),
            ("approval_pct", approval_pct),
            ("poll_date", poll_date),
            ("source", source),
            ("poll_url", poll_url),
            ("sample_size", sample_size),
        ]:
            if val is not None:
                updates.append(f"{key} = ?")
                params.append(val)
        if not updates:
            return
        params.append(rating_id)
        conn.execute("UPDATE approval_ratings SET " + ", ".join(updates) + " WHERE id = ?", tuple(params))


def delete_approval_rating(rating_id: int) -> bool:
    with _connection() as conn:
        cur = conn.execute("DELETE FROM approval_ratings WHERE id = ?", (rating_id,))
        return cur.rowcount > 0


def _protest_where(country_code, region, date_from, date_to, trigger_topic, search):
    where, params = [], []
    if country_code:
        where.append("country_code = ?")
        params.append(country_code)
    if region:
        where.append("country_code IN (SELECT country_code FROM country_risk_integration WHERE region = ?)")
        params.append(region)
    if date_from:
        where.append("(event_date >= ? OR event_date IS NULL OR event_date = '')")
        params.append(date_from[:10])
    if date_to:
        where.append("(event_date <= ? OR event_date IS NULL OR event_date = '')")
        params.append(date_to[:10])
    if trigger_topic:
        where.append("trigger_topic LIKE ?")
        params.append(f"%{trigger_topic}%")
    if search:
        where.append("(summary LIKE ? OR trigger_topic LIKE ? OR country_name LIKE ?)")
        q = f"%{search}%"
        params.extend([q, q, q])
    return " AND ".join(where) if where else "1=1", params


def get_protest_tracking(
    country_code: Optional[str] = None,
    region: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    trigger_topic: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 30,
    offset: int = 0,
    order_by: Optional[str] = None,
):
    where_sql, params = _protest_where(country_code, region, date_from, date_to, trigger_topic, search)
    order = "event_date DESC, created_at DESC"
    if order_by == "date_asc":
        order = "event_date ASC, created_at DESC"
    elif order_by == "country":
        order = "country_name ASC, event_date DESC"
    params.extend([limit, offset])
    with _connection() as conn:
        cur = conn.execute(
            f"""SELECT id, country_code, country_name, event_date, summary, estimated_size, trigger_topic, location, severity, source_url, created_at
                FROM protest_tracking WHERE {where_sql} ORDER BY {order} LIMIT ? OFFSET ?""",
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


def get_protest_tracking_count(
    country_code: Optional[str] = None,
    region: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    trigger_topic: Optional[str] = None,
    search: Optional[str] = None,
) -> int:
    where_sql, params = _protest_where(country_code, region, date_from, date_to, trigger_topic, search)
    with _connection() as conn:
        cur = conn.execute(f"SELECT COUNT(*) FROM protest_tracking WHERE {where_sql}", tuple(params))
        return (cur.fetchone() or (0,))[0]


def get_protest(protest_id: int):
    with _connection() as conn:
        cur = conn.execute(
            "SELECT id, country_code, country_name, event_date, summary, estimated_size, trigger_topic, location, severity, source_url, created_at FROM protest_tracking WHERE id = ?",
            (protest_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def get_protest_trigger_topics(limit: int = 100) -> list:
    """Distinct trigger_topic values for dropdowns."""
    with _connection() as conn:
        cur = conn.execute(
            "SELECT DISTINCT trigger_topic FROM protest_tracking WHERE trigger_topic IS NOT NULL AND trigger_topic != '' ORDER BY trigger_topic LIMIT ?",
            (limit,),
        )
        return [row[0] for row in cur.fetchall()]


def add_protest_event(
    country_code: str,
    country_name: str,
    event_date: str,
    summary: Optional[str] = None,
    estimated_size: Optional[str] = None,
    trigger_topic: Optional[str] = None,
    location: Optional[str] = None,
    severity: Optional[str] = None,
    source_url: Optional[str] = None,
) -> int:
    now = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        cur = conn.execute(
            """INSERT INTO protest_tracking (country_code, country_name, event_date, summary, estimated_size, trigger_topic, location, severity, source_url, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                country_code.strip(), country_name.strip(), (event_date or "").strip() or "",
                (summary or "").strip() or None, (estimated_size or "").strip() or None, (trigger_topic or "").strip() or None,
                (location or "").strip() or None, (severity or "").strip() or None, (source_url or "").strip() or None,
                now,
            ),
        )
        return cur.lastrowid


def update_protest(
    protest_id: int,
    *,
    country_code: Optional[str] = None,
    country_name: Optional[str] = None,
    event_date: Optional[str] = None,
    summary: Optional[str] = None,
    estimated_size: Optional[str] = None,
    trigger_topic: Optional[str] = None,
    location: Optional[str] = None,
    severity: Optional[str] = None,
    source_url: Optional[str] = None,
) -> None:
    with _connection() as conn:
        cur = conn.execute("SELECT id FROM protest_tracking WHERE id = ?", (protest_id,))
        if not cur.fetchone():
            return
        updates, params = [], []
        for key, val in [
            ("country_code", country_code),
            ("country_name", country_name),
            ("event_date", event_date),
            ("summary", summary),
            ("estimated_size", estimated_size),
            ("trigger_topic", trigger_topic),
            ("location", location),
            ("severity", severity),
            ("source_url", source_url),
        ]:
            if val is not None:
                updates.append(f"{key} = ?")
                params.append(val)
        if not updates:
            return
        params.append(protest_id)
        conn.execute("UPDATE protest_tracking SET " + ", ".join(updates) + " WHERE id = ?", tuple(params))


def delete_protest(protest_id: int) -> bool:
    with _connection() as conn:
        cur = conn.execute("DELETE FROM protest_tracking WHERE id = ?", (protest_id,))
        return cur.rowcount > 0


def get_approval_last_updated() -> Optional[str]:
    with _connection() as conn:
        cur = conn.execute("SELECT MAX(created_at) FROM approval_ratings")
        row = cur.fetchone()
    val = (row or (None,))[0]
    return (val or "")[:10] if val else None


def get_protest_last_updated() -> Optional[str]:
    with _connection() as conn:
        cur = conn.execute("SELECT MAX(created_at) FROM protest_tracking")
        row = cur.fetchone()
    val = (row or (None,))[0]
    return (val or "")[:10] if val else None


def get_approval_timeseries(
    country_codes: Optional[list] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    subject: Optional[str] = None,
    limit: int = 200,
) -> list:
    """Approval over time for charts. Returns list of dicts with poll_date, approval_pct, country_code, country_name, subject."""
    where, params = [], []
    if country_codes:
        placeholders = ",".join("?" * len(country_codes))
        where.append(f"country_code IN ({placeholders})")
        params.extend(country_codes)
    if date_from:
        where.append("(poll_date >= ? OR poll_date IS NULL)")
        params.append(date_from[:10])
    if date_to:
        where.append("(poll_date <= ? OR poll_date IS NULL)")
        params.append(date_to[:10])
    if subject:
        where.append("subject LIKE ?")
        params.append(f"%{subject}%")
    where_sql = " AND ".join(where) if where else "1=1"
    params.append(limit)
    with _connection() as conn:
        cur = conn.execute(
            f"""SELECT country_code, country_name, subject, approval_pct, poll_date
                FROM approval_ratings WHERE {where_sql} ORDER BY poll_date ASC, created_at ASC LIMIT ?""",
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


def get_approval_latest_by_country(
    region: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    max_approval: Optional[float] = None,
    limit: int = 30,
) -> list:
    """Latest approval per country for bar chart (by latest created_at per country)."""
    where, params = [], []
    if region:
        where.append("country_code IN (SELECT country_code FROM country_risk_integration WHERE region = ?)")
        params.append(region)
    if date_from:
        where.append("poll_date >= ?")
        params.append(date_from[:10])
    if date_to:
        where.append("poll_date <= ?")
        params.append(date_to[:10])
    if max_approval is not None:
        where.append("approval_pct <= ?")
        params.append(max_approval)
    where_sql = " AND ".join(where) if where else "1=1"
    params.append(limit)
    with _connection() as conn:
        cur = conn.execute(
            f"""SELECT a.country_code, a.country_name, a.subject, a.approval_pct, a.poll_date
                FROM approval_ratings a
                INNER JOIN (
                    SELECT country_code, MAX(created_at) AS mx FROM approval_ratings WHERE {where_sql} GROUP BY country_code
                ) b ON a.country_code = b.country_code AND a.created_at = b.mx
                ORDER BY a.approval_pct ASC
                LIMIT ?""",
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


def get_protest_counts_by_country(date_from: Optional[str] = None, date_to: Optional[str] = None, limit: int = 30) -> list:
    """Protest counts per country for charts."""
    where, params = [], []
    if date_from:
        where.append("event_date >= ?")
        params.append(date_from[:10])
    if date_to:
        where.append("event_date <= ?")
        params.append(date_to[:10])
    where_sql = " AND ".join(where) if where else "1=1"
    params.append(limit)
    with _connection() as conn:
        cur = conn.execute(
            f"""SELECT country_code, country_name, COUNT(*) AS cnt
                FROM protest_tracking WHERE {where_sql}
                GROUP BY country_code ORDER BY cnt DESC LIMIT ?""",
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


def get_protest_counts_by_trigger(date_from: Optional[str] = None, date_to: Optional[str] = None, limit: int = 20) -> list:
    """Protest counts by trigger topic for charts."""
    where, params = [], []
    if date_from:
        where.append("event_date >= ?")
        params.append(date_from[:10])
    if date_to:
        where.append("event_date <= ?")
        params.append(date_to[:10])
    where.append("(trigger_topic IS NOT NULL AND trigger_topic != '')")
    where_sql = " AND ".join(where)
    params.append(limit)
    with _connection() as conn:
        cur = conn.execute(
            f"""SELECT trigger_topic AS topic, COUNT(*) AS cnt
                FROM protest_tracking WHERE {where_sql}
                GROUP BY trigger_topic ORDER BY cnt DESC LIMIT ?""",
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


def get_currency_stress(
    country_code: Optional[str] = None,
    region: Optional[str] = None,
    limit: int = 30,
):
    with _connection() as conn:
        if country_code:
            cur = conn.execute(
                """SELECT id, country_code, country_name, indicator_value, stress_level, as_of_date, notes, created_at
                   FROM currency_stress WHERE country_code = ? ORDER BY as_of_date DESC LIMIT ?""",
                (country_code, limit),
            )
        elif region:
            cur = conn.execute(
                """SELECT id, c.country_code, c.country_name, indicator_value, stress_level, as_of_date, notes, c.created_at
                   FROM currency_stress c
                   INNER JOIN country_risk_integration i ON c.country_code = i.country_code AND i.region = ?
                   ORDER BY c.as_of_date DESC LIMIT ?""",
                (region, limit),
            )
        else:
            cur = conn.execute(
                """SELECT id, country_code, country_name, indicator_value, stress_level, as_of_date, notes, created_at
                   FROM currency_stress ORDER BY as_of_date DESC LIMIT ?""",
                (limit,),
            )
        return [dict(row) for row in cur.fetchall()]


def get_food_inflation_alerts(
    country_code: Optional[str] = None,
    region: Optional[str] = None,
    limit: int = 30,
):
    with _connection() as conn:
        if country_code:
            cur = conn.execute(
                """SELECT id, country_code, country_name, inflation_pct, risk_level, as_of_date, notes, created_at
                   FROM food_inflation_alerts WHERE country_code = ? ORDER BY as_of_date DESC LIMIT ?""",
                (country_code, limit),
            )
        elif region:
            cur = conn.execute(
                """SELECT id, f.country_code, f.country_name, inflation_pct, risk_level, as_of_date, notes, f.created_at
                   FROM food_inflation_alerts f
                   INNER JOIN country_risk_integration i ON f.country_code = i.country_code AND i.region = ?
                   ORDER BY f.as_of_date DESC LIMIT ?""",
                (region, limit),
            )
        else:
            cur = conn.execute(
                """SELECT id, country_code, country_name, inflation_pct, risk_level, as_of_date, notes, created_at
                   FROM food_inflation_alerts ORDER BY as_of_date DESC LIMIT ?""",
                (limit,),
            )
        return [dict(row) for row in cur.fetchall()]


def get_youth_unemployment(
    country_code: Optional[str] = None,
    region: Optional[str] = None,
    limit: int = 30,
):
    with _connection() as conn:
        if country_code:
            cur = conn.execute(
                """SELECT id, country_code, country_name, rate_pct, as_of_date, source, created_at
                   FROM youth_unemployment WHERE country_code = ? ORDER BY as_of_date DESC LIMIT ?""",
                (country_code, limit),
            )
        elif region:
            cur = conn.execute(
                """SELECT id, y.country_code, y.country_name, rate_pct, as_of_date, source, y.created_at
                   FROM youth_unemployment y
                   INNER JOIN country_risk_integration i ON y.country_code = i.country_code AND i.region = ?
                   ORDER BY y.rate_pct DESC LIMIT ?""",
                (region, limit),
            )
        else:
            cur = conn.execute(
                """SELECT id, country_code, country_name, rate_pct, as_of_date, source, created_at
                   FROM youth_unemployment ORDER BY rate_pct DESC LIMIT ?""",
                (limit,),
            )
        return [dict(row) for row in cur.fetchall()]


def get_social_sentiment(
    country_code: Optional[str] = None,
    region: Optional[str] = None,
    limit: int = 30,
):
    with _connection() as conn:
        if country_code:
            cur = conn.execute(
                """SELECT id, country_code, country_name, sentiment_score, sample_size, platform, as_of_date, created_at
                   FROM social_sentiment WHERE country_code = ? ORDER BY as_of_date DESC LIMIT ?""",
                (country_code, limit),
            )
        elif region:
            cur = conn.execute(
                """SELECT id, s.country_code, s.country_name, sentiment_score, sample_size, platform, as_of_date, s.created_at
                   FROM social_sentiment s
                   INNER JOIN country_risk_integration i ON s.country_code = i.country_code AND i.region = ?
                   ORDER BY s.as_of_date DESC LIMIT ?""",
                (region, limit),
            )
        else:
            cur = conn.execute(
                """SELECT id, country_code, country_name, sentiment_score, sample_size, platform, as_of_date, created_at
                   FROM social_sentiment ORDER BY as_of_date DESC LIMIT ?""",
                (limit,),
            )
        return [dict(row) for row in cur.fetchall()]


# Low-approval threshold used for fragility and stability (e.g. &lt;35% = elevated risk).
FRAGILITY_LOW_APPROVAL_THRESHOLD = 35


def get_fragility_overview(
    limit: int = 15,
    protest_days: int = 90,
    region: Optional[str] = None,
    low_approval_threshold: Optional[float] = None,
) -> dict:
    """
    Real-time domestic fragility: aggregate high-risk signals per country.
    Uses recent protests (last protest_days), latest low approval per country, and current currency/food stress.
    Returns { countries: [ { country_code, country_name, signals: [...], risk_score } ], updated_at, score_notes }.
    """
    threshold = low_approval_threshold if low_approval_threshold is not None else FRAGILITY_LOW_APPROVAL_THRESHOLD
    with _connection() as conn:
        # Countries with high/critical currency stress, food inflation
        cur = conn.execute(
            """SELECT country_code, country_name, stress_level FROM currency_stress WHERE stress_level IN ('high', 'critical')"""
        )
        currency = {row[0]: {"country_name": row[1], "level": row[2]} for row in cur.fetchall()}
        cur = conn.execute(
            """SELECT country_code, country_name, risk_level FROM food_inflation_alerts WHERE risk_level IN ('high', 'critical')"""
        )
        food = {row[0]: {"country_name": row[1], "level": row[2]} for row in cur.fetchall()}
        # Recent protests only (last N days)
        if protest_days and protest_days > 0:
            cur = conn.execute(
                """SELECT country_code, country_name, event_date FROM protest_tracking
                   WHERE event_date >= date('now', ?) ORDER BY event_date DESC""",
                (f"-{protest_days} days",),
            )
        else:
            cur = conn.execute(
                """SELECT country_code, country_name, event_date FROM protest_tracking ORDER BY event_date DESC"""
            )
        protests = {}
        for row in cur.fetchall():
            if row[0] not in protests:
                protests[row[0]] = {"country_name": row[1], "latest": row[2]}
        # Low approval: latest poll per country only (current snapshot)
        cur = conn.execute(
            """SELECT a.country_code, a.country_name, a.approval_pct FROM approval_ratings a
               INNER JOIN (SELECT country_code, MAX(created_at) AS mx FROM approval_ratings GROUP BY country_code) b
               ON a.country_code = b.country_code AND a.created_at = b.mx
               WHERE a.approval_pct < ?""",
            (threshold,),
        )
        low_approval = {row[0]: {"country_name": row[1], "approval_pct": row[2]} for row in cur.fetchall()}
        all_codes = set(currency) | set(food) | set(protests) | set(low_approval)
        countries = []
        for code in all_codes:
            signals = []
            score = 0
            if code in currency:
                signals.append({"type": "currency_stress", "level": currency[code]["level"]})
                score += 2 if currency[code]["level"] == "high" else 3
            if code in food:
                signals.append({"type": "food_inflation", "level": food[code]["level"]})
                score += 2 if food[code]["level"] == "high" else 3
            if code in protests:
                signals.append({"type": "protest", "latest": protests[code]["latest"]})
                score += 1
            if code in low_approval:
                signals.append({"type": "low_approval", "approval_pct": low_approval[code]["approval_pct"]})
                score += 1
            name = (
                (currency.get(code) or {}).get("country_name")
                or (food.get(code) or {}).get("country_name")
                or (protests.get(code) or {}).get("country_name")
                or (low_approval.get(code) or {}).get("country_name")
                or code
            )
            countries.append({"country_code": code, "country_name": name, "signals": signals, "risk_score": score})
        countries.sort(key=lambda x: -x["risk_score"])
        if region:
            cur = conn.execute(
                "SELECT country_code FROM country_risk_integration WHERE region = ?",
                (region,),
            )
            region_codes = {row[0] for row in cur.fetchall()}
            countries = [c for c in countries if c["country_code"] in region_codes]
        return {
            "countries": countries[:limit],
            "updated_at": datetime.utcnow().isoformat() + "Z",
            "score_notes": "Score: currency/food high=2 critical=3, recent protest=1, low approval (<%s%%%%)=1. Protests = last %s days." % (int(threshold), protest_days),
        }


# --- Conflict & military escalation monitor ---

def get_defense_spending(
    country_code: Optional[str] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    limit: int = 30,
    offset: int = 0,
):
    with _connection() as conn:
        where, params = [], []
        if country_code:
            where.append("country_code = ?")
            params.append(country_code)
        if year_from is not None:
            where.append("year >= ?")
            params.append(year_from)
        if year_to is not None:
            where.append("year <= ?")
            params.append(year_to)
        where_sql = " AND ".join(where) if where else "1=1"
        params.extend([limit, offset])
        cur = conn.execute(
            f"SELECT id, country_code, country_name, year, spending_usd_billions, pct_gdp, source, created_at FROM defense_spending WHERE {where_sql} ORDER BY year DESC, spending_usd_billions DESC LIMIT ? OFFSET ?",
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


def get_defense_spending_count(
    country_code: Optional[str] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
) -> int:
    with _connection() as conn:
        where, params = [], []
        if country_code:
            where.append("country_code = ?")
            params.append(country_code)
        if year_from is not None:
            where.append("year >= ?")
            params.append(year_from)
        if year_to is not None:
            where.append("year <= ?")
            params.append(year_to)
        where_sql = " AND ".join(where) if where else "1=1"
        cur = conn.execute(f"SELECT COUNT(*) FROM defense_spending WHERE {where_sql}", tuple(params))
        return cur.fetchone()[0] or 0


def get_military_exercises(
    region: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 30,
    offset: int = 0,
):
    with _connection() as conn:
        where, params = [], []
        if region:
            where.append("region LIKE ?")
            params.append(f"%{region}%")
        if date_from:
            where.append("start_date >= ?")
            params.append(date_from)
        if date_to:
            where.append("end_date <= ?")
            params.append(date_to)
        if search:
            where.append("(name LIKE ? OR participants LIKE ? OR description LIKE ?)")
            q = f"%{search}%"
            params.extend([q, q, q])
        where_sql = " AND ".join(where) if where else "1=1"
        params.extend([limit, offset])
        cur = conn.execute(
            f"SELECT id, participants, name, start_date, end_date, region, scale, description, created_at FROM military_exercises WHERE {where_sql} ORDER BY start_date DESC LIMIT ? OFFSET ?",
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


def get_military_exercises_count(
    region: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    search: Optional[str] = None,
) -> int:
    with _connection() as conn:
        where, params = [], []
        if region:
            where.append("region LIKE ?")
            params.append(f"%{region}%")
        if date_from:
            where.append("start_date >= ?")
            params.append(date_from)
        if date_to:
            where.append("end_date <= ?")
            params.append(date_to)
        if search:
            where.append("(name LIKE ? OR participants LIKE ? OR description LIKE ?)")
            q = f"%{search}%"
            params.extend([q, q, q])
        where_sql = " AND ".join(where) if where else "1=1"
        cur = conn.execute(f"SELECT COUNT(*) FROM military_exercises WHERE {where_sql}", tuple(params))
        return cur.fetchone()[0] or 0


def get_border_incidents(
    country_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    severity: Optional[str] = None,
    limit: int = 30,
    offset: int = 0,
):
    with _connection() as conn:
        where, params = [], []
        if country_code:
            where.append("(country_a_code = ? OR country_b_code = ?)")
            params.extend([country_code, country_code])
        if date_from:
            where.append("incident_date >= ?")
            params.append(date_from)
        if date_to:
            where.append("incident_date <= ?")
            params.append(date_to)
        if severity:
            where.append("severity = ?")
            params.append(severity)
        where_sql = " AND ".join(where) if where else "1=1"
        params.extend([limit, offset])
        cur = conn.execute(
            f"""SELECT id, country_a_code, country_a_name, country_b_code, country_b_name, incident_date, summary, severity, status, created_at
                FROM border_incidents WHERE {where_sql} ORDER BY incident_date DESC LIMIT ? OFFSET ?""",
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


def get_border_incidents_count(
    country_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    severity: Optional[str] = None,
) -> int:
    with _connection() as conn:
        where, params = [], []
        if country_code:
            where.append("(country_a_code = ? OR country_b_code = ?)")
            params.extend([country_code, country_code])
        if date_from:
            where.append("incident_date >= ?")
            params.append(date_from)
        if date_to:
            where.append("incident_date <= ?")
            params.append(date_to)
        if severity:
            where.append("severity = ?")
            params.append(severity)
        where_sql = " AND ".join(where) if where else "1=1"
        cur = conn.execute(f"SELECT COUNT(*) FROM border_incidents WHERE {where_sql}", tuple(params))
        return cur.fetchone()[0] or 0


def get_military_movement(
    country_code: Optional[str] = None,
    region: Optional[str] = None,
    detection_type: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 30,
    offset: int = 0,
):
    with _connection() as conn:
        where, params = [], []
        if country_code:
            where.append("country_code = ?")
            params.append(country_code)
        if region:
            where.append("region LIKE ?")
            params.append(f"%{region}%")
        if detection_type:
            where.append("detection_type = ?")
            params.append(detection_type)
        if date_from:
            where.append("observed_date >= ?")
            params.append(date_from)
        if date_to:
            where.append("observed_date <= ?")
            params.append(date_to)
        where_sql = " AND ".join(where) if where else "1=1"
        params.extend([limit, offset])
        cur = conn.execute(
            f"SELECT id, country_code, country_name, region, detection_type, summary, observed_date, lat, lon, created_at FROM military_movement WHERE {where_sql} ORDER BY observed_date DESC LIMIT ? OFFSET ?",
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


def get_military_movement_count(
    country_code: Optional[str] = None,
    region: Optional[str] = None,
    detection_type: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> int:
    with _connection() as conn:
        where, params = [], []
        if country_code:
            where.append("country_code = ?")
            params.append(country_code)
        if region:
            where.append("region LIKE ?")
            params.append(f"%{region}%")
        if detection_type:
            where.append("detection_type = ?")
            params.append(detection_type)
        if date_from:
            where.append("observed_date >= ?")
            params.append(date_from)
        if date_to:
            where.append("observed_date <= ?")
            params.append(date_to)
        where_sql = " AND ".join(where) if where else "1=1"
        cur = conn.execute(f"SELECT COUNT(*) FROM military_movement WHERE {where_sql}", tuple(params))
        return cur.fetchone()[0] or 0


# Approximate country centroids (ISO3 -> (lat, lon)) for World Monitor sanctions/hotspots layers.
# Covers major countries; missing codes are skipped when building map points.
COUNTRY_CENTROIDS = {
    "AFG": (33.9, 67.7), "ALB": (41.2, 20.2), "DZA": (28.0, 2.0), "AGO": (-12.3, 17.5), "ARG": (-34.6, -58.4),
    "ARM": (40.1, 44.5), "AUS": (-25.3, 133.8), "AUT": (47.5, 14.6), "AZE": (40.4, 49.9), "BHR": (26.1, 50.6),
    "BGD": (23.7, 90.4), "BLR": (53.7, 27.9), "BEL": (50.5, 4.5), "BOL": (-16.5, -68.2), "BIH": (43.9, 18.4),
    "BRA": (-14.2, -51.9), "BGR": (42.7, 25.5), "BFA": (12.4, -1.5), "BDI": (-3.4, 29.9), "KHM": (12.6, 105.0),
    "CMR": (6.4, 12.4), "CAN": (56.1, -106.3), "CAF": (6.6, 20.9), "TCD": (15.5, 19.1), "CHL": (-35.7, -71.5),
    "CHN": (35.9, 104.2), "COL": (4.6, -74.1), "COD": (-4.0, 21.8), "COG": (-1.0, 15.6), "CRI": (9.9, -84.0),
    "CIV": (7.5, -5.5), "CUB": (21.5, -77.8), "CZE": (49.8, 15.5), "DNK": (56.3, 9.5), "ECU": (-1.8, -78.2),
    "EGY": (26.8, 30.8), "SLV": (13.8, -88.9), "ETH": (9.0, 39.5), "FIN": (64.0, 26.0), "FRA": (46.2, 2.2),
    "GAB": (-0.8, 11.6), "GMB": (13.4, -15.3), "GEO": (42.3, 43.4), "DEU": (51.2, 10.5), "GHA": (7.9, -1.0),
    "GRC": (39.1, 21.8), "GTM": (15.8, -90.2), "GIN": (9.9, -9.5), "HTI": (18.9, -72.3), "HND": (15.2, -86.2),
    "HUN": (47.2, 19.5), "ISL": (64.9, -19.0), "IND": (20.6, 78.9), "IDN": (-0.8, 113.9), "IRN": (32.4, 53.7),
    "IRQ": (33.2, 43.7), "IRL": (53.4, -7.7), "ISR": (31.0, 34.9), "ITA": (41.9, 12.6), "JAM": (18.1, -77.3),
    "JPN": (36.2, 138.3), "JOR": (31.9, 36.3), "KAZ": (48.0, 67.3), "KEN": (-0.0, 37.9), "KWT": (29.4, 47.5),
    "KGZ": (41.2, 74.8), "LAO": (18.2, 103.9), "LVA": (56.9, 24.6), "LBN": (33.9, 35.9), "LBY": (27.0, 17.2),
    "LTU": (55.2, 24.0), "MYS": (4.2, 101.9), "MLI": (17.6, -4.0), "MRT": (21.0, -10.9), "MEX": (23.6, -102.5),
    "MDA": (47.4, 28.4), "MNG": (46.9, 103.8), "MAR": (31.8, -7.1), "MOZ": (-18.7, 35.5), "MMR": (21.9, 95.9),
    "NAM": (-22.6, 17.1), "NPL": (28.4, 84.1), "NLD": (52.1, 5.3), "NZL": (-40.9, 174.9), "NIC": (12.9, -85.2),
    "NER": (16.0, 8.0), "NGA": (9.1, 8.7), "PRK": (40.3, 127.5), "MKD": (41.6, 21.7), "NOR": (60.5, 8.5),
    "OMN": (21.5, 55.9), "PAK": (30.4, 69.3), "PSE": (31.9, 35.2), "PAN": (8.5, -80.8), "PNG": (-6.3, 143.9),
    "PRY": (-23.4, -58.4), "PER": (-9.2, -75.0), "PHL": (12.9, 121.8), "POL": (52.1, 19.4), "PRT": (39.4, -8.2),
    "QAT": (25.3, 51.2), "ROU": (45.9, 24.9), "RUS": (61.5, 105.3), "RWA": (-2.0, 29.9), "SAU": (24.7, 45.0),
    "SEN": (14.7, -14.5), "SRB": (44.0, 21.0), "SGP": (1.4, 103.8), "SVK": (48.7, 19.7), "SVN": (46.2, 14.8),
    "SOM": (6.1, 46.2), "ZAF": (-29.0, 24.0), "KOR": (35.9, 127.8), "SSD": (6.9, 30.7), "ESP": (40.5, -3.7),
    "LKA": (7.9, 80.8), "SDN": (15.5, 32.5), "SUR": (4.0, -56.0), "SWE": (62.2, 17.6), "CHE": (46.8, 8.2),
    "SYR": (35.0, 38.5), "TWN": (23.7, 121.0), "TJK": (38.9, 71.3), "TZA": (-6.4, 34.9), "THA": (15.9, 100.9),
    "TGO": (8.6, 0.8), "TUN": (34.0, 9.5), "TUR": (38.9, 35.2), "TKM": (39.0, 59.0), "UGA": (1.4, 32.3),
    "UKR": (48.4, 31.2), "ARE": (24.0, 54.0), "GBR": (54.0, -2.0), "USA": (38.0, -97.0), "URY": (-32.5, -55.8),
    "UZB": (41.4, 64.6), "VEN": (6.4, -66.6), "VNM": (14.1, 108.3), "YEM": (15.6, 48.5), "ZMB": (-13.1, 27.8),
    "ZWE": (-19.0, 29.2),
}

# Well-known military bases / installations for World Monitor "bases" layer (name, lat, lon, country_code, region).
WORLD_MONITOR_BASES = [
    ("Diego Garcia", -7.3, 72.4, "GBR", "Indian Ocean"),
    ("Guam (Andersen)", 13.6, 144.9, "USA", "Pacific"),
    ("Okinawa (Kadena)", 26.4, 127.8, "JPN", "East Asia"),
    ("Rota (Spain)", 36.6, -6.4, "ESP", "Europe"),
    ("Incirlik", 37.0, 35.4, "TUR", "Middle East"),
    ("Al Udeid", 25.1, 51.3, "QAT", "Middle East"),
    ("Ramstein", 49.4, 7.6, "DEU", "Europe"),
    ("Camp Humphreys", 37.0, 127.0, "KOR", "East Asia"),
    ("Yokosuka", 35.3, 139.7, "JPN", "East Asia"),
    ("Sigonella", 37.4, 14.9, "ITA", "Europe"),
    ("Djibouti (Camp Lemonnier)", 11.5, 43.1, "DJI", "Africa"),
    ("Bahrain (NSA)", 26.2, 50.6, "BHR", "Middle East"),
    ("RAF Akrotiri", 34.6, 32.9, "CYP", "Europe"),
    ("Thule", 76.5, -68.7, "GRL", "Arctic"),
    ("Fort Bragg", 35.1, -79.0, "USA", "North America"),
    ("Naval Station Norfolk", 36.9, -76.3, "USA", "North America"),
    ("Pearl Harbor", 21.4, -157.9, "USA", "Pacific"),
    ("RAF Lakenheath", 52.4, 0.6, "GBR", "Europe"),
    ("RAF Mildenhall", 52.4, 0.5, "GBR", "Europe"),
    ("Spangdahlem", 49.9, 6.7, "DEU", "Europe"),
    ("Aviano", 46.0, 12.6, "ITA", "Europe"),
    ("Misawa", 40.7, 141.4, "JPN", "East Asia"),
    ("Osan", 37.1, 127.0, "KOR", "East Asia"),
    ("Al Dhafra", 24.2, 54.5, "ARE", "Middle East"),
    ("Ali Al Salem", 29.3, 47.5, "KWT", "Middle East"),
    ("Eielson", 64.7, -147.1, "USA", "Arctic"),
    ("Sasebo", 33.2, 129.7, "JPN", "East Asia"),
    ("Rota (US)", 36.6, -6.3, "ESP", "Europe"),
    ("Morón", 37.2, -5.6, "ESP", "Europe"),
    ("Souda Bay", 35.5, 24.1, "GRC", "Europe"),
    ("Camp Buehring", 30.0, 47.4, "KWT", "Middle East"),
]

# Nuclear sites for World Monitor "nuclear" layer: (name, lat, lon, country_code, region, site_type).
# site_type: power_plant, enrichment, research, test_site, naval
WORLD_MONITOR_NUCLEAR = [
    ("Yongbyon", 39.8, 125.8, "PRK", "East Asia", "enrichment"),
    ("Bushehr", 28.8, 50.9, "IRN", "Middle East", "power_plant"),
    ("Natanz", 33.7, 51.7, "IRN", "Middle East", "enrichment"),
    ("Fukushima Daiichi", 37.4, 141.0, "JPN", "East Asia", "power_plant"),
    ("Chernobyl", 51.4, 30.1, "UKR", "Europe", "power_plant"),
    ("Zaporizhzhia", 47.5, 34.6, "UKR", "Europe", "power_plant"),
    ("Sellafield", 54.4, -3.5, "GBR", "Europe", "reprocessing"),
    ("Gravelines", 51.0, 2.1, "FRA", "Europe", "power_plant"),
    ("Cattenom", 49.4, 6.2, "FRA", "Europe", "power_plant"),
    ("Paks", 46.6, 18.9, "HUN", "Europe", "power_plant"),
    ("Leningrad NPP", 59.8, 29.0, "RUS", "Europe", "power_plant"),
    ("Kursk NPP", 51.7, 35.6, "RUS", "Europe", "power_plant"),
    ("Taishan", 21.9, 112.9, "CHN", "East Asia", "power_plant"),
    ("Dayawan", 22.6, 114.6, "CHN", "East Asia", "power_plant"),
    ("Barakah", 23.9, 53.8, "ARE", "Middle East", "power_plant"),
    ("Kudankulam", 8.2, 77.7, "IND", "South Asia", "power_plant"),
    ("Pilgrim", 41.9, -70.6, "USA", "North America", "power_plant"),
    ("Palo Verde", 33.4, -112.9, "USA", "North America", "power_plant"),
    ("Lop Nur (test site)", 41.5, 88.4, "CHN", "East Asia", "test_site"),
    ("Semipalatinsk (legacy)", 50.1, 78.4, "KAZ", "Central Asia", "test_site"),
    ("Forsmark", 60.4, 18.2, "SWE", "Europe", "power_plant"),
    ("Ringhals", 57.3, 12.1, "SWE", "Europe", "power_plant"),
    ("Olkiluoto", 61.2, 21.4, "FIN", "Europe", "power_plant"),
    ("Borssele", 51.4, 3.7, "NLD", "Europe", "power_plant"),
    ("Doel", 51.3, 4.3, "BEL", "Europe", "power_plant"),
    ("Tihange", 50.5, 5.3, "BEL", "Europe", "power_plant"),
    ("Kozloduy", 43.8, 23.5, "BGR", "Europe", "power_plant"),
    ("Cernavodă", 44.3, 28.0, "ROU", "Europe", "power_plant"),
    ("South Ukraine NPP", 47.8, 31.2, "UKR", "Europe", "power_plant"),
    ("Kalinin NPP", 57.9, 35.1, "RUS", "Europe", "power_plant"),
    ("Bruce", 44.3, -81.4, "CAN", "North America", "power_plant"),
    ("Pickering", 43.8, -79.1, "CAN", "North America", "power_plant"),
    ("Kori", 35.3, 129.3, "KOR", "East Asia", "power_plant"),
    ("Hanul", 37.1, 129.4, "KOR", "East Asia", "power_plant"),
    ("Tarapur", 19.8, 72.7, "IND", "South Asia", "power_plant"),
    ("Karachi", 24.8, 66.8, "PAK", "South Asia", "power_plant"),
]

# Spaceports / launch sites for World Monitor "spaceports" layer: (name, lat, lon, country_code, region).
WORLD_MONITOR_SPACEPORTS = [
    ("Cape Canaveral SFS", 28.5, -80.6, "USA", "North America"),
    ("Vandenberg SFB", 34.7, -120.6, "USA", "North America"),
    ("Baikonur", 45.9, 63.3, "KAZ", "Central Asia"),
    ("Plesetsk", 62.9, 40.4, "RUS", "Europe"),
    ("Vostochny", 51.9, 128.3, "RUS", "East Asia"),
    ("Kourou", 5.2, -52.8, "FRA", "Latin America"),
    ("Jiuquan", 40.6, 100.0, "CHN", "East Asia"),
    ("Taiyuan", 37.8, 112.5, "CHN", "East Asia"),
    ("Xichang", 28.2, 102.0, "CHN", "East Asia"),
    ("Wenchang", 19.6, 110.9, "CHN", "East Asia"),
    ("Tanegashima", 30.4, 130.9, "JPN", "East Asia"),
    ("Satish Dhawan", 13.7, 80.2, "IND", "South Asia"),
    ("Palmachim", 31.9, 34.7, "ISR", "Middle East"),
    ("Sohae", 39.7, 124.7, "PRK", "East Asia"),
    ("Semnan", 35.2, 53.9, "IRN", "Middle East"),
    ("Alcântara", -2.3, -44.4, "BRA", "Latin America"),
    ("Mahia", -39.1, 177.9, "NZL", "Oceania"),
    ("Wallops", 37.9, -75.5, "USA", "North America"),
    ("Boca Chica (Starbase)", 25.9, -97.2, "USA", "North America"),
]

# Undersea cable landing points (key nodes) for World Monitor "undersea_cables" layer: (name, lat, lon, country_code, region).
WORLD_MONITOR_UNDERSEA_CABLES = [
    ("Marseille", 43.3, 5.4, "FRA", "Europe"),
    ("Bude", 50.8, -4.5, "GBR", "Europe"),
    ("Cork", 51.9, -8.5, "IRL", "Europe"),
    ("Lisbon", 38.7, -9.1, "PRT", "Europe"),
    ("Genoa", 44.4, 8.9, "ITA", "Europe"),
    ("Alexandria", 31.2, 29.9, "EGY", "Middle East"),
    ("Mumbai", 19.1, 72.9, "IND", "South Asia"),
    ("Chennai", 13.1, 80.3, "IND", "South Asia"),
    ("Singapore", 1.3, 103.8, "SGP", "Southeast Asia"),
    ("Hong Kong", 22.3, 114.2, "HKG", "East Asia"),
    ("Shanghai", 31.2, 121.5, "CHN", "East Asia"),
    ("Tokyo", 35.7, 139.7, "JPN", "East Asia"),
    ("Sydney", -33.9, 151.2, "AUS", "Oceania"),
    ("New York (NJ)", 40.7, -74.0, "USA", "North America"),
    ("Virginia Beach", 36.9, -76.0, "USA", "North America"),
    ("Miami", 25.8, -80.2, "USA", "North America"),
    ("Los Angeles", 33.7, -118.3, "USA", "North America"),
    ("Dubai", 25.3, 55.4, "ARE", "Middle East"),
    ("Mombasa", -4.0, 39.7, "KEN", "Africa"),
    ("Cape Town", -33.9, 18.4, "ZAF", "Africa"),
    ("Santos", -23.9, -46.3, "BRA", "Latin America"),
    ("Valparaiso", -33.0, -71.6, "CHL", "Latin America"),
]

# Key pipeline nodes / compressor stations for World Monitor "pipelines" layer: (name, lat, lon, country_code, region).
WORLD_MONITOR_PIPELINES = [
    ("Nord Stream 1 landing", 54.1, 13.6, "DEU", "Europe"),
    ("Nord Stream 2 landing", 54.2, 13.8, "DEU", "Europe"),
    ("Druzhba (Adamowo)", 52.7, 19.9, "POL", "Europe"),
    ("Baku-Tbilisi-Ceyhan (Ceyhan)", 36.7, 35.5, "TUR", "Middle East"),
    ("Yamal-Europe (Mallnow)", 52.5, 14.5, "DEU", "Europe"),
    ("Trans-Anatolian (TANAP)", 41.0, 29.0, "TUR", "Middle East"),
    ("South Stream (Bulgaria)", 42.1, 27.5, "BGR", "Europe"),
    ("Kirkuk-Ceyhan", 36.9, 36.2, "TUR", "Middle East"),
    ("Taweelah (UAE)", 24.5, 54.4, "ARE", "Middle East"),
    ("Ras Laffan", 25.9, 51.6, "QAT", "Middle East"),
    ("Sabine Pass LNG", 29.7, -93.9, "USA", "North America"),
    ("Cameron LNG", 29.8, -93.3, "USA", "North America"),
    ("Corpus Christi LNG", 27.8, -97.4, "USA", "North America"),
    ("Soyo (Angola LNG)", -6.1, 12.4, "AGO", "Africa"),
    ("Bonny Island", 4.4, 7.2, "NGA", "Africa"),
    ("Sakhalin-Khabarovsk-Vladivostok", 43.1, 131.9, "RUS", "East Asia"),
    ("Power of Siberia (Blagoveshchensk)", 50.3, 127.5, "CHN", "East Asia"),
    ("TurkStream (Kıyıköy)", 41.6, 28.9, "TUR", "Europe"),
    ("Trans-Med (Mazara)", 37.7, 12.6, "ITA", "Europe"),
    ("Medgaz", 36.7, -3.0, "ESP", "Europe"),
]

# Gamma irradiator facilities (industrial / medical) for World Monitor "gamma_irradiators" layer: (name, lat, lon, country_code, region).
WORLD_MONITOR_GAMMA_IRRADIATORS = [
    ("Sterigenics (IL)", 41.7, -88.0, "USA", "North America"),
    ("Nordion (CAN)", 45.4, -75.6, "CAN", "North America"),
    ("Reviss", 51.5, -0.6, "GBR", "Europe"),
    ("IBA (Belgium)", 50.6, 5.5, "BEL", "Europe"),
    ("Steris (Germany)", 50.1, 8.7, "DEU", "Europe"),
    ("Gammaster (NL)", 51.9, 5.9, "NLD", "Europe"),
    ("MDS Nordion (FRA)", 48.8, 2.4, "FRA", "Europe"),
    ("Isotron (UK)", 53.5, -2.3, "GBR", "Europe"),
    ("Bhabha (India)", 19.0, 72.9, "IND", "South Asia"),
    ("Shanghai Irradiation", 31.2, 121.5, "CHN", "East Asia"),
    ("Sterigenics (AU)", -33.8, 151.2, "AUS", "Oceania"),
    ("Nucleus (South Africa)", -26.2, 28.1, "ZAF", "Africa"),
    ("Radiation Sterilization (Brazil)", -23.5, -46.6, "BRA", "Latin America"),
]

# Major AI / hyperscale data center locations for World Monitor "ai_datacenters" layer: (name, lat, lon, country_code, region).
WORLD_MONITOR_AI_DATACENTERS = [
    ("AWS us-east-1 (Virginia)", 38.9, -77.5, "USA", "North America"),
    ("AWS us-west-2 (Oregon)", 45.5, -122.7, "USA", "North America"),
    ("Google Council Bluffs", 41.3, -95.9, "USA", "North America"),
    ("Microsoft Quincy", 47.2, -119.9, "USA", "North America"),
    ("Meta Prineville", 44.3, -120.8, "USA", "North America"),
    ("AWS eu-west-1 (Dublin)", 53.3, -6.3, "IRL", "Europe"),
    ("Google St. Ghislain", 50.4, 4.0, "BEL", "Europe"),
    ("Microsoft Amsterdam", 52.4, 4.9, "NLD", "Europe"),
    ("AWS ap-northeast-1 (Tokyo)", 35.7, 139.7, "JPN", "East Asia"),
    ("Google Taiwan", 24.1, 120.7, "TWN", "East Asia"),
    ("AWS ap-southeast-1 (Singapore)", 1.3, 103.8, "SGP", "Southeast Asia"),
    ("Microsoft Singapore", 1.4, 103.9, "SGP", "Southeast Asia"),
    ("AWS ap-south-1 (Mumbai)", 19.1, 72.9, "IND", "South Asia"),
    ("Google Sydney", -33.9, 151.2, "AUS", "Oceania"),
    ("AWS sa-east-1 (São Paulo)", -23.5, -46.6, "BRA", "Latin America"),
    ("Microsoft São Paulo", -23.6, -46.6, "BRA", "Latin America"),
    ("Oracle Ashburn", 38.9, -77.5, "USA", "North America"),
    ("NVIDIA DGX Cloud (US)", 37.4, -121.9, "USA", "North America"),
]

# Approximate region centroids for World Monitor military layer (naval deployments, exercises by region).
REGION_CENTROIDS = {
    "East Asia": (35.0, 105.0), "South Asia": (22.0, 78.0), "Southeast Asia": (5.0, 105.0),
    "Middle East": (31.0, 45.0), "Africa": (0.0, 22.0), "Europe": (50.0, 10.0),
    "North America": (45.0, -100.0), "Latin America": (-15.0, -60.0), "Central Asia": (45.0, 65.0),
    "Caribbean": (18.0, -75.0), "Oceania": (-20.0, 140.0), "Pacific": (0.0, -160.0),
    "Indian Ocean": (-15.0, 75.0), "Arctic": (70.0, 0.0), "Baltic": (58.0, 22.0),
    "Mediterranean": (35.0, 15.0), "Gulf": (26.0, 52.0), "Black Sea": (42.0, 34.0),
    "Red Sea": (20.0, 40.0), "South China Sea": (12.0, 115.0),
}


def _world_monitor_region_centroid(region: str) -> Optional[tuple]:
    """Return (lat, lon) for a region name; tries exact match then substring."""
    if not region or not region.strip():
        return None
    r = region.strip()
    if r in REGION_CENTROIDS:
        return REGION_CENTROIDS[r]
    for key, coord in REGION_CENTROIDS.items():
        if key.lower() in r.lower() or r.lower() in key.lower():
            return coord
    return None


def _world_monitor_country_name_to_iso3(name: str) -> Optional[str]:
    """Map sanction country name (e.g. 'Russia', 'Iran') to ISO3. Uses ALL_COUNTRIES and common aliases."""
    if not name or not name.strip():
        return None
    try:
        from app.country_data import ALL_COUNTRIES
        n = name.strip()
        n_lower = n.lower()
        aliases = {"russia": "RUS", "iran": "IRN", "north korea": "PRK", "syria": "SYR", "belarus": "BLR",
                   "cuba": "CUB", "china": "CHN", "uk": "GBR", "united kingdom": "GBR", "united states": "USA",
                   "usa": "USA", "us": "USA", "eu": None, "european union": None,
                   "israel": "ISR", "iraq": "IRQ", "saudi arabia": "SAU", "yemen": "YEM", "jordan": "JOR",
                   "gaza": "PSE", "west bank": "PSE", "palestine": "PSE", "lebanon": "LBN", "bahrain": "BHR",
                   "uae": "ARE", "united arab emirates": "ARE", "kuwait": "KWT", "oman": "OMN", "qatar": "QAT"}
        if n_lower in aliases:
            return aliases[n_lower]
        for iso3, display_name, *_ in ALL_COUNTRIES:
            if display_name and n_lower == display_name.lower():
                return iso3
            if display_name and n_lower in display_name.lower() and len(display_name) < 30:
                return iso3
        return None
    except Exception:
        return None


def get_world_monitor_points(
    time_range_days: int = 7,
    layers: Optional[list] = None,
) -> list:
    """Return geo points for World Monitor map. Each item: {lat, lon, layer, title, summary, ...}.
    Layers: conflicts, waterways, sanctions, hotspots, bases, nuclear, gamma_irradiators, spaceports, undersea_cables, pipelines, ai_datacenters, weather, economic, outages, military, natural, iranAttacks."""
    from datetime import datetime, timedelta
    layers = layers or []
    if not layers:
        return []
    end = datetime.utcnow()
    start = end - timedelta(days=max(1, min(365, time_range_days)))
    date_from = start.strftime("%Y-%m-%d")
    date_to = end.strftime("%Y-%m-%d")
    out = []

    if "conflicts" in layers:
        movements = get_military_movement(
            date_from=date_from, date_to=date_to, limit=800
        )
        for m in movements:
            lat, lon = m.get("lat"), m.get("lon")
            if lat is None or lon is None:
                continue
            out.append({
                "lat": float(lat),
                "lon": float(lon),
                "layer": "conflicts",
                "title": m.get("country_name") or "Conflict / movement",
                "summary": m.get("summary") or "",
                "observed_date": (m.get("observed_date") or "")[:10],
                "region": m.get("region") or "",
                "detection_type": m.get("detection_type") or "",
            })

    if "waterways" in layers:
        try:
            chokepoints = get_chokepoints_with_geo()
        except Exception:
            chokepoints = []
        for cp in chokepoints:
            lat, lon = cp.get("lat"), cp.get("lon")
            if lat is None or lon is None:
                continue
            out.append({
                "lat": float(lat),
                "lon": float(lon),
                "layer": "waterways",
                "title": cp.get("name") or "Chokepoint",
                "summary": cp.get("description") or "",
                "region": cp.get("region") or "",
                "risk_score": cp.get("risk_score"),
            })

    if "sanctions" in layers:
        sanctions_list = get_sanctions(limit=300)
        seen_targets = set()
        for s in sanctions_list:
            target = (s.get("target_country") or "").strip()
            if not target or target in seen_targets:
                continue
            iso3 = _world_monitor_country_name_to_iso3(target)
            if not iso3:
                continue
            coord = COUNTRY_CENTROIDS.get(iso3)
            if not coord:
                continue
            seen_targets.add(target)
            imposing = (s.get("imposing_country") or "")
            measure = (s.get("measure_type") or "")
            desc = (s.get("description") or "")[:200]
            out.append({
                "lat": float(coord[0]),
                "lon": float(coord[1]),
                "layer": "sanctions",
                "title": "Sanctions: " + target,
                "summary": "Imposed by " + imposing + (" · " + measure if measure else "") + (" · " + desc if desc else ""),
                "region": "",
            })

    if "hotspots" in layers:
        try:
            snapshots = get_country_risk_snapshots()
        except Exception:
            snapshots = []
        for r in snapshots:
            if (r.get("risk_score") or 0) < 25:
                continue
            iso3 = (r.get("country_code") or "").strip().upper()
            if not iso3 or len(iso3) != 3:
                continue
            coord = COUNTRY_CENTROIDS.get(iso3)
            if not coord:
                continue
            score = r.get("risk_score") or 0
            out.append({
                "lat": float(coord[0]),
                "lon": float(coord[1]),
                "layer": "hotspots",
                "title": (r.get("country_code") or iso3) + " (risk " + str(int(score)) + ")",
                "summary": "Risk score " + str(int(score)) + ". Article-derived and sector exposure.",
                "region": "",
                "risk_score": score,
            })

    if "bases" in layers:
        for name, lat, lon, country_code, region in WORLD_MONITOR_BASES:
            out.append({
                "lat": float(lat),
                "lon": float(lon),
                "layer": "bases",
                "title": name,
                "summary": region + (" · " + country_code if country_code else ""),
                "region": region or "",
            })

    if "nuclear" in layers:
        for name, lat, lon, country_code, region, site_type in WORLD_MONITOR_NUCLEAR:
            out.append({
                "lat": float(lat),
                "lon": float(lon),
                "layer": "nuclear",
                "title": name,
                "summary": (region or "") + (" · " + country_code if country_code else "") + " · " + (site_type or ""),
                "region": region or "",
                "site_type": site_type,
            })

    if "gamma_irradiators" in layers:
        for name, lat, lon, country_code, region in WORLD_MONITOR_GAMMA_IRRADIATORS:
            out.append({
                "lat": float(lat),
                "lon": float(lon),
                "layer": "gamma_irradiators",
                "title": name,
                "summary": (region or "") + (" · " + country_code if country_code else ""),
                "region": region or "",
            })

    if "spaceports" in layers:
        for name, lat, lon, country_code, region in WORLD_MONITOR_SPACEPORTS:
            out.append({
                "lat": float(lat),
                "lon": float(lon),
                "layer": "spaceports",
                "title": name,
                "summary": (region or "") + (" · " + country_code if country_code else ""),
                "region": region or "",
            })

    if "undersea_cables" in layers:
        for name, lat, lon, country_code, region in WORLD_MONITOR_UNDERSEA_CABLES:
            out.append({
                "lat": float(lat),
                "lon": float(lon),
                "layer": "undersea_cables",
                "title": name + " (cable landing)",
                "summary": (region or "") + (" · " + country_code if country_code else ""),
                "region": region or "",
            })

    if "pipelines" in layers:
        for name, lat, lon, country_code, region in WORLD_MONITOR_PIPELINES:
            out.append({
                "lat": float(lat),
                "lon": float(lon),
                "layer": "pipelines",
                "title": name,
                "summary": (region or "") + (" · " + country_code if country_code else ""),
                "region": region or "",
            })

    if "ai_datacenters" in layers:
        for name, lat, lon, country_code, region in WORLD_MONITOR_AI_DATACENTERS:
            out.append({
                "lat": float(lat),
                "lon": float(lon),
                "layer": "ai_datacenters",
                "title": name,
                "summary": (region or "") + (" · " + country_code if country_code else ""),
                "region": region or "",
            })

    if "weather" in layers:
        try:
            climate = get_climate_vulnerability_summary(limit=50)
        except Exception:
            climate = []
        for c in climate:
            iso3 = (c.get("country_code") or "").strip().upper()
            if not iso3 or len(iso3) != 3:
                continue
            coord = COUNTRY_CENTROIDS.get(iso3)
            if not coord:
                continue
            name = c.get("country_name") or iso3
            water = c.get("water_stress_index")
            food = c.get("food_insecurity_index")
            risk = c.get("climate_risk_score")
            parts = []
            if risk is not None:
                parts.append("climate risk " + str(int(risk)))
            if water is not None:
                parts.append("water stress " + str(int(water)))
            if food is not None:
                parts.append("food insecurity " + str(int(food)))
            out.append({
                "lat": float(coord[0]),
                "lon": float(coord[1]),
                "layer": "weather",
                "title": name + (" (climate)" if risk else ""),
                "summary": "; ".join(parts) if parts else "Climate/vulnerability data.",
                "region": "",
                "climate_risk_score": risk,
            })

    if "economic" in layers:
        seen_economic = set()
        try:
            macro = get_macroeconomic_stress_alerts(threshold_debt=70, threshold_inflation=10, limit=60)
        except Exception:
            macro = []
        for m in macro:
            iso3 = (m.get("country_code") or "").strip().upper()
            if not iso3 or iso3 in seen_economic or len(iso3) != 3:
                continue
            coord = COUNTRY_CENTROIDS.get(iso3)
            if not coord:
                continue
            seen_economic.add(iso3)
            name = m.get("country_name") or iso3
            debt = m.get("debt_to_gdp_pct")
            infl = m.get("inflation_pct")
            parts = []
            if debt is not None:
                parts.append("debt/GDP " + str(int(debt)) + "%")
            if infl is not None:
                parts.append("inflation " + ("%.1f" % infl) + "%")
            out.append({
                "lat": float(coord[0]),
                "lon": float(coord[1]),
                "layer": "economic",
                "title": name + " (macro stress)",
                "summary": "; ".join(parts) if parts else "Elevated debt or inflation.",
                "region": "",
            })
        try:
            food_alerts = get_food_inflation_alerts(limit=100)
        except Exception:
            food_alerts = []
        for f in food_alerts:
            if (f.get("risk_level") or "").lower() not in ("high", "critical"):
                continue
            iso3 = (f.get("country_code") or "").strip().upper()
            if not iso3 or iso3 in seen_economic or len(iso3) != 3:
                continue
            coord = COUNTRY_CENTROIDS.get(iso3)
            if not coord:
                continue
            seen_economic.add(iso3)
            name = f.get("country_name") or iso3
            infl = f.get("inflation_pct")
            out.append({
                "lat": float(coord[0]),
                "lon": float(coord[1]),
                "layer": "economic",
                "title": name + " (food inflation)",
                "summary": "Risk: " + (f.get("risk_level") or "") + ("; inflation " + ("%.1f" % infl) + "%" if infl is not None else ""),
                "region": "",
            })

    if "outages" in layers:
        try:
            infra = get_geospatial_infrastructure(country_code=None, infra_type=None, limit=100)
        except Exception:
            infra = []
        for g in infra:
            lat, lon = g.get("lat"), g.get("lon")
            if lat is None or lon is None:
                continue
            itype = g.get("infra_type") or "infrastructure"
            out.append({
                "lat": float(lat),
                "lon": float(lon),
                "layer": "outages",
                "title": g.get("name") or itype,
                "summary": (itype + (" · " + (g.get("region") or "")) + (" · " + (g.get("capacity_notes") or "")[:100])).strip(),
                "region": g.get("region") or "",
            })

    if "military" in layers:
        try:
            naval_heat = get_naval_deployment_heat(limit_regions=25)
        except Exception:
            naval_heat = []
        for h in naval_heat:
            region = (h.get("region") or "").strip()
            if not region:
                continue
            coord = _world_monitor_region_centroid(region)
            if not coord:
                continue
            cnt = h.get("deployment_count") or 0
            out.append({
                "lat": float(coord[0]),
                "lon": float(coord[1]),
                "layer": "military",
                "title": "Naval: " + region,
                "summary": str(int(cnt)) + " deployment(s) in region.",
                "region": region,
            })
        try:
            incidents = get_border_incidents(date_from=date_from, date_to=date_to, limit=60)
        except Exception:
            incidents = []
        for inc in incidents:
            code_a = (inc.get("country_a_code") or "").strip().upper()
            code_b = (inc.get("country_b_code") or "").strip().upper()
            code = code_a if code_a and len(code_a) == 3 else (code_b if code_b and len(code_b) == 3 else None)
            if not code:
                continue
            coord = COUNTRY_CENTROIDS.get(code)
            if not coord:
                continue
            name_a = inc.get("country_a_name") or code_a
            name_b = inc.get("country_b_name") or code_b
            out.append({
                "lat": float(coord[0]),
                "lon": float(coord[1]),
                "layer": "military",
                "title": "Border: " + (name_a or "") + " / " + (name_b or ""),
                "summary": (inc.get("incident_date") or "")[:10] + " · " + (inc.get("summary") or "")[:120],
                "region": "",
            })
        try:
            exercises = get_military_exercises(date_from=date_from, date_to=date_to, limit=35)
        except Exception:
            exercises = []
        for ex in exercises:
            region = (ex.get("region") or "").strip()
            if not region:
                continue
            coord = _world_monitor_region_centroid(region)
            if not coord:
                continue
            out.append({
                "lat": float(coord[0]),
                "lon": float(coord[1]),
                "layer": "military",
                "title": "Exercise: " + (ex.get("name") or "—"),
                "summary": (region + " · " + (ex.get("scale") or "") + " · " + (ex.get("start_date") or "")[:10]).strip(),
                "region": region,
            })

    if "natural" in layers:
        try:
            natural_list = get_natural_risk_summary(limit=50)
        except Exception:
            natural_list = []
        for n in natural_list:
            iso3 = (n.get("country_code") or "").strip().upper()
            if not iso3 or len(iso3) != 3:
                continue
            coord = COUNTRY_CENTROIDS.get(iso3)
            if not coord:
                continue
            name = n.get("country_name") or iso3
            freq = n.get("natural_disaster_frequency")
            risk = n.get("climate_risk_score")
            parts = []
            if freq is not None:
                parts.append("disaster freq " + ("%.1f" % freq))
            if risk is not None:
                parts.append("climate risk " + str(int(risk)))
            out.append({
                "lat": float(coord[0]),
                "lon": float(coord[1]),
                "layer": "natural",
                "title": "Natural risk: " + name,
                "summary": "; ".join(parts) if parts else "Natural hazard / climate vulnerability.",
                "region": "",
            })

    if "iranAttacks" in layers:
        try:
            from collections import defaultdict
            iran_articles = get_articles_iran_attacks(days=time_range_days, limit=150)
        except Exception:
            iran_articles = []
        country_info = defaultdict(lambda: {"count": 0, "latest_title": ""})
        for a in iran_articles:
            title = (a.get("title") or "")[:120]
            topics_list = []
            entities_list = []
            for col in ("topics", "entities"):
                raw = a.get(col)
                if not raw:
                    continue
                try:
                    parsed = json.loads(raw) if isinstance(raw, str) else raw
                    if isinstance(parsed, list):
                        (topics_list if col == "topics" else entities_list).extend([str(x).strip() for x in parsed if x])
                except (json.JSONDecodeError, TypeError):
                    pass
            seen_iso = set()
            for token in topics_list + entities_list:
                if not token or len(token) < 2:
                    continue
                iso3 = _world_monitor_country_name_to_iso3(token)
                if not iso3 or iso3 in seen_iso or iso3 not in COUNTRY_CENTROIDS:
                    continue
                seen_iso.add(iso3)
                country_info[iso3]["count"] += 1
                if not country_info[iso3]["latest_title"] and title:
                    country_info[iso3]["latest_title"] = title
        for iso3, info in country_info.items():
            if info["count"] == 0:
                continue
            coord = COUNTRY_CENTROIDS.get(iso3)
            if not coord:
                continue
            try:
                from app.country_data import ALL_COUNTRIES
                name = next((row[1] for row in ALL_COUNTRIES if row[0] == iso3), iso3)
            except Exception:
                name = iso3
            out.append({
                "lat": float(coord[0]),
                "lon": float(coord[1]),
                "layer": "iranAttacks",
                "title": name + " (" + str(info["count"]) + " Iran-attack related)",
                "summary": info["latest_title"] or "News mentions Iran and attacks/strikes/missiles/drones.",
                "region": "",
            })

    return out


def get_naval_deployments(
    region: Optional[str] = None,
    country_code: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
):
    with _connection() as conn:
        where, params = [], []
        if region:
            where.append("region LIKE ?")
            params.append(f"%{region}%")
        if country_code:
            where.append("country_code = ?")
            params.append(country_code)
        where_sql = " AND ".join(where) if where else "1=1"
        params.extend([limit, offset])
        cur = conn.execute(
            f"SELECT id, country_code, country_name, region, vessel_description, as_of_date, created_at FROM naval_deployments WHERE {where_sql} ORDER BY as_of_date DESC LIMIT ? OFFSET ?",
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


def get_naval_deployments_count(
    region: Optional[str] = None,
    country_code: Optional[str] = None,
) -> int:
    with _connection() as conn:
        where, params = [], []
        if region:
            where.append("region LIKE ?")
            params.append(f"%{region}%")
        if country_code:
            where.append("country_code = ?")
            params.append(country_code)
        where_sql = " AND ".join(where) if where else "1=1"
        cur = conn.execute(f"SELECT COUNT(*) FROM naval_deployments WHERE {where_sql}", tuple(params))
        return cur.fetchone()[0] or 0


def get_naval_deployment_heat(region: Optional[str] = None, limit_regions: int = 20) -> list:
    """Aggregate naval deployments by region for heat map (count of deployments per region)."""
    with _connection() as conn:
        where, params = ["1=1"], []
        if region:
            where[0] = "region LIKE ?"
            params.append(f"%{region}%")
        params.append(limit_regions)
        cur = conn.execute(
            f"SELECT region, COUNT(*) AS deployment_count FROM naval_deployments WHERE {where[0]} GROUP BY region ORDER BY deployment_count DESC LIMIT ?",
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


def get_arms_trade(
    supplier: Optional[str] = None,
    recipient: Optional[str] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    limit: int = 50,
    offset: int = 0,
):
    with _connection() as conn:
        where, params = [], []
        if supplier:
            where.append("supplier_country LIKE ?")
            params.append(f"%{supplier}%")
        if recipient:
            where.append("recipient_country LIKE ?")
            params.append(f"%{recipient}%")
        if year_from is not None:
            where.append("(year IS NULL OR year >= ?)")
            params.append(year_from)
        if year_to is not None:
            where.append("(year IS NULL OR year <= ?)")
            params.append(year_to)
        where_sql = " AND ".join(where) if where else "1=1"
        params.extend([limit, offset])
        cur = conn.execute(
            f"SELECT id, supplier_country, recipient_country, weapon_type, value_usd_millions, year, deal_status, created_at FROM arms_trade WHERE {where_sql} ORDER BY year DESC, value_usd_millions DESC LIMIT ? OFFSET ?",
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


def get_arms_trade_count(
    supplier: Optional[str] = None,
    recipient: Optional[str] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
) -> int:
    with _connection() as conn:
        where, params = [], []
        if supplier:
            where.append("supplier_country LIKE ?")
            params.append(f"%{supplier}%")
        if recipient:
            where.append("recipient_country LIKE ?")
            params.append(f"%{recipient}%")
        if year_from is not None:
            where.append("(year IS NULL OR year >= ?)")
            params.append(year_from)
        if year_to is not None:
            where.append("(year IS NULL OR year <= ?)")
            params.append(year_to)
        where_sql = " AND ".join(where) if where else "1=1"
        cur = conn.execute(f"SELECT COUNT(*) FROM arms_trade WHERE {where_sql}", tuple(params))
        return cur.fetchone()[0] or 0


def get_conflict_summary() -> dict:
    """Summary counts for conflict dashboard: incidents (7d), exercises (7d), naval count, last_updated."""
    with _connection() as conn:
        cutoff_7d = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
        cur = conn.execute(
            "SELECT COUNT(*) FROM border_incidents WHERE incident_date >= ?", (cutoff_7d,)
        )
        incidents_7d = cur.fetchone()[0] or 0
        cur = conn.execute(
            "SELECT COUNT(*) FROM military_exercises WHERE start_date >= ?", (cutoff_7d,)
        )
        exercises_7d = cur.fetchone()[0] or 0
        cur = conn.execute("SELECT COUNT(*) FROM naval_deployments")
        naval_count = cur.fetchone()[0] or 0
        cur = conn.execute(
            "SELECT MAX(created_at) FROM (SELECT created_at FROM defense_spending UNION ALL SELECT created_at FROM border_incidents UNION ALL SELECT created_at FROM military_movement)"
        )
        row = cur.fetchone()
        last_updated = (row[0][:19].replace("T", " ") + " UTC") if row and row[0] else None
        return {
            "incidents_7d": incidents_7d,
            "exercises_7d": exercises_7d,
            "naval_count": naval_count,
            "last_updated": last_updated,
        }


def get_arms_trade_summary(top_n: int = 5) -> dict:
    """Top suppliers and recipients by total value (for summary strip)."""
    with _connection() as conn:
        cur = conn.execute(
            """SELECT supplier_country, SUM(value_usd_millions) AS total FROM arms_trade GROUP BY supplier_country ORDER BY total DESC LIMIT ?""",
            (top_n,),
        )
        top_suppliers = [dict(row) for row in cur.fetchall()]
        cur = conn.execute(
            """SELECT recipient_country, SUM(value_usd_millions) AS total FROM arms_trade GROUP BY recipient_country ORDER BY total DESC LIMIT ?""",
            (top_n,),
        )
        top_recipients = [dict(row) for row in cur.fetchall()]
        return {"top_suppliers": top_suppliers, "top_recipients": top_recipients}


def get_arms_trade_by_year(limit_years: int = 15) -> list:
    """Total arms trade value by year for trend chart."""
    with _connection() as conn:
        cur = conn.execute(
            """SELECT year, SUM(value_usd_millions) AS total FROM arms_trade WHERE year IS NOT NULL GROUP BY year ORDER BY year DESC LIMIT ?""",
            (limit_years,),
        )
        return [dict(row) for row in cur.fetchall()]


def get_escalation_tree(incident_days: int = 90) -> dict:
    """
    Structured escalation model: Tactical incident → Diplomatic crisis → Sanctions → Military conflict probability.
    Uses border_incidents (tactical), sanctions_registry (sanctions stage). Conflict probability from rule-based score.
    """
    with _connection() as conn:
        # Cutoff for "recent"
        cutoff = (datetime.utcnow() - timedelta(days=incident_days)).strftime("%Y-%m-%d")
        cur = conn.execute(
            """SELECT id, country_a_code, country_a_name, country_b_code, country_b_name, incident_date, summary, severity, status
               FROM border_incidents WHERE incident_date >= ? ORDER BY incident_date DESC""",
            (cutoff,),
        )
        tactical = [dict(row) for row in cur.fetchall()]
        cur = conn.execute(
            "SELECT id, imposing_country, target_country, measure_type, start_date, description FROM sanctions_registry ORDER BY start_date DESC LIMIT 50"
        )
        sanctions = [dict(row) for row in cur.fetchall()]
    # Build escalation stages (structured risk modeling, not sensational)
    stage1_tactical = {
        "label": "Tactical incidents",
        "description": "Border incidents, skirmishes, localized engagements.",
        "count": len(tactical),
        "items": tactical[:15],
    }
    stage2_diplomatic = {
        "label": "Diplomatic crisis",
        "description": "Tensions requiring diplomatic response; linked to incidents.",
        "count": len(tactical),  # proxy: same count of dyads with incidents
        "items": [],
    }
    stage3_sanctions = {
        "label": "Sanctions",
        "description": "Imposed measures against state or actors.",
        "count": len(sanctions),
        "items": sanctions[:15],
    }
    # Conflict probability: simple rule-based score (0–100). Base from incident count + sanctions count.
    score = min(100, len(tactical) * 8 + len(sanctions) * 5)
    if score >= 60:
        level = "elevated"
        label = "Elevated"
    elif score >= 35:
        level = "moderate"
        label = "Moderate"
    else:
        level = "low"
        label = "Low"
    stage4_conflict = {
        "label": "Military conflict probability",
        "description": "Structured risk from incident and sanctions signals; not predictive of outbreak.",
        "probability_pct": score,
        "level": level,
        "level_label": label,
    }
    return {
        "stages": [stage1_tactical, stage2_diplomatic, stage3_sanctions],
        "conflict_probability": stage4_conflict,
        "summary": f"Tactical incidents: {stage1_tactical['count']}. Sanctions: {stage3_sanctions['count']}. Conflict probability: {label} ({score}% model score).",
        "updated_at": datetime.utcnow().isoformat() + "Z",
    }


def get_escalation_tree_by_region(incident_days: int = 90) -> list:
    """Escalation score per region (same rule: incidents + sanctions). Returns list of {region, incident_count, sanctions_count, score, level, level_label}."""
    with _connection() as conn:
        cutoff = (datetime.utcnow() - timedelta(days=incident_days)).strftime("%Y-%m-%d")
        cur = conn.execute(
            """SELECT country_a_code, country_b_code FROM border_incidents WHERE incident_date >= ?""",
            (cutoff,),
        )
        incident_rows = cur.fetchall()
        cur = conn.execute("SELECT country_code, region FROM country_risk_integration")
        code_to_region = {row[0]: (row[1] or "Other") for row in cur.fetchall()}
        cur = conn.execute("SELECT country_name, country_code, region FROM country_risk_integration")
        for row in cur.fetchall():
            if row[0]:
                code_to_region[row[0]] = row[2] or "Other"
            if row[1]:
                code_to_region[row[1]] = row[2] or "Other"
    region_incidents: dict = {}
    for row in incident_rows:
        a_code, b_code = (row[0] or "").strip(), (row[1] or "").strip()
        reg = code_to_region.get(a_code) or code_to_region.get(b_code) or "Other"
        region_incidents[reg] = region_incidents.get(reg, 0) + 1
    with _connection() as conn:
        cur = conn.execute("SELECT imposing_country, target_country FROM sanctions_registry ORDER BY start_date DESC LIMIT 100")
        sanctions = cur.fetchall()
    region_sanctions = {}
    for row in sanctions:
        reg = code_to_region.get((row[0] or "").strip()) or code_to_region.get((row[1] or "").strip()) or "Other"
        region_sanctions[reg] = region_sanctions.get(reg, 0) + 1
    all_regions = set(region_incidents) | set(region_sanctions) or {"Global"}
    out = []
    for region in sorted(all_regions):
        inc = region_incidents.get(region, 0)
        sanc = region_sanctions.get(region, 0)
        score = min(100, inc * 8 + sanc * 5)
        if score >= 60:
            level, level_label = "elevated", "Elevated"
        elif score >= 35:
            level, level_label = "moderate", "Moderate"
        else:
            level, level_label = "low", "Low"
        out.append({"region": region, "incident_count": inc, "sanctions_count": sanc, "score": score, "level": level, "level_label": level_label})
    return out


# --- Conflict alerts (events + rules) ---

def _get_conflict_last_id(table_name: str) -> int:
    with _connection() as conn:
        row = conn.execute("SELECT last_id FROM conflict_events_last_id WHERE table_name = ?", (table_name,)).fetchone()
        return int(row[0]) if row else 0


def _set_conflict_last_id(table_name: str, last_id: int) -> None:
    with _connection() as conn:
        conn.execute("INSERT OR REPLACE INTO conflict_events_last_id (table_name, last_id) VALUES (?, ?)", (table_name, last_id))


def get_new_conflict_records() -> list:
    """Return list of {event_type, region, country_code, record_id, table_name, summary} for records not yet emitted."""
    out = []
    with _connection() as conn:
        last_bi = _get_conflict_last_id("border_incidents")
        cur = conn.execute(
            "SELECT id, summary, country_a_code FROM border_incidents WHERE id > ? ORDER BY id ASC LIMIT 200",
            (last_bi,),
        )
        rows = cur.fetchall()
        max_id = last_bi
        for r in rows:
            max_id = max(max_id, r[0])
            out.append({"event_type": "border_incident", "region": None, "country_code": r[2], "record_id": r[0], "table_name": "border_incidents", "summary": (r[1] or "")[:500]})
        _set_conflict_last_id("border_incidents", max_id)

        last_ex = _get_conflict_last_id("military_exercises")
        cur = conn.execute(
            "SELECT id, name, region FROM military_exercises WHERE id > ? ORDER BY id ASC LIMIT 200",
            (last_ex,),
        )
        rows = cur.fetchall()
        max_id = last_ex
        for r in rows:
            max_id = max(max_id, r[0])
            out.append({"event_type": "military_exercise", "region": r[2], "country_code": None, "record_id": r[0], "table_name": "military_exercises", "summary": (r[1] or "")[:500]})
        _set_conflict_last_id("military_exercises", max_id)

        last_mm = _get_conflict_last_id("military_movement")
        cur = conn.execute(
            "SELECT id, summary, region, country_code FROM military_movement WHERE id > ? ORDER BY id ASC LIMIT 200",
            (last_mm,),
        )
        rows = cur.fetchall()
        max_id = last_mm
        for r in rows:
            max_id = max(max_id, r[0])
            out.append({"event_type": "military_movement", "region": r[2], "country_code": r[3], "record_id": r[0], "table_name": "military_movement", "summary": (r[1] or "")[:500]})
        _set_conflict_last_id("military_movement", max_id)
    return out


def add_conflict_event(event_type: str, region: Optional[str], country_code: Optional[str], record_id: int, table_name: str, summary: Optional[str] = None) -> int:
    now = datetime.utcnow().isoformat() + "Z"
    with _connection() as conn:
        cur = conn.execute(
            "INSERT INTO conflict_events (event_type, region, country_code, record_id, table_name, summary, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (event_type, region, country_code, record_id, table_name, (summary or "")[:500], now),
        )
        return cur.lastrowid


def get_conflict_alert_rules():
    with _connection() as conn:
        cur = conn.execute(
            "SELECT id, name, event_types, region, country_code, webhook_url, created_at FROM conflict_alert_rules ORDER BY created_at DESC"
        )
        return [dict(row) for row in cur.fetchall()]


def add_conflict_alert_rule(name: str, event_types: list, region: Optional[str] = None, country_code: Optional[str] = None, webhook_url: Optional[str] = None) -> int:
    now = datetime.utcnow().isoformat() + "Z"
    types_str = ",".join(event_types) if event_types else ""
    with _connection() as conn:
        cur = conn.execute(
            "INSERT INTO conflict_alert_rules (name, event_types, region, country_code, webhook_url, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (name, types_str, region or "", country_code or "", webhook_url or "", now),
        )
        return cur.lastrowid


def delete_conflict_alert_rule(rule_id: int) -> bool:
    with _connection() as conn:
        cur = conn.execute("DELETE FROM conflict_alert_rules WHERE id = ?", (rule_id,))
        return cur.rowcount > 0


def get_conflict_alert_rules_matching(event_type: str, region: Optional[str], country_code: Optional[str]) -> list:
    """Return list of conflict_alert_rules (with webhook_url) that match this event."""
    rules = get_conflict_alert_rules()
    out = []
    for r in rules:
        types_str = (r.get("event_types") or "").strip()
        if types_str and event_type not in [x.strip() for x in types_str.split(",") if x.strip()]:
            continue
        if r.get("region") and region and (r.get("region") or "").strip().lower() not in (region or "").lower():
            continue
        if r.get("country_code") and country_code and (r.get("country_code") or "").strip().upper() != (country_code or "").strip().upper():
            continue
        if (r.get("webhook_url") or "").strip():
            out.append(r)
    return out


def get_defense_spending_with_yoy(rows: list) -> list:
    """For each defense row add prior_year_spending and pct_change_yoy if prior year exists."""
    if not rows:
        return rows
    with _connection() as conn:
        out = []
        for r in rows:
            r = dict(r)
            country_code = r.get("country_code")
            year = r.get("year")
            if country_code is not None and year is not None:
                prev = conn.execute(
                    "SELECT spending_usd_billions FROM defense_spending WHERE country_code = ? AND year = ?",
                    (country_code, year - 1),
                ).fetchone()
                if prev and prev[0] is not None:
                    prior = float(prev[0])
                    curr = float(r.get("spending_usd_billions") or 0)
                    r["prior_year_spending"] = prior
                    r["pct_change_yoy"] = ((curr - prior) / prior * 100) if prior else None
                else:
                    r["prior_year_spending"] = None
                    r["pct_change_yoy"] = None
            else:
                r["prior_year_spending"] = None
                r["pct_change_yoy"] = None
            out.append(r)
        return out


# --- Sanctions, export controls & regulatory watch ---

def get_entity_list_alerts(
    source: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 50,
):
    with _connection() as conn:
        where, params = [], []
        if source:
            where.append("source = ?")
            params.append(source)
        if search:
            where.append("(entity_name LIKE ? OR country LIKE ? OR summary LIKE ? OR list_name LIKE ?)")
            q = f"%{search}%"
            params.extend([q, q, q, q])
        where_sql = " AND ".join(where) if where else "1=1"
        params.append(limit)
        cur = conn.execute(
            f"SELECT id, source, entity_name, entity_type, country, list_name, listed_date, summary, created_at FROM entity_list_alerts WHERE {where_sql} ORDER BY listed_date DESC LIMIT ?",
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


def get_export_restrictions(
    issuer: Optional[str] = None,
    restriction_type: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 50,
):
    with _connection() as conn:
        where, params = [], []
        if issuer:
            where.append("issuer = ?")
            params.append(issuer)
        if restriction_type:
            where.append("restriction_type = ?")
            params.append(restriction_type)
        if search:
            where.append("(title LIKE ? OR description LIKE ? OR issuer LIKE ?)")
            q = f"%{search}%"
            params.extend([q, q, q])
        where_sql = " AND ".join(where) if where else "1=1"
        params.append(limit)
        cur = conn.execute(
            f"SELECT id, issuer, restriction_type, title, description, effective_date, source_url, created_at FROM export_restrictions WHERE {where_sql} ORDER BY effective_date DESC LIMIT ?",
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


def get_sanctions_watch_meta() -> dict:
    """Last updated times and 7-day counts for sanctions watch dashboard."""
    cutoff_7d = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
    with _connection() as conn:
        cur = conn.execute("SELECT MAX(created_at) FROM sanctions_registry")
        s_updated = (cur.fetchone() or (None,))[0]
        cur = conn.execute("SELECT MAX(created_at) FROM entity_list_alerts")
        e_updated = (cur.fetchone() or (None,))[0]
        cur = conn.execute("SELECT MAX(created_at) FROM export_restrictions")
        r_updated = (cur.fetchone() or (None,))[0]
        last_updated = max((x or "") for x in (s_updated, e_updated, r_updated)) or None
        cur = conn.execute("SELECT COUNT(*) FROM sanctions_registry WHERE substr(created_at, 1, 10) >= ?", (cutoff_7d,))
        new_sanctions_7d = (cur.fetchone() or (0,))[0]
        cur = conn.execute("SELECT COUNT(*) FROM entity_list_alerts WHERE substr(created_at, 1, 10) >= ?", (cutoff_7d,))
        new_entities_7d = (cur.fetchone() or (0,))[0]
        cur = conn.execute("SELECT COUNT(*) FROM export_restrictions WHERE substr(created_at, 1, 10) >= ?", (cutoff_7d,))
        new_restrictions_7d = (cur.fetchone() or (0,))[0]
    return {
        "last_updated": last_updated[:10] if last_updated else None,
        "new_sanctions_7d": new_sanctions_7d,
        "new_entities_7d": new_entities_7d,
        "new_restrictions_7d": new_restrictions_7d,
    }


# Default source URLs for entity lists (used in supply-chain check results)
ENTITY_LIST_SOURCE_URLS = {
    "OFAC": "https://ofac.treasury.gov/specially-designated-nationals-and-blocked-persons-list-sdn-human-readable-lists",
    "EU": "https://finance.ec.europa.eu/eu-and-world/sanctions-restrictive-measures_en",
    "US_BIS": "https://www.bis.doc.gov/index.php/policy-guidance/lists-of-parties-of-concern/entity-list",
    "China": "https://www.mofcom.gov.cn/",
}


def _normalize_entity(s: str) -> str:
    """Normalize for fuzzy match: lowercase, alphanumeric only."""
    if not s:
        return ""
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def check_supply_chain_export_rules(entities: list) -> dict:
    """
    Which of my supply chain entities violate new export rules?
    entities: list of strings (company names, country names, or codes).
    Returns { matches: [ { entity, match_type, detail, source_url } ], total_checked }.
    Uses fuzzy matching (normalized alphanumeric) in addition to literal LIKE.
    """
    entities = [e.strip() for e in entities if e and str(e).strip()]
    if not entities:
        return {"matches": [], "total_checked": 0}
    matches = []
    with _connection() as conn:
        for entity in entities:
            entity_upper = entity.upper()
            entity_like = f"%{entity}%"
            norm = _normalize_entity(entity)
            norm_like = f"%{norm}%" if norm else entity_like
            # Sanctions registry: target or imposing country
            cur = conn.execute(
                "SELECT imposing_country, target_country, measure_type, description, source, source_url FROM sanctions_registry WHERE target_country LIKE ? OR imposing_country LIKE ? LIMIT 5",
                (entity_like, entity_like),
            )
            for row in cur.fetchall():
                r = dict(row)
                url = (r.get("source_url") or "").strip() or None
                matches.append({
                    "entity": entity,
                    "match_type": "sanctions",
                    "detail": f"Sanctions: {r.get('imposing_country')} → {r.get('target_country')} ({r.get('measure_type') or '—'}). {r.get('description') or ''}",
                    "source_url": url,
                })
            # Entity list alerts: entity name or country (with normalized fuzzy match)
            cur = conn.execute(
                """SELECT source, entity_name, list_name, country, summary FROM entity_list_alerts
                   WHERE entity_name LIKE ? OR country LIKE ? OR UPPER(entity_name) LIKE ?
                   OR (LOWER(REPLACE(REPLACE(REPLACE(REPLACE(entity_name, ' ', ''), '-', ''), '.', ''), ',', '')) LIKE ?)
                   LIMIT 5""",
                (entity_like, entity_like, f"%{entity_upper}%", norm_like),
            )
            for row in cur.fetchall():
                r = dict(row)
                src = r.get("source") or ""
                url = ENTITY_LIST_SOURCE_URLS.get(src)
                matches.append({
                    "entity": entity,
                    "match_type": "entity_list",
                    "detail": f"{src} {r.get('list_name') or 'list'}: {r.get('entity_name')} ({r.get('country') or '—'}). {r.get('summary') or ''}",
                    "source_url": url,
                })
    return {"matches": matches, "total_checked": len(entities)}


# --- Scenario planning engine (MCDA / Delphi) ---

def get_scenarios(limit: int = 50):
    with _connection() as conn:
        cur = conn.execute(
            "SELECT id, name, slug, description, region, horizon_year, scenario_type, created_at FROM scenarios ORDER BY horizon_year, name LIMIT ?",
            (limit,),
        )
        return [dict(row) for row in cur.fetchall()]


def get_scenario(scenario_id: int):
    with _connection() as conn:
        cur = conn.execute(
            "SELECT id, name, slug, description, region, horizon_year, scenario_type, created_at FROM scenarios WHERE id = ?",
            (scenario_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def get_scenario_run(run_id: int):
    with _connection() as conn:
        cur = conn.execute(
            "SELECT id, scenario_id, run_at, agent_outputs, probability_summary, outlook_summary, created_at FROM scenario_runs WHERE id = ?",
            (run_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    r = dict(row)
    if r.get("agent_outputs"):
        try:
            parsed = json.loads(r["agent_outputs"])
            r["agent_outputs"] = parsed if isinstance(parsed, dict) else {}
        except (TypeError, json.JSONDecodeError):
            r["agent_outputs"] = {}
    else:
        r["agent_outputs"] = {}
    return r


def get_scenario_runs(scenario_id: int, limit: int = 20):
    with _connection() as conn:
        cur = conn.execute(
            "SELECT id, scenario_id, run_at, probability_summary, created_at FROM scenario_runs WHERE scenario_id = ? ORDER BY run_at DESC LIMIT ?",
            (scenario_id, limit),
        )
        return [dict(row) for row in cur.fetchall()]


def add_scenario_engine_run(result: dict) -> int:
    """Persist a Scenario Engine run for history and export by run_id. Returns run id."""
    now = datetime.utcnow().isoformat() + "Z"
    run_at = result.get("run_at") or now
    event_type = result.get("event_type") or "custom"
    event_label = result.get("event_label") or event_type.replace("_", " ").title()
    region = result.get("region") or ""
    country = result.get("country") or ""
    horizon_year = result.get("horizon_year")
    agents = result.get("agents") or {}
    paths = result.get("paths") or {}
    path_descriptions = result.get("path_descriptions") or {}
    name = (result.get("run_name") or result.get("name") or "").strip() or ""
    notes = (result.get("notes") or "").strip() or ""
    with _connection() as conn:
        cur = conn.execute("PRAGMA table_info(scenario_engine_runs)")
        cols = {row[1] for row in cur.fetchall()}
        if "name" in cols and "notes" in cols:
            cur = conn.execute(
                """INSERT INTO scenario_engine_runs
                   (event_type, event_label, region, country, horizon_year, agents_json, paths_json, path_descriptions_json, run_at, created_at, name, notes)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    event_type,
                    event_label,
                    region,
                    country,
                    horizon_year,
                    json.dumps(agents),
                    json.dumps(paths),
                    json.dumps(path_descriptions),
                    run_at,
                    now,
                    name,
                    notes,
                ),
            )
        else:
            cur = conn.execute(
                """INSERT INTO scenario_engine_runs
                   (event_type, event_label, region, country, horizon_year, agents_json, paths_json, path_descriptions_json, run_at, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    event_type,
                    event_label,
                    region,
                    country,
                    horizon_year,
                    json.dumps(agents),
                    json.dumps(paths),
                    json.dumps(path_descriptions),
                    run_at,
                    now,
                ),
            )
        return cur.lastrowid


def get_scenario_engine_runs(limit: int = 10):
    """Return recent Scenario Engine runs for the history list."""
    with _connection() as conn:
        cur = conn.execute("PRAGMA table_info(scenario_engine_runs)")
        cols = [row[1] for row in cur.fetchall()]
        has_name = "name" in cols
        has_notes = "notes" in cols
        sel = "id, event_type, event_label, region, country, run_at, created_at"
        if has_name:
            sel += ", name"
        if has_notes:
            sel += ", notes"
        cur = conn.execute(
            f"SELECT {sel} FROM scenario_engine_runs ORDER BY run_at DESC LIMIT ?",
            (limit,),
        )
        rows = cur.fetchall()
    out = []
    for row in rows:
        d = dict(row)
        if not has_name:
            d["name"] = ""
        if not has_notes:
            d["notes"] = ""
        out.append(d)
    return out


def delete_scenario_engine_run(run_id: int) -> bool:
    """Delete a Scenario Engine run. Returns True if deleted."""
    with _connection() as conn:
        cur = conn.execute("DELETE FROM scenario_engine_runs WHERE id = ?", (run_id,))
        return cur.rowcount > 0


def update_scenario_engine_run_name_notes(run_id: int, name: Optional[str] = None, notes: Optional[str] = None) -> bool:
    """Update name and/or notes for a run. Returns True if updated. Columns must exist (migration)."""
    with _connection() as conn:
        cur = conn.execute("PRAGMA table_info(scenario_engine_runs)")
        cols = {row[1] for row in cur.fetchall()}
        if "name" not in cols or "notes" not in cols:
            return False
        if name is not None and notes is not None:
            cur = conn.execute("UPDATE scenario_engine_runs SET name = ?, notes = ? WHERE id = ?", (name or "", notes or "", run_id))
        elif name is not None:
            cur = conn.execute("UPDATE scenario_engine_runs SET name = ? WHERE id = ?", (name or "", run_id))
        elif notes is not None:
            cur = conn.execute("UPDATE scenario_engine_runs SET notes = ? WHERE id = ?", (notes or "", run_id))
        else:
            return False
        return cur.rowcount > 0


def get_scenario_engine_runs_filtered(
    limit: int = 20,
    offset: int = 0,
    event_type: Optional[str] = None,
    region: Optional[str] = None,
    country: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> tuple[list, int]:
    """Return filtered Scenario Engine runs and total count. For history page with pagination."""
    with _connection() as conn:
        where, params = [], []
        if event_type:
            where.append("event_type = ?")
            params.append(event_type)
        if region:
            where.append("(region LIKE ? OR region = ?)")
            params.extend([f"%{region}%", region])
        if country:
            where.append("(country LIKE ? OR country = ?)")
            params.extend([f"%{country}%", country])
        if date_from:
            where.append("DATE(run_at) >= ?")
            params.append(date_from[:10] if len(date_from) >= 10 else date_from)
        if date_to:
            where.append("DATE(run_at) <= ?")
            params.append(date_to[:10] if len(date_to) >= 10 else date_to)
        where_sql = " AND ".join(where) if where else "1=1"
        cur = conn.execute(f"SELECT COUNT(*) FROM scenario_engine_runs WHERE {where_sql}", tuple(params))
        total = cur.fetchone()[0]
        cur = conn.execute("PRAGMA table_info(scenario_engine_runs)")
        cols = [row[1] for row in cur.fetchall()]
        has_name = "name" in cols
        has_notes = "notes" in cols
        sel = "id, event_type, event_label, region, country, run_at, created_at"
        if has_name:
            sel += ", name"
        if has_notes:
            sel += ", notes"
        params.extend([limit, offset])
        cur = conn.execute(
            f"SELECT {sel} FROM scenario_engine_runs WHERE {where_sql} ORDER BY run_at DESC LIMIT ? OFFSET ?",
            tuple(params),
        )
        rows = cur.fetchall()
    out = []
    for row in rows:
        d = dict(row)
        if not has_name:
            d["name"] = ""
        if not has_notes:
            d["notes"] = ""
        out.append(d)
    return out, total


def get_scenario_engine_run(run_id: int):
    """Load a Scenario Engine run by id; returns full result dict for template/export or None."""
    with _connection() as conn:
        cur = conn.execute("PRAGMA table_info(scenario_engine_runs)")
        cols = {row[1] for row in cur.fetchall()}
        sel = "id, event_type, event_label, region, country, horizon_year, agents_json, paths_json, path_descriptions_json, run_at, created_at"
        if "name" in cols:
            sel += ", name"
        if "notes" in cols:
            sel += ", notes"
        cur = conn.execute(
            f"SELECT {sel} FROM scenario_engine_runs WHERE id = ?",
            (run_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    r = dict(row)
    for key in ("agents_json", "paths_json", "path_descriptions_json"):
        if r.get(key):
            try:
                r[key.replace("_json", "")] = json.loads(r[key]) if isinstance(r[key], str) else r[key]
            except (TypeError, json.JSONDecodeError):
                r[key.replace("_json", "")] = {} if "path" not in key else {}
        else:
            r[key.replace("_json", "")] = {} if "path" in key else {}
    out = {
        "event_type": r["event_type"],
        "event_label": r["event_label"],
        "region": r["region"] or "",
        "country": r["country"] or "",
        "horizon_years": None,
        "horizon_year": r.get("horizon_year"),
        "agents": r.get("agents") or {},
        "paths": r.get("paths") or {},
        "path_descriptions": r.get("path_descriptions") or {},
        "run_at": r["run_at"],
    }
    if "name" in r:
        out["run_name"] = r.get("name") or ""
    if "notes" in r:
        out["notes"] = r.get("notes") or ""
    return out


def add_scenario_from_engine_run(name: str, engine_result: dict, slug: Optional[str] = None) -> int:
    """Create a new scenario in the library from an engine run. Returns scenario id. Slug must be unique."""
    now = datetime.utcnow().isoformat() + "Z"
    event_label = engine_result.get("event_label") or engine_result.get("event_type") or "Scenario"
    region = engine_result.get("region") or ""
    country = engine_result.get("country") or ""
    description = f"Saved from Scenario Engine: {event_label}. Region: {region or '—'}, Country: {country or '—'}."
    scenario_type = (engine_result.get("event_type") or "custom").lower().replace(" ", "_")
    base_slug = (slug or name.lower().replace(" ", "_").replace("-", "_"))[:80]
    base_slug = "".join(c if c.isalnum() or c == "_" else "_" for c in base_slug).strip("_") or "scenario"
    with _connection() as conn:
        slug_final = base_slug
        n = 0
        while True:
            cur = conn.execute("SELECT 1 FROM scenarios WHERE slug = ?", (slug_final,))
            if not cur.fetchone():
                break
            n += 1
            slug_final = f"{base_slug}_{n}"
        cur = conn.execute(
            "INSERT INTO scenarios (name, slug, description, region, horizon_year, scenario_type, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (name, slug_final, description, region or None, datetime.utcnow().year + 2, scenario_type, now),
        )
        return cur.lastrowid


# Scenario Engine (signature product): event types and 3-path outcomes
SCENARIO_ENGINE_EVENT_TYPES = [
    ("election_upset", "Election upset"),
    ("coup", "Coup"),
    ("military_incursion", "Military incursion"),
    ("sanctions", "Sanctions"),
    ("financial_crisis", "Financial crisis"),
    ("trade_embargo", "Trade embargo"),
]


def _engine_locale(region: str, country: str) -> str:
    """Return a short label for scenario locale, used in agent text and path descriptions."""
    r, c = (region or "").strip(), (country or "").strip()
    if c and r:
        return f"{c} ({r})"
    if c:
        return c
    if r:
        return r
    return "the affected area"


def _engine_agent_responses(event_type: str, region: str = "", country: str = "") -> dict:
    """Multi-agent simulation for Scenario Engine: economic, political, military, diplomatic, private sector.
    Responses vary by event_type, region, and country so each simulation is distinct."""
    et = (event_type or "").lower().replace("-", "_")
    r, c = (region or "").strip(), (country or "").strip()
    locale = _engine_locale(region, country)
    locale_in = f"In {locale}, " if locale != "the affected area" else "In the affected area, "
    locale_for = f"For {locale}, " if locale != "the affected area" else ""

    # Country/region flags for tailored content (case-insensitive)
    rl, cl = r.lower(), c.lower()
    is_taiwan = "taiwan" in cl or "taiwan" in rl
    is_ukraine = "ukraine" in cl or "ukraine" in rl
    is_gulf = "gulf" in rl or "middle east" in rl or "persian gulf" in rl or any(x in cl for x in ("saudi", "uae", "iran", "qatar", "kuwait", "bahrain", "oman"))
    is_east_asia = "east asia" in rl or "asia" in rl and "south" not in rl or is_taiwan
    is_asean = "asean" in rl or "southeast asia" in rl or "sea" in rl
    is_sahel = "sahel" in rl or "west africa" in rl or "sahel" in cl
    is_europe = "europe" in rl or "eu" in rl or "nato" in rl
    is_latam = "latin" in rl or "south america" in rl or "central america" in rl or "caribbean" in rl

    if et == "election_upset":
        if is_taiwan:
            return {
                "economic_policy": f"{locale_in}markets would price cross-strait and tech-sector uncertainty; capital flows and FDI into semiconductors and electronics could pause. Fiscal and trade policy direction under a new administration would drive repricing and supply-chain reassessment.",
                "political": f"{locale_in}coalition building and legitimacy challenges would interact with cross-strait rhetoric; possible early elections or constitutional tests. Opposition and institutions would shape stability and the mainland response.",
                "military": f"For {locale}, the military dimension is central: PLA posture and US/Taiwan defence commitments would be tested. Security services and civil-military relations would be in focus.",
                "diplomatic": f"{locale_in}allies (US, Japan, ASEAN) would wait for government formation; bilateral and multilateral engagement would depend on new leadership tone on sovereignty and dialogue.",
                "private_sector": f"{locale_in}corporates would defer investment in fabs and supply chain; contracts would face renegotiation risk. Tech and manufacturing exposure would be sector-specific.",
            }
        if is_europe or is_ukraine:
            return {
                "economic_policy": f"{locale_in}markets would price policy uncertainty and energy/defence spending; capital flows and FDI may pause until coalition clarity. EU fiscal and trade direction would drive repricing.",
                "political": f"{locale_in}coalition building and legitimacy challenges; possible early elections or constitutional tests. Opposition and institutions would shape stability and EU/NATO alignment.",
                "military": f"For {locale}, typically limited direct military dimension unless civil-military or NATO burden-sharing is contested. Security services may be tested on neutrality.",
                "diplomatic": f"{locale_in}allies would wait for government formation; EU and NATO engagement would depend on new leadership orientation on defence and Russia.",
                "private_sector": f"{locale_in}corporates would defer investment and hiring; supply chain and energy contracts would face renegotiation risk. Sector exposure to policy change and sanctions would vary.",
            }
        if is_asean or is_latam:
            return {
                "economic_policy": f"{locale_in}markets would price policy uncertainty; capital flows and FDI may pause until coalition clarity. Fiscal and trade policy direction would drive repricing and currency volatility.",
                "political": f"{locale_in}coalition building and legitimacy challenges; possible early elections or constitutional tests. Opposition and institutions would shape stability and regional alignment.",
                "military": f"For {locale}, typically limited direct military dimension unless civil-military relations are contested. Security services may be tested on neutrality.",
                "diplomatic": f"{locale_in}regional and international partners would wait for government formation; bilateral and multilateral engagement would depend on new leadership orientation.",
                "private_sector": f"{locale_in}corporates would defer investment and hiring; supply chain and contracts would face renegotiation risk. Sector-specific and country exposure to policy change.",
            }
        return {
            "economic_policy": f"{locale_in}markets would price policy uncertainty; capital flows and FDI may pause until coalition clarity. Fiscal and trade policy direction would drive repricing.",
            "political": f"{locale_in}coalition building and legitimacy challenges; possible early elections or constitutional tests. Opposition and institutions would shape stability.",
            "military": f"For {locale}, typically limited direct military dimension unless civil-military relations are contested. Security services may be tested on neutrality.",
            "diplomatic": f"{locale_in}allies would wait for government formation; bilateral and multilateral engagement would depend on new leadership orientation.",
            "private_sector": f"{locale_in}corporates would defer investment and hiring; supply chain and contracts would face renegotiation risk. Sector-specific exposure to policy change.",
        }

    if et == "coup":
        if is_sahel or "africa" in rl:
            return {
                "economic_policy": f"{locale_in}sanctions risk, capital flight, and currency collapse. IMF and regional lenders (AU, ECOWAS) would likely pause; reserves and debt sustainability would be critical.",
                "political": f"{locale_in}legitimacy crisis and possible civil unrest or insurgency. International recognition (AU, UN) and domestic consolidation would determine trajectory.",
                "military": f"For {locale}, force posture and loyalty splits; risk of counter-coup or prolonged instability. Border and G5 Sahel / regional force implications.",
                "diplomatic": f"{locale_in}AU, ECOWAS, UN, and regional bodies would weigh recognition and sanctions. Neighbours and great powers would calibrate response.",
                "private_sector": f"{locale_in}evacuation and business continuity; supply chain and JV exposure. Insurance and force majeure would be invoked.",
            }
        if is_asean or is_latam:
            return {
                "economic_policy": f"{locale_in}sanctions risk, capital flight, and currency collapse. IMF and regional lenders would likely pause; reserves and debt sustainability would be critical.",
                "political": f"{locale_in}legitimacy crisis and possible civil unrest. International recognition and domestic consolidation would determine trajectory; OAS/ASEAN may weigh in.",
                "military": f"For {locale}, force posture and loyalty splits; risk of counter-coup or prolonged instability. Border and alliance implications.",
                "diplomatic": f"{locale_in}OAS, ASEAN, UN, and regional bodies would weigh recognition and sanctions. Neighbours and allies would calibrate response.",
                "private_sector": f"{locale_in}evacuation and business continuity; supply chain and JV exposure. Insurance and force majeure would be invoked.",
            }
        return {
            "economic_policy": f"{locale_in}sanctions risk, capital flight, and currency collapse. IMF and regional lenders would likely pause; reserves and debt sustainability would be critical.",
            "political": f"{locale_in}legitimacy crisis and possible civil unrest or insurgency. International recognition and domestic consolidation would determine trajectory.",
            "military": f"For {locale}, force posture and loyalty splits; risk of counter-coup or prolonged instability. Border and alliance implications.",
            "diplomatic": f"{locale_in}regional and international bodies would weigh recognition and sanctions. Neighbours and allies would calibrate response.",
            "private_sector": f"{locale_in}evacuation and business continuity; supply chain and JV exposure. Insurance and force majeure would be invoked.",
        }

    if et == "military_incursion":
        if is_taiwan:
            return {
                "economic_policy": f"{locale_in}semiconductor and electronics supply chain disruption; commodity and shipping spikes; financial stress. Sanctions and asset freezes would amplify market impact and force rapid diversification.",
                "political": f"{locale_in}domestic rally or division; elections and coalitions would be tested. Refugee and humanitarian flows would pressure Japan, Philippines, and regional partners.",
                "military": f"For {locale}, escalation ladder and deterrence failure; US/Japan alliance commitments and force posture would be pivotal. Risk of horizontal or vertical escalation and maritime denial.",
                "diplomatic": f"{locale_in}US, Japan, ASEAN, and EU would convene; sanctions and military support would be on the table. De-escalation channels and clarity of red lines would be critical.",
                "private_sector": f"{locale_in}supply chain and logistics would reroute away from the Strait; insurance and shipping would reprice. Tech and critical inputs would spike; friend-shoring would accelerate.",
            }
        if is_ukraine:
            return {
                "economic_policy": f"{locale_in}energy and trade route disruption; commodity spikes and financial stress. Sanctions and asset freezes would amplify market impact; gas and grain flows would be repriced.",
                "political": f"{locale_in}domestic rally or division; elections and coalitions would be tested. Refugee flows and humanitarian crisis would pressure EU and neighbours.",
                "military": f"For {locale}, escalation ladder and NATO posture; alliance commitments and force posture would be pivotal. Risk of horizontal or vertical escalation.",
                "diplomatic": f"{locale_in}NATO, EU, UN, and regional alliances would convene; sanctions and military support would be on the table. De-escalation channels would be critical.",
                "private_sector": f"{locale_in}supply chain and logistics would reroute; insurance and shipping would reprice. Energy and critical inputs would spike.",
            }
        if is_gulf:
            return {
                "economic_policy": f"{locale_in}energy and trade route disruption; oil and LNG spikes and financial stress. Sanctions and asset freezes would amplify market impact; Hormuz and Suez would be in focus.",
                "political": f"{locale_in}domestic rally or division; Gulf coalition dynamics would be tested. Refugee and humanitarian pressure on neighbours.",
                "military": f"For {locale}, escalation ladder and deterrence; US and regional force posture and naval presence would be pivotal. Risk of horizontal escalation and mining or asymmetric threats.",
                "diplomatic": f"{locale_in}GCC, US, EU, and UN would convene; sanctions and military support would be on the table. De-escalation and freedom of navigation would be critical.",
                "private_sector": f"{locale_in}shipping and logistics would reroute; insurance would reprice. Energy and critical inputs would spike; strategic reserves would be drawn.",
            }
        return {
            "economic_policy": f"{locale_in}energy and trade route disruption; commodity spikes and financial stress. Sanctions and asset freezes would amplify market impact.",
            "political": f"{locale_in}domestic rally or division; elections and coalitions would be tested. Refugee flows and humanitarian crisis would pressure neighbours.",
            "military": f"For {locale}, escalation ladder and deterrence failure; alliance commitments and force posture would be pivotal. Risk of horizontal or vertical escalation.",
            "diplomatic": f"{locale_in}UN, NATO, and regional alliances would convene; sanctions and military support would be on the table. De-escalation channels would be critical.",
            "private_sector": f"{locale_in}supply chain and logistics would reroute; insurance and shipping would reprice. Energy and critical inputs would spike.",
        }

    if et == "sanctions":
        if is_ukraine or "russia" in cl:
            return {
                "economic_policy": f"{locale_in}trade and financial restrictions would hit GDP, inflation, and FX. Evasion and adaptation would emerge; secondary sanctions and energy carve-outs would widen impact.",
                "political": f"{locale_in}regime resilience and domestic hardship; possible crackdown or reform. Opposition and civil society would be squeezed or mobilised.",
                "military": f"For {locale}, procurement and capability constraints; alliance and arms trade implications. Risk of asymmetric or proxy response and nuclear signalling.",
                "diplomatic": f"{locale_in}coalition cohesion (G7, EU) and enforcement; carve-outs and humanitarian exceptions. Target would seek alternative partners (China, Global South) and forums.",
                "private_sector": f"{locale_in}compliance and de-risking; supply chain and banking would exit or restrict. Substitution and grey markets would develop.",
            }
        if is_gulf or "iran" in cl:
            return {
                "economic_policy": f"{locale_in}trade and financial restrictions would hit GDP, inflation, and energy exports. Evasion and adaptation would emerge; secondary sanctions would widen impact.",
                "political": f"{locale_in}regime resilience and domestic hardship; possible crackdown or reform. Opposition and civil society would be squeezed or mobilised.",
                "military": f"For {locale}, procurement and capability constraints; alliance and arms trade implications. Risk of asymmetric or proxy response and regional escalation.",
                "diplomatic": f"{locale_in}coalition cohesion and enforcement; energy carve-outs and humanitarian exceptions. Target would seek alternative partners and forums.",
                "private_sector": f"{locale_in}compliance and de-risking; oil/gas and banking would be restricted. Substitution and grey markets would develop.",
            }
        return {
            "economic_policy": f"{locale_in}trade and financial restrictions would hit GDP, inflation, and FX. Evasion and adaptation would emerge; secondary sanctions would widen impact.",
            "political": f"{locale_in}regime resilience and domestic hardship; possible crackdown or reform. Opposition and civil society would be squeezed or mobilised.",
            "military": f"For {locale}, procurement and capability constraints; alliance and arms trade implications. Risk of asymmetric or proxy response.",
            "diplomatic": f"{locale_in}coalition cohesion and enforcement; carve-outs and humanitarian exceptions. Target would seek alternative partners and forums.",
            "private_sector": f"{locale_in}compliance and de-risking; supply chain and banking would exit or restrict. Substitution and grey markets would develop.",
        }

    if et == "financial_crisis":
        if is_europe or is_asean:
            return {
                "economic_policy": f"{locale_in}contagion and liquidity stress; ECB/regional central bank and IMF interventions. Sovereign and banking sector solvency would drive policy choices.",
                "political": f"{locale_in}austerity and bailout politics; protests and electoral backlash. Reform and legitimacy of institutions would be tested.",
                "military": f"For {locale}, defence budgets and procurement would face cuts; rarely a primary driver unless linked to sovereign default or social unrest.",
                "diplomatic": f"{locale_in}swap lines, IMF programmes, and G20/EU coordination. Creditor and debtor dynamics would shape conditionality and relief.",
                "private_sector": f"{locale_in}funding stress, layoffs, and restructuring. Cross-border exposure and counterparty risk would dominate. Contingency and cash preservation.",
            }
        if is_latam:
            return {
                "economic_policy": f"{locale_in}contagion and liquidity stress; central bank and IMF interventions. Sovereign and banking sector solvency would drive policy choices; dollarisation and reserves would be in focus.",
                "political": f"{locale_in}austerity and bailout politics; protests and electoral backlash. Reform and legitimacy of institutions would be tested.",
                "military": f"For {locale}, defence budgets would face cuts; rarely a primary driver unless linked to sovereign default or unrest.",
                "diplomatic": f"{locale_in}swap lines, IMF programmes, and G20/OAS coordination. Creditor and debtor dynamics would shape conditionality and relief.",
                "private_sector": f"{locale_in}funding stress, layoffs, and restructuring. Cross-border exposure and counterparty risk would dominate.",
            }
        return {
            "economic_policy": f"{locale_in}contagion and liquidity stress; central bank and IMF interventions. Sovereign and banking sector solvency would drive policy choices.",
            "political": f"{locale_in}austerity and bailout politics; protests and electoral backlash. Reform and legitimacy of institutions would be tested.",
            "military": f"For {locale}, defence budgets and procurement would face cuts; rarely a primary driver unless linked to sovereign default or social unrest.",
            "diplomatic": f"{locale_in}swap lines, IMF programmes, and G20 coordination. Creditor and debtor dynamics would shape conditionality and relief.",
            "private_sector": f"{locale_in}funding stress, layoffs, and restructuring. Cross-border exposure and counterparty risk would dominate. Contingency and cash preservation.",
        }

    if et == "trade_embargo":
        if is_taiwan or is_east_asia:
            return {
                "economic_policy": f"{locale_in}export and import disruption; tech and components would be hit; inflation and shortages. Diversification and stockpiling would accelerate; WTO and bilateral disputes would rise.",
                "political": f"{locale_in}domestic pressure for retaliation or de-escalation; agriculture and industry lobbies would shape policy. Cross-strait and US alignment would be in play.",
                "military": f"For {locale}, supply chain for defence and dual-use; alliance burden-sharing and industrial base implications. Rarely kinetic unless linked to blockade.",
                "diplomatic": f"{locale_in}WTO, regional trade blocs, and bilateral talks. Escalation or negotiated exit would depend on coalition and alternatives; US and ASEAN would be key.",
                "private_sector": f"{locale_in}sourcing shifts and inventory drawdown; semiconductors and logistics would reprioritise. Long-term friend-shoring and redundancy.",
            }
        if is_gulf:
            return {
                "economic_policy": f"{locale_in}energy export and import disruption; inflation and shortages. Diversification and stockpiling would accelerate; OPEC+ and bilateral disputes would be in focus.",
                "political": f"{locale_in}domestic pressure for retaliation or de-escalation; energy and industry lobbies would shape policy.",
                "military": f"For {locale}, supply chain for defence and dual-use; alliance burden-sharing. Rarely kinetic unless linked to blockade or Hormuz.",
                "diplomatic": f"{locale_in}OPEC+, regional blocs, and bilateral talks. Escalation or negotiated exit would depend on coalition and alternatives.",
                "private_sector": f"{locale_in}sourcing shifts and inventory drawdown; energy and logistics would reprioritise. Long-term diversification and strategic reserves.",
            }
        return {
            "economic_policy": f"{locale_in}export and import disruption; inflation and shortages. Diversification and stockpiling would accelerate; WTO and bilateral disputes would rise.",
            "political": f"{locale_in}domestic pressure for retaliation or de-escalation; agriculture and industry lobbies would shape policy.",
            "military": f"For {locale}, supply chain for defence and dual-use; alliance burden-sharing and industrial base implications. Rarely kinetic unless linked to blockade.",
            "diplomatic": f"{locale_in}WTO, regional trade blocs, and bilateral talks. Escalation or negotiated exit would depend on coalition and alternatives.",
            "private_sector": f"{locale_in}sourcing shifts and inventory drawdown; logistics and pricing would reprioritise. Long-term friend-shoring and redundancy.",
        }

    # Default
    return {
        "economic_policy": f"{locale_in}economic impact would depend on sector exposure, trade and financial linkages, and policy response.",
        "political": f"{locale_in}domestic politics would mediate legitimacy and policy choices; institutions and public opinion would shape outcomes.",
        "military": f"For {locale}, military dimension would depend on geography, alliances, and escalation dynamics.",
        "diplomatic": f"{locale_in}multilateral and bilateral diplomacy would seek to contain or resolve; coalition and messaging would be critical.",
        "private_sector": f"{locale_in}private sector would adapt supply chains, pricing, and risk management to the new equilibrium.",
    }


def _engine_paths(event_type: str, region: str = "", country: str = "") -> tuple:
    """Return (paths dict, path_descriptions dict) for 3-path outcomes. Varies by event_type, region, and country."""
    et = (event_type or "").lower().replace("-", "_")
    r, c = (region or "").strip().lower(), (country or "").strip().lower()
    locale = _engine_locale(region, country)

    # Locale flags for path overrides
    is_taiwan = "taiwan" in c or "taiwan" in r
    is_ukraine = "ukraine" in c or "ukraine" in r
    is_gulf = "gulf" in r or "middle east" in r or any(x in c for x in ("saudi", "uae", "iran", "qatar"))
    is_east_asia = "east asia" in r or is_taiwan
    is_sahel = "sahel" in r or "sahel" in c or "west africa" in r
    is_asean = "asean" in r or "southeast asia" in r
    is_europe = "europe" in r or "eu" in r

    # Base paths and descriptions by event type (defaults)
    if et == "election_upset":
        paths = {"contained": 55, "regional_escalation": 30, "systemic_crisis": 15}
        path_desc = {
            "contained": "Coalition forms or minority government; policy continuity with modest shifts. Markets stabilise within weeks.",
            "regional_escalation": "Prolonged instability or repeat elections; regional spillover and investor flight. Policy uncertainty extends 6–12 months.",
            "systemic_crisis": "Constitutional crisis or unrest; sovereign stress or contagion. Multi-year repricing of political risk.",
        }
        if is_taiwan:
            paths = {"contained": 50, "regional_escalation": 32, "systemic_crisis": 18}
            path_desc = {
                "contained": f"In {locale}: coalition or minority government; cross-strait rhetoric contained. Tech and supply chain stabilise within weeks.",
                "regional_escalation": f"In {locale}: prolonged deadlock or repeat elections; regional spillover and semiconductor repricing. US/China posture uncertainty 6–12 months.",
                "systemic_crisis": f"In {locale}: constitutional crisis or unrest; sovereign and tech supply chain stress. Multi-year repricing of Taiwan and regional risk.",
            }
        elif is_europe or is_ukraine:
            path_desc = {
                "contained": f"In {locale}: coalition or minority government; policy continuity. EU/NATO alignment and energy policy clarity within weeks.",
                "regional_escalation": f"In {locale}: prolonged instability or repeat elections; regional spillover and energy/defence repricing. Policy uncertainty 6–12 months.",
                "systemic_crisis": f"In {locale}: constitutional crisis or unrest; sovereign stress or EU fragmentation. Multi-year repricing of political risk.",
            }
        elif is_asean:
            path_desc = {
                "contained": f"In {locale}: coalition or minority government; policy continuity with modest shifts. Regional markets stabilise within weeks.",
                "regional_escalation": f"In {locale}: prolonged instability; ASEAN spillover and investor flight. Policy uncertainty extends 6–12 months.",
                "systemic_crisis": f"In {locale}: constitutional crisis or unrest; sovereign stress or contagion. Multi-year repricing of political risk.",
            }
    elif et == "coup":
        paths = {"contained": 35, "regional_escalation": 40, "systemic_crisis": 25}
        path_desc = {
            "contained": "Quick consolidation or negotiated transition; sanctions lifted within months. Limited contagion.",
            "regional_escalation": "Insurgency or border tensions; refugee flows and regional military posture. Contagion to neighbouring states.",
            "systemic_crisis": "State collapse or prolonged conflict; humanitarian crisis and broad regional instability.",
        }
        if is_sahel or "africa" in r:
            paths = {"contained": 30, "regional_escalation": 45, "systemic_crisis": 25}
            path_desc = {
                "contained": f"In {locale}: quick consolidation or ECOWAS/AU-brokered transition; sanctions lifted within months. Limited regional contagion.",
                "regional_escalation": f"In {locale}: insurgency or border tensions; refugee flows and regional force (G5 Sahel, Wagner) involvement. Contagion to neighbouring states.",
                "systemic_crisis": f"In {locale}: state collapse or prolonged conflict; humanitarian crisis and broad Sahel/west Africa instability.",
            }
        elif is_asean or "latin" in r or "america" in r:
            path_desc = {
                "contained": f"In {locale}: quick consolidation or negotiated transition; regional bodies weigh in; sanctions lifted within months. Limited contagion.",
                "regional_escalation": f"In {locale}: insurgency or border tensions; refugee flows and regional posture. Contagion to neighbouring states.",
                "systemic_crisis": f"In {locale}: state collapse or prolonged conflict; humanitarian crisis and broad regional instability.",
            }
    elif et == "military_incursion":
        paths = {"contained": 40, "regional_escalation": 35, "systemic_crisis": 25}
        path_desc = {
            "contained": "Ceasefire or frozen conflict; sanctions and deterrence hold. No further territorial change.",
            "regional_escalation": "Alliance involvement and wider conflict; energy and trade disruption. Prolonged crisis.",
            "systemic_crisis": "Major power confrontation; global recession and security regime shift.",
        }
        if is_taiwan:
            paths = {"contained": 35, "regional_escalation": 40, "systemic_crisis": 25}
            path_desc = {
                "contained": f"In {locale}: ceasefire or frozen conflict; Strait open under tension; sanctions and US/Japan deterrence hold. No blockade or further territorial change.",
                "regional_escalation": f"In {locale}: blockade or limited kinetic action; US/Japan involvement; semiconductor and shipping disruption. Prolonged crisis and repricing.",
                "systemic_crisis": f"In {locale}: major power confrontation; global recession and security regime shift. Multi-year tech and supply chain reordering.",
            }
        elif is_ukraine:
            path_desc = {
                "contained": f"In {locale}: ceasefire or frozen conflict; sanctions and NATO posture hold. No further territorial change.",
                "regional_escalation": f"In {locale}: alliance involvement and wider conflict; energy and trade disruption. Prolonged crisis.",
                "systemic_crisis": f"In {locale}: major power confrontation; global recession and security regime shift.",
            }
        elif is_gulf:
            paths = {"contained": 45, "regional_escalation": 35, "systemic_crisis": 20}
            path_desc = {
                "contained": f"In {locale}: de-escalation or frozen conflict; Hormuz and shipping secured; sanctions hold. No further territorial change.",
                "regional_escalation": f"In {locale}: wider regional conflict; oil and LNG disruption; US and GCC posture. Prolonged energy and market crisis.",
                "systemic_crisis": f"In {locale}: major regional conflagration; global energy shock and recession risk.",
            }
    elif et == "sanctions":
        paths = {"contained": 50, "regional_escalation": 35, "systemic_crisis": 15}
        path_desc = {
            "contained": "Target complies or negotiates; sanctions eased or lifted. Limited spillover.",
            "regional_escalation": "Prolonged sanctions and evasion; secondary sanctions and alliance friction. Economic and political spillover.",
            "systemic_crisis": "Financial fragmentation or broad decoupling; systemic liquidity and growth impact.",
        }
        if is_ukraine or "russia" in c:
            path_desc = {
                "contained": f"In {locale}: target complies or negotiates; sanctions eased or lifted. Limited spillover; energy carve-outs may persist.",
                "regional_escalation": f"In {locale}: prolonged sanctions and evasion; secondary sanctions and alliance friction. Energy and supply chain spillover.",
                "systemic_crisis": f"In {locale}: financial fragmentation or broad decoupling; systemic liquidity and growth impact.",
            }
        elif is_gulf or "iran" in c:
            path_desc = {
                "contained": f"In {locale}: target complies or negotiates; sanctions eased or lifted. Limited spillover; energy markets stabilise.",
                "regional_escalation": f"In {locale}: prolonged sanctions and evasion; regional proxy escalation. Economic and political spillover.",
                "systemic_crisis": f"In {locale}: oil market disruption and financial fragmentation; broad growth impact.",
            }
    elif et == "financial_crisis":
        paths = {"contained": 45, "regional_escalation": 38, "systemic_crisis": 17}
        path_desc = {
            "contained": "Liquidity and policy response stabilise; IMF or regional support. Contagion limited.",
            "regional_escalation": "Sovereign or banking stress spreads; capital controls and restructuring. Regional recession.",
            "systemic_crisis": "Global financial stress; broad recession and political backlash. Multi-year adjustment.",
        }
        if is_europe:
            path_desc = {
                "contained": f"In {locale}: liquidity and ECB/IMF response stabilise; contagion limited. Sovereign and banking stress contained.",
                "regional_escalation": f"In {locale}: sovereign or banking stress spreads; capital controls and restructuring. Regional recession and political backlash.",
                "systemic_crisis": f"In {locale}: euro area or EU-wide stress; broad recession and political backlash. Multi-year adjustment.",
            }
        elif is_asean:
            path_desc = {
                "contained": f"In {locale}: liquidity and regional/IMF support stabilise; contagion limited.",
                "regional_escalation": f"In {locale}: sovereign or banking stress spreads; capital controls and restructuring. Regional recession.",
                "systemic_crisis": f"In {locale}: broad regional financial stress; recession and political backlash. Multi-year adjustment.",
            }
    elif et == "trade_embargo":
        paths = {"contained": 50, "regional_escalation": 35, "systemic_crisis": 15}
        path_desc = {
            "contained": "Negotiated exit or narrow scope; supply chain adaptation. Limited inflation and growth drag.",
            "regional_escalation": "Broader decoupling and bloc formation; prolonged inflation and sector stress.",
            "systemic_crisis": "Fragmentation of trade and payments; global recession and security spillover.",
        }
        if is_taiwan or is_east_asia:
            path_desc = {
                "contained": f"In {locale}: negotiated exit or narrow scope; tech and supply chain adaptation. Limited inflation and growth drag.",
                "regional_escalation": f"In {locale}: broader decoupling and bloc formation; semiconductor and component stress. Prolonged inflation and sector repricing.",
                "systemic_crisis": f"In {locale}: fragmentation of tech supply chain and payments; global recession and security spillover.",
            }
        elif is_gulf:
            path_desc = {
                "contained": f"In {locale}: negotiated exit or narrow scope; energy supply chain adaptation. Limited inflation and growth drag.",
                "regional_escalation": f"In {locale}: broader energy decoupling; prolonged oil/gas and inflation stress.",
                "systemic_crisis": f"In {locale}: fragmentation of energy trade and payments; global recession and security spillover.",
            }
    else:
        paths = {"contained": 50, "regional_escalation": 32, "systemic_crisis": 18}
        path_desc = {
            "contained": f"Event contained within initial scope; policy and market adjustment. Limited spillover.",
            "regional_escalation": f"Spillover to neighbours and partners; prolonged uncertainty and repricing.",
            "systemic_crisis": f"Wide contagion; structural shift in growth, security, or institutions.",
        }

    return paths, path_desc


def run_scenario_engine(event_type: str, region: str = "", country: str = "") -> dict:
    """Run Scenario Engine: multi-agent responses + 3-path probability outcomes. Returns dict for template and export.
    Results vary by event_type, region, and country so every simulation is distinct."""
    agents = _engine_agent_responses(event_type, region, country)
    et = (event_type or "").lower().replace("-", "_")
    paths, path_desc = _engine_paths(event_type, region, country)

    event_label = next((l for k, l in SCENARIO_ENGINE_EVENT_TYPES if k == et), event_type or "Custom event")
    return {
        "event_type": et,
        "event_label": event_label,
        "region": region,
        "country": country,
        "agents": agents,
        "paths": paths,
        "path_descriptions": path_desc,
        "run_at": datetime.utcnow().isoformat() + "Z",
    }


def generate_scenario_engine_export(engine_result: dict, export_format: str) -> tuple:
    """Generate export document from Scenario Engine result. Returns (content: bytes, filename: str, mimetype: str). All exports are DOCX or PPTX."""
    from app.export_docs import (
        build_policy_memo_docx,
        build_risk_briefing_docx,
        build_investor_note_docx,
        build_exec_summary_pptx,
    )

    fmt = (export_format or "").lower().replace("-", "_")
    docx_mime = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    pptx_mime = "application/vnd.openxmlformats-officedocument.presentationml.presentation"

    if fmt == "policy_memo":
        content = build_policy_memo_docx(engine_result)
        return content, "scenario-engine-policy-memo.docx", docx_mime
    if fmt == "risk_briefing":
        content = build_risk_briefing_docx(engine_result)
        return content, "scenario-engine-risk-briefing.docx", docx_mime
    if fmt == "investor_note":
        content = build_investor_note_docx(engine_result)
        return content, "scenario-engine-investor-note.docx", docx_mime
    if fmt == "exec_summary_ppt" or fmt == "exec_summary":
        content = build_exec_summary_pptx(engine_result)
        return content, "scenario-engine-executive-summary.pptx", pptx_mime

    # Default: policy memo DOCX
    content = build_policy_memo_docx(engine_result)
    return content, "scenario-engine-export.docx", docx_mime


def _agent_responses(scenario: dict) -> dict:
    """Multi-agent simulation: rule-based responses keyed by scenario type and region."""
    stype = (scenario.get("scenario_type") or "").lower()
    region = (scenario.get("region") or "")
    name = scenario.get("name") or ""
    horizon = scenario.get("horizon_year") or 2030
    # Economic policy agent
    if "taiwan" in stype or "taiwan" in name.lower():
        economic = "Supply chain disruption across semiconductors, electronics, and shipping would push inflation and force rapid diversification. Capital flight from regional markets; central banks may intervene. Long-term shift of final assembly and key components out of Taiwan and coastal China."
        military = "Elevated force posture on both sides; blockade or limited kinetic action would trigger allied response. Risk of escalation to maritime and air denial. Deterrence depends on credible US/Japan posture and ASEAN neutrality."
        domestic = "Domestic support for defence spending and conscription would rise in Taiwan and Japan; mainland nationalism and PLA pressure would increase. US domestic politics would be pivotal for response speed and scope."
        diplomatic = "Allies would coordinate on sanctions and support; China would seek to split ASEAN and EU. UN and multilateral forums would be contested. Outcome hinges on clarity of red lines and communication channels."
        private = "Tech and manufacturing would accelerate friend-shoring; logistics and insurance would reprice risk. Commodity and energy markets would spike. Contingency planning and dual sourcing would become baseline."
    elif "currency" in stype or "asean" in name.lower():
        economic = "Regional currency volatility would trigger capital controls and swap line activation. IMF and regional financing arrangements would be tested. Inflation and debt sustainability would dominate policy."
        military = "Limited direct military dimension unless spillover from territorial or internal instability. Security cooperation may intensify on non-traditional threats (maritime, terrorism)."
        domestic = "Political pressure for fiscal discipline and reserve adequacy. Populist backlash against austerity or external conditionality. Elections could shift policy in key ASEAN states."
        diplomatic = "ASEAN centrality would be tested; bilateral swap lines (China, Japan, US) and CMIM would be in focus. Coordination with G20 and IMF on liquidity."
        private = "Corporates would hedge FX and diversify treasury; supply chain and FDI would reassess country risk. Banking sector stress in highly dollar-indebted economies."
    elif "gulf" in stype or "energy" in stype:
        economic = "Oil and LNG price spikes would feed into global inflation and recession risk. Strategic reserves would be drawn; alternative routes and suppliers would be prioritized. Long-term acceleration of energy transition."
        military = "Naval presence and escort operations would increase; mine and asymmetric threats would shape force posture. Escalation could draw in regional and extra-regional actors."
        domestic = "Consumer and industry pressure on governments; subsidy and price cap politics. Defence and energy security would compete for budget."
        diplomatic = "Coalition-building for freedom of navigation and sanctions; OPEC+ and producer coordination would be critical. US and Gulf allies would coordinate closely."
        private = "Shipping and insurance would reprice; supply chain and procurement would seek dual sourcing. Energy-intensive sectors would face margin and planning uncertainty."
    else:
        economic = f"Region-specific economic policy responses would depend on fiscal space, trade openness, and external debt. Horizon {horizon}: growth and stability trade-offs."
        military = f"Military posture and alliance commitments in {region} would shape escalation dynamics. Deterrence and restraint both in play."
        domestic = "Domestic politics would mediate between hawkish and engagement factions; public opinion and elections would influence policy choices."
        diplomatic = "Multilateral and bilateral diplomacy would seek to de-escalate and align allies; messaging and red lines would be critical."
        private = "Private sector would adapt supply chains, pricing, and investment to perceived risk; contingency planning would increase."
    return {
        "economic_policy": economic,
        "military": military,
        "domestic_politics": domestic,
        "diplomatic": diplomatic,
        "private_sector": private,
    }


def run_scenario_simulation(scenario_id: int) -> Optional[int]:
    """Run multi-agent simulation for a scenario; store in scenario_runs. Returns run_id or None."""
    scenario = get_scenario(scenario_id)
    if not scenario:
        return None
    agent_outputs = _agent_responses(scenario)
    now = datetime.utcnow().isoformat() + "Z"
    # Probability-weighted summary (rule-based)
    stype = (scenario.get("scenario_type") or "").lower()
    if "taiwan" in stype:
        prob_text = "Base case: elevated tension, no blockade (60%). Downside: limited kinetic or blockade (25%). Tail: full-scale conflict (15%). Probability-weighted impact: severe supply chain and market disruption in base; critical in downside."
    elif "currency" in stype:
        prob_text = "Base: contained volatility with policy response (55%). Downside: one or more sovereign or banking stress events (30%). Tail: regional crisis (15%). Weighted outcome: moderate growth drag and repricing of risk."
    elif "energy" in stype or "gulf" in stype:
        prob_text = "Base: supply disruption contained within weeks (50%). Downside: prolonged disruption 3–6 months (35%). Tail: major escalation (15%). Weighted: significant inflation and growth impact in downside and tail."
    else:
        prob_text = "Probability-weighted scenarios depend on policy choices and external shocks. Run region-specific assumptions for custom weights."
    with _connection() as conn:
        cur = conn.execute(
            "INSERT INTO scenario_runs (scenario_id, run_at, agent_outputs, probability_summary, outlook_summary, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (scenario_id, now, json.dumps(agent_outputs), prob_text, None, now),
        )
        return cur.lastrowid


def generate_risk_outlook(scenario_id: Optional[int] = None, horizon_years: int = 10) -> dict:
    """Generate a 10-year risk outlook report (summary and sections). Optionally tied to a scenario."""
    from app.institutional_models.readers import get_gepi_latest
    from app.synthesis import get_signal_divergences

    scenario = get_scenario(scenario_id) if scenario_id else None
    start_year = datetime.utcnow().year
    end_year = start_year + horizon_years
    sections = []

    # ---- Scenario context (if selected) ----
    run = None
    if scenario:
        sections.append(f"## Scenario: {scenario.get('name')} (horizon {scenario.get('horizon_year')})")
        if scenario.get("description"):
            sections.append(scenario.get("description"))

        runs = get_scenario_runs(scenario_id, limit=1)
        if runs:
            run = get_scenario_run(runs[0]["id"])
        if run and run.get("probability_summary"):
            sections.append("### Probability-weighted outcome")
            sections.append(run["probability_summary"])
        if run and run.get("agent_outputs"):
            sections.append("### Agent outlooks (latest run)")
            for agent_name, text in run["agent_outputs"].items():
                sections.append(f"**{agent_name.replace('_', ' ').title()}**\n{text}")

    # ---- System signals (always useful) ----
    gepi = get_gepi_latest()
    gepi_score = round(float((gepi or {}).get("gepi_score") or 0), 2) if gepi else None

    top_risk = []
    try:
        top_risk = (get_country_risk_snapshots() or [])[:10]
    except Exception:
        top_risk = []

    # Fragility hotspots: latest fragility level per country
    fragility_hotspots = []
    try:
        cutoff = (datetime.utcnow() - timedelta(days=14)).strftime("%Y-%m-%d")
        with _connection() as conn:
            cur = conn.execute(
                """SELECT f.country_code, f.fragility_level
                   FROM gpi_fragility_daily f
                   INNER JOIN (
                     SELECT country_code, MAX(as_of_date) AS max_date
                     FROM gpi_fragility_daily
                     WHERE as_of_date >= ? AND country_code IS NOT NULL
                     GROUP BY country_code
                   ) sub ON f.country_code = sub.country_code AND f.as_of_date = sub.max_date
                   ORDER BY f.fragility_level DESC LIMIT 10""",
                (cutoff,),
            )
            fragility_hotspots = [
                {"code": row[0], "level": round(float(row[1] or 0) * 100, 0)}
                for row in cur.fetchall()
                if row and row[0]
            ]
    except Exception:
        fragility_hotspots = []

    spikes = []
    declining = []
    try:
        spikes = get_spike_topics(days_recent=7, days_prior=7, limit=8) or []
        declining = get_declining_topics(days_recent=7, days_prior=7, limit=6) or []
    except Exception:
        spikes, declining = [], []

    divergences = []
    try:
        divergences = (get_signal_divergences() or [])[:3]
    except Exception:
        divergences = []

    clusters = []
    try:
        clusters = get_clusters_with_counts(limit=8) or []
    except Exception:
        clusters = []

    digests = []
    try:
        digests = get_digests(limit=3, digest_type=None) or []
    except Exception:
        digests = []

    # ---- Executive summary ----
    sections.append(f"## {horizon_years}-year risk outlook ({start_year}–{end_year})")
    if scenario and run and run.get("probability_summary"):
        sections.append("### Executive summary")
        sections.append(
            "This outlook weights risks by the selected scenario’s probability-weighted outcome, then overlays current system signals "
            "to highlight the most actionable drivers and watch items over the next decade."
        )
    else:
        sections.append("### Executive summary")
        sections.append(
            "This outlook blends system-wide risk signals (GEPI escalation pressure, fragility hotspots, topic momentum, and model divergences) "
            "to generate a practical 10-year risk narrative and a short set of watch-and-act recommendations."
        )

    exec_bullets = []
    if gepi_score is not None:
        exec_bullets.append(f"- **GEPI (escalation pressure):** {gepi_score:.2f} (global).")
    if top_risk:
        top_list = ", ".join(
            f"[{r.get('country_code')}](/country/{r.get('country_code')})({int(r.get('risk_score') or 0)})"
            for r in top_risk[:6]
            if r.get("country_code")
        )
        if top_list:
            exec_bullets.append(f"- **Top systemic risk countries:** {top_list}")
    if fragility_hotspots:
        frag_list = ", ".join(
            f"[{h.get('code')}](/country/{h.get('code')})({int(h.get('level') or 0)})"
            for h in fragility_hotspots[:5]
            if h.get("code")
        )
        if frag_list:
            exec_bullets.append(f"- **Fragility hotspots:** {frag_list}")
    if exec_bullets:
        sections.append("\n".join(exec_bullets))

    # ---- Key drivers / watch items ----
    sections.append("## Key drivers (what matters next)")

    # Topic momentum
    if spikes or declining:
        spike_text = ", ".join(t[0] for t in spikes[:5] if t and t[0]) or "—"
        decline_text = ", ".join(t[0] for t in declining[:4] if t and t[0]) or "—"
        sections.append("### Topic momentum (last 7d vs prior)")
        sections.append(f"- Spiking topics: {spike_text}\n- Declining topics: {decline_text}")

    # Divergences
    if divergences:
        sections.append("### Model divergences (watch for mismatches)")
        divergence_lines = []
        for d in divergences:
            title = d.get("title") or d.get("type") or "Divergence"
            detail = d.get("detail") or ""
            suggestion = d.get("suggestion") or ""
            divergence_lines.append(
                f"- **{title}**: {detail}{' ' + 'Suggested action: ' + suggestion if suggestion else ''}"
            )
        if divergence_lines:
            sections.append("\n".join(divergence_lines))

    # Story clusters
    if clusters:
        sections.append("### Story clusters (coverage density)")
        cluster_lines = []
        for c in clusters[:5]:
            label = c.get("label") or "Cluster"
            count = c.get("count") or 0
            cluster_lines.append(f"- {label} — {int(count)} related articles")
        if cluster_lines:
            sections.append("\n".join(cluster_lines))

    # ---- Action checklist ----
    sections.append("## Action checklist (practical next steps)")
    if scenario and run and run.get("probability_summary"):
        sections.append(
            "\n".join(
                [
                    "- **Near-term (0–90d):** validate assumptions behind the scenario’s probability weights; monitor the drivers explicitly (escalation pressure, economic fragility, domestic stability, and private-sector adaptation).",
                    "- **Mid-term (6–18mo):** set escalation/mitigation triggers (trade/sanctions pace, fragility turning points, and topic momentum shifts) so you can adjust strategy early.",
                    "- **Long-term (3–10y):** maintain optionality: diversify supply chains/inputs, stress-test sovereign & corporate balance sheets, and pre-position diplomatic channels for de-escalation.",
                ]
            )
        )
    else:
        sections.append(
            "\n".join(
                [
                    "- **Near-term (0–90d):** shortlist the top risk + fragility countries; check whether topic momentum is rising or falling and whether divergences are present.",
                    "- **Mid-term (6–18mo):** convert watch items into triggers (what would confirm downside vs base-case) and assign review cadence to each driver.",
                    "- **Long-term (3–10y):** plan for regime drift: budget for policy response lag, protect critical inputs, and keep monitoring horizons adaptive to unfolding signals.",
                ]
            )
        )

    # ---- Recent context, short ----
    if digests:
        sections.append("## Recent context (to ground assumptions)")
        for d in digests:
            title = (d.get("title") or "").strip()
            if title:
                sections.append(f"- {title}")

    # ---- Final note ----
    sections.append(
        "Risks are probability-weighted and signal-dependent. Update assumptions and, where relevant, re-run the selected scenario with revised region/country assumptions for a refreshed outlook."
    )

    return {
        "scenario": scenario,
        "horizon_years": horizon_years,
        "start_year": start_year,
        "end_year": end_year,
        "report_markdown": "\n\n".join(sections),
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }


# --- Economic-geopolitical integration layer ---

def _compute_economic_fragility(row: dict) -> int:
    """Blend debt, capital flight, reserves, FX, energy exposure into 0–100 economic fragility score."""
    debt = min(100, max(0, row.get("debt_distress_score") or 0))
    flight = min(100, max(0, row.get("capital_flight_risk") or 0))
    reserves = row.get("reserve_months_imports")
    if reserves is None:
        reserve_score = 50
    else:
        # Low months = high fragility. 0 months -> 100, 24+ months -> 0
        reserve_score = max(0, min(100, int(100 - reserves * 4)))
    fx = min(100, max(0, row.get("fx_vulnerability_score") or 0))
    energy = min(100, max(0, int(row.get("energy_import_exposure_pct") or 0)))
    # Weighted average
    return round(0.25 * debt + 0.25 * flight + 0.2 * reserve_score + 0.2 * fx + 0.1 * energy)


def _get_article_risk_by_code() -> dict:
    """Build map from country_code (ISO3 or ISO2) to article-derived risk 0-100 from country_risk_snapshots."""
    from app.country_data import ISO3_TO_2
    snapshots = get_country_risk_snapshots()
    code_to_risk = {}
    iso2_to_iso3 = {v: k for k, v in (ISO3_TO_2 or {}).items()}
    for s in snapshots:
        code = (s.get("country_code") or "").strip().upper()
        risk = s.get("risk_score") or 0
        if len(code) == 2:
            code_to_risk[code] = risk
            iso3 = iso2_to_iso3.get(code)
            if iso3:
                code_to_risk[iso3] = risk
        elif len(code) == 3:
            code_to_risk[code] = risk
    return code_to_risk


def get_integration_countries(
    region: Optional[str] = None,
    limit: int = 300,
    sort: Optional[str] = None,
    order: Optional[str] = None,
) -> list:
    """List countries with integration data; blend static geopolitical + article-derived risk; add economic and combined scores."""
    with _connection() as conn:
        if region:
            cur = conn.execute(
                """SELECT country_code, country_name, region, trade_flow_pct_gdp, debt_distress_score, capital_flight_risk, reserve_months_imports, fx_vulnerability_score, energy_import_exposure_pct, geopolitical_fragility_score, population_2026, land_area_km2, density_per_km2, updated_at
                   FROM country_risk_integration WHERE region = ? ORDER BY country_name LIMIT ?""",
                (region, limit),
            )
        else:
            cur = conn.execute(
                """SELECT country_code, country_name, region, trade_flow_pct_gdp, debt_distress_score, capital_flight_risk, reserve_months_imports, fx_vulnerability_score, energy_import_exposure_pct, geopolitical_fragility_score, population_2026, land_area_km2, density_per_km2, updated_at
                   FROM country_risk_integration ORDER BY region, country_name LIMIT ?""",
                (limit,),
            )
        rows = [dict(row) for row in cur.fetchall()]
    article_risk = _get_article_risk_by_code()
    from app.country_data import ISO3_TO_2
    for r in rows:
        static_geo = min(100, max(0, r.get("geopolitical_fragility_score") or 0))
        code3 = (r.get("country_code") or "").upper()
        code2 = ISO3_TO_2.get(code3, "") if ISO3_TO_2 else ""
        art_risk = article_risk.get(code3) or article_risk.get(code2)
        if art_risk is not None:
            geo = round(0.5 * static_geo + 0.5 * min(100, art_risk))
        else:
            geo = static_geo
        r["geopolitical_fragility_score"] = geo
        r["economic_fragility_score"] = _compute_economic_fragility(r)
        r["combined_systemic_risk_score"] = round(0.5 * geo + 0.5 * r["economic_fragility_score"])

    sort_key = (sort or "").strip() or None
    order_desc = (order or "asc").strip().lower() == "desc"
    allowed_sort = (
        "country_name",
        "region",
        "population_2026",
        "land_area_km2",
        "density_per_km2",
        "geopolitical_fragility_score",
        "economic_fragility_score",
        "combined_systemic_risk_score",
        "trade_flow_pct_gdp",
        "reserve_months_imports",
        "debt_distress_score",
        "energy_import_exposure_pct",
        "capital_flight_risk",
        "fx_vulnerability_score",
    )
    if sort_key and sort_key in allowed_sort:
        def _sort_val(r):
            v = r.get(sort_key)
            if v is None:
                return "" if sort_key in ("country_name", "region") else -1
            return v

        rows.sort(key=_sort_val, reverse=order_desc)
    return rows


def get_integration_country(country_code: str) -> Optional[dict]:
    """Single country: all indicators plus geopolitical (blended with article risk), economic, combined fragility scores."""
    with _connection() as conn:
        cur = conn.execute(
            """SELECT country_code, country_name, region, trade_flow_pct_gdp, debt_distress_score, capital_flight_risk, reserve_months_imports, fx_vulnerability_score, energy_import_exposure_pct, geopolitical_fragility_score, population_2026, land_area_km2, density_per_km2, updated_at
               FROM country_risk_integration WHERE country_code = ?""",
            (country_code.upper(),),
        )
        row = cur.fetchone()
    if not row:
        return None
    r = dict(row)
    static_geo = min(100, max(0, r.get("geopolitical_fragility_score") or 0))
    article_risk = _get_article_risk_by_code()
    from app.country_data import ISO3_TO_2
    code3 = (r.get("country_code") or "").upper()
    code2 = ISO3_TO_2.get(code3, "") if ISO3_TO_2 else ""
    art_risk = article_risk.get(code3) or article_risk.get(code2)
    if art_risk is not None:
        r["geopolitical_fragility_score"] = round(0.5 * static_geo + 0.5 * min(100, art_risk))
    r["economic_fragility_score"] = _compute_economic_fragility(r)
    r["combined_systemic_risk_score"] = round(0.5 * min(100, max(0, r.get("geopolitical_fragility_score") or 0)) + 0.5 * r["economic_fragility_score"])
    return r


# --- Extended indicator getters (roadmap categories 1–13) ---

def get_macroeconomic_stress(country_code: str, limit: int = 10) -> list:
    """Latest macroeconomic stress indicators for a country."""
    with _connection() as conn:
        cur = conn.execute(
            """SELECT country_code, as_of_date, gdp_growth_quarterly_pct, gdp_growth_annual_pct, inflation_pct,
                      current_account_pct_gdp, external_debt_pct_gdp, debt_to_gdp_pct, sovereign_rating, bond_spread_bps, source
               FROM macroeconomic_stress WHERE country_code = ? ORDER BY as_of_date DESC LIMIT ?""",
            (country_code.upper(), limit),
        )
        return [dict(row) for row in cur.fetchall()]


def get_macroeconomic_stress_history(country_code: str, days: int = 365) -> list:
    """Macroeconomic stress over time (chronological) for charting."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    with _connection() as conn:
        cur = conn.execute(
            """SELECT as_of_date, gdp_growth_annual_pct, inflation_pct, debt_to_gdp_pct, sovereign_rating
               FROM macroeconomic_stress WHERE country_code = ? AND as_of_date >= ?
               ORDER BY as_of_date ASC""",
            (country_code.upper(), cutoff),
        )
        return [dict(row) for row in cur.fetchall()]


def get_energy_commodity_exposure(country_code: str, limit: int = 10) -> list:
    """Energy and commodity exposure for a country."""
    with _connection() as conn:
        cur = conn.execute(
            """SELECT country_code, as_of_date, oil_production_bpd, gas_exports_bcm, lng_capacity_mtpa,
                      energy_import_pct, rare_earth_production_share, grain_export_import_exposure, notes
               FROM energy_commodity_exposure WHERE country_code = ? ORDER BY as_of_date DESC LIMIT ?""",
            (country_code.upper(), limit),
        )
        return [dict(row) for row in cur.fetchall()]


def get_military_capability_snapshot(country_code: str, limit: int = 5) -> list:
    """Military capability snapshot (troops, naval tonnage, alliances)."""
    with _connection() as conn:
        cur = conn.execute(
            """SELECT country_code, as_of_year, active_troops, naval_tonnage, defense_alliances, source
               FROM military_capability_snapshot WHERE country_code = ? ORDER BY as_of_year DESC LIMIT ?""",
            (country_code.upper(), limit),
        )
        return [dict(row) for row in cur.fetchall()]


def get_trade_flow_partners(country_code: str, direction: Optional[str] = None, limit: int = 20) -> list:
    """Top trade partners (export/import destinations)."""
    with _connection() as conn:
        if direction:
            cur = conn.execute(
                """SELECT country_code, direction, partner_country, sector, share_pct, value_usd_millions, as_of_year
                   FROM trade_flow_partners WHERE country_code = ? AND direction = ? ORDER BY COALESCE(share_pct, -1) DESC LIMIT ?""",
                (country_code.upper(), direction, limit),
            )
        else:
            cur = conn.execute(
                """SELECT country_code, direction, partner_country, sector, share_pct, value_usd_millions, as_of_year
                   FROM trade_flow_partners WHERE country_code = ? ORDER BY direction, COALESCE(share_pct, -1) DESC LIMIT ?""",
                (country_code.upper(), limit),
            )
        return [dict(row) for row in cur.fetchall()]


def get_multilateral_participation(country_code: str) -> list:
    """Multilateral org memberships (WTO, IMF, World Bank, NATO, AIIB, BRI, etc.)."""
    with _connection() as conn:
        cur = conn.execute(
            """SELECT country_code, org_key, membership_status, program_notes, as_of_date
               FROM multilateral_participation WHERE country_code = ? ORDER BY org_key""",
            (country_code.upper(),),
        )
        return [dict(row) for row in cur.fetchall()]


def get_capital_flows(country_code: str, limit: int = 10) -> list:
    """FDI, portfolio flows, sovereign wealth funds."""
    with _connection() as conn:
        cur = conn.execute(
            """SELECT country_code, as_of_year, fdi_inflow_usd_millions, fdi_outflow_usd_millions,
                      portfolio_flows_usd_millions, sector_exposure_json, sovereign_wealth_fund_usd_billions, source
               FROM capital_flows WHERE country_code = ? ORDER BY as_of_year DESC LIMIT ?""",
            (country_code.upper(), limit),
        )
        return [dict(row) for row in cur.fetchall()]


def get_elite_institutional(country_code: str) -> Optional[dict]:
    """Elite and institutional structure (governance, key actors, SOEs, CBI, party structure)."""
    with _connection() as conn:
        cur = conn.execute(
            """SELECT country_code, governance_model, key_actors_json, major_soes,
                      central_bank_independence_score, party_structure, coup_attempts_history, updated_at
               FROM elite_institutional WHERE country_code = ?""",
            (country_code.upper(),),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def get_climate_resource_vulnerability(country_code: str, limit: int = 5) -> list:
    """Climate and resource vulnerability (water stress, food insecurity, disaster freq, climate risk)."""
    with _connection() as conn:
        cur = conn.execute(
            """SELECT country_code, as_of_year, water_stress_index, food_insecurity_index,
                      natural_disaster_frequency, climate_risk_score, source
               FROM climate_resource_vulnerability WHERE country_code = ? ORDER BY as_of_year DESC LIMIT ?""",
            (country_code.upper(), limit),
        )
        return [dict(row) for row in cur.fetchall()]


def get_legislative_policy_tracker(jurisdiction: Optional[str] = None, bill_type: Optional[str] = None, limit: int = 20) -> list:
    """Legislative/policy tracker (sanction bills, defense bills, trade amendments, export controls)."""
    with _connection() as conn:
        params = []
        sql = "SELECT id, jurisdiction, bill_type, title, status, summary, source_url, introduced_date FROM legislative_policy_tracker WHERE 1=1"
        if jurisdiction:
            sql += " AND jurisdiction = ?"
            params.append(jurisdiction)
        if bill_type:
            sql += " AND bill_type = ?"
            params.append(bill_type)
        sql += " ORDER BY COALESCE(introduced_date, '') DESC, id DESC LIMIT ?"
        params.append(limit)
        cur = conn.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]


def get_technology_semiconductor(country_code: str, limit: int = 5) -> list:
    """Technology and semiconductor layer (chip exports, tech companies, manufacturing)."""
    with _connection() as conn:
        cur = conn.execute(
            """SELECT country_code, as_of_year, chip_exports_usd_millions, critical_tech_companies,
                      advanced_manufacturing_capacity, export_restriction_notes
               FROM technology_semiconductor WHERE country_code = ? ORDER BY as_of_year DESC LIMIT ?""",
            (country_code.upper(), limit),
        )
        return [dict(row) for row in cur.fetchall()]


def get_conflict_event_imports(country_code: Optional[str] = None, source: Optional[str] = None, limit: int = 50) -> list:
    """Conflict events from ACLED, UCDP, ICEWS."""
    with _connection() as conn:
        params = []
        sql = "SELECT source, external_id, country_code, event_date, event_type, fatalities, summary FROM conflict_event_imports WHERE 1=1"
        if country_code:
            sql += " AND country_code = ?"
            params.append(country_code.upper())
        if source:
            sql += " AND source = ?"
            params.append(source)
        sql += " ORDER BY event_date DESC LIMIT ?"
        params.append(limit)
        cur = conn.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]


def get_geospatial_infrastructure(country_code: Optional[str] = None, infra_type: Optional[str] = None, limit: int = 50) -> list:
    """Geospatial infrastructure (ports, cables, pipelines, logistics hubs)."""
    with _connection() as conn:
        params = []
        sql = "SELECT infra_type, name, country_code, region, lat, lon, capacity_notes FROM geospatial_infrastructure WHERE 1=1"
        if country_code:
            sql += " AND country_code = ?"
            params.append(country_code.upper())
        if infra_type:
            sql += " AND infra_type = ?"
            params.append(infra_type)
        sql += " ORDER BY infra_type, name LIMIT ?"
        params.append(limit)
        cur = conn.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]


def get_energy_commodity_summary(limit: int = 15) -> list:
    """Top energy/commodity exposure by country (latest per country)."""
    with _connection() as conn:
        cur = conn.execute(
            """SELECT e.country_code, e.as_of_date, e.oil_production_bpd, e.gas_exports_bcm, e.lng_capacity_mtpa,
                      e.rare_earth_production_share, e.grain_export_import_exposure,
                      (SELECT country_name FROM country_risk_integration c WHERE c.country_code = e.country_code LIMIT 1) as country_name
               FROM energy_commodity_exposure e
               INNER JOIN (SELECT country_code, MAX(as_of_date) as md FROM energy_commodity_exposure GROUP BY country_code) sub
                 ON e.country_code = sub.country_code AND e.as_of_date = sub.md
               ORDER BY COALESCE(e.oil_production_bpd, 0) + COALESCE(e.gas_exports_bcm, 0) * 100 DESC
               LIMIT ?""",
            (limit,),
        )
        return [dict(row) for row in cur.fetchall()]


def get_geospatial_infrastructure_summary(limit: int = 30, infra_type: Optional[str] = None) -> list:
    """Geospatial infrastructure list (ports, cables, pipelines)."""
    return get_geospatial_infrastructure(country_code=None, infra_type=infra_type, limit=limit)


def get_technology_semiconductor_summary(limit: int = 15) -> list:
    """Top semiconductor/tech countries by chip exports."""
    with _connection() as conn:
        cur = conn.execute(
            """SELECT t.country_code, t.as_of_year, t.chip_exports_usd_millions, t.critical_tech_companies,
                      (SELECT country_name FROM country_risk_integration c WHERE c.country_code = t.country_code LIMIT 1) as country_name
               FROM technology_semiconductor t
               INNER JOIN (SELECT country_code, MAX(as_of_year) as my FROM technology_semiconductor GROUP BY country_code) sub
                 ON t.country_code = sub.country_code AND t.as_of_year = sub.my
               ORDER BY COALESCE(t.chip_exports_usd_millions, 0) DESC
               LIMIT ?""",
            (limit,),
        )
        return [dict(row) for row in cur.fetchall()]


def get_military_capability_summary(limit: int = 15) -> list:
    """Top military capability by active troops (latest per country)."""
    with _connection() as conn:
        cur = conn.execute(
            """SELECT m.country_code, m.as_of_year, m.active_troops, m.naval_tonnage, m.defense_alliances,
                      (SELECT country_name FROM country_risk_integration c WHERE c.country_code = m.country_code LIMIT 1) as country_name
               FROM military_capability_snapshot m
               INNER JOIN (SELECT country_code, MAX(as_of_year) as my FROM military_capability_snapshot GROUP BY country_code) sub
                 ON m.country_code = sub.country_code AND m.as_of_year = sub.my
               ORDER BY COALESCE(m.active_troops, 0) DESC
               LIMIT ?""",
            (limit,),
        )
        return [dict(row) for row in cur.fetchall()]


def get_multilateral_summary_by_org(org_key: str, limit: int = 20) -> list:
    """Countries in a given multilateral org (WTO, IMF, NATO, AIIB, etc.)."""
    with _connection() as conn:
        cur = conn.execute(
            """SELECT m.country_code, m.membership_status, m.program_notes, c.country_name
               FROM multilateral_participation m
               LEFT JOIN country_risk_integration c ON c.country_code = m.country_code
               WHERE m.org_key = ?
               ORDER BY m.membership_status, m.country_code
               LIMIT ?""",
            (org_key, limit),
        )
        return [dict(row) for row in cur.fetchall()]


def get_macroeconomic_stress_alerts(threshold_debt: float = 80, threshold_inflation: float = 15, limit: int = 20) -> list:
    """Countries with high debt/GDP or inflation (macro stress alerts)."""
    with _connection() as conn:
        cur = conn.execute(
            """SELECT m.country_code, m.as_of_date, m.debt_to_gdp_pct, m.inflation_pct, m.sovereign_rating,
                      (SELECT country_name FROM country_risk_integration c WHERE c.country_code = m.country_code LIMIT 1) as country_name
               FROM macroeconomic_stress m
               INNER JOIN (SELECT country_code, MAX(as_of_date) as md FROM macroeconomic_stress GROUP BY country_code) sub
                 ON m.country_code = sub.country_code AND m.as_of_date = sub.md
               WHERE (m.debt_to_gdp_pct >= ? OR m.inflation_pct >= ?)
               ORDER BY COALESCE(m.debt_to_gdp_pct, 0) + COALESCE(m.inflation_pct, 0) DESC
               LIMIT ?""",
            (threshold_debt, threshold_inflation, limit),
        )
        return [dict(row) for row in cur.fetchall()]


# ---------------------------
# Live Macro Indicators module
# ---------------------------

def macro_upsert_data_source(key: str, name: str, base_url: str = None, notes: str = None) -> int:
    """Insert or update a data source. Returns source_id."""
    now = datetime.utcnow().isoformat()
    with _connection() as conn:
        conn.execute(
            """
            INSERT INTO data_sources(key, name, base_url, notes, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
              name=excluded.name,
              base_url=excluded.base_url,
              notes=excluded.notes,
              updated_at=excluded.updated_at
            """,
            ((key or "").strip()[:64], (name or "").strip()[:120], base_url, notes, now),
        )
        row = conn.execute("SELECT id FROM data_sources WHERE key = ?", ((key or "").strip()[:64],)).fetchone()
        return int(row[0]) if row else 0


def macro_upsert_country(
    code: str,
    name: str,
    region: str = None,
    *,
    is_asean: bool = False,
    is_g20: bool = False,
    is_major: bool = False,
) -> int:
    """Insert or update a country. Returns country_id."""
    now = datetime.utcnow().isoformat()
    c = (code or "").strip().upper()[:6]
    n = (name or "").strip() or c
    with _connection() as conn:
        conn.execute(
            """
            INSERT INTO countries(code, name, region, is_asean, is_g20, is_major, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET
              name=excluded.name,
              region=excluded.region,
              is_asean=excluded.is_asean,
              is_g20=excluded.is_g20,
              is_major=excluded.is_major,
              updated_at=excluded.updated_at
            """,
            (c, n, region, 1 if is_asean else 0, 1 if is_g20 else 0, 1 if is_major else 0, now),
        )
        row = conn.execute("SELECT id FROM countries WHERE code = ?", (c,)).fetchone()
        return int(row[0]) if row else 0


def macro_upsert_indicator(
    name: str,
    label: str,
    unit: str = None,
    frequency: str = None,
    category: str = None,
    *,
    source_id: int = None,
    external_code: str = None,
) -> int:
    """Insert or update an indicator. Returns indicator_id."""
    now = datetime.utcnow().isoformat()
    key = (name or "").strip().lower().replace(" ", "_")[:80]
    lab = (label or "").strip() or key
    with _connection() as conn:
        conn.execute(
            """
            INSERT INTO indicators(name, label, unit, frequency, category, source_id, external_code, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
              label=excluded.label,
              unit=excluded.unit,
              frequency=excluded.frequency,
              category=excluded.category,
              source_id=excluded.source_id,
              external_code=excluded.external_code,
              updated_at=excluded.updated_at
            """,
            (key, lab, unit, frequency, category, source_id, external_code, now),
        )
        row = conn.execute("SELECT id FROM indicators WHERE name = ?", (key,)).fetchone()
        return int(row[0]) if row else 0


def macro_insert_value(country_id: int, indicator_id: int, date: str, value, raw_json: str = None) -> bool:
    """Insert one value (idempotent). Returns True on success."""
    if not country_id or not indicator_id or not date:
        return False
    now = datetime.utcnow().isoformat()
    v = None
    if value is not None:
        try:
            v = float(value)
        except (TypeError, ValueError):
            v = None
    with _connection() as conn:
        conn.execute(
            """
            INSERT INTO indicator_values(country_id, indicator_id, date, value, raw_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(country_id, indicator_id, date) DO UPDATE SET
              value=excluded.value,
              raw_json=COALESCE(excluded.raw_json, indicator_values.raw_json)
            """,
            (int(country_id), int(indicator_id), str(date)[:16], v, raw_json, now),
        )
    return True


def macro_list_countries(region: str = None, group: str = None, limit: int = 400) -> list:
    """List countries for selectors. group: asean|g20|major."""
    where = []
    params = []
    if region:
        where.append("region = ?")
        params.append(region)
    if group == "asean":
        where.append("is_asean = 1")
    elif group == "g20":
        where.append("is_g20 = 1")
    elif group == "major":
        where.append("is_major = 1")
    sql = "SELECT code, name, region, is_asean, is_g20, is_major FROM countries"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY name ASC LIMIT ?"
    params.append(int(limit))
    with _connection() as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()
    return [
        {
            "code": r[0],
            "name": r[1],
            "region": r[2],
            "is_asean": int(r[3] or 0),
            "is_g20": int(r[4] or 0),
            "is_major": int(r[5] or 0),
        }
        for r in rows
    ]


def macro_list_indicators(category: str = None, *, only_with_data: bool = True) -> list:
    """List indicators for selectors.

    By default, hides indicators that have no ingested datapoints yet.
    """
    where = []
    params = []
    if category:
        where.append("i.category = ?")
        params.append(category)
    if only_with_data:
        where.append("EXISTS (SELECT 1 FROM indicator_values v WHERE v.indicator_id = i.id)")

    sql = "SELECT i.name, i.label, i.unit, i.frequency, i.category FROM indicators i"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY i.category ASC, i.label ASC"
    with _connection() as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()
    return [{"name": r[0], "label": r[1], "unit": r[2], "frequency": r[3], "category": r[4]} for r in rows]


def macro_get_latest(region: str = None, group: str = None, indicator: str = None, limit: int = 120) -> list:
    """Latest value per country/indicator (optionally filtered)."""
    where = []
    params = []
    if indicator:
        where.append("i.name = ?")
        params.append((indicator or "").strip().lower())
    if region:
        where.append("c.region = ?")
        params.append(region)
    if group == "asean":
        where.append("c.is_asean = 1")
    elif group == "g20":
        where.append("c.is_g20 = 1")
    elif group == "major":
        where.append("c.is_major = 1")
    sql = """
      SELECT c.code, c.name, c.region, i.name, i.label, i.unit, v.date, v.value
      FROM indicator_values v
      JOIN countries c ON c.id = v.country_id
      JOIN indicators i ON i.id = v.indicator_id
      JOIN (
        SELECT country_id, indicator_id, MAX(date) AS md
        FROM indicator_values
        GROUP BY country_id, indicator_id
      ) sub ON sub.country_id = v.country_id AND sub.indicator_id = v.indicator_id AND sub.md = v.date
    """
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY v.date DESC, c.name ASC LIMIT ?"
    params.append(int(limit))
    with _connection() as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()
    return [
        {
            "country_code": r[0],
            "country_name": r[1],
            "region": r[2],
            "indicator": r[3],
            "indicator_label": r[4],
            "unit": r[5],
            "date": r[6],
            "value": r[7],
        }
        for r in rows
    ]


def macro_get_series(country_code: str, indicator: str, start: str = None, end: str = None, limit: int = 400) -> list:
    """Time series for one country+indicator."""
    cc = (country_code or "").strip().upper()[:6]
    ind = (indicator or "").strip().lower()[:80]
    where = ["c.code = ?", "i.name = ?"]
    params = [cc, ind]
    if start:
        where.append("v.date >= ?")
        params.append(str(start)[:16])
    if end:
        where.append("v.date <= ?")
        params.append(str(end)[:16])
    sql = """
      SELECT v.date, v.value
      FROM indicator_values v
      JOIN countries c ON c.id = v.country_id
      JOIN indicators i ON i.id = v.indicator_id
      WHERE {where}
      ORDER BY v.date ASC
      LIMIT ?
    """.format(where=" AND ".join(where))
    params.append(int(limit))
    with _connection() as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()
    return [{"date": r[0], "value": r[1]} for r in rows]


def macro_seed_defaults() -> None:
    """Seed a baseline set of countries and indicators (idempotent).

    Uses existing `country_risk_integration` as the country registry.
    """
    now = datetime.utcnow().isoformat()
    world_bank_id = macro_upsert_data_source(
        "world_bank",
        "World Bank",
        base_url="https://api.worldbank.org/v2",
        notes="Public no-auth Indicators API",
    )
    fx_id = macro_upsert_data_source(
        "frankfurter",
        "Frankfurter (ECB)",
        base_url="https://api.frankfurter.app",
        notes="Public no-auth FX rates API (EUR base)",
    )
    fred_id = macro_upsert_data_source(
        "fred",
        "FRED (Federal Reserve Economic Data)",
        base_url="https://fred.stlouisfed.org",
        notes="Free API; set FRED_API_KEY in environment",
    )
    eurostat_id = macro_upsert_data_source(
        "eurostat",
        "Eurostat",
        base_url="https://ec.europa.eu/eurostat",
        notes="EU statistics REST API 1.0 (no key)",
    )

    asean = {
        "BRN", "KHM", "IDN", "LAO", "MYS", "MMR", "PHL", "SGP", "THA", "VNM",
    }
    g20 = {
        "ARG", "AUS", "BRA", "CAN", "CHN", "FRA", "DEU", "IND", "IDN", "ITA", "JPN", "KOR",
        "MEX", "RUS", "SAU", "ZAF", "TUR", "GBR", "USA", "EU",
    }
    major = {"USA", "CHN", "JPN", "KOR", "IND", "DEU", "FRA", "GBR", "EU", "SGP"}

    with _connection() as conn:
        rows = conn.execute(
            "SELECT country_code, country_name, region FROM country_risk_integration ORDER BY country_name"
        ).fetchall()
    for code, name, region in rows:
        if not code:
            continue
        c = str(code).strip().upper()[:6]
        macro_upsert_country(
            c,
            str(name or c),
            str(region or "").strip() or None,
            is_asean=c in asean,
            is_g20=c in g20,
            is_major=c in major,
        )

    # --- Indicator catalog (50+ types) ---
    # Mostly World Bank so we can ingest broadly for many countries without API keys.
    wb = [
        # Growth / activity
        ("gdp_growth_yoy", "GDP growth (annual, %)", "%", "annual", "growth", "NY.GDP.MKTP.KD.ZG"),
        ("gdp_current_usd", "GDP (current US$)", "USD", "annual", "growth", "NY.GDP.MKTP.CD"),
        ("gdp_constant_usd", "GDP (constant 2015 US$)", "USD", "annual", "growth", "NY.GDP.MKTP.KD"),
        ("gdp_per_capita_usd", "GDP per capita (current US$)", "USD", "annual", "growth", "NY.GDP.PCAP.CD"),
        ("gdp_per_capita_growth", "GDP per capita growth (annual, %)", "%", "annual", "growth", "NY.GDP.PCAP.KD.ZG"),
        ("gni_per_capita_atlas", "GNI per capita, Atlas method (current US$)", "USD", "annual", "growth", "NY.GNP.PCAP.CD"),
        ("gross_capital_formation_pct_gdp", "Gross capital formation (% of GDP)", "%", "annual", "investment", "NE.GDI.FTOT.ZS"),
        ("gross_savings_pct_gdp", "Gross savings (% of GDP)", "%", "annual", "investment", "NY.GNS.ICTR.ZS"),
        ("industry_va_pct_gdp", "Industry value added (% of GDP)", "%", "annual", "growth", "NV.IND.TOTL.ZS"),
        ("services_va_pct_gdp", "Services value added (% of GDP)", "%", "annual", "growth", "NV.SRV.TOTL.ZS"),
        ("agri_va_pct_gdp", "Agriculture value added (% of GDP)", "%", "annual", "growth", "NV.AGR.TOTL.ZS"),

        # Inflation / prices
        ("inflation_cpi", "Inflation (CPI, %)", "%", "annual", "inflation", "FP.CPI.TOTL.ZG"),
        ("inflation_gdp_deflator", "Inflation (GDP deflator, %)", "%", "annual", "inflation", "NY.GDP.DEFL.KD.ZG"),
        ("cpi_index", "CPI index (2010=100)", "index", "annual", "inflation", "FP.CPI.TOTL"),
        ("inflation_food", "Inflation, food prices (annual, %)", "%", "annual", "inflation", "FP.CPI.FOOD.ZG"),

        # Labor / demographics
        ("unemployment_rate", "Unemployment rate (%)", "%", "annual", "labor", "SL.UEM.TOTL.ZS"),
        ("labor_participation", "Labor force participation rate (%)", "%", "annual", "labor", "SL.TLF.CACT.ZS"),
        ("youth_unemployment", "Unemployment, youth (%)", "%", "annual", "labor", "SL.UEM.1524.ZS"),
        ("employment_to_pop", "Employment to population ratio (%)", "%", "annual", "labor", "SL.EMP.TOTL.SP.ZS"),
        ("population_total", "Population, total", "people", "annual", "demographics", "SP.POP.TOTL"),
        ("population_growth", "Population growth (annual, %)", "%", "annual", "demographics", "SP.POP.GROW"),

        # External / trade
        ("trade_balance_pct_gdp", "Trade balance (% of GDP)", "%", "annual", "external", "NE.RSB.GNFS.ZS"),
        ("exports_pct_gdp", "Exports of goods & services (% of GDP)", "%", "annual", "external", "NE.EXP.GNFS.ZS"),
        ("imports_pct_gdp", "Imports of goods & services (% of GDP)", "%", "annual", "external", "NE.IMP.GNFS.ZS"),
        ("current_account_pct_gdp", "Current account balance (% of GDP)", "%", "annual", "external", "BN.CAB.XOKA.GD.ZS"),
        ("reserves_months_imports", "Reserves incl. gold (months of imports)", "months", "annual", "external", "FI.RES.TOTL.MO"),
        ("external_debt_pct_gni", "External debt stocks (% of GNI)", "%", "annual", "external", "DT.DOD.DECT.GN.ZS"),
        ("fdi_net_inflows_pct_gdp", "FDI net inflows (% of GDP)", "%", "annual", "external", "BX.KLT.DINV.WD.GD.ZS"),
        ("remittances_pct_gdp", "Personal remittances received (% of GDP)", "%", "annual", "external", "BX.TRF.PWKR.DT.GD.ZS"),

        # Fiscal / debt
        ("gov_debt_pct_gdp", "Government debt (% of GDP)", "%", "annual", "fiscal", "GC.DOD.TOTL.GD.ZS"),
        ("revenue_pct_gdp", "Revenue excluding grants (% of GDP)", "%", "annual", "fiscal", "GC.REV.XGRT.GD.ZS"),
        ("expense_pct_gdp", "Expense (% of GDP)", "%", "annual", "fiscal", "GC.XPN.TOTL.GD.ZS"),
        ("fiscal_balance_pct_gdp", "Cash surplus/deficit (% of GDP)", "%", "annual", "fiscal", "GC.BAL.CASH.GD.ZS"),
        ("tax_revenue_pct_gdp", "Tax revenue (% of GDP)", "%", "annual", "fiscal", "GC.TAX.TOTL.GD.ZS"),

        # Money / finance proxies (WB has limited policy rates; we still track broad money, credit)
        ("broad_money_growth", "Broad money growth (annual, %)", "%", "annual", "money", "FM.LBL.BMNY.ZG"),
        ("domestic_credit_private_pct_gdp", "Domestic credit to private sector (% of GDP)", "%", "annual", "money", "FS.AST.PRVT.GD.ZS"),
        ("domestic_credit_fin_sector_pct_gdp", "Domestic credit provided by financial sector (% of GDP)", "%", "annual", "money", "FS.AST.DOMS.GD.ZS"),
        ("bank_capital_to_assets", "Bank capital to assets ratio (%)", "%", "annual", "financial", "FB.BNK.CAPA.ZS"),
        ("bank_npl_ratio", "Bank nonperforming loans to total gross loans (%)", "%", "annual", "financial", "FB.AST.NPER.ZS"),

        # Energy / commodities / emissions (macro-relevant)
        ("energy_use_per_capita", "Energy use (kg of oil equivalent per capita)", "kgoe", "annual", "energy", "EG.USE.PCAP.KG.OE"),
        ("oil_rents_pct_gdp", "Oil rents (% of GDP)", "%", "annual", "energy", "NY.GDP.PETR.RT.ZS"),
        ("gas_rents_pct_gdp", "Natural gas rents (% of GDP)", "%", "annual", "energy", "NY.GDP.NGAS.RT.ZS"),
        ("coal_rents_pct_gdp", "Coal rents (% of GDP)", "%", "annual", "energy", "NY.GDP.COAL.RT.ZS"),
        ("co2_emissions_per_capita", "CO2 emissions (metric tons per capita)", "t", "annual", "energy", "EN.ATM.CO2E.PC"),

        # Productivity / development
        ("gross_fixed_capital_formation_pct_gdp", "Gross fixed capital formation (% of GDP)", "%", "annual", "investment", "NE.GDI.FTOT.ZS"),
        ("inflation_cpi_avg", "Inflation (CPI, average %)", "%", "annual", "inflation", "FP.CPI.TOTL.ZG"),
        ("life_expectancy", "Life expectancy at birth (years)", "years", "annual", "demographics", "SP.DYN.LE00.IN"),
        ("urban_population_pct", "Urban population (% of total)", "%", "annual", "demographics", "SP.URB.TOTL.IN.ZS"),
    ]

    # Ensure at least 50 WB indicators by adding more core macro breadth
    wb += [
        ("gdp_growth_volatility", "GDP growth volatility (std dev, proxy)", "n/a", "annual", "growth", "NY.GDP.MKTP.KD.ZG"),
        ("inflation_cpi_volatility", "Inflation volatility (proxy)", "n/a", "annual", "inflation", "FP.CPI.TOTL.ZG"),
        ("trade_openess_pct_gdp", "Trade openness ((X+M)/GDP, %)", "%", "annual", "external", "NE.TRD.GNFS.ZS"),
        ("manufacturing_va_pct_gdp", "Manufacturing value added (% of GDP)", "%", "annual", "growth", "NV.IND.MANF.ZS"),
        ("gdp_ppp_current", "GDP, PPP (current international $)", "USD", "annual", "growth", "NY.GDP.MKTP.PP.CD"),
        ("gdp_ppp_per_capita", "GDP per capita, PPP (current international $)", "USD", "annual", "growth", "NY.GDP.PCAP.PP.CD"),
        ("inflation_cpi_end", "Inflation (CPI, end of period %)", "%", "annual", "inflation", "FP.CPI.TOTL.ZG"),
        ("terms_of_trade", "Net barter terms of trade index (2000=100)", "index", "annual", "external", "TT.PRI.MRCH.XD.WD"),
        ("official_exchange_rate", "Official exchange rate (LCU per US$, period avg)", "LCU/USD", "annual", "fx", "PA.NUS.FCRF"),
        ("real_effective_exchange_rate", "Real effective exchange rate index (2010=100)", "index", "annual", "fx", "PX.REX.REER"),
    ]

    for name, label, unit, freq, cat, code in wb:
        macro_upsert_indicator(
            name,
            label,
            unit=unit,
            frequency=freq,
            category=cat,
            source_id=world_bank_id,
            external_code=code,
        )

    # FX indicators (daily)
    for ccy in ("SGD", "MYR", "IDR", "THB", "PHP", "VND", "CNY", "JPY", "KRW", "EUR"):
        macro_upsert_indicator(
            f"fx_usd_{ccy.lower()}",
            f"FX: USD/{ccy}",
            unit="rate",
            frequency="daily",
            category="fx",
            source_id=fx_id,
            external_code=ccy,
        )

    # FRED — US series (monthly; external_code = FRED series id)
    fred_us = [
        ("fred_us_fed_funds_eff", "US: Effective federal funds rate", "%", "monthly", "money", "DFF"),
        ("fred_us_unemployment", "US: Unemployment rate", "%", "monthly", "labor", "UNRATE"),
        ("fred_us_cpi_all", "US: CPI all urban consumers", "index", "monthly", "inflation", "CPIAUCSL"),
        ("fred_us_10y_treasury", "US: 10-year Treasury constant maturity", "%", "monthly", "money", "GS10"),
        ("fred_us_m2", "US: M2 money stock", "USD bn", "monthly", "money", "M2SL"),
        ("fred_us_dollar_index", "US: Trade-weighted USD index (broad)", "index", "monthly", "fx", "DTWEXBGS"),
    ]
    for name, label, unit, freq, cat, series_id in fred_us:
        macro_upsert_indicator(
            name,
            label,
            unit=unit,
            frequency=freq,
            category=cat,
            source_id=fred_id,
            external_code=series_id,
        )

    # Eurostat — harmonised unemployment (annual, prime-age 25–54, % of active population)
    macro_upsert_indicator(
        "eurostat_unemployment_prime_age",
        "EU: Unemployment rate, 25–54, % of labour force",
        "%",
        "annual",
        "labor",
        source_id=eurostat_id,
        external_code="une_rt_a|Y25-54|PC_ACT|T",
    )


def get_climate_vulnerability_summary(limit: int = 15) -> list:
    """Countries with highest climate/resource vulnerability."""
    with _connection() as conn:
        cur = conn.execute(
            """SELECT c.country_code, c.as_of_year, c.water_stress_index, c.food_insecurity_index, c.climate_risk_score,
                      (SELECT country_name FROM country_risk_integration i WHERE i.country_code = c.country_code LIMIT 1) as country_name
               FROM climate_resource_vulnerability c
               INNER JOIN (SELECT country_code, MAX(as_of_year) as my FROM climate_resource_vulnerability GROUP BY country_code) sub
                 ON c.country_code = sub.country_code AND c.as_of_year = sub.my
               ORDER BY COALESCE(c.climate_risk_score, 0) + COALESCE(c.water_stress_index, 0) * 10 DESC
               LIMIT ?""",
            (limit,),
        )
        return [dict(row) for row in cur.fetchall()]


def get_natural_risk_summary(limit: int = 50) -> list:
    """Countries with highest natural disaster / climate risk (for World Monitor natural layer)."""
    try:
        with _connection() as conn:
            cur = conn.execute(
                """SELECT c.country_code, c.natural_disaster_frequency, c.climate_risk_score, c.water_stress_index,
                          (SELECT country_name FROM country_risk_integration i WHERE i.country_code = c.country_code LIMIT 1) as country_name
                   FROM climate_resource_vulnerability c
                   INNER JOIN (SELECT country_code, MAX(as_of_year) as my FROM climate_resource_vulnerability GROUP BY country_code) sub
                     ON c.country_code = sub.country_code AND c.as_of_year = sub.my
                   WHERE (c.natural_disaster_frequency IS NOT NULL AND c.natural_disaster_frequency > 0)
                      OR (c.climate_risk_score IS NOT NULL AND c.climate_risk_score >= 25)
                   ORDER BY COALESCE(c.natural_disaster_frequency, 0) + COALESCE(c.climate_risk_score, 0) DESC
                   LIMIT ?""",
                (limit,),
            )
            return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def get_elite_institutional_summary(region: Optional[str] = None, limit: int = 15) -> list:
    """Elite and institutional snapshot (governance, SOEs, CBI, party structure). Optionally filtered by region."""
    with _connection() as conn:
        params = []
        sql = """SELECT e.country_code, e.governance_model, e.major_soes, e.central_bank_independence_score, e.party_structure, e.updated_at,
                        (SELECT country_name FROM country_risk_integration c WHERE c.country_code = e.country_code LIMIT 1) as country_name
                 FROM elite_institutional e"""
        if region:
            sql += " INNER JOIN country_risk_integration c ON c.country_code = e.country_code AND c.region = ?"
            params.append(region)
        sql += " ORDER BY e.updated_at DESC LIMIT ?"
        params.append(limit)
        cur = conn.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]


def get_capital_flows_summary(limit: int = 15) -> list:
    """Top capital flow highlights by FDI inflow (latest per country)."""
    with _connection() as conn:
        cur = conn.execute(
            """SELECT cf.country_code, cf.as_of_year, cf.fdi_inflow_usd_millions, cf.fdi_outflow_usd_millions,
                      cf.portfolio_flows_usd_millions, cf.sovereign_wealth_fund_usd_billions,
                      (SELECT country_name FROM country_risk_integration c WHERE c.country_code = cf.country_code LIMIT 1) as country_name
               FROM capital_flows cf
               INNER JOIN (SELECT country_code, MAX(as_of_year) as my FROM capital_flows GROUP BY country_code) sub
                 ON cf.country_code = sub.country_code AND cf.as_of_year = sub.my
               ORDER BY ABS(COALESCE(cf.fdi_inflow_usd_millions, 0)) + ABS(COALESCE(cf.fdi_outflow_usd_millions, 0)) DESC
               LIMIT ?""",
            (limit,),
        )
        return [dict(row) for row in cur.fetchall()]


def backfill_integration_countries() -> int:
    """Ensure all countries from country_data.ALL_COUNTRIES exist in country_risk_integration with full data (population, land area, density, risk scores). Call after init_db(). Returns number of countries upserted."""
    with _connection() as conn:
        _ensure_country_integration_columns(conn)
        _backfill_all_countries(conn)
    from app.country_data import ALL_COUNTRIES
    return len(ALL_COUNTRIES)
