"""Blend institutional models into article context for implications."""
from typing import Optional

# Topic/entity -> country codes (ISO3)
TOPIC_TO_COUNTRIES = {
    "Russia-Ukraine": ["RUS", "UKR"],
    "US-China": ["USA", "CHN"],
    "Middle East": ["ISR", "PSE", "IRN", "SAU", "SYR", "LBN", "EGY"],
    "NATO": ["USA", "GBR", "DEU", "FRA", "POL", "LTU", "LVA", "EST"],
    "Asia-Pacific": ["PRK", "JPN", "KOR", "IDN", "PHL", "AUS"],
    "Europe": ["DEU", "FRA", "ITA", "ESP", "NLD", "BEL"],
    "Trade & Economy": [],  # Broad, no specific countries
    "Climate & Energy": [],
    "Defense": [],
}

ENTITY_TO_CODE = {
    "United States": "USA", "USA": "USA", "US": "USA",
    "China": "CHN",
    "Russia": "RUS",
    "Ukraine": "UKR",
    "Israel": "ISR",
    "Iran": "IRN",
    "North Korea": "PRK",
    "South Korea": "KOR",
    "Japan": "JPN",
    "India": "IND",
    "Pakistan": "PAK",
    "France": "FRA",
    "Germany": "DEU",
    "UK": "GBR", "Britain": "GBR",
    "Turkey": "TUR",
    "Saudi Arabia": "SAU",
    "Egypt": "EGY",
    "Syria": "SYR",
    "Iraq": "IRQ",
    "Afghanistan": "AFG",
    "Taiwan": "TWN",
    "Australia": "AUS",
    "Canada": "CAN",
    "Brazil": "BRA",
    "Mexico": "MEX",
    "South Africa": "ZAF",
    "Nigeria": "NGA",
    "Indonesia": "IDN",
    "Vietnam": "VNM",
    "Philippines": "PHL",
    "EU": "DEU",  # Proxy
}


def _extract_country_codes(article: dict) -> list[str]:
    """Extract country codes from article topics and entities."""
    codes = set()
    topics = article.get("topics_list") or []
    if not topics and article.get("topics"):
        try:
            import json
            t = article["topics"]
            topics = json.loads(t) if isinstance(t, str) else (t or [])
        except (json.JSONDecodeError, TypeError):
            topics = []
    for t in topics:
        for cc in TOPIC_TO_COUNTRIES.get(t, [])[:3]:  # Limit per topic
            codes.add(cc)
    entities = article.get("entities_list")
    if entities is None and article.get("entities"):
        try:
            import json
            e = article["entities"]
            entities = json.loads(e) if isinstance(e, str) else (e or [])
        except (json.JSONDecodeError, TypeError):
            entities = []
    entities = entities or []
    for e in entities:
        name = (e or "").strip()
        if name in ENTITY_TO_CODE:
            codes.add(ENTITY_TO_CODE[name])
    return list(codes)[:8]  # Cap at 8


def get_article_implications(article: dict) -> dict:
    """
    Return institutional model context for an article.
    Keys: gepi, country_scores (list of {code, name, cdei, sfi, fragility}), implications (list of strings).
    """
    from .readers import (
        get_gepi_latest,
        get_cdei_by_country,
        get_sfi_by_country,
        get_fragility_by_country,
    )
    from app.models import get_integration_countries

    codes = _extract_country_codes(article)
    event_type = (article.get("event_type") or "").strip()
    out = {
        "gepi": None,
        "country_scores": [],
        "implications": [],
    }

    gepi = get_gepi_latest()
    if gepi:
        out["gepi"] = {
            "score": round(float(gepi.get("gepi_score") or 0), 3),
            "confidence": round(float(gepi.get("confidence_score") or 0) * 100),
        }
        if event_type in ("Military", "Sanctions") or not event_type:
            out["implications"].append(
                f"Global escalation pressure (GEPI): {out['gepi']['score']:.2f} — "
                + ("elevated" if out["gepi"]["score"] >= 0.6 else "moderate" if out["gepi"]["score"] >= 0.4 else "low")
            )

    name_map = {}
    for c in get_integration_countries(limit=300) or []:
        cc = (c.get("country_code") or "").strip()
        if cc:
            name_map[cc] = c.get("country_name") or cc

    for code in codes:
        row = {"code": code, "name": name_map.get(code, code)}
        cdei = get_cdei_by_country(country_code=code)
        sfi = get_sfi_by_country(country_code=code)
        frag = get_fragility_by_country(country_code=code)
        if cdei:
            row["cdei"] = round(float(cdei[0].get("cdei_total") or 0), 2)
        if sfi:
            row["sfi"] = round(float(sfi[0].get("sfi_score") or 0), 2)
        if frag:
            row["fragility"] = round(float(frag[0].get("fragility_level") or 0) * 100)
        out["country_scores"].append(row)

        if event_type == "Sanctions" and row.get("sfi") is not None:
            out["implications"].append(
                f"{row['name']} — Sanctions friction (SFI): {row['sfi']:.1f}"
            )
        if event_type == "Military" and row.get("cdei") is not None:
            out["implications"].append(
                f"{row['name']} — Chokepoint exposure (CDEI): {row['cdei']:.2f}"
            )
        if row.get("fragility") and row["fragility"] >= 50:
            out["implications"].append(
                f"{row['name']} — Fragility: {row['fragility']}% (elevated domestic risk)"
            )

    if not out["implications"] and out["gepi"]:
        out["implications"].append(
            f"Monitor escalation pressure (GEPI {out['gepi']['score']:.2f}) for related developments."
        )

    return out
