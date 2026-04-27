"""CDEI (Chokepoint Exposure Index) computation.
Computes country vulnerability from chokepoint risk × exposure.
"""
from datetime import datetime
from typing import Dict, List, Optional

SECTOR_TO_EXPOSURE = {
    "oil": ("energy", "trade"),
    "lng": ("energy", "trade"),
    "containers": ("trade",),
    "grain": ("trade",),
    "semiconductors": ("tech",),
    "critical_minerals": ("tech", "trade"),
}


def _get_connection():
    from app.models import _connection
    return _connection


def _sync_gpi_chokepoints(conn) -> None:
    """Sync gpi_chokepoints from main chokepoints. Uses risk_score/100 as daily_risk_score."""
    now = datetime.utcnow().isoformat() + "Z"
    cur = conn.execute(
        """SELECT slug, name, COALESCE(risk_score, 50) FROM chokepoints"""
    )
    for row in cur:
        slug, name, risk = row[0], row[1], (row[2] or 50) / 100.0
        conn.execute(
            """INSERT INTO gpi_chokepoints (slug, name, daily_risk_score, risk_source_explanation, last_updated)
               VALUES (?, ?, ?, 'From main chokepoints.risk_score', ?)
               ON CONFLICT(slug) DO UPDATE SET name=excluded.name, daily_risk_score=excluded.daily_risk_score, last_updated=excluded.last_updated""",
            (slug, name, risk, now),
        )


def _sync_exposure_from_flows(conn) -> None:
    """Build gpi_country_chokepoint_exposure from chokepoint_flows."""
    now = datetime.utcnow().isoformat() + "Z"
    cur = conn.execute(
        """SELECT cf.country_code, LOWER(TRIM(c.slug)) AS slug, cf.sector, cf.exposure_pct, cf.impact_if_closed
           FROM chokepoint_flows cf JOIN chokepoints c ON c.id = cf.chokepoint_id"""
    )
    agg = {}
    for row in cur:
        country, slug, sector, pct, impact = row[0], row[1] or "", row[2] or "", (row[3] or 0) / 100.0, (row[4] or "").lower()
        key = (country, slug)
        if key not in agg:
            agg[key] = {"trade": 0.0, "energy": 0.0, "tech": 0.0, "reroute": 1.0}
        exposure_map = SECTOR_TO_EXPOSURE.get(sector.lower(), ("trade",))
        for typ in exposure_map:
            if typ in agg[key]:
                agg[key][typ] += pct
        if "high" in (impact or ""):
            agg[key]["reroute"] = max(agg[key]["reroute"], 1.5)
        elif "medium" in (impact or ""):
            agg[key]["reroute"] = max(agg[key]["reroute"], 1.2)
    for (country, slug), v in agg.items():
        trade = min(1.0, v["trade"] + v["energy"] * 0.5 + v["tech"] * 0.5)
        energy = min(1.0, v["energy"])
        tech = min(1.0, v["tech"])
        if trade > 0 or energy > 0 or tech > 0:
            conn.execute(
                """INSERT OR REPLACE INTO gpi_country_chokepoint_exposure
                   (country_code, chokepoint_slug, trade_share_percentage, energy_share_percentage,
                    tech_supply_share_percentage, reroute_penalty_factor, last_updated)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (country, slug, trade * 100, energy * 100, tech * 100, v["reroute"], now),
            )


def _get_chokepoint_risk(conn) -> Dict[str, float]:
    """Return slug -> daily_risk_score. Fallback to chokepoints.risk_score if gpi empty."""
    cur = conn.execute("SELECT slug, daily_risk_score FROM gpi_chokepoints")
    out = {row[0]: row[1] for row in cur}
    if not out:
        cur = conn.execute("SELECT slug, COALESCE(risk_score, 50) FROM chokepoints")
        out = {row[0]: (row[1] or 50) / 100.0 for row in cur}
    return out


def compute_cdei(as_of: Optional[str] = None) -> int:
    """Compute CDEI for all countries. Returns count of rows written."""
    as_of = as_of or datetime.utcnow().strftime("%Y-%m-%d")
    now = datetime.utcnow().isoformat() + "Z"
    _conn = _get_connection()
    count = 0
    with _conn() as conn:
        _sync_gpi_chokepoints(conn)
        _sync_exposure_from_flows(conn)
        risks = _get_chokepoint_risk(conn)
        cur = conn.execute(
            """SELECT country_code, chokepoint_slug, trade_share_percentage, energy_share_percentage,
                      tech_supply_share_percentage, reroute_penalty_factor
               FROM gpi_country_chokepoint_exposure"""
        )
        by_country = {}
        for row in cur:
            cc, slug, trade, energy, tech, penalty = row[0], row[1], (row[2] or 0) / 100.0, (row[3] or 0) / 100.0, (row[4] or 0) / 100.0, row[5] or 1.0
            risk = risks.get(slug, 0.5)
            exposure = (trade + energy + tech) / 3.0 if (trade or energy or tech) else 0.0
            contrib = exposure * risk * penalty
            if cc not in by_country:
                by_country[cc] = {"total": 0.0, "energy": 0.0, "trade": 0.0, "tech": 0.0}
            by_country[cc]["total"] += contrib
            by_country[cc]["energy"] += (energy or 0) * risk * penalty
            by_country[cc]["trade"] += (trade or 0) * risk * penalty
            by_country[cc]["tech"] += (tech or 0) * risk * penalty
        for cc, v in by_country.items():
            conn.execute(
                """INSERT OR REPLACE INTO gpi_cdei_daily
                   (as_of_date, country_code, cdei_total, cdei_energy, cdei_trade, cdei_tech,
                    methodology_version, input_data_timestamp, confidence_score, last_updated)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (as_of[:10], cc, v["total"], v["energy"], v["trade"], v["tech"], "1.0", now, 0.85, now),
            )
            count += 1
    return count
