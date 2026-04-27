"""Country Fragility Curve - latent fragility from observable indicators + article-derived signals."""
import json
from datetime import datetime, timedelta
from typing import Dict, Optional

def _get_connection():
    from app.models import _connection
    return _connection


def _get_article_escalation_by_country(conn, cutoff: str) -> Dict[str, float]:
    """Per country: normalized article escalation signal (high-impact articles 0-10 scale, last 90 days)."""
    from app.institutional_models.article_implications import _extract_country_codes
    cur = conn.execute(
        """SELECT id, topics, entities, impact_score FROM articles
           WHERE (COALESCE(NULLIF(trim(published_utc), ''), scraped_at) >= ?)
             AND (topics IS NOT NULL AND topics != '' OR entities IS NOT NULL AND entities != '')""",
        (cutoff,),
    )
    by_country = {}
    for row in cur:
        d = dict(row)
        topics = []
        if d.get("topics"):
            try:
                topics = json.loads(d["topics"]) if isinstance(d["topics"], str) else d["topics"]
            except (json.JSONDecodeError, TypeError):
                pass
        entities = []
        if d.get("entities"):
            try:
                entities = json.loads(d["entities"]) if isinstance(d["entities"], str) else d["entities"]
            except (json.JSONDecodeError, TypeError):
                pass
        d["topics_list"] = topics
        d["entities_list"] = entities
        codes = _extract_country_codes(d)
        impact = max(0, min(10, int(d.get("impact_score") or 0)))
        if impact >= 4:
            weight = impact / 10.0
            for cc in codes:
                by_country[cc] = by_country.get(cc, 0) + weight
    if not by_country:
        return {}
    mx = max(by_country.values())
    return {cc: min(1.0, v / max(mx, 0.01)) for cc, v in by_country.items()}


def _get_indicators(conn, as_of: str) -> Dict[str, Dict[str, float]]:
    """Per country: inflation, protest_freq, fx_vol, fiscal_stress, repression, elite_instability, article_escalation. Normalize 0-1."""
    cutoff = (datetime.strptime(as_of[:10], "%Y-%m-%d") - timedelta(days=90)).strftime("%Y-%m-%d")
    by_country = {}
    cur = conn.execute(
        """SELECT country_code, COUNT(*) FROM protest_tracking WHERE event_date >= ? GROUP BY country_code""",
        (cutoff,),
    )
    max_protests = 1
    for row in cur:
        by_country.setdefault(row[0], {}).update(protest_freq=float(row[1]))
        max_protests = max(max_protests, row[1])
    for cc in by_country:
        by_country[cc]["protest_freq"] = min(1.0, by_country[cc].get("protest_freq", 0) / max_protests)
    cur = conn.execute(
        """SELECT country_code FROM currency_stress WHERE country_code IS NOT NULL""",
    )
    for row in cur:
        cc = row[0]
        if cc not in by_country:
            by_country[cc] = {}
        by_country[cc].setdefault("fx_vol", 0.5)
    article_esc = _get_article_escalation_by_country(conn, cutoff)
    for cc, val in article_esc.items():
        if cc not in by_country:
            by_country[cc] = {}
        by_country[cc]["article_escalation"] = val
    cur = conn.execute("SELECT country_code FROM country_risk_integration")
    for row in cur:
        cc = row[0]
        if cc not in by_country:
            by_country[cc] = {}
        by_country[cc].setdefault("inflation", 0.3)
        by_country[cc].setdefault("fiscal_stress", 0.3)
        by_country[cc].setdefault("repression", 0.2)
        by_country[cc].setdefault("elite_instability", 0.2)
        by_country[cc].setdefault("article_escalation", 0.0)
    return by_country


def compute_fragility(as_of: Optional[str] = None) -> int:
    """Compute fragility level from weighted indicators. Returns count."""
    as_of = as_of or datetime.utcnow().strftime("%Y-%m-%d")
    now = datetime.utcnow().isoformat() + "Z"
    weights = {"inflation": 0.18, "protest_freq": 0.22, "fx_vol": 0.18, "fiscal_stress": 0.14, "repression": 0.09, "elite_instability": 0.09, "article_escalation": 0.10}
    _conn = _get_connection()
    count = 0
    with _conn() as conn:
        indicators = _get_indicators(conn, as_of)
        for cc, ind in indicators.items():
            level = sum(ind.get(k, 0.3) * w for k, w in weights.items())
            level = max(0.0, min(1.0, level))
            shock = 1 if ind.get("protest_freq", 0) > 0.7 or ind.get("fx_vol", 0) > 0.7 or ind.get("article_escalation", 0) > 0.7 else 0
            low = max(0, level - 0.1)
            high = min(1, level + 0.1)
            conn.execute(
                """INSERT OR REPLACE INTO gpi_fragility_daily
                   (as_of_date, country_code, fragility_level, fragility_trend, shock_indicator, uncertainty_low, uncertainty_high,
                    methodology_version, input_data_timestamp, confidence_score, last_updated)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (as_of[:10], cc, level, 0.0, shock, low, high, "1.0", now, 0.75, now),
            )
            count += 1
    return count
