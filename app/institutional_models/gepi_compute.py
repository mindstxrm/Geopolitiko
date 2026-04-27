"""GEPI (Escalation Pressure Index) computation.
Computes daily channel scores, standardization, and final GEPI.
"""
import json
import math
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from .gepi_channels import GEPI_CHANNELS, GEPI_WEIGHTS, GEPI_WEIGHTS_VERSION


def _get_connection():
    from app.models import _connection
    return _connection


def _compute_channel_raw(conn, channel: str, as_of: str) -> float:
    """Get raw value for a channel on a given date. Returns count or proxy score."""
    as_of_dt = datetime.strptime(as_of[:10], "%Y-%m-%d").date()
    cutoff_180 = (as_of_dt - timedelta(days=180)).strftime("%Y-%m-%d")
    day_str = as_of[:10]

    if channel == "military_activity":
        cur = conn.execute(
            """SELECT COUNT(*) FROM border_incidents WHERE substr(incident_date, 1, 10) = ?""",
            (day_str,),
        )
        bi = (cur.fetchone() or (0,))[0]
        cur = conn.execute(
            """SELECT COUNT(*) FROM military_movement WHERE substr(observed_date, 1, 10) = ?""",
            (day_str,),
        )
        mm = (cur.fetchone() or (0,))[0]
        cur = conn.execute(
            """SELECT COUNT(*) FROM conflict_events WHERE substr(created_at, 1, 10) = ?""",
            (day_str,),
        )
        ce = (cur.fetchone() or (0,))[0]
        return float(bi + mm + ce)

    if channel == "sanctions_activity":
        cur = conn.execute(
            """SELECT COUNT(*) FROM sanctions_registry WHERE substr(start_date, 1, 10) = ?""",
            (day_str,),
        )
        return float((cur.fetchone() or (0,))[0])

    if channel == "hostile_rhetoric":
        cur = conn.execute(
            """SELECT COALESCE(AVG(impact_score), 0) * COUNT(*) / 10.0 FROM articles
               WHERE substr(published_utc, 1, 10) = ? AND (topics LIKE '%Russia-Ukraine%' OR topics LIKE '%US-China%' OR topics LIKE '%Middle East%')""",
            (day_str,),
        )
        row = cur.fetchone()
        return float((row[0] or 0)) if row else 0.0

    if channel == "domestic_unrest":
        cur = conn.execute(
            """SELECT COUNT(*) FROM protest_tracking WHERE event_date = ?""",
            (day_str,),
        )
        return float((cur.fetchone() or (0,))[0])

    if channel == "diplomacy_breakdown":
        cur = conn.execute(
            """SELECT COUNT(*) FROM treaties WHERE has_escalation_clause = 1
               AND (clauses_json LIKE '%withdraw%' OR clauses_json LIKE '%terminate%' OR summary LIKE '%withdraw%' OR summary LIKE '%breakdown%')""",
        )
        return float((cur.fetchone() or (0,))[0])

    if channel == "supply_chain_tension":
        cur = conn.execute(
            """SELECT COALESCE(AVG(risk_score), 0) FROM chokepoints""",
        )
        avg_risk = (cur.fetchone() or (0,))[0] or 0
        cur = conn.execute(
            """SELECT COUNT(*) FROM chokepoints WHERE risk_score >= 70""",
        )
        high_risk = (cur.fetchone() or (0,))[0] or 0
        return float(avg_risk) * 0.01 + float(high_risk)

    return 0.0


def _rolling_stats(conn, channel: str, as_of: str, country_code: Optional[str]) -> Tuple[float, float]:
    """Compute 180-day rolling mean and std. Uses stored scores if available, else computes raw on-the-fly."""
    as_of_dt = datetime.strptime(as_of[:10], "%Y-%m-%d").date()
    start = (as_of_dt - timedelta(days=180)).strftime("%Y-%m-%d")
    end = as_of[:10]
    vals = []
    cur = conn.execute(
        """SELECT raw_value FROM gpi_gepi_channel_scores
           WHERE channel_name = ? AND as_of_date >= ? AND as_of_date <= ?
           AND (country_code IS NULL OR country_code = ?)""",
        (channel, start, end, country_code or ""),
    )
    for row in cur:
        vals.append(row[0])
    if len(vals) < 30:
        for i in range(181):
            d = (as_of_dt - timedelta(days=i)).strftime("%Y-%m-%d")
            vals.append(_compute_channel_raw(conn, channel, d + "T00:00:00"))
    if not vals:
        return 0.0, 1.0
    n = len(vals)
    mean = sum(vals) / n
    var = sum((x - mean) ** 2 for x in vals) / max(n, 1)
    std = math.sqrt(var) if var > 0 else 1.0
    return mean, std


def compute_gepi_channel_scores(as_of: Optional[str] = None) -> int:
    """Compute and store standardized channel scores for GEPI. Returns rows written."""
    as_of = as_of or datetime.utcnow().strftime("%Y-%m-%d")
    now = datetime.utcnow().isoformat() + "Z"
    _conn = _get_connection()
    count = 0
    with _conn() as conn:
        for channel in GEPI_CHANNELS:
            raw = _compute_channel_raw(conn, channel, as_of)
            mean, std = _rolling_stats(conn, channel, as_of, None)
            z = (raw - mean) / std if std > 0 else 0.0
            conn.execute(
                """INSERT OR REPLACE INTO gpi_gepi_channel_scores
                   (as_of_date, country_code, channel_name, raw_value, rolling_mean_180, rolling_std_180,
                    standardized_score, methodology_version, input_data_timestamp, last_updated)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (as_of[:10], None, channel, raw, mean, std, z, "1.0", now, now),
            )
            count += 1
    return count


def compute_gepi_daily(as_of: Optional[str] = None) -> Dict:
    """Compute final GEPI score: weighted sum of channels, logistic transform to 0-1.
    Returns dict with gepi_score, model_version, uncertainty_estimate."""
    as_of = as_of or datetime.utcnow().strftime("%Y-%m-%d")
    now = datetime.utcnow().isoformat() + "Z"
    _conn = _get_connection()
    with _conn() as conn:
        cur = conn.execute(
            """SELECT version, weights_json FROM gpi_gepi_weights ORDER BY effective_from DESC LIMIT 1"""
        )
        row = cur.fetchone()
        weights = GEPI_WEIGHTS
        version = GEPI_WEIGHTS_VERSION
        if row:
            version = row[0]
            try:
                weights = json.loads(row[1])
            except (json.JSONDecodeError, TypeError):
                pass

        cur = conn.execute(
            """SELECT channel_name, standardized_score FROM gpi_gepi_channel_scores
               WHERE as_of_date = ? AND country_code IS NULL""",
            (as_of[:10],),
        )
        scores = {row[0]: row[1] for row in cur}
        weighted_sum = 0.0
        w_total = 0.0
        vals = []
        for ch, w in weights.items():
            s = scores.get(ch, 0.0)
            weighted_sum += s * w
            w_total += w
            vals.append(s * w)
        if w_total > 0:
            weighted_sum /= w_total
        try:
            import numpy as np
            uncertainty = float(np.var(vals)) if vals else 0.0
        except ImportError:
            uncertainty = sum((v - weighted_sum) ** 2 for v in vals) / max(len(vals), 1) if vals else 0.0
        logistic = 1.0 / (1.0 + math.exp(-weighted_sum))
        conn.execute(
            """INSERT OR REPLACE INTO gpi_gepi_daily
               (as_of_date, country_code, gepi_score, model_version, uncertainty_estimate,
                methodology_version, input_data_timestamp, confidence_score, last_updated)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (as_of[:10], None, logistic, version, uncertainty, "1.0", now, 1.0 - min(uncertainty, 1.0), now),
        )
    return {
        "gepi_score": logistic,
        "model_version": version,
        "timestamp": now,
        "uncertainty_estimate": uncertainty,
    }


def run_gepi(as_of: Optional[str] = None) -> Dict:
    """Run full GEPI pipeline: channel scores then daily. Returns final GEPI dict."""
    compute_gepi_channel_scores(as_of)
    return compute_gepi_daily(as_of)
