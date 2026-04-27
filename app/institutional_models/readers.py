"""Read functions for institutional model results."""
from datetime import datetime, timedelta
from typing import List, Optional

def _conn():
    from app.models import _connection
    return _connection()


def get_gepi_latest() -> Optional[dict]:
    """Latest GEPI daily score (global)."""
    with _conn() as conn:
        cur = conn.execute(
            """SELECT as_of_date, gepi_score, model_version, uncertainty_estimate, confidence_score, last_updated
               FROM gpi_gepi_daily WHERE country_code IS NULL ORDER BY as_of_date DESC LIMIT 1"""
        )
        row = cur.fetchone()
    return dict(row) if row else None


def get_gepi_history(days: int = 30) -> List[dict]:
    """GEPI daily history."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    with _conn() as conn:
        cur = conn.execute(
            """SELECT as_of_date, gepi_score, uncertainty_estimate, confidence_score
               FROM gpi_gepi_daily WHERE country_code IS NULL AND as_of_date >= ? ORDER BY as_of_date""",
            (cutoff,),
        )
        return [dict(row) for row in cur]


def get_cdei_by_country(country_code: Optional[str] = None, as_of: Optional[str] = None) -> List[dict]:
    """CDEI scores. Filter by country and/or date."""
    with _conn() as conn:
        where, params = [], []
        if country_code:
            where.append("country_code = ?")
            params.append(country_code)
        if as_of:
            where.append("as_of_date = ?")
            params.append(as_of[:10])
        sql = "SELECT * FROM gpi_cdei_daily"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY as_of_date DESC, cdei_total DESC LIMIT 200"
        cur = conn.execute(sql, tuple(params))
        return [dict(row) for row in cur]


def get_sfi_by_country(country_code: Optional[str] = None, as_of: Optional[str] = None) -> List[dict]:
    """SFI scores."""
    with _conn() as conn:
        where, params = [], []
        if country_code:
            where.append("country_code = ?")
            params.append(country_code)
        if as_of:
            where.append("as_of_date = ?")
            params.append(as_of[:10])
        sql = "SELECT * FROM gpi_sfi_daily"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY as_of_date DESC, sfi_score DESC LIMIT 200"
        cur = conn.execute(sql, tuple(params))
        return [dict(row) for row in cur]


def get_fragility_by_country(country_code: Optional[str] = None, as_of: Optional[str] = None) -> List[dict]:
    """Fragility scores."""
    with _conn() as conn:
        where, params = [], []
        if country_code:
            where.append("country_code = ?")
            params.append(country_code)
        if as_of:
            where.append("as_of_date = ?")
            params.append(as_of[:10])
        sql = "SELECT * FROM gpi_fragility_daily"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY as_of_date DESC LIMIT 200"
        cur = conn.execute(sql, tuple(params))
        return [dict(row) for row in cur]


def get_fragility_history(country_code: str, days: int = 90) -> List[dict]:
    """Fragility scores over time (chronological) for charting."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    with _conn() as conn:
        cur = conn.execute(
            """SELECT as_of_date, fragility_level, model_version
               FROM gpi_fragility_daily
               WHERE country_code = ? AND as_of_date >= ?
               ORDER BY as_of_date ASC""",
            (country_code.upper(), cutoff),
        )
        return [dict(row) for row in cur]


def get_cdei_history(country_code: str, days: int = 90) -> List[dict]:
    """CDEI scores over time (chronological) for charting."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    with _conn() as conn:
        cur = conn.execute(
            """SELECT as_of_date, cdei_total, cdei_energy, cdei_trade, cdei_tech
               FROM gpi_cdei_daily
               WHERE country_code = ? AND as_of_date >= ?
               ORDER BY as_of_date ASC""",
            (country_code.upper(), cutoff),
        )
        return [dict(row) for row in cur]


def get_sfi_history(country_code: str, days: int = 90) -> List[dict]:
    """SFI scores over time (chronological) for charting."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    with _conn() as conn:
        cur = conn.execute(
            """SELECT as_of_date, sfi_score, active_sanction_count
               FROM gpi_sfi_daily
               WHERE country_code = ? AND as_of_date >= ?
               ORDER BY as_of_date ASC""",
            (country_code.upper(), cutoff),
        )
        return [dict(row) for row in cur]


def get_gepi_history_by_country(country_code: str, days: int = 90) -> List[dict]:
    """GEPI per-country history if available; falls back to empty list."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    with _conn() as conn:
        cur = conn.execute(
            """SELECT as_of_date, gepi_score, uncertainty_estimate
               FROM gpi_gepi_daily
               WHERE country_code = ? AND as_of_date >= ?
               ORDER BY as_of_date ASC""",
            (country_code.upper(), cutoff),
        )
        return [dict(row) for row in cur]


def get_gepi_channel_scores(as_of: Optional[str] = None) -> List[dict]:
    """Channel scores for a given date."""
    as_of = as_of or datetime.utcnow().strftime("%Y-%m-%d")
    with _conn() as conn:
        cur = conn.execute(
            """SELECT channel_name, raw_value, standardized_score, rolling_mean_180, rolling_std_180
               FROM gpi_gepi_channel_scores WHERE as_of_date = ? AND country_code IS NULL""",
            (as_of[:10],),
        )
        return [dict(row) for row in cur]
