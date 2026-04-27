"""Read GPI UN analytics from gpi_un_* tables."""
from typing import List, Optional

from app.models import _connection


def get_rolling_alignment(
    country_a: Optional[str] = None,
    country_b: Optional[str] = None,
    window: Optional[str] = None,
    limit: int = 100,
) -> List[dict]:
    with _connection() as conn:
        where, params = [], []
        if country_a:
            where.append("(country_a = ? OR country_b = ?)")
            params.extend([country_a, country_a])
        if country_b:
            where.append("(country_a = ? OR country_b = ?)")
            params.extend([country_b, country_b])
        if window:
            where.append("window = ?")
            params.append(window)
        where_sql = " AND ".join(where) if where else "1=1"
        params.append(limit)
        cur = conn.execute(
            f"""SELECT country_a, country_b, window, end_date, similarity_score, vote_count_used, delta_1y, delta_3y, method_version
               FROM gpi_un_alignment_rolling WHERE {where_sql} ORDER BY end_date DESC, similarity_score DESC LIMIT ?""",
            tuple(params),
        )
        return [dict(r) for r in cur.fetchall()]


def get_alignment_by_issue(
    country_a: Optional[str] = None,
    country_b: Optional[str] = None,
    issue_tag: Optional[str] = None,
    limit: int = 100,
) -> List[dict]:
    with _connection() as conn:
        where, params = [], []
        if country_a:
            where.append("(country_a = ? OR country_b = ?)")
            params.extend([country_a, country_a])
        if country_b:
            where.append("(country_a = ? OR country_b = ?)")
            params.extend([country_b, country_b])
        if issue_tag:
            where.append("issue_tag = ?")
            params.append(issue_tag)
        where_sql = " AND ".join(where) if where else "1=1"
        params.append(limit)
        cur = conn.execute(
            f"""SELECT country_a, country_b, issue_tag, year, similarity_score, vote_count_used, method_version
               FROM gpi_un_alignment_by_issue WHERE {where_sql} ORDER BY year DESC, similarity_score DESC LIMIT ?""",
            tuple(params),
        )
        return [dict(r) for r in cur.fetchall()]


def get_bloc_cohesion(bloc_name: Optional[str] = None, limit: int = 50) -> List[dict]:
    with _connection() as conn:
        where, params = [], []
        if bloc_name:
            where.append("bloc_name = ?")
            params.append(bloc_name)
        where_sql = " AND ".join(where) if where else "1=1"
        params.append(limit)
        cur = conn.execute(
            f"""SELECT bloc_name, period, cohesion_score, vote_count_used, method_version
               FROM gpi_un_bloc_cohesion WHERE {where_sql} ORDER BY period DESC LIMIT ?""",
            tuple(params),
        )
        return [dict(r) for r in cur.fetchall()]


def get_bloc_member_positions(bloc_name: str, period: Optional[str] = None) -> List[dict]:
    with _connection() as conn:
        where, params = ["bloc_name = ?"], [bloc_name]
        if period:
            where.append("period = ?")
            params.append(period)
        cur = conn.execute(
            f"""SELECT bloc_name, country_code, period, alignment_to_bloc, rank_in_bloc, method_version
               FROM gpi_un_bloc_member_position WHERE {" AND ".join(where)} ORDER BY rank_in_bloc""",
            tuple(params),
        )
        return [dict(r) for r in cur.fetchall()]


def get_alignment_shocks(limit: int = 50, shock_only: bool = False) -> List[dict]:
    with _connection() as conn:
        where = "shock_flag = 1" if shock_only else "1=1"
        cur = conn.execute(
            f"""SELECT country_code, reference, period_end, baseline_score, current_score, delta, shock_flag, method_version
               FROM gpi_un_alignment_shocks WHERE {where} ORDER BY ABS(delta) DESC LIMIT ?""",
            (limit,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_country_volatility(country_code: Optional[str] = None, limit: int = 50) -> List[dict]:
    with _connection() as conn:
        where, params = [], []
        if country_code:
            where.append("country_code = ?")
            params.append(country_code)
        where_sql = " AND ".join(where) if where else "1=1"
        params.append(limit)
        cur = conn.execute(
            f"""SELECT country_code, period, volatility_score, abstain_rate, absent_rate, method_version
               FROM gpi_un_country_volatility WHERE {where_sql} ORDER BY period DESC, volatility_score DESC LIMIT ?""",
            tuple(params),
        )
        return [dict(r) for r in cur.fetchall()]


def get_global_polarization(limit: int = 24) -> List[dict]:
    with _connection() as conn:
        cur = conn.execute(
            """SELECT period, polarization_score, us_china_similarity, method_version
               FROM gpi_un_global_polarization ORDER BY period DESC LIMIT ?""",
            (limit,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_country_alignment_summary(country_code: str) -> dict:
    """Alignment to USA and CHN (12m, 36m), drift, volatility, shocks."""
    align_usa = get_rolling_alignment(country_a=country_code, country_b="USA", limit=4)
    align_chn = get_rolling_alignment(country_a=country_code, country_b="CHN", limit=4)
    vol = get_country_volatility(country_code=country_code, limit=1)
    shocks = get_alignment_shocks(limit=10, shock_only=True)
    shocks = [s for s in shocks if s.get("country_code") == country_code]
    by_issue = get_alignment_by_issue(country_a=country_code, country_b="USA", limit=10)
    return {
        "country_code": country_code,
        "alignment_to_usa": align_usa,
        "alignment_to_china": align_chn,
        "volatility": vol[0] if vol else None,
        "recent_shocks": shocks[:5],
        "alignment_by_issue_usa": by_issue,
    }
