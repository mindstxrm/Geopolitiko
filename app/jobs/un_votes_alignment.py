"""Rolling alignment scores between country pairs (12m, 36m windows)."""
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from app.models import _connection
from config import DATABASE_PATH

METHOD_VERSION = "rolling_v1"


def _get_votes_by_window(conn, window_months: int, end_date: str) -> Dict[str, Dict[str, str]]:
    """Returns {resolution_id: {country_code: vote_value}} for votes in window."""
    try:
        end_dt = datetime.strptime(end_date[:10], "%Y-%m-%d")
    except ValueError:
        return {}
    start_dt = end_dt - timedelta(days=window_months * 31)
    start_str = start_dt.strftime("%Y-%m-%d")
    cur = conn.execute(
        """SELECT resolution_id, country_code, vote_value
           FROM gpi_un_votes_raw
           WHERE date >= ? AND date <= ? AND body = 'UNGA' AND vote_value != 'ABSENT'""",
        (start_str, end_date[:10]),
    )
    by_res = defaultdict(dict)
    for row in cur.fetchall():
        by_res[row["resolution_id"]][row["country_code"]] = row["vote_value"]
    return dict(by_res)


def _similarity(votes_a: Dict[str, str], votes_b: Dict[str, str], by_res: Dict[str, Dict[str, str]]) -> Tuple[float, int]:
    """Compute % same votes (excluding ABSENT). Treat ABSTAIN as partial match (0.5)."""
    agreed = 0
    total = 0
    for res_id, res_votes in by_res.items():
        va = votes_a.get(res_id)
        vb = votes_b.get(res_id)
        if va is None or vb is None:
            continue
        total += 1
        if va == vb:
            agreed += 1
        elif (va == "ABSTAIN" or vb == "ABSTAIN") and (va in ("YES", "NO") or vb in ("YES", "NO")):
            agreed += 0.5  # Partial
    score = (agreed / total * 100) if total else 0
    return round(score, 2), total


def run_rolling_alignment(end_date: Optional[str] = None, country_pairs: Optional[List[Tuple[str, str]]] = None) -> int:
    """
    Compute rolling 12m and 36m alignment. If country_pairs None, compute for all pairs
    that share enough votes (expensive). For MVP, pass key pairs like [("USA","CHN"), ("USA","RUS")].
    """
    from app.models import init_db
    init_db(DATABASE_PATH)

    if not end_date:
        end_date = datetime.utcnow().strftime("%Y-%m-%d")
    end_date = end_date[:10]

    with _connection() as conn:
        # Ensure gpi_un_votes_raw has data
        cur = conn.execute("SELECT COUNT(*) as n FROM gpi_un_votes_raw")
        if cur.fetchone()["n"] == 0:
            from app.un_votes import migrate_un_votes_to_gpi_raw
            migrate_un_votes_to_gpi_raw(conn)

        by_res_12 = _get_votes_by_window(conn, 12, end_date)
        by_res_36 = _get_votes_by_window(conn, 36, end_date)
        if not by_res_12 and not by_res_36:
            return 0

        countries = set()
        for res_votes in list(by_res_36.values()) + list(by_res_12.values()):
            countries.update(res_votes.keys())
        countries = sorted(countries)

        if country_pairs is None:
            # Key pairs: USA/CHN with major countries, USA-RUS, CHN-RUS, ASEAN
            key = ["USA", "CHN", "RUS", "GBR", "FRA", "DEU", "JPN", "IND", "BRA", "ZAF"]
            asean = ["SGP", "IDN", "MYS", "THA", "VNM", "PHL"]
            seen = set()
            country_pairs = []
            for c in key + asean:
                for ref in ["USA", "CHN"]:
                    if c == ref:
                        continue
                    pair = (min(ref, c), max(ref, c))
                    if pair not in seen:
                        seen.add(pair)
                        country_pairs.append(pair)

        rows_12 = {c: defaultdict(str) for c in countries}
        rows_36 = {c: defaultdict(str) for c in countries}
        for res_id, res_votes in by_res_12.items():
            for cc, vv in res_votes.items():
                if cc in rows_12:
                    rows_12[cc][res_id] = vv
        for res_id, res_votes in by_res_36.items():
            for cc, vv in res_votes.items():
                if cc in rows_36:
                    rows_36[cc][res_id] = vv

        # Previous window for delta
        prev_12_end = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=365)).strftime("%Y-%m-%d")
        prev_36_end = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=365 * 3)).strftime("%Y-%m-%d")
        by_res_12_prev = _get_votes_by_window(conn, 12, prev_12_end)
        by_res_36_prev = _get_votes_by_window(conn, 36, prev_36_end)
        rows_12_prev = {c: defaultdict(str) for c in countries}
        rows_36_prev = {c: defaultdict(str) for c in countries}
        for res_id, res_votes in by_res_12_prev.items():
            for cc, vv in res_votes.items():
                if cc in rows_12_prev:
                    rows_12_prev[cc][res_id] = vv
        for res_id, res_votes in by_res_36_prev.items():
            for cc, vv in res_votes.items():
                if cc in rows_36_prev:
                    rows_36_prev[cc][res_id] = vv

        count = 0
        for ca, cb in country_pairs:
            if ca not in countries or cb not in countries:
                continue
            for window, by_res, rows, rows_prev in [
                ("12m", by_res_12, rows_12, rows_12_prev),
                ("36m", by_res_36, rows_36, rows_36_prev),
            ]:
                score, n = _similarity(rows[ca], rows[cb], by_res)
                if n < 5:
                    continue
                delta_1y = delta_3y = None
                if window == "12m" and by_res_12_prev:
                    score_prev, _ = _similarity(rows_12_prev[ca], rows_12_prev[cb], by_res_12_prev)
                    delta_1y = round(score - score_prev, 2)
                if window == "36m" and by_res_36_prev:
                    score_prev, _ = _similarity(rows_36_prev[ca], rows_36_prev[cb], by_res_36_prev)
                    delta_3y = round(score - score_prev, 2)

                conn.execute(
                    """INSERT OR REPLACE INTO gpi_un_alignment_rolling
                       (country_a, country_b, window, end_date, similarity_score, vote_count_used, delta_1y, delta_3y, method_version)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (ca, cb, window, end_date, score, n, delta_1y, delta_3y, METHOD_VERSION),
                )
                count += 1

        conn.execute(
            """INSERT INTO gpi_un_model_changelog (model_name, version, run_at, notes) VALUES (?, ?, ?, ?)""",
            ("rolling_alignment", METHOD_VERSION, datetime.utcnow().isoformat(), f"Computed {count} pairs"),
        )
    return count
