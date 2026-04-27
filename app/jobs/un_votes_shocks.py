"""Alignment shock detector: flag countries with sudden deviation from baseline."""
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional

from app.models import _connection, init_db
from config import DATABASE_PATH

METHOD_VERSION = "shocks_v1"
SHOCK_THRESHOLD = 15.0


def run_alignment_shocks(period_end: Optional[str] = None) -> int:
    """Compare current 6mo alignment to 3y baseline; flag if delta exceeds threshold."""
    init_db(DATABASE_PATH)
    if not period_end:
        period_end = datetime.utcnow().strftime("%Y-%m-%d")
    end_dt = datetime.strptime(period_end[:10], "%Y-%m-%d")
    current_start = (end_dt - timedelta(days=180)).strftime("%Y-%m-%d")
    baseline_start = (end_dt - timedelta(days=365 * 3)).strftime("%Y-%m-%d")

    with _connection() as conn:
        cur = conn.execute(
            """SELECT resolution_id, country_code, vote_value FROM gpi_un_votes_raw
               WHERE date >= ? AND date <= ? AND vote_value != 'ABSENT'""",
            (current_start, period_end),
        )
        current_by_res = defaultdict(dict)
        for row in cur.fetchall():
            current_by_res[row["resolution_id"]][row["country_code"]] = row["vote_value"]

        cur = conn.execute(
            """SELECT resolution_id, country_code, vote_value FROM gpi_un_votes_raw
               WHERE date >= ? AND date < ? AND vote_value != 'ABSENT'""",
            (baseline_start, current_start),
        )
        baseline_by_res = defaultdict(dict)
        for row in cur.fetchall():
            baseline_by_res[row["resolution_id"]][row["country_code"]] = row["vote_value"]

        def _score(votes_a, votes_b, by_res):
            agreed = total = 0
            for rid, res_v in by_res.items():
                va, vb = votes_a.get(rid), votes_b.get(rid)
                if va and vb:
                    total += 1
                    agreed += 1 if va == vb else (0.5 if "ABSTAIN" in (va, vb) else 0)
            return (agreed / total * 100) if total else None

        refs = ["USA", "CHN"]
        count = 0
        countries = set()
        for r in current_by_res.values():
            countries.update(r.keys())
        for r in baseline_by_res.values():
            countries.update(r.keys())

        for cc in countries:
            if cc in refs:
                continue
            for ref in refs:
                baseline = _score(
                    {r: baseline_by_res[r].get(cc) for r in baseline_by_res if baseline_by_res[r].get(cc)},
                    {r: baseline_by_res[r].get(ref) for r in baseline_by_res if baseline_by_res[r].get(ref)},
                    baseline_by_res,
                )
                current = _score(
                    {r: current_by_res[r].get(cc) for r in current_by_res if current_by_res[r].get(cc)},
                    {r: current_by_res[r].get(ref) for r in current_by_res if current_by_res[r].get(ref)},
                    current_by_res,
                )
                if baseline is None or current is None:
                    continue
                delta = round(current - baseline, 2)
                shock_flag = 1 if abs(delta) >= SHOCK_THRESHOLD else 0
                conn.execute(
                    """INSERT OR REPLACE INTO gpi_un_alignment_shocks
                       (country_code, reference, period_end, baseline_score, current_score, delta, shock_flag, method_version)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (cc, ref, period_end, round(baseline, 2), round(current, 2), delta, shock_flag, METHOD_VERSION),
                )
                count += 1

        conn.execute(
            """INSERT INTO gpi_un_model_changelog (model_name, version, run_at, notes) VALUES (?, ?, ?, ?)""",
            ("alignment_shocks", METHOD_VERSION, datetime.utcnow().isoformat(), f"Flagged {count}"),
        )
    return count
