"""Global polarization index: US–China divergence and overall landscape."""
from collections import defaultdict
from datetime import datetime
from typing import Optional

from app.models import _connection, init_db
from config import DATABASE_PATH

METHOD_VERSION = "polarization_v1"


def run_global_polarization(period: Optional[str] = None) -> int:
    """Compute US-China similarity and polarization (divergence from midpoint)."""
    init_db(DATABASE_PATH)
    if not period:
        period = datetime.utcnow().strftime("%Y-%m")
    year, month = period[:4], period[5:7] if len(period) >= 7 else "01"
    start = f"{year}-{month}-01"
    with _connection() as conn:
        cur = conn.execute(
            """SELECT resolution_id, country_code, vote_value FROM gpi_un_votes_raw
               WHERE date >= date(?, '-12 months') AND date <= ?
               AND vote_value IN ('YES','NO','ABSTAIN')""",
            (start, start),
        )
        by_res = defaultdict(dict)
        for row in cur.fetchall():
            by_res[row["resolution_id"]][row["country_code"]] = row["vote_value"]

        # US-China similarity
        us_china_agreed = us_china_total = 0
        for rid, votes in by_res.items():
            vu, vc = votes.get("USA"), votes.get("CHN")
            if vu and vc:
                us_china_total += 1
                us_china_agreed += 1 if vu == vc else (0.5 if "ABSTAIN" in (vu, vc) else 0)
        us_china_sim = round((us_china_agreed / us_china_total * 100), 2) if us_china_total else 0

        # Midpoint = for each resolution, (US+CHN)/2 in {-1,0,1} space. Then each country's distance.
        # Simplification: polarization = 100 - us_china_similarity (higher = more polarized)
        polarization_score = round(100 - us_china_sim, 2)

        conn.execute(
            """INSERT OR REPLACE INTO gpi_un_global_polarization
               (period, polarization_score, us_china_similarity, method_version)
               VALUES (?, ?, ?, ?)""",
            (period, polarization_score, us_china_sim, METHOD_VERSION),
        )
        conn.execute(
            """INSERT INTO gpi_un_model_changelog (model_name, version, run_at, notes) VALUES (?, ?, ?, ?)""",
            ("global_polarization", METHOD_VERSION, datetime.utcnow().isoformat(), ""),
        )
    return 1
