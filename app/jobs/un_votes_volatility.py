"""Foreign policy volatility: year-to-year consistency of alignment."""
from collections import defaultdict
from datetime import datetime
from typing import Optional

from app.models import _connection, init_db
from config import DATABASE_PATH

METHOD_VERSION = "volatility_v1"
REFS = ["USA", "CHN"]


def run_country_volatility(period: Optional[str] = None) -> int:
    """Compute volatility (year-to-year alignment variance) and abstain/absent rates."""
    init_db(DATABASE_PATH)
    if not period:
        period = str(datetime.utcnow().year)
    year = int(period[:4])
    with _connection() as conn:
        cur = conn.execute(
            """SELECT resolution_id, country_code, vote_value, strftime('%Y', date) as y
               FROM gpi_un_votes_raw WHERE date >= ? AND date < ?""",
            (f"{year-3}-01-01", f"{year+1}-01-01"),
        )
        by_year_country = defaultdict(lambda: defaultdict(dict))
        for row in cur.fetchall():
            y = row["y"]
            if not y:
                continue
            by_year_country[y][row["resolution_id"]][row["country_code"]] = row["vote_value"]

        count = 0
        countries = set()
        for yv in by_year_country.values():
            for rv in yv.values():
                countries.update(rv.keys())
        countries -= set(REFS)

        for cc in countries:
            scores = []
            for y in range(year - 2, year + 1):
                if str(y) not in by_year_country:
                    continue
                by_res = by_year_country[str(y)]
                for ref in REFS:
                    agreed = total = 0
                    for rid, votes in by_res.items():
                        va, vb = votes.get(cc), votes.get(ref)
                        if va and vb and va != "ABSENT" and vb != "ABSENT":
                            total += 1
                            agreed += 1 if va == vb else (0.5 if "ABSTAIN" in (va, vb) else 0)
                    if total >= 3:
                        scores.append((agreed / total) * 100)

            abstain_n = absent_n = total_n = 0
            for y in [str(year)]:
                if y not in by_year_country:
                    continue
                for rid, votes in by_year_country[y].items():
                    v = votes.get(cc)
                    if v:
                        total_n += 1
                        if v == "ABSTAIN":
                            abstain_n += 1
                        elif v == "ABSENT":
                            absent_n += 1
            abstain_rate = round(abstain_n / total_n * 100, 2) if total_n else 0
            absent_rate = round(absent_n / total_n * 100, 2) if total_n else 0

            volatility = 0.0
            if len(scores) >= 2:
                mean = sum(scores) / len(scores)
                variance = sum((s - mean) ** 2 for s in scores) / len(scores)
                volatility = round(variance ** 0.5, 2)

            conn.execute(
                """INSERT OR REPLACE INTO gpi_un_country_volatility
                   (country_code, period, volatility_score, abstain_rate, absent_rate, method_version)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (cc, period, volatility, abstain_rate, absent_rate, METHOD_VERSION),
            )
            count += 1

        conn.execute(
            """INSERT INTO gpi_un_model_changelog (model_name, version, run_at, notes) VALUES (?, ?, ?, ?)""",
            ("country_volatility", METHOD_VERSION, datetime.utcnow().isoformat(), f"Processed {count}"),
        )
    return count
