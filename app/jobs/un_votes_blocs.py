"""Bloc cohesion and member position within blocs."""
from collections import defaultdict
from datetime import datetime
from typing import Optional

from app.models import _connection, init_db
from app.un_votes.blocs import BLOCS
from config import DATABASE_PATH

METHOD_VERSION = "bloc_v1"


def _centroid_vote(res_votes: dict, members: list) -> dict:
    """For each resolution, majority vote among bloc members. Returns {res_id: vote}."""
    out = {}
    for res_id, votes in res_votes.items():
        bloc_votes = [votes.get(m) for m in members if votes.get(m) in ("YES", "NO", "ABSTAIN")]
        if not bloc_votes:
            continue
        yes_n = bloc_votes.count("YES")
        no_n = bloc_votes.count("NO")
        abstain_n = bloc_votes.count("ABSTAIN")
        if yes_n >= no_n and yes_n >= abstain_n:
            out[res_id] = "YES"
        elif no_n >= yes_n and no_n >= abstain_n:
            out[res_id] = "NO"
        else:
            out[res_id] = "ABSTAIN"
    return out


def _align_to_vector(member_votes: dict, centroid: dict) -> float:
    """Similarity of member votes to centroid. 1.0 = perfect match."""
    agreed = total = 0
    for res_id, cv in centroid.items():
        mv = member_votes.get(res_id)
        if mv is None:
            continue
        total += 1
        agreed += 1 if mv == cv else (0.5 if mv == "ABSTAIN" or cv == "ABSTAIN" else 0)
    return (agreed / total * 100) if total else 0


def run_bloc_cohesion(period: Optional[str] = None) -> int:
    """Compute bloc cohesion and member alignment for each bloc."""
    init_db(DATABASE_PATH)
    if not period:
        period = datetime.utcnow().strftime("%Y-%m")
    year = period[:4]
    with _connection() as conn:
        cur = conn.execute(
            """SELECT resolution_id, country_code, vote_value FROM gpi_un_votes_raw
               WHERE strftime('%Y', date) = ? AND vote_value != 'ABSENT'""",
            (year,),
        )
        by_res = defaultdict(dict)
        for row in cur.fetchall():
            by_res[row["resolution_id"]][row["country_code"]] = row["vote_value"]

        count = 0
        for bloc_name, members in BLOCS.items():
            present = [m for m in members if any(by_res[r].get(m) for r in by_res)]
            if len(present) < 2:
                continue
            centroid = _centroid_vote(by_res, present)
            if len(centroid) < 5:
                continue
            pair_agreements = 0
            pair_total = 0
            for i, a in enumerate(present):
                for b in present[i + 1:]:
                    for rid, cv in centroid.items():
                        va, vb = by_res.get(rid, {}).get(a), by_res.get(rid, {}).get(b)
                        if va and vb:
                            pair_total += 1
                            pair_agreements += 1 if va == vb else (0.5 if "ABSTAIN" in (va, vb) else 0)
            cohesion = round((pair_agreements / pair_total * 100) if pair_total else 0, 2)
            conn.execute(
                """INSERT OR REPLACE INTO gpi_un_bloc_cohesion
                   (bloc_name, period, cohesion_score, vote_count_used, method_version)
                   VALUES (?, ?, ?, ?, ?)""",
                (bloc_name, period, cohesion, pair_total, METHOD_VERSION),
            )
            count += 1

            member_scores = []
            for m in present:
                member_votes = {r: by_res[r].get(m) for r in centroid if by_res[r].get(m)}
                score = _align_to_vector(member_votes, centroid)
                member_scores.append((m, score))
            member_scores.sort(key=lambda x: -x[1])
            for rank, (cc, score) in enumerate(member_scores, 1):
                conn.execute(
                    """INSERT OR REPLACE INTO gpi_un_bloc_member_position
                       (bloc_name, country_code, period, alignment_to_bloc, rank_in_bloc, method_version)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (bloc_name, cc, period, round(score, 2), rank, METHOD_VERSION),
                )
                count += 1

        conn.execute(
            """INSERT INTO gpi_un_model_changelog (model_name, version, run_at, notes) VALUES (?, ?, ?, ?)""",
            ("bloc_cohesion", METHOD_VERSION, datetime.utcnow().isoformat(), f"Processed {len(BLOCS)} blocs"),
        )
    return count
