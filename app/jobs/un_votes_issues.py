"""Issue tags and alignment-by-issue for UN resolutions."""
from collections import defaultdict
from datetime import datetime
from typing import Optional

from app.models import _connection, init_db
from app.un_votes.issue_tags import tag_resolution, METHOD_VERSION as TAG_VERSION
from config import DATABASE_PATH

ALIGNMENT_VERSION = "by_issue_v1"


def run_issue_tags() -> int:
    """Tag resolutions in gpi_un_votes_raw, populate gpi_un_issue_tags and gpi_un_resolutions."""
    init_db(DATABASE_PATH)
    with _connection() as conn:
        cur = conn.execute(
            """SELECT DISTINCT resolution_id, resolution_title, MIN(date) as date
               FROM gpi_un_votes_raw GROUP BY resolution_id"""
        )
        rows = cur.fetchall()
        count = 0
        for row in rows:
            rid, title, date = row["resolution_id"], row["resolution_title"], row["date"]
            tags = tag_resolution(title or "", rid)
            for issue_tag, conf in tags:
                conn.execute(
                    """INSERT OR REPLACE INTO gpi_un_issue_tags
                       (resolution_id, issue_tag, tag_confidence, method_version) VALUES (?, ?, ?, ?)""",
                    (rid, issue_tag, conf, TAG_VERSION),
                )
                count += 1
            conn.execute(
                """INSERT OR REPLACE INTO gpi_un_resolutions
                   (resolution_id, body, date, title, issue_tag, importance_weight) VALUES (?, ?, ?, ?, ?, ?)""",
                (rid, "UNGA", date or "", title or "", tags[0][0] if tags else "other", 1.0),
            )
        conn.execute(
            """INSERT INTO gpi_un_model_changelog (model_name, version, run_at, notes) VALUES (?, ?, ?, ?)""",
            ("issue_tags", TAG_VERSION, datetime.utcnow().isoformat(), f"Tagged {len(rows)} resolutions"),
        )
    return count


def run_alignment_by_issue(year: Optional[int] = None) -> int:
    """Compute alignment by issue for key country pairs."""
    init_db(DATABASE_PATH)
    if not year:
        year = datetime.utcnow().year
    with _connection() as conn:
        cur = conn.execute(
            """SELECT resolution_id, issue_tag FROM gpi_un_issue_tags WHERE tag_confidence >= 0.5"""
        )
        res_to_issue = {r["resolution_id"]: r["issue_tag"] for r in cur.fetchall()}
        if not res_to_issue:
            run_issue_tags()
            cur = conn.execute(
                """SELECT resolution_id, issue_tag FROM gpi_un_issue_tags WHERE tag_confidence >= 0.5"""
            )
            res_to_issue = {r["resolution_id"]: r["issue_tag"] for r in cur.fetchall()}

        cur = conn.execute(
            """SELECT resolution_id, country_code, vote_value FROM gpi_un_votes_raw
               WHERE strftime('%Y', date) = ? AND vote_value != 'ABSENT'""",
            (str(year),),
        )
        by_issue = defaultdict(lambda: defaultdict(dict))
        for row in cur.fetchall():
            rid, cc, vv = row["resolution_id"], row["country_code"], row["vote_value"]
            issue = res_to_issue.get(rid, "other")
            by_issue[issue][rid][cc] = vv

        key_pairs = [
            ("USA", "CHN"), ("USA", "RUS"), ("CHN", "RUS"), ("USA", "GBR"), ("USA", "FRA"),
            ("USA", "DEU"), ("USA", "JPN"), ("USA", "IND"), ("USA", "SGP"),
        ]
        count = 0
        for ca, cb in key_pairs:
            for issue, res_votes in by_issue.items():
                agreed = total = 0
                for rid, votes in res_votes.items():
                    va, vb = votes.get(ca), votes.get(cb)
                    if va and vb:
                        total += 1
                        agreed += 1 if va == vb else (0.5 if "ABSTAIN" in (va, vb) else 0)
                if total < 3:
                    continue
                score = round(agreed / total * 100, 2)
                conn.execute(
                    """INSERT OR REPLACE INTO gpi_un_alignment_by_issue
                       (country_a, country_b, issue_tag, year, similarity_score, vote_count_used, method_version)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (min(ca, cb), max(ca, cb), issue, year, score, total, ALIGNMENT_VERSION),
                )
                count += 1
        conn.execute(
            """INSERT INTO gpi_un_model_changelog (model_name, version, run_at, notes) VALUES (?, ?, ?, ?)""",
            ("alignment_by_issue", ALIGNMENT_VERSION, datetime.utcnow().isoformat(), f"Computed {count}"),
        )
    return count
