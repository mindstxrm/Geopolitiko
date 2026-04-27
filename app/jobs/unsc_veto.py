"""UNSC veto and P5 divergence. Placeholder when UNSC data (body=UNSC) is not available."""
from datetime import datetime
from typing import Optional

from app.models import _connection, init_db
from app.un_votes.blocs import P5
from config import DATABASE_PATH

METHOD_VERSION = "unsc_v1"


def run_unsc_veto(period: Optional[str] = None) -> int:
    """Identify veto events and P5 divergence. No-op if no UNSC votes in gpi_un_votes_raw."""
    init_db(DATABASE_PATH)
    if not period:
        period = str(datetime.utcnow().year)
    with _connection() as conn:
        cur = conn.execute("SELECT COUNT(*) as n FROM gpi_un_votes_raw WHERE body = 'UNSC'")
        if cur.fetchone()["n"] == 0:
            # No UNSC data - insert placeholder row for P5 divergence
            conn.execute(
                """INSERT OR REPLACE INTO gpi_unsc_p5_divergence
                   (period, divergence_score, veto_count_total, method_version)
                   VALUES (?, 0, 0, ?)""",
                (period, METHOD_VERSION + "_no_data"),
            )
            conn.execute(
                """INSERT INTO gpi_un_model_changelog (model_name, version, run_at, notes) VALUES (?, ?, ?, ?)""",
                ("unsc_veto", METHOD_VERSION, datetime.utcnow().isoformat(), "No UNSC data; placeholder"),
            )
            return 0
        # TODO: when UNSC data exists, identify P5 NO votes on substantive resolutions as vetos
        # and compute divergence
        return 0
