"""Alignment scores: Treaty-Behavior Consistency (TBCS) and multi-layer alignment."""
from datetime import datetime
from typing import Dict, List, Optional

def _get_connection():
    from app.models import _connection
    return _connection


def _treaty_score(conn, country_a: str, country_b: str) -> float:
    """Count treaties between pair, weighted by clause strength. Normalize to 0-1."""
    cur = conn.execute(
        """SELECT COUNT(*), SUM(CASE WHEN has_escalation_clause = 1 THEN 1.5 ELSE 1.0 END)
           FROM treaties WHERE (party_a = ? AND party_b = ?) OR (party_a = ? AND party_b = ?)""",
        (country_a, country_b, country_b, country_a),
    )
    row = cur.fetchone()
    if not row or not row[0]:
        return 0.0
    cnt, wsum = row[0], row[1] or 0
    return min(1.0, (wsum or 0) / 5.0)


def _behavior_score(conn, country_a: str, country_b: str) -> float:
    """UN vote similarity, sanctions co-sign. Normalize to 0-1."""
    cur = conn.execute(
        """SELECT alignment_score FROM voting_alignment
           WHERE (country_a = ? AND country_b = ?) OR (country_a = ? AND country_b = ?)""",
        (country_a, country_b, country_b, country_a),
    )
    row = cur.fetchone()
    if row and row[0] is not None:
        return max(0.0, min(1.0, (row[0] + 1) / 2.0))
    return 0.5


def compute_tbcs(as_of: Optional[str] = None) -> int:
    """Compute Treaty-Behavior Consistency for country pairs. Returns count."""
    as_of = as_of or datetime.utcnow().strftime("%Y-%m-%d")
    now = datetime.utcnow().isoformat() + "Z"
    _conn = _get_connection()
    count = 0
    with _conn() as conn:
        cur = conn.execute("SELECT DISTINCT party_a, party_b FROM treaties WHERE party_a IS NOT NULL AND party_b IS NOT NULL")
        pairs = set()
        for row in cur:
            a, b = (row[0] or "").strip(), (row[1] or "").strip()
            if a and b and a != b:
                pairs.add((min(a, b), max(a, b)))
        cur = conn.execute("SELECT DISTINCT country_a, country_b FROM voting_alignment")
        for row in cur:
            a, b = (row[0] or "").strip(), (row[1] or "").strip()
            if a and b and a != b:
                pairs.add((min(a, b), max(a, b)))
        for a, b in pairs:
            treaty_sc = _treaty_score(conn, a, b)
            behavior_sc = _behavior_score(conn, a, b)
            consistency = 1.0 - abs(treaty_sc - behavior_sc)
            conn.execute(
                """INSERT OR REPLACE INTO gpi_alignment_tbcs
                   (country_a, country_b, treaty_score, behavior_score, consistency_score, methodology_version, last_updated)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (a, b, treaty_sc, behavior_sc, consistency, "1.0", now),
            )
            count += 1
    return count


def compute_multi_layer(as_of: Optional[str] = None) -> int:
    """Compute multi-layer alignment (vote, trade, security, tech, narrative). Uses vote as primary for now."""
    as_of = as_of or datetime.utcnow().strftime("%Y-%m-%d")
    now = datetime.utcnow().isoformat() + "Z"
    _conn = _get_connection()
    count = 0
    with _conn() as conn:
        cur = conn.execute("SELECT country_a, country_b, alignment_score FROM voting_alignment")
        for row in cur:
            a, b = (row[0] or "").strip(), (row[1] or "").strip()
            vote_al = max(0.0, min(1.0, (row[2] or 0) + 0.5)) if row[2] is not None else 0.5
            conn.execute(
                """INSERT OR REPLACE INTO gpi_alignment_multi
                   (country_a, country_b, vote_alignment, trade_alignment, security_alignment, tech_alignment, narrative_alignment, methodology_version, last_updated)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (a, b, vote_al, 0.5, 0.5, 0.5, 0.5, "1.0", now),
            )
            count += 1
    return count
