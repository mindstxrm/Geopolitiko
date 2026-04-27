"""UN vote analytics schema and migration for GPI."""
import hashlib
import sqlite3


# Vote value normalization: map raw values to YES/NO/ABSTAIN/ABSENT
VOTE_VALUE_MAP = {
    "yes": "YES",
    "in favor": "YES",
    "no": "NO",
    "against": "NO",
    "abstain": "ABSTAIN",
    "abstaining": "ABSTAIN",
    "absent": "ABSENT",
    "non-member": "ABSENT",
    "non-voting": "ABSENT",
    "non voting": "ABSENT",
}


def _normalize_vote_value(raw: str) -> str:
    """Normalize vote string to YES/NO/ABSTAIN/ABSENT."""
    if not raw:
        return "ABSENT"
    v = str(raw).strip().lower()
    return VOTE_VALUE_MAP.get(v, "ABSENT")


def _make_vote_id(resolution_id: str, country_code: str) -> str:
    """Generate unique vote_id for gpi_un_votes_raw."""
    key = f"{resolution_id}|{country_code}"
    return hashlib.sha256(key.encode()).hexdigest()[:32]


def init_gpi_un_tables(conn: sqlite3.Connection) -> None:
    """Create all GPI UN vote analytics tables. Call from app.models.init_db."""
    # Raw vote records (normalized)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_un_votes_raw (
            vote_id TEXT PRIMARY KEY,
            body TEXT NOT NULL,
            session TEXT,
            year INTEGER,
            date TEXT NOT NULL,
            resolution_id TEXT NOT NULL,
            resolution_title TEXT,
            issue_tag TEXT,
            country_code TEXT NOT NULL,
            vote_value TEXT NOT NULL CHECK (vote_value IN ('YES','NO','ABSTAIN','ABSENT')),
            UNIQUE(resolution_id, country_code)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gpi_un_votes_raw_country ON gpi_un_votes_raw(country_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gpi_un_votes_raw_resolution ON gpi_un_votes_raw(resolution_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gpi_un_votes_raw_date ON gpi_un_votes_raw(date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gpi_un_votes_raw_body ON gpi_un_votes_raw(body)")

    # Resolutions metadata
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_un_resolutions (
            resolution_id TEXT PRIMARY KEY,
            body TEXT NOT NULL,
            date TEXT NOT NULL,
            title TEXT,
            text_url TEXT,
            issue_tag TEXT,
            importance_weight REAL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gpi_un_resolutions_issue ON gpi_un_resolutions(issue_tag)")

    # Resolution issue tags (multi-tag per resolution)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_un_issue_tags (
            resolution_id TEXT NOT NULL,
            issue_tag TEXT NOT NULL,
            tag_confidence REAL,
            method_version TEXT,
            PRIMARY KEY (resolution_id, issue_tag)
        )
    """)

    # Rolling pairwise alignment
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_un_alignment_rolling (
            country_a TEXT NOT NULL,
            country_b TEXT NOT NULL,
            window TEXT NOT NULL,
            end_date TEXT NOT NULL,
            similarity_score REAL NOT NULL,
            vote_count_used INTEGER NOT NULL,
            delta_1y REAL,
            delta_3y REAL,
            method_version TEXT,
            PRIMARY KEY (country_a, country_b, window, end_date)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gpi_un_align_rolling_end ON gpi_un_alignment_rolling(end_date)")

    # Alignment by issue
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_un_alignment_by_issue (
            country_a TEXT NOT NULL,
            country_b TEXT NOT NULL,
            issue_tag TEXT NOT NULL,
            year INTEGER NOT NULL,
            similarity_score REAL NOT NULL,
            vote_count_used INTEGER NOT NULL,
            method_version TEXT,
            PRIMARY KEY (country_a, country_b, issue_tag, year)
        )
    """)

    # Bloc cohesion
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_un_bloc_cohesion (
            bloc_name TEXT NOT NULL,
            period TEXT NOT NULL,
            cohesion_score REAL NOT NULL,
            vote_count_used INTEGER NOT NULL,
            method_version TEXT,
            PRIMARY KEY (bloc_name, period)
        )
    """)

    # Bloc member position
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_un_bloc_member_position (
            bloc_name TEXT NOT NULL,
            country_code TEXT NOT NULL,
            period TEXT NOT NULL,
            alignment_to_bloc REAL NOT NULL,
            rank_in_bloc INTEGER,
            method_version TEXT,
            PRIMARY KEY (bloc_name, country_code, period)
        )
    """)

    # UNSC veto events
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_unsc_veto_events (
            resolution_id TEXT NOT NULL,
            date TEXT NOT NULL,
            veto_country TEXT NOT NULL,
            veto_count INTEGER DEFAULT 1,
            topic_tag TEXT,
            PRIMARY KEY (resolution_id, veto_country)
        )
    """)

    # P5 divergence
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_unsc_p5_divergence (
            period TEXT PRIMARY KEY,
            divergence_score REAL NOT NULL,
            veto_count_total INTEGER NOT NULL,
            method_version TEXT
        )
    """)

    # Alignment shocks
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_un_alignment_shocks (
            country_code TEXT NOT NULL,
            reference TEXT NOT NULL,
            period_end TEXT NOT NULL,
            baseline_score REAL NOT NULL,
            current_score REAL NOT NULL,
            delta REAL NOT NULL,
            shock_flag INTEGER NOT NULL,
            method_version TEXT,
            PRIMARY KEY (country_code, reference, period_end)
        )
    """)

    # Country volatility
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_un_country_volatility (
            country_code TEXT NOT NULL,
            period TEXT NOT NULL,
            volatility_score REAL NOT NULL,
            abstain_rate REAL,
            absent_rate REAL,
            method_version TEXT,
            PRIMARY KEY (country_code, period)
        )
    """)

    # Global polarization
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_un_global_polarization (
            period TEXT PRIMARY KEY,
            polarization_score REAL NOT NULL,
            us_china_similarity REAL,
            method_version TEXT
        )
    """)

    # UN model changelog (separate from institutional gpi_model_changelog)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_un_model_changelog (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model_name TEXT NOT NULL,
            version TEXT NOT NULL,
            run_at TEXT NOT NULL,
            notes TEXT
        )
    """)


def migrate_un_votes_to_gpi_raw(conn: sqlite3.Connection) -> int:
    """
    Copy from existing un_votes table into gpi_un_votes_raw.
    body=UNGA, session/year derived from vote_date, vote_value normalized to YES/NO/ABSTAIN/ABSENT.
    Returns number of rows migrated.
    """
    init_gpi_un_tables(conn)
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='un_votes'"
    )
    if not cur.fetchone():
        return 0

    cur = conn.execute(
        """SELECT resolution_id, resolution_title, country_code, vote, vote_date
           FROM un_votes"""
    )
    rows = cur.fetchall()
    if not rows:
        return 0

    migrated = 0
    for row in rows:
        resolution_id, resolution_title, country_code, vote, vote_date = row
        vote_value = _normalize_vote_value(vote)
        vote_id = _make_vote_id(str(resolution_id), str(country_code))
        date_str = (vote_date or "")[:10] if vote_date else "0000-00-00"
        try:
            year = int(date_str[:4]) if len(date_str) >= 4 and date_str[:4].isdigit() else None
        except (ValueError, TypeError):
            year = None
        session = str(year) if year else None

        conn.execute(
            """INSERT OR REPLACE INTO gpi_un_votes_raw
               (vote_id, body, session, year, date, resolution_id, resolution_title, issue_tag, country_code, vote_value)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (vote_id, "UNGA", session, year, date_str, resolution_id, resolution_title or "", None, country_code, vote_value),
        )
        migrated += 1
    return migrated
