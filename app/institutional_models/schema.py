"""Create all gpi_* database tables for institutional models."""
import sqlite3
from datetime import datetime


def init_gpi_tables(conn: sqlite3.Connection) -> None:
    """Create all gpi_ tables. Call from app.models.init_db."""
    # GEPI channel scores (per day, per country-issue or global)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_gepi_channel_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            as_of_date TEXT NOT NULL,
            country_code TEXT,
            channel_name TEXT NOT NULL,
            raw_value REAL NOT NULL,
            rolling_mean_180 REAL,
            rolling_std_180 REAL,
            standardized_score REAL,
            methodology_version TEXT NOT NULL DEFAULT '1.0',
            input_data_timestamp TEXT,
            last_updated TEXT NOT NULL,
            model_notes TEXT,
            UNIQUE(as_of_date, country_code, channel_name)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gepi_channel_as_of ON gpi_gepi_channel_scores(as_of_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gepi_channel_country ON gpi_gepi_channel_scores(country_code)")

    # GEPI weights (versioned)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_gepi_weights (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version TEXT NOT NULL UNIQUE,
            weights_json TEXT NOT NULL,
            effective_from TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    _ensure_gepi_weights_seed(conn)

    # GEPI daily output
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_gepi_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            as_of_date TEXT NOT NULL,
            country_code TEXT,
            gepi_score REAL NOT NULL,
            model_version TEXT NOT NULL DEFAULT '1.0',
            uncertainty_estimate REAL,
            methodology_version TEXT NOT NULL,
            input_data_timestamp TEXT,
            confidence_score REAL,
            last_updated TEXT NOT NULL,
            model_notes TEXT,
            UNIQUE(as_of_date, country_code)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gepi_daily_as_of ON gpi_gepi_daily(as_of_date)")

    # CDEI chokepoints (mirrors main chokepoints with daily risk; slug links to chokepoints)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_chokepoints (
            chokepoint_id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            daily_risk_score REAL DEFAULT 0,
            risk_source_explanation TEXT,
            last_updated TEXT NOT NULL
        )
    """)

    # CDEI country-chokepoint exposure (country_code, chokepoint_slug for flexibility)
    _ensure_gpi_exposure_slug(conn)

    # CDEI daily output
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_cdei_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            as_of_date TEXT NOT NULL,
            country_code TEXT NOT NULL,
            cdei_total REAL NOT NULL,
            cdei_energy REAL DEFAULT 0,
            cdei_trade REAL DEFAULT 0,
            cdei_tech REAL DEFAULT 0,
            methodology_version TEXT NOT NULL,
            input_data_timestamp TEXT,
            confidence_score REAL,
            last_updated TEXT NOT NULL,
            UNIQUE(as_of_date, country_code)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cdei_daily_as_of ON gpi_cdei_daily(as_of_date)")

    # SFI - extend sanctions via view or we add columns via _ensure; use separate gpi table for SFI scores
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_sfi_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            as_of_date TEXT NOT NULL,
            country_code TEXT NOT NULL,
            sfi_score REAL NOT NULL,
            active_sanction_count INTEGER DEFAULT 0,
            methodology_version TEXT NOT NULL,
            input_data_timestamp TEXT,
            confidence_score REAL,
            last_updated TEXT NOT NULL,
            model_notes TEXT,
            UNIQUE(as_of_date, country_code)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sfi_daily_as_of ON gpi_sfi_daily(as_of_date)")

    # GEG events
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            actors TEXT,
            targets TEXT,
            country_code TEXT,
            start_date TEXT NOT NULL,
            end_date TEXT,
            source_list TEXT,
            credibility_score REAL,
            confidence_score REAL,
            impact_channels TEXT,
            created_at TEXT NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gpi_events_date ON gpi_events(start_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gpi_events_country ON gpi_events(country_code)")

    # GEG event links
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_event_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_event_id INTEGER NOT NULL,
            target_event_id INTEGER NOT NULL,
            relationship_type TEXT NOT NULL,
            confidence_score REAL,
            inferred_at TEXT NOT NULL,
            FOREIGN KEY (source_event_id) REFERENCES gpi_events(event_id),
            FOREIGN KEY (target_event_id) REFERENCES gpi_events(event_id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gpi_links_source ON gpi_event_links(source_event_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gpi_links_target ON gpi_event_links(target_event_id)")

    # Alignment: Treaty-Behavior Consistency
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_alignment_tbcs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_a TEXT NOT NULL,
            country_b TEXT NOT NULL,
            treaty_score REAL NOT NULL,
            behavior_score REAL NOT NULL,
            consistency_score REAL NOT NULL,
            methodology_version TEXT NOT NULL,
            input_data_timestamp TEXT,
            last_updated TEXT NOT NULL,
            UNIQUE(country_a, country_b)
        )
    """)

    # Multi-layer alignment
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_alignment_multi (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_a TEXT NOT NULL,
            country_b TEXT NOT NULL,
            vote_alignment REAL,
            trade_alignment REAL,
            security_alignment REAL,
            tech_alignment REAL,
            narrative_alignment REAL,
            methodology_version TEXT NOT NULL,
            last_updated TEXT NOT NULL,
            UNIQUE(country_a, country_b)
        )
    """)

    # Escalation events (labels)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_escalation_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_date TEXT NOT NULL,
            country_code TEXT,
            region TEXT,
            event_type TEXT NOT NULL,
            description TEXT,
            created_at TEXT NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gepi_escalation_date ON gpi_escalation_events(event_date)")

    # Escalation probability model validation
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_model_validation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model_name TEXT NOT NULL,
            validation_date TEXT NOT NULL,
            brier_score REAL,
            auroc REAL,
            precision_at_top REAL,
            training_window_start TEXT,
            training_window_end TEXT,
            created_at TEXT NOT NULL
        )
    """)

    # Fragility daily
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_fragility_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            as_of_date TEXT NOT NULL,
            country_code TEXT NOT NULL,
            fragility_level REAL NOT NULL,
            fragility_trend REAL,
            shock_indicator INTEGER DEFAULT 0,
            uncertainty_low REAL,
            uncertainty_high REAL,
            methodology_version TEXT NOT NULL,
            input_data_timestamp TEXT,
            confidence_score REAL,
            last_updated TEXT NOT NULL,
            model_notes TEXT,
            UNIQUE(as_of_date, country_code)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fragility_daily_as_of ON gpi_fragility_daily(as_of_date)")

    # Model changelog
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpi_model_changelog (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model_name TEXT NOT NULL,
            version TEXT NOT NULL,
            change_description TEXT,
            changed_at TEXT NOT NULL
        )
    """)


def _ensure_gpi_exposure_slug(conn: sqlite3.Connection) -> None:
    """Ensure gpi_country_chokepoint_exposure exists with chokepoint_slug. Recreate if old schema."""
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='gpi_country_chokepoint_exposure'")
    if not cur.fetchone():
        conn.execute("""
            CREATE TABLE gpi_country_chokepoint_exposure (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_code TEXT NOT NULL,
                chokepoint_slug TEXT NOT NULL,
                trade_share_percentage REAL DEFAULT 0,
                energy_share_percentage REAL DEFAULT 0,
                tech_supply_share_percentage REAL DEFAULT 0,
                reroute_penalty_factor REAL DEFAULT 1.0,
                last_updated TEXT NOT NULL,
                UNIQUE(country_code, chokepoint_slug)
            )
        """)
    else:
        cur = conn.execute("PRAGMA table_info(gpi_country_chokepoint_exposure)")
        cols = {r[1] for r in cur.fetchall()}
        if "chokepoint_id" in cols or "chokepoint_slug" not in cols:
            conn.execute("DROP TABLE gpi_country_chokepoint_exposure")
            conn.execute("""
                CREATE TABLE gpi_country_chokepoint_exposure (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    country_code TEXT NOT NULL,
                    chokepoint_slug TEXT NOT NULL,
                    trade_share_percentage REAL DEFAULT 0,
                    energy_share_percentage REAL DEFAULT 0,
                    tech_supply_share_percentage REAL DEFAULT 0,
                    reroute_penalty_factor REAL DEFAULT 1.0,
                    last_updated TEXT NOT NULL,
                    UNIQUE(country_code, chokepoint_slug)
                )
            """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cdei_exposure_country ON gpi_country_chokepoint_exposure(country_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cdei_exposure_slug ON gpi_country_chokepoint_exposure(chokepoint_slug)")


def _ensure_gepi_weights_seed(conn: sqlite3.Connection) -> None:
    """Seed GEPI weights if empty."""
    cur = conn.execute("SELECT 1 FROM gpi_gepi_weights LIMIT 1")
    if cur.fetchone():
        return
    import json
    from .gepi_channels import GEPI_WEIGHTS, GEPI_WEIGHTS_VERSION
    now = datetime.utcnow().isoformat() + "Z"
    conn.execute(
        "INSERT INTO gpi_gepi_weights (version, weights_json, effective_from, created_at) VALUES (?, ?, ?, ?)",
        (GEPI_WEIGHTS_VERSION, json.dumps(GEPI_WEIGHTS), now[:10], now),
    )
