"""Separate SQLite database for Analyst Desk proposals (not news.db)."""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

_AD_DB_PATH: str | None = None
MESSAGE_TYPES = (
    "document_ingested",
    "document_translated",
    "document_enriched",
    "event_created",
    "event_updated",
    "event_resolved",
    "score_updated",
    "risk_threshold_crossed",
    "alert_required",
    "briefing_required",
    "scenario_required",
)


def init_analyst_desk_db(path: str) -> None:
    """Create analyst desk tables. Safe to call multiple times."""
    global _AD_DB_PATH
    _AD_DB_PATH = path
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with _ad_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS proposals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id TEXT NOT NULL,
                run_type TEXT NOT NULL,
                title TEXT,
                body TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                reviewed_body TEXT,
                reviewer_note TEXT,
                source_context_json TEXT,
                countries_json TEXT,
                focus_country TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_proposals_status ON proposals(status)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_proposals_agent ON proposals(agent_id)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id TEXT NOT NULL UNIQUE,
                agent_name TEXT NOT NULL,
                agent_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                region TEXT,
                schedule_type TEXT,
                model_name TEXT,
                metadata_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_agents_type ON agents(agent_type)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_country_coverage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id TEXT NOT NULL,
                country_code TEXT NOT NULL,
                tier INTEGER,
                created_at TEXT NOT NULL,
                UNIQUE(agent_id, country_code)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_coverage_country ON agent_country_coverage(country_code)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL UNIQUE,
                agent_id TEXT NOT NULL,
                task_type TEXT NOT NULL,
                input_payload_json TEXT,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                error_message TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_agent_tasks_agent ON agent_tasks(agent_id, started_at DESC)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_type TEXT NOT NULL,
                from_agent_id TEXT,
                to_agent_id TEXT,
                entity_type TEXT,
                entity_id TEXT,
                priority TEXT NOT NULL DEFAULT 'normal',
                payload_json TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                processed_at TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_agent_messages_status ON agent_messages(status, created_at DESC)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_name TEXT NOT NULL UNIQUE,
                source_type TEXT NOT NULL DEFAULT 'rss',
                base_url TEXT,
                language TEXT,
                reliability_score REAL NOT NULL DEFAULT 0.6,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER,
                external_ref TEXT,
                title TEXT,
                url TEXT,
                published_at TEXT,
                fetched_at TEXT NOT NULL,
                language TEXT,
                raw_text TEXT,
                html_content TEXT,
                content_hash TEXT,
                author TEXT,
                document_type TEXT NOT NULL DEFAULT 'news',
                ingestion_status TEXT NOT NULL DEFAULT 'new',
                UNIQUE(url),
                UNIQUE(content_hash)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_raw_documents_status ON raw_documents(ingestion_status, fetched_at DESC)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS document_countries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doc_id INTEGER NOT NULL,
                country_code TEXT NOT NULL,
                relevance_score REAL NOT NULL DEFAULT 0.5,
                role_type TEXT NOT NULL DEFAULT 'mentioned',
                UNIQUE(doc_id, country_code, role_type)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS document_topics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doc_id INTEGER NOT NULL,
                topic_name TEXT NOT NULL,
                confidence_score REAL NOT NULL DEFAULT 0.6,
                UNIQUE(doc_id, topic_name)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_signature TEXT,
                event_title TEXT NOT NULL,
                event_summary TEXT,
                event_type TEXT,
                primary_country_code TEXT,
                region TEXT,
                event_start_at TEXT,
                event_last_updated_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                importance_score REAL NOT NULL DEFAULT 0.0,
                confidence_score REAL NOT NULL DEFAULT 0.5,
                verification_status TEXT NOT NULL DEFAULT 'pending',
                created_by_agent TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        # Migration-safe: ensure event_signature exists before creating its index.
        cols = {r[1] for r in conn.execute("PRAGMA table_info(events)").fetchall()}
        if "event_signature" not in cols:
            conn.execute("ALTER TABLE events ADD COLUMN event_signature TEXT")
            # Re-read schema to avoid any SQLite edge cases.
            cols = {r[1] for r in conn.execute("PRAGMA table_info(events)").fetchall()}
        if "verification_status" not in cols:
            conn.execute("ALTER TABLE events ADD COLUMN verification_status TEXT NOT NULL DEFAULT 'pending'")
            cols = {r[1] for r in conn.execute("PRAGMA table_info(events)").fetchall()}
        if "event_signature" in cols:
            try:
                conn.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_events_signature_unique ON events(event_signature)"
                )
            except sqlite3.OperationalError:
                # If something still races/mismatches, don't block server startup.
                pass
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_country ON events(primary_country_code, event_last_updated_at DESC)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS event_documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL,
                doc_id INTEGER NOT NULL,
                is_primary_source INTEGER NOT NULL DEFAULT 0,
                relevance_score REAL NOT NULL DEFAULT 0.7,
                UNIQUE(event_id, doc_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS event_countries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL,
                country_code TEXT NOT NULL,
                role_type TEXT NOT NULL DEFAULT 'affected',
                UNIQUE(event_id, country_code, role_type)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS event_topics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL,
                topic_name TEXT NOT NULL,
                weight REAL NOT NULL DEFAULT 0.5,
                UNIQUE(event_id, topic_name)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS event_scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL,
                geopolitical_score REAL NOT NULL DEFAULT 0.0,
                economic_score REAL NOT NULL DEFAULT 0.0,
                military_score REAL NOT NULL DEFAULT 0.0,
                market_score REAL NOT NULL DEFAULT 0.0,
                urgency_score REAL NOT NULL DEFAULT 0.0,
                confidence_score REAL NOT NULL DEFAULT 0.5,
                scored_at TEXT NOT NULL,
                scored_by_agent TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER,
                country_code TEXT,
                region TEXT,
                channel TEXT NOT NULL DEFAULT 'internal',
                alert_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                headline TEXT NOT NULL,
                body TEXT NOT NULL,
                created_at TEXT NOT NULL,
                sent_at TEXT,
                delivery_status TEXT NOT NULL DEFAULT 'pending',
                error_message TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts(delivery_status, created_at DESC)"
        )
        alert_cols = {r[1] for r in conn.execute("PRAGMA table_info(alerts)").fetchall()}
        if "channel" not in alert_cols:
            conn.execute("ALTER TABLE alerts ADD COLUMN channel TEXT NOT NULL DEFAULT 'internal'")
        if "error_message" not in alert_cols:
            conn.execute("ALTER TABLE alerts ADD COLUMN error_message TEXT")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS briefings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                briefing_type TEXT NOT NULL,
                scope_type TEXT NOT NULL,
                scope_id TEXT,
                title TEXT NOT NULL,
                summary TEXT,
                body_markdown TEXT NOT NULL,
                generated_at TEXT NOT NULL,
                generated_by_agent TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_briefings_type_date ON briefings(briefing_type, generated_at DESC)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS country_risk_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_code TEXT NOT NULL,
                snapshot_date TEXT NOT NULL,
                political_risk REAL NOT NULL DEFAULT 0.0,
                conflict_risk REAL NOT NULL DEFAULT 0.0,
                sanctions_risk REAL NOT NULL DEFAULT 0.0,
                macro_risk REAL NOT NULL DEFAULT 0.0,
                supply_chain_risk REAL NOT NULL DEFAULT 0.0,
                overall_risk REAL NOT NULL DEFAULT 0.0,
                watch_level TEXT NOT NULL DEFAULT 'watch',
                generated_by_agent TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_country_risk_recent ON country_risk_snapshots(country_code, snapshot_date DESC)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS regional_risk_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                region TEXT NOT NULL,
                snapshot_date TEXT NOT NULL,
                overall_risk REAL NOT NULL DEFAULT 0.0,
                summary TEXT,
                top_risk_driver TEXT,
                generated_by_agent TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_regional_risk_recent ON regional_risk_snapshots(region, snapshot_date DESC)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS watchlist_movers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_code TEXT NOT NULL,
                previous_overall_risk REAL NOT NULL DEFAULT 0.0,
                current_overall_risk REAL NOT NULL DEFAULT 0.0,
                delta REAL NOT NULL DEFAULT 0.0,
                direction TEXT NOT NULL,
                reason TEXT,
                snapshot_date TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_watchlist_movers_recent ON watchlist_movers(snapshot_date DESC, delta DESC)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scenarios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope_type TEXT NOT NULL,
                scope_id TEXT NOT NULL,
                scenario_type TEXT NOT NULL,
                probability_estimate REAL NOT NULL DEFAULT 0.0,
                scenario_text TEXT NOT NULL,
                trigger_conditions TEXT,
                generated_at TEXT NOT NULL,
                generated_by_agent TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_scenarios_recent ON scenarios(generated_at DESC, scope_type, scope_id)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS risk_change_traces (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_code TEXT NOT NULL,
                snapshot_date TEXT NOT NULL,
                event_id INTEGER NOT NULL,
                dimension TEXT NOT NULL,
                contribution REAL NOT NULL DEFAULT 0.0
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_risk_traces_recent ON risk_change_traces(country_code, snapshot_date DESC)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS extracted_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doc_id INTEGER NOT NULL,
                country_code TEXT,
                metric_kind TEXT NOT NULL,
                label TEXT,
                value_numeric REAL,
                value_text TEXT,
                unit TEXT,
                snippet TEXT,
                confidence REAL NOT NULL DEFAULT 0.55,
                extracted_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_extracted_metrics_country ON extracted_metrics(country_code, extracted_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_extracted_metrics_doc ON extracted_metrics(doc_id)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS verification_checks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL,
                check_status TEXT NOT NULL,
                confidence REAL NOT NULL DEFAULT 0.5,
                reason TEXT,
                checked_by_agent TEXT NOT NULL,
                checked_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_verification_checks_event ON verification_checks(event_id, checked_at DESC)"
        )
        # raw_documents: track metric extraction pass
        rd_cols = {r[1] for r in conn.execute("PRAGMA table_info(raw_documents)").fetchall()}
        if "metrics_extracted_at" not in rd_cols:
            conn.execute("ALTER TABLE raw_documents ADD COLUMN metrics_extracted_at TEXT")
        # proposals: terminal publish audit
        prop_cols = {r[1] for r in conn.execute("PRAGMA table_info(proposals)").fetchall()}
        if "published_to_terminal_at" not in prop_cols:
            conn.execute("ALTER TABLE proposals ADD COLUMN published_to_terminal_at TEXT")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dead_letter_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_name TEXT NOT NULL,
                task_id TEXT,
                payload_json TEXT,
                error_message TEXT NOT NULL,
                retries INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_dead_letter_tasks_recent ON dead_letter_tasks(created_at DESC)"
        )


@contextmanager
def _ad_connection() -> Iterator[sqlite3.Connection]:
    if not _AD_DB_PATH:
        raise RuntimeError("Analyst desk DB not initialized; call init_analyst_desk_db(path) first")
    conn = sqlite3.connect(_AD_DB_PATH, timeout=60.0)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        yield conn
        conn.commit()
    finally:
        conn.close()


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def insert_proposal(
    agent_id: str,
    run_type: str,
    title: str,
    body: str,
    source_context: dict[str, Any],
    countries: list[str],
    focus_country: str | None = None,
) -> int:
    now = _now_iso()
    ctx = json.dumps(source_context, ensure_ascii=False)
    cj = json.dumps(countries, ensure_ascii=False)
    with _ad_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO proposals (
                agent_id, run_type, title, body, status, source_context_json,
                countries_json, focus_country, created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?)
            """,
            (agent_id, run_type, title, body, ctx, cj, focus_country, now, now),
        )
        return int(cur.lastrowid)


def list_proposals(status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
    with _ad_connection() as conn:
        if status:
            cur = conn.execute(
                """
                SELECT * FROM proposals WHERE status = ?
                ORDER BY datetime(created_at) DESC LIMIT ?
                """,
                (status, limit),
            )
        else:
            cur = conn.execute(
                """
                SELECT * FROM proposals
                ORDER BY datetime(created_at) DESC LIMIT ?
                """,
                (limit,),
            )
        return [_row_to_dict(row) for row in cur.fetchall()]


def get_proposal(proposal_id: int) -> dict[str, Any] | None:
    with _ad_connection() as conn:
        cur = conn.execute("SELECT * FROM proposals WHERE id = ?", (proposal_id,))
        row = cur.fetchone()
        return _row_to_dict(row) if row else None


def update_proposal_review(
    proposal_id: int,
    status: str,
    reviewed_body: str | None,
    reviewer_note: str | None,
) -> None:
    now = _now_iso()
    with _ad_connection() as conn:
        conn.execute(
            """
            UPDATE proposals SET
                status = ?,
                reviewed_body = ?,
                reviewer_note = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (status, reviewed_body, reviewer_note, now, proposal_id),
        )


def mark_proposal_published_to_terminal(proposal_id: int) -> None:
    now = _now_iso()
    with _ad_connection() as conn:
        conn.execute(
            "UPDATE proposals SET published_to_terminal_at = ?, updated_at = ? WHERE id = ?",
            (now, now, proposal_id),
        )


def list_enriched_documents_pending_metrics(limit: int = 120) -> list[dict[str, Any]]:
    """Raw documents that are enriched but not yet scanned for quantitative signals."""
    with _ad_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, title, raw_text, url, fetched_at
            FROM raw_documents
            WHERE ingestion_status = 'enriched'
              AND (metrics_extracted_at IS NULL OR metrics_extracted_at = '')
            ORDER BY datetime(fetched_at) DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def mark_document_metrics_extracted(doc_id: int) -> None:
    now = _now_iso()
    with _ad_connection() as conn:
        conn.execute(
            "UPDATE raw_documents SET metrics_extracted_at = ? WHERE id = ?",
            (now, doc_id),
        )


def list_document_country_codes(doc_id: int) -> list[str]:
    with _ad_connection() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT country_code FROM document_countries
            WHERE doc_id = ? ORDER BY relevance_score DESC
            """,
            (doc_id,),
        ).fetchall()
        return [str(r["country_code"]).upper() for r in rows if r["country_code"]]


def insert_extracted_metric(
    *,
    doc_id: int,
    country_code: str | None,
    metric_kind: str,
    label: str | None,
    value_numeric: float | None,
    value_text: str | None,
    unit: str | None,
    snippet: str | None,
    confidence: float = 0.55,
) -> int:
    now = _now_iso()
    with _ad_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO extracted_metrics (
                doc_id, country_code, metric_kind, label, value_numeric, value_text,
                unit, snippet, confidence, extracted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                doc_id,
                (country_code or "").upper() or None,
                metric_kind,
                (label or "")[:120] or None,
                value_numeric,
                (value_text or "")[:500] or None,
                (unit or "")[:40] or None,
                (snippet or "")[:500] or None,
                float(confidence),
                now,
            ),
        )
        return int(cur.lastrowid)


def list_extracted_metrics_for_countries(
    country_codes: list[str],
    *,
    days: int = 14,
    limit: int = 80,
) -> list[dict[str, Any]]:
    """Recent numeric extractions for terminal snapshot (linked to desk docs)."""
    if not country_codes:
        return []
    codes = [c.strip().upper() for c in country_codes if c and str(c).strip()]
    if not codes:
        return []
    since = datetime.now(timezone.utc) - timedelta(days=max(1, days))
    since_s = since.strftime("%Y-%m-%dT%H:%M:%SZ")
    placeholders = ",".join("?" * len(codes))
    with _ad_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT m.id, m.doc_id, m.country_code, m.metric_kind, m.label, m.value_numeric,
                   m.value_text, m.unit, m.snippet, m.confidence, m.extracted_at, d.title AS doc_title, d.url AS doc_url
            FROM extracted_metrics m
            LEFT JOIN raw_documents d ON d.id = m.doc_id
            WHERE m.country_code IN ({placeholders})
              AND datetime(m.extracted_at) >= datetime(?)
            ORDER BY datetime(m.extracted_at) DESC
            LIMIT ?
            """,
            (*codes, since_s, limit),
        ).fetchall()
        return [dict(r) for r in rows]


def sync_agents_registry(agents_map: dict[str, dict[str, Any]]) -> None:
    """Upsert static agent definitions and country coverage into orchestration tables."""
    now = _now_iso()
    with _ad_connection() as conn:
        for agent_id, cfg in (agents_map or {}).items():
            label = (cfg.get("label") or agent_id).strip()
            countries = [str(c).upper() for c in (cfg.get("countries") or []) if c]
            tiers = cfg.get("country_tiers") or {}
            parts = agent_id.split("_")
            region = parts[0] if parts else ""
            metadata = {
                "description": cfg.get("description") or "",
                "countries": countries,
                "country_tiers": tiers,
            }
            conn.execute(
                """
                INSERT INTO agents (
                    agent_id, agent_name, agent_type, status, region, schedule_type, model_name,
                    metadata_json, created_at, updated_at
                ) VALUES (?, ?, 'regional_monitor', 'active', ?, 'daily_weekly', 'heuristic_or_llm', ?, ?, ?)
                ON CONFLICT(agent_id) DO UPDATE SET
                    agent_name = excluded.agent_name,
                    region = excluded.region,
                    metadata_json = excluded.metadata_json,
                    updated_at = excluded.updated_at
                """,
                (agent_id, label, region, json.dumps(metadata, ensure_ascii=False), now, now),
            )
            for cc in countries:
                tier = tiers.get(cc)
                conn.execute(
                    """
                    INSERT INTO agent_country_coverage (agent_id, country_code, tier, created_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(agent_id, country_code) DO UPDATE SET
                        tier = excluded.tier
                    """,
                    (agent_id, cc, tier, now),
                )


def create_agent_task(
    agent_id: str,
    task_type: str,
    input_payload: dict[str, Any] | None = None,
) -> str:
    task_id = f"{agent_id}:{task_type}:{datetime.now(timezone.utc).timestamp():.6f}"
    with _ad_connection() as conn:
        conn.execute(
            """
            INSERT INTO agent_tasks (
                task_id, agent_id, task_type, input_payload_json, status, started_at
            ) VALUES (?, ?, ?, ?, 'running', ?)
            """,
            (
                task_id,
                agent_id,
                task_type,
                json.dumps(input_payload or {}, ensure_ascii=False),
                _now_iso(),
            ),
        )
    return task_id


def complete_agent_task(task_id: str) -> None:
    with _ad_connection() as conn:
        conn.execute(
            """
            UPDATE agent_tasks
            SET status = 'completed', completed_at = ?, error_message = NULL
            WHERE task_id = ?
            """,
            (_now_iso(), task_id),
        )


def fail_agent_task(task_id: str, error_message: str) -> None:
    with _ad_connection() as conn:
        conn.execute(
            """
            UPDATE agent_tasks
            SET status = 'failed', completed_at = ?, error_message = ?
            WHERE task_id = ?
            """,
            (_now_iso(), (error_message or "")[:4000], task_id),
        )


def emit_agent_message(
    message_type: str,
    *,
    from_agent_id: str | None = None,
    to_agent_id: str | None = None,
    entity_type: str | None = None,
    entity_id: str | int | None = None,
    priority: str = "normal",
    payload: dict[str, Any] | None = None,
) -> int:
    if message_type not in MESSAGE_TYPES:
        raise ValueError(f"Unsupported message_type: {message_type}")
    with _ad_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO agent_messages (
                message_type, from_agent_id, to_agent_id, entity_type, entity_id,
                priority, payload_json, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)
            """,
            (
                message_type,
                from_agent_id,
                to_agent_id,
                entity_type,
                str(entity_id) if entity_id is not None else None,
                priority,
                json.dumps(payload or {}, ensure_ascii=False),
                _now_iso(),
            ),
        )
        return int(cur.lastrowid)


def list_recent_agent_tasks(limit: int = 100) -> list[dict[str, Any]]:
    with _ad_connection() as conn:
        cur = conn.execute(
            """
            SELECT task_id, agent_id, task_type, status, started_at, completed_at, error_message
            FROM agent_tasks
            ORDER BY datetime(started_at) DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(r) for r in cur.fetchall()]


def ensure_source(source_name: str, source_type: str = "rss", base_url: str | None = None) -> int:
    now = _now_iso()
    with _ad_connection() as conn:
        conn.execute(
            """
            INSERT INTO sources (source_name, source_type, base_url, language, reliability_score, active, created_at, updated_at)
            VALUES (?, ?, ?, 'en', 0.6, 1, ?, ?)
            ON CONFLICT(source_name) DO UPDATE SET
                source_type = excluded.source_type,
                base_url = COALESCE(excluded.base_url, sources.base_url),
                updated_at = excluded.updated_at
            """,
            (source_name, source_type, base_url, now, now),
        )
        cur = conn.execute("SELECT id FROM sources WHERE source_name = ?", (source_name,))
        row = cur.fetchone()
        return int(row["id"]) if row else 0


def insert_raw_document(
    *,
    source_id: int | None,
    external_ref: str | None,
    title: str,
    url: str | None,
    published_at: str | None,
    fetched_at: str | None,
    raw_text: str | None,
    language: str = "en",
    document_type: str = "news",
) -> int | None:
    fetched = fetched_at or _now_iso()
    text = raw_text or ""
    content_hash = f"{(title or '').strip()}::{text[:200]}".lower()
    with _ad_connection() as conn:
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO raw_documents (
                source_id, external_ref, title, url, published_at, fetched_at, language, raw_text,
                html_content, content_hash, author, document_type, ingestion_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL, ?, 'new')
            """,
            (
                source_id,
                external_ref,
                title,
                url,
                published_at,
                fetched,
                language,
                text,
                content_hash,
                document_type,
            ),
        )
        if cur.rowcount:
            return int(cur.lastrowid)
        if url:
            row = conn.execute("SELECT id FROM raw_documents WHERE url = ?", (url,)).fetchone()
            if row:
                return int(row["id"])
        row = conn.execute("SELECT id FROM raw_documents WHERE content_hash = ?", (content_hash,)).fetchone()
        return int(row["id"]) if row else None


def set_document_enrichment(
    doc_id: int,
    countries: list[tuple[str, float, str]],
    topics: list[tuple[str, float]],
) -> None:
    with _ad_connection() as conn:
        for cc, score, role in countries:
            conn.execute(
                """
                INSERT OR REPLACE INTO document_countries (doc_id, country_code, relevance_score, role_type)
                VALUES (?, ?, ?, ?)
                """,
                (doc_id, cc.upper(), float(score), role),
            )
        for topic, score in topics:
            conn.execute(
                """
                INSERT OR REPLACE INTO document_topics (doc_id, topic_name, confidence_score)
                VALUES (?, ?, ?)
                """,
                (doc_id, topic, float(score)),
            )
        conn.execute(
            "UPDATE raw_documents SET ingestion_status = 'enriched' WHERE id = ?",
            (doc_id,),
        )


def get_raw_document(doc_id: int) -> dict[str, Any] | None:
    with _ad_connection() as conn:
        row = conn.execute("SELECT * FROM raw_documents WHERE id = ?", (doc_id,)).fetchone()
        return dict(row) if row else None


def dequeue_agent_messages(
    message_type: str | None = None,
    status: str = "pending",
    limit: int = 50,
) -> list[dict[str, Any]]:
    with _ad_connection() as conn:
        if message_type:
            rows = conn.execute(
                """
                SELECT * FROM agent_messages
                WHERE status = ? AND message_type = ?
                ORDER BY datetime(created_at) ASC
                LIMIT ?
                """,
                (status, message_type, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM agent_messages
                WHERE status = ?
                ORDER BY datetime(created_at) ASC
                LIMIT ?
                """,
                (status, limit),
            ).fetchall()
        out = []
        for row in rows:
            d = dict(row)
            try:
                d["payload"] = json.loads(d.get("payload_json") or "{}")
            except json.JSONDecodeError:
                d["payload"] = {}
            out.append(d)
        return out


def mark_message_processed(message_id: int, success: bool = True) -> None:
    with _ad_connection() as conn:
        conn.execute(
            """
            UPDATE agent_messages
            SET status = ?, processed_at = ?
            WHERE id = ?
            """,
            ("processed" if success else "failed", _now_iso(), message_id),
        )


def upsert_event_from_document(
    *,
    doc_id: int,
    event_title: str,
    event_summary: str,
    event_type: str,
    primary_country_code: str | None,
    region: str | None,
    importance_score: float,
    confidence_score: float,
    topics: list[str],
    countries: list[tuple[str, str]],
    created_by_agent: str,
    event_signature: str | None = None,
) -> int:
    now = _now_iso()
    with _ad_connection() as conn:
        row = None
        if event_signature:
            row = conn.execute(
                "SELECT id FROM events WHERE event_signature = ? LIMIT 1",
                (event_signature,),
            ).fetchone()
        if not row:
            # fallback matching for older rows without signatures
            row = conn.execute(
                """
                SELECT id FROM events
                WHERE event_title = ? AND COALESCE(primary_country_code, '') = COALESCE(?, '') AND COALESCE(event_type, '') = COALESCE(?, '')
                ORDER BY id DESC LIMIT 1
                """,
                (event_title, primary_country_code, event_type),
            ).fetchone()
        if row:
            event_id = int(row["id"])
            conn.execute(
                """
                UPDATE events
                SET event_summary = ?, event_last_updated_at = ?, importance_score = ?, confidence_score = ?,
                    event_signature = COALESCE(?, event_signature), updated_at = ?
                WHERE id = ?
                """,
                (event_summary, now, importance_score, confidence_score, event_signature, now, event_id),
            )
        else:
            cur = conn.execute(
                """
                INSERT INTO events (
                    event_signature, event_title, event_summary, event_type, primary_country_code, region, event_start_at,
                    event_last_updated_at, status, importance_score, confidence_score, verification_status,
                    created_by_agent, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, 'pending', ?, ?, ?)
                """,
                (
                    event_signature,
                    event_title,
                    event_summary,
                    event_type,
                    primary_country_code,
                    region,
                    now,
                    now,
                    importance_score,
                    confidence_score,
                    created_by_agent,
                    now,
                    now,
                ),
            )
            event_id = int(cur.lastrowid)
        conn.execute(
            """
            INSERT OR REPLACE INTO event_documents (event_id, doc_id, is_primary_source, relevance_score)
            VALUES (?, ?, 1, 0.9)
            """,
            (event_id, doc_id),
        )
        for cc, role in countries:
            conn.execute(
                """
                INSERT OR REPLACE INTO event_countries (event_id, country_code, role_type)
                VALUES (?, ?, ?)
                """,
                (event_id, cc.upper(), role),
            )
        for t in topics:
            conn.execute(
                """
                INSERT OR REPLACE INTO event_topics (event_id, topic_name, weight)
                VALUES (?, ?, 0.7)
                """,
                (event_id, t),
            )
        conn.execute(
            """
            INSERT INTO event_scores (
                event_id, geopolitical_score, economic_score, military_score, market_score,
                urgency_score, confidence_score, scored_at, scored_by_agent
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                importance_score,
                importance_score,
                importance_score if event_type in ("military", "security") else max(0.0, importance_score - 10),
                max(0.0, importance_score - 5),
                importance_score,
                confidence_score,
                now,
                created_by_agent,
            ),
        )
        return event_id


def list_recent_events(limit: int = 50) -> list[dict[str, Any]]:
    with _ad_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, event_title, event_type, primary_country_code, region, importance_score,
                   confidence_score, event_last_updated_at, status
            FROM events
            ORDER BY datetime(event_last_updated_at) DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def list_events_for_verification(limit: int = 120) -> list[dict[str, Any]]:
    with _ad_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, event_title, event_summary, event_type, primary_country_code, importance_score,
                   confidence_score, verification_status, event_last_updated_at
            FROM events
            WHERE verification_status IS NULL OR verification_status = '' OR verification_status = 'pending'
            ORDER BY datetime(event_last_updated_at) DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def set_event_verification(
    *,
    event_id: int,
    status: str,
    confidence: float,
    reason: str,
    checked_by_agent: str,
) -> None:
    now = _now_iso()
    with _ad_connection() as conn:
        conn.execute(
            """
            UPDATE events
            SET verification_status = ?, updated_at = ?
            WHERE id = ?
            """,
            (status, now, int(event_id)),
        )
        conn.execute(
            """
            INSERT INTO verification_checks (
                event_id, check_status, confidence, reason, checked_by_agent, checked_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (int(event_id), status, float(confidence), (reason or "")[:500], checked_by_agent, now),
        )


def list_recent_verification_checks(limit: int = 100) -> list[dict[str, Any]]:
    with _ad_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, event_id, check_status, confidence, reason, checked_by_agent, checked_at
            FROM verification_checks
            ORDER BY datetime(checked_at) DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def create_alert(
    *,
    event_id: int | None,
    country_code: str | None,
    region: str | None,
    alert_type: str,
    severity: str,
    headline: str,
    body: str,
    channel: str = "internal",
) -> int:
    now = _now_iso()
    with _ad_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO alerts (
                event_id, country_code, region, channel, alert_type, severity, headline, body, created_at, delivery_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
            """,
            (
                event_id,
                country_code,
                region,
                channel,
                alert_type,
                severity,
                headline[:240],
                body[:4000],
                now,
            ),
        )
        return int(cur.lastrowid)


def mark_alert_sent(alert_id: int) -> None:
    with _ad_connection() as conn:
        conn.execute(
            "UPDATE alerts SET delivery_status = 'sent', sent_at = ?, error_message = NULL WHERE id = ?",
            (_now_iso(), alert_id),
        )


def mark_alert_failed(alert_id: int, error_message: str) -> None:
    with _ad_connection() as conn:
        conn.execute(
            "UPDATE alerts SET delivery_status = 'failed', error_message = ? WHERE id = ?",
            ((error_message or "")[:3000], alert_id),
        )


def list_recent_alerts(limit: int = 100) -> list[dict[str, Any]]:
    with _ad_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, event_id, country_code, channel, alert_type, severity, headline, delivery_status, created_at, sent_at, error_message
            FROM alerts
            ORDER BY datetime(created_at) DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def create_briefing(
    *,
    briefing_type: str,
    scope_type: str,
    scope_id: str | None,
    title: str,
    summary: str,
    body_markdown: str,
    generated_by_agent: str,
) -> int:
    now = _now_iso()
    with _ad_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO briefings (
                briefing_type, scope_type, scope_id, title, summary, body_markdown, generated_at, generated_by_agent
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                briefing_type,
                scope_type,
                scope_id,
                title[:220],
                (summary or "")[:2000],
                body_markdown,
                now,
                generated_by_agent,
            ),
        )
        return int(cur.lastrowid)


def list_recent_briefings(limit: int = 50, briefing_type: str | None = None) -> list[dict[str, Any]]:
    with _ad_connection() as conn:
        if briefing_type:
            rows = conn.execute(
                """
                SELECT id, briefing_type, scope_type, scope_id, title, summary, generated_at, generated_by_agent
                FROM briefings
                WHERE briefing_type = ?
                ORDER BY datetime(generated_at) DESC
                LIMIT ?
                """,
                (briefing_type, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, briefing_type, scope_type, scope_id, title, summary, generated_at, generated_by_agent
                FROM briefings
                ORDER BY datetime(generated_at) DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]


def get_briefing(briefing_id: int) -> dict[str, Any] | None:
    with _ad_connection() as conn:
        row = conn.execute("SELECT * FROM briefings WHERE id = ?", (briefing_id,)).fetchone()
        return dict(row) if row else None


def create_country_risk_snapshot(
    *,
    country_code: str,
    snapshot_date: str,
    political_risk: float,
    conflict_risk: float,
    sanctions_risk: float,
    macro_risk: float,
    supply_chain_risk: float,
    overall_risk: float,
    watch_level: str,
    generated_by_agent: str,
) -> int:
    now = _now_iso()
    with _ad_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO country_risk_snapshots (
                country_code, snapshot_date, political_risk, conflict_risk, sanctions_risk, macro_risk,
                supply_chain_risk, overall_risk, watch_level, generated_by_agent, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                country_code.upper(),
                snapshot_date,
                float(political_risk),
                float(conflict_risk),
                float(sanctions_risk),
                float(macro_risk),
                float(supply_chain_risk),
                float(overall_risk),
                watch_level,
                generated_by_agent,
                now,
            ),
        )
        return int(cur.lastrowid)


def list_recent_country_risk_snapshots(limit: int = 100) -> list[dict[str, Any]]:
    with _ad_connection() as conn:
        rows = conn.execute(
            """
            SELECT country_code, snapshot_date, political_risk, conflict_risk, sanctions_risk,
                   macro_risk, supply_chain_risk, overall_risk, watch_level, generated_by_agent
            FROM country_risk_snapshots
            ORDER BY datetime(snapshot_date) DESC, overall_risk DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def create_regional_risk_snapshot(
    *,
    region: str,
    snapshot_date: str,
    overall_risk: float,
    summary: str,
    top_risk_driver: str,
    generated_by_agent: str,
) -> int:
    now = _now_iso()
    with _ad_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO regional_risk_snapshots (
                region, snapshot_date, overall_risk, summary, top_risk_driver, generated_by_agent, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (region, snapshot_date, float(overall_risk), summary, top_risk_driver, generated_by_agent, now),
        )
        return int(cur.lastrowid)


def list_recent_regional_risk_snapshots(limit: int = 50) -> list[dict[str, Any]]:
    with _ad_connection() as conn:
        rows = conn.execute(
            """
            SELECT region, snapshot_date, overall_risk, summary, top_risk_driver, generated_by_agent
            FROM regional_risk_snapshots
            ORDER BY datetime(snapshot_date) DESC, overall_risk DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def create_watchlist_mover(
    *,
    country_code: str,
    previous_overall_risk: float,
    current_overall_risk: float,
    delta: float,
    direction: str,
    reason: str,
    snapshot_date: str,
) -> int:
    now = _now_iso()
    with _ad_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO watchlist_movers (
                country_code, previous_overall_risk, current_overall_risk, delta, direction, reason, snapshot_date, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                country_code.upper(),
                float(previous_overall_risk),
                float(current_overall_risk),
                float(delta),
                direction,
                reason[:400] if reason else None,
                snapshot_date,
                now,
            ),
        )
        return int(cur.lastrowid)


def list_recent_watchlist_movers(limit: int = 50) -> list[dict[str, Any]]:
    with _ad_connection() as conn:
        rows = conn.execute(
            """
            SELECT country_code, previous_overall_risk, current_overall_risk, delta, direction, reason, snapshot_date
            FROM watchlist_movers
            ORDER BY datetime(snapshot_date) DESC, delta DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def latest_country_snapshot_map() -> dict[str, dict[str, Any]]:
    with _ad_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                country_code, snapshot_date,
                political_risk, conflict_risk, sanctions_risk, macro_risk, supply_chain_risk,
                overall_risk, watch_level
            FROM country_risk_snapshots
            ORDER BY datetime(snapshot_date) DESC
            """
        ).fetchall()
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        cc = (r["country_code"] or "").upper()
        if cc and cc not in out:
            out[cc] = dict(r)
    return out


def create_scenario(
    *,
    scope_type: str,
    scope_id: str,
    scenario_type: str,
    probability_estimate: float,
    scenario_text: str,
    trigger_conditions: list[str] | None,
    generated_by_agent: str,
) -> int:
    now = _now_iso()
    triggers = json.dumps(trigger_conditions or [], ensure_ascii=False)
    with _ad_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO scenarios (
                scope_type, scope_id, scenario_type, probability_estimate, scenario_text, trigger_conditions,
                generated_at, generated_by_agent
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                scope_type,
                scope_id.upper(),
                scenario_type,
                float(probability_estimate),
                scenario_text,
                triggers,
                now,
                generated_by_agent,
            ),
        )
        return int(cur.lastrowid)


def list_recent_scenarios(limit: int = 50, scope_type: str | None = None, scope_id: str | None = None) -> list[dict[str, Any]]:
    with _ad_connection() as conn:
        if scope_type and scope_id:
            rows = conn.execute(
                """
                SELECT id, scope_type, scope_id, scenario_type, probability_estimate, scenario_text, trigger_conditions, generated_at, generated_by_agent
                FROM scenarios
                WHERE scope_type = ? AND scope_id = ?
                ORDER BY datetime(generated_at) DESC
                LIMIT ?
                """,
                (scope_type, scope_id.upper(), limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, scope_type, scope_id, scenario_type, probability_estimate, scenario_text, trigger_conditions, generated_at, generated_by_agent
                FROM scenarios
                ORDER BY datetime(generated_at) DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            raw = d.get("trigger_conditions")
            if raw:
                try:
                    d["trigger_conditions"] = json.loads(raw)
                except json.JSONDecodeError:
                    d["trigger_conditions"] = []
            else:
                d["trigger_conditions"] = []
            out.append(d)
        return out


def get_scenario(scenario_id: int) -> dict[str, Any] | None:
    with _ad_connection() as conn:
        row = conn.execute(
            """
            SELECT id, scope_type, scope_id, scenario_type, probability_estimate, scenario_text, trigger_conditions, generated_at, generated_by_agent
            FROM scenarios WHERE id = ?
            """,
            (scenario_id,),
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        raw = d.get("trigger_conditions")
        if raw:
            try:
                d["trigger_conditions"] = json.loads(raw)
            except json.JSONDecodeError:
                d["trigger_conditions"] = []
        else:
            d["trigger_conditions"] = []
        return d


def delete_risk_traces(country_code: str, snapshot_date: str) -> None:
    with _ad_connection() as conn:
        conn.execute(
            "DELETE FROM risk_change_traces WHERE country_code = ? AND snapshot_date = ?",
            (country_code.upper(), snapshot_date),
        )


def insert_risk_trace(
    *,
    country_code: str,
    snapshot_date: str,
    event_id: int,
    dimension: str,
    contribution: float,
) -> None:
    with _ad_connection() as conn:
        conn.execute(
            """
            INSERT INTO risk_change_traces (country_code, snapshot_date, event_id, dimension, contribution)
            VALUES (?, ?, ?, ?, ?)
            """,
            (country_code.upper(), snapshot_date, int(event_id), dimension, float(contribution)),
        )


def list_risk_traces_for_country(
    *,
    country_code: str,
    snapshot_date: str | None = None,
    dimension: str | None = None,
    limit: int = 30,
) -> list[dict[str, Any]]:
    with _ad_connection() as conn:
        cc = (country_code or "").upper()
        if not cc:
            return []
        if not snapshot_date:
            row = conn.execute(
                "SELECT MAX(snapshot_date) AS sd FROM risk_change_traces WHERE country_code = ?",
                (cc,),
            ).fetchone()
            snapshot_date = row["sd"] if row else None
        clauses: list[str] = ["country_code = ?"]
        params: list[Any] = [cc]
        if snapshot_date:
            clauses.append("snapshot_date = ?")
            params.append(snapshot_date)
        if dimension:
            clauses.append("dimension = ?")
            params.append(dimension)
        where_sql = " AND ".join(clauses)
        rows = conn.execute(
            f"""
            SELECT id, country_code, snapshot_date, event_id, dimension, contribution
            FROM risk_change_traces
            WHERE {where_sql}
            ORDER BY contribution DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
        return [dict(r) for r in rows]


def get_country_recent_snapshots(country_code: str, limit: int = 2) -> list[dict[str, Any]]:
    with _ad_connection() as conn:
        rows = conn.execute(
            """
            SELECT overall_risk, snapshot_date
            FROM country_risk_snapshots
            WHERE country_code = ?
            ORDER BY datetime(snapshot_date) DESC
            LIMIT ?
            """,
            ((country_code or "").upper(), limit),
        ).fetchall()
        return [dict(r) for r in rows]


def query_events_text(query: str, limit: int = 30) -> list[dict[str, Any]]:
    q = (query or "").strip()
    if not q:
        return []
    pat = f"%{q}%"
    with _ad_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, event_title, event_summary, event_type, primary_country_code, importance_score, event_last_updated_at
            FROM events
            WHERE event_title LIKE ? OR event_summary LIKE ? OR event_type LIKE ? OR primary_country_code LIKE ?
            ORDER BY importance_score DESC, datetime(event_last_updated_at) DESC
            LIMIT ?
            """,
            (pat, pat, pat, pat, limit),
        ).fetchall()
        return [dict(r) for r in rows]


def get_events_by_ids(event_ids: list[int], limit: int | None = None) -> list[dict[str, Any]]:
    """Fetch normalized events by id for trace viewers."""
    if not event_ids:
        return []
    ids = [int(x) for x in event_ids if x is not None]
    if limit is not None:
        ids = ids[:limit]
    placeholders = ",".join("?" * len(ids))
    with _ad_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT id, event_title, event_type, primary_country_code, importance_score, event_last_updated_at
            FROM events
            WHERE id IN ({placeholders})
            """,
            tuple(ids),
        ).fetchall()
        return [dict(r) for r in rows]


def list_recent_agent_messages(limit: int = 100, status: str | None = None) -> list[dict[str, Any]]:
    with _ad_connection() as conn:
        if status:
            rows = conn.execute(
                """
                SELECT id, message_type, from_agent_id, to_agent_id, entity_type, entity_id, priority, status, created_at, processed_at
                FROM agent_messages
                WHERE status = ?
                ORDER BY datetime(created_at) DESC
                LIMIT ?
                """,
                (status, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, message_type, from_agent_id, to_agent_id, entity_type, entity_id, priority, status, created_at, processed_at
                FROM agent_messages
                ORDER BY datetime(created_at) DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]


def count_proposals_by_status() -> dict[str, int]:
    with _ad_connection() as conn:
        rows = conn.execute(
            """
            SELECT status, COUNT(*) AS n
            FROM proposals
            GROUP BY status
            """
        ).fetchall()
    out = {"pending": 0, "approved": 0, "rejected": 0}
    for r in rows:
        st = (r["status"] or "").strip().lower()
        if st in out:
            out[st] = int(r["n"] or 0)
    out["total"] = sum(out.values())
    return out


def list_stuck_agent_messages(older_than_minutes: int = 60, limit: int = 100) -> list[dict[str, Any]]:
    with _ad_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, message_type, from_agent_id, to_agent_id, entity_type, entity_id, priority, status, created_at
            FROM agent_messages
            WHERE status = 'pending'
              AND datetime(created_at) <= datetime('now', ?)
            ORDER BY datetime(created_at) ASC
            LIMIT ?
            """,
            (f"-{max(1, int(older_than_minutes))} minutes", limit),
        ).fetchall()
    return [dict(r) for r in rows]


def list_recent_extracted_metrics(limit: int = 200, country_code: str | None = None) -> list[dict[str, Any]]:
    with _ad_connection() as conn:
        if country_code:
            rows = conn.execute(
                """
                SELECT m.id, m.doc_id, m.country_code, m.metric_kind, m.label, m.value_numeric, m.value_text,
                       m.unit, m.snippet, m.confidence, m.extracted_at, d.title AS doc_title, d.url AS doc_url
                FROM extracted_metrics m
                LEFT JOIN raw_documents d ON d.id = m.doc_id
                WHERE m.country_code = ?
                ORDER BY datetime(m.extracted_at) DESC
                LIMIT ?
                """,
                (country_code.strip().upper(), limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT m.id, m.doc_id, m.country_code, m.metric_kind, m.label, m.value_numeric, m.value_text,
                       m.unit, m.snippet, m.confidence, m.extracted_at, d.title AS doc_title, d.url AS doc_url
                FROM extracted_metrics m
                LEFT JOIN raw_documents d ON d.id = m.doc_id
                ORDER BY datetime(m.extracted_at) DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
    return [dict(r) for r in rows]


def list_published_proposals(limit: int = 100) -> list[dict[str, Any]]:
    with _ad_connection() as conn:
        rows = conn.execute(
            """
            SELECT * FROM proposals
            WHERE status = 'approved' AND published_to_terminal_at IS NOT NULL AND published_to_terminal_at != ''
            ORDER BY datetime(published_to_terminal_at) DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [_row_to_dict(r) for r in rows]


def prune_operational_data(
    *,
    keep_tasks_days: int = 30,
    keep_messages_days: int = 14,
    keep_metrics_days: int = 30,
) -> dict[str, int]:
    """Delete old operational rows to control SQLite growth."""
    with _ad_connection() as conn:
        cur_t = conn.execute(
            """
            DELETE FROM agent_tasks
            WHERE datetime(started_at) < datetime('now', ?)
            """,
            (f"-{max(1, int(keep_tasks_days))} days",),
        )
        cur_m = conn.execute(
            """
            DELETE FROM agent_messages
            WHERE status IN ('processed', 'failed')
              AND datetime(created_at) < datetime('now', ?)
            """,
            (f"-{max(1, int(keep_messages_days))} days",),
        )
        cur_x = conn.execute(
            """
            DELETE FROM extracted_metrics
            WHERE datetime(extracted_at) < datetime('now', ?)
            """,
            (f"-{max(1, int(keep_metrics_days))} days",),
        )
    return {
        "tasks_deleted": int(cur_t.rowcount or 0),
        "messages_deleted": int(cur_m.rowcount or 0),
        "metrics_deleted": int(cur_x.rowcount or 0),
    }


def create_dead_letter_task(
    *,
    task_name: str,
    task_id: str | None,
    payload: dict[str, Any] | None,
    error_message: str,
    retries: int = 0,
) -> int:
    with _ad_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO dead_letter_tasks (
                task_name, task_id, payload_json, error_message, retries, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                task_name,
                task_id,
                json.dumps(payload or {}, ensure_ascii=False),
                (error_message or "")[:4000],
                int(retries),
                _now_iso(),
            ),
        )
        return int(cur.lastrowid)


def list_recent_dead_letter_tasks(limit: int = 100) -> list[dict[str, Any]]:
    with _ad_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, task_name, task_id, payload_json, error_message, retries, created_at
            FROM dead_letter_tasks
            ORDER BY datetime(created_at) DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            raw = d.get("payload_json")
            if raw and isinstance(raw, str):
                try:
                    d["payload"] = json.loads(raw)
                except json.JSONDecodeError:
                    d["payload"] = {}
            else:
                d["payload"] = {}
            out.append(d)
        return out


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    d = dict(row)
    for src_key, out_key in (
        ("source_context_json", "source_context"),
        ("countries_json", "countries"),
    ):
        raw = d.pop(src_key, None)
        if raw and isinstance(raw, str):
            try:
                d[out_key] = json.loads(raw)
            except json.JSONDecodeError:
                d[out_key] = {}
        else:
            d[out_key] = []
            if out_key == "source_context":
                d[out_key] = {}
    return d
