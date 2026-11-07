"""
Desk → Terminal intel domain (extracted from models.py sprawl).

Callers may import from here or keep using app.models re-exports.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_desk_terminal_intel_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS desk_terminal_intel (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            proposal_id INTEGER NOT NULL UNIQUE,
            agent_id TEXT NOT NULL,
            run_type TEXT NOT NULL,
            title TEXT,
            body_markdown TEXT NOT NULL,
            countries_csv TEXT,
            focus_country TEXT,
            metrics_json TEXT NOT NULL DEFAULT '[]',
            reviewer_note TEXT,
            published_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_desk_intel_published ON desk_terminal_intel(published_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_desk_intel_focus ON desk_terminal_intel(focus_country, published_at DESC)"
    )


@contextmanager
def _connection_for_path(path: str):
    conn = sqlite3.connect(path, timeout=60.0)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def upsert_desk_terminal_intel(
    *,
    proposal_id: int,
    agent_id: str,
    run_type: str,
    title: Optional[str],
    body_markdown: str,
    countries_csv: Optional[str],
    focus_country: Optional[str],
    metrics_json: str,
    reviewer_note: Optional[str],
    database_path: str,
) -> None:
    now = _now()
    with _connection_for_path(database_path) as conn:
        ensure_desk_terminal_intel_table(conn)
        conn.execute(
            """
            INSERT INTO desk_terminal_intel (
                proposal_id, agent_id, run_type, title, body_markdown, countries_csv,
                focus_country, metrics_json, reviewer_note, published_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(proposal_id) DO UPDATE SET
                agent_id = excluded.agent_id,
                run_type = excluded.run_type,
                title = excluded.title,
                body_markdown = excluded.body_markdown,
                countries_csv = excluded.countries_csv,
                focus_country = excluded.focus_country,
                metrics_json = excluded.metrics_json,
                reviewer_note = excluded.reviewer_note,
                updated_at = excluded.updated_at
            """,
            (
                proposal_id,
                agent_id,
                run_type,
                title,
                body_markdown,
                countries_csv,
                focus_country,
                metrics_json,
                reviewer_note,
                now,
                now,
            ),
        )


def parse_metrics_row(r: dict) -> dict:
    mj = r.get("metrics_json") or "[]"
    try:
        r["metrics"] = json.loads(mj) if isinstance(mj, str) else []
    except json.JSONDecodeError:
        r["metrics"] = []
    if "metrics_json" in r:
        del r["metrics_json"]
    return r
