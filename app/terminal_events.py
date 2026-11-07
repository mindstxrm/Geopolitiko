"""
Terminal-facing event spine: mirror high-signal Desk events into news.db
so Situation Room / country views can read a durable product table.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any

from config import DATABASE_PATH

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_terminal_events_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS terminal_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            desk_event_id INTEGER NOT NULL UNIQUE,
            event_signature TEXT,
            title TEXT NOT NULL,
            summary TEXT,
            event_type TEXT,
            primary_country TEXT,
            region TEXT,
            importance_score REAL,
            confidence_score REAL,
            verification_status TEXT,
            confirmation_status TEXT,
            entities_json TEXT,
            source_count INTEGER DEFAULT 0,
            published_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_terminal_events_country ON terminal_events(primary_country, published_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_terminal_events_published ON terminal_events(published_at DESC)"
    )


def upsert_terminal_event(
    *,
    desk_event_id: int,
    title: str,
    summary: str | None = None,
    event_type: str | None = None,
    primary_country: str | None = None,
    region: str | None = None,
    importance_score: float | None = None,
    confidence_score: float | None = None,
    verification_status: str | None = None,
    confirmation_status: str | None = None,
    entities: list[dict[str, Any]] | None = None,
    source_count: int = 0,
    event_signature: str | None = None,
    database_path: str | None = None,
) -> int:
    path = database_path or DATABASE_PATH
    now = _now()
    conn = sqlite3.connect(path, timeout=30.0)
    try:
        ensure_terminal_events_table(conn)
        cur = conn.execute(
            """
            INSERT INTO terminal_events (
                desk_event_id, event_signature, title, summary, event_type, primary_country, region,
                importance_score, confidence_score, verification_status, confirmation_status,
                entities_json, source_count, published_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(desk_event_id) DO UPDATE SET
                event_signature = excluded.event_signature,
                title = excluded.title,
                summary = excluded.summary,
                event_type = excluded.event_type,
                primary_country = excluded.primary_country,
                region = excluded.region,
                importance_score = excluded.importance_score,
                confidence_score = excluded.confidence_score,
                verification_status = excluded.verification_status,
                confirmation_status = excluded.confirmation_status,
                entities_json = excluded.entities_json,
                source_count = excluded.source_count,
                updated_at = excluded.updated_at
            """,
            (
                desk_event_id,
                event_signature,
                (title or "")[:500],
                (summary or "")[:4000] if summary else None,
                event_type,
                (primary_country or "").upper() or None,
                region,
                importance_score,
                confidence_score,
                verification_status,
                confirmation_status,
                json.dumps(entities or [], ensure_ascii=False),
                int(source_count or 0),
                now,
                now,
            ),
        )
        conn.commit()
        return int(cur.lastrowid or 0)
    finally:
        conn.close()


def sync_recent_desk_events_to_terminal(limit: int = 50, database_path: str | None = None) -> dict[str, Any]:
    """Pull recent Desk events and upsert into news.db terminal_events."""
    from app.analyst_desk.confirmation import assess_confirmation
    from app.analyst_desk.store import list_recent_events
    from app.entities import extract_entities_from_text

    events = list_recent_events(limit=limit)
    synced = 0
    for e in events:
        title = e.get("event_title") or e.get("title") or "Untitled event"
        summary = e.get("event_summary") or e.get("summary") or ""
        text = f"{title}\n{summary}"
        cc = e.get("primary_country_code") or e.get("primary_country")
        entities = extract_entities_from_text(text, countries=[cc] if cc else None)
        conf = assess_confirmation(text=text, sources=[])
        # Prefer existing verification; overlay confirmation for sensitive text
        confirmation_status = conf["status"]
        try:
            upsert_terminal_event(
                desk_event_id=int(e["id"]),
                title=title,
                summary=summary,
                event_type=e.get("event_type"),
                primary_country=cc,
                region=e.get("region"),
                importance_score=e.get("importance_score"),
                confidence_score=e.get("confidence_score"),
                verification_status=e.get("verification_status"),
                confirmation_status=confirmation_status,
                entities=entities,
                source_count=0,
                event_signature=e.get("event_signature"),
                database_path=database_path,
            )
            synced += 1
        except Exception:
            logger.exception("sync terminal event failed desk_id=%s", e.get("id"))
    return {"ok": True, "synced": synced, "limit": limit}


def list_terminal_events(
    *,
    country: str | None = None,
    limit: int = 50,
    database_path: str | None = None,
) -> list[dict[str, Any]]:
    path = database_path or DATABASE_PATH
    conn = sqlite3.connect(path, timeout=10.0)
    conn.row_factory = sqlite3.Row
    try:
        ensure_terminal_events_table(conn)
        if country:
            rows = conn.execute(
                """
                SELECT * FROM terminal_events
                WHERE primary_country = ?
                ORDER BY datetime(published_at) DESC
                LIMIT ?
                """,
                (country.strip().upper(), max(1, min(limit, 200))),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM terminal_events
                ORDER BY datetime(published_at) DESC
                LIMIT ?
                """,
                (max(1, min(limit, 200)),),
            ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            raw = d.get("entities_json")
            if raw and isinstance(raw, str):
                try:
                    d["entities"] = json.loads(raw)
                except json.JSONDecodeError:
                    d["entities"] = []
            else:
                d["entities"] = []
            out.append(d)
        return out
    finally:
        conn.close()
