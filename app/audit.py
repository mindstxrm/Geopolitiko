"""Append-only audit log for approve / reject / publish / role changes."""
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


def ensure_audit_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            actor TEXT,
            actor_role TEXT,
            action TEXT NOT NULL,
            entity_type TEXT,
            entity_id TEXT,
            detail_json TEXT,
            request_id TEXT,
            ip TEXT
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_log(created_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_log(action, created_at DESC)"
    )


def record_audit(
    action: str,
    *,
    actor: str | None = None,
    actor_role: str | None = None,
    entity_type: str | None = None,
    entity_id: str | int | None = None,
    detail: dict[str, Any] | None = None,
    request_id: str | None = None,
    ip: str | None = None,
    database_path: str | None = None,
) -> None:
    """Best-effort write; never raises to callers."""
    path = database_path or DATABASE_PATH
    try:
        from flask import g, has_request_context, request, session

        if has_request_context():
            if not request_id:
                request_id = getattr(g, "request_id", None)
            if not ip:
                ip = (request.headers.get("X-Forwarded-For") or request.remote_addr or "")[:120]
            if not actor:
                desk = session.get("analyst_desk_user")
                if isinstance(desk, dict) and desk.get("username"):
                    actor = str(desk.get("username"))
                else:
                    try:
                        from flask_login import current_user

                        if current_user.is_authenticated:
                            actor = getattr(current_user, "username", None) or str(current_user.id)
                    except Exception:
                        pass
            if not actor_role:
                try:
                    from app.authz import current_role

                    actor_role = current_role()
                except Exception:
                    actor_role = None
    except Exception:
        pass

    try:
        conn = sqlite3.connect(path, timeout=10.0)
        try:
            ensure_audit_table(conn)
            conn.execute(
                """
                INSERT INTO audit_log
                  (created_at, actor, actor_role, action, entity_type, entity_id, detail_json, request_id, ip)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _now(),
                    (actor or "")[:200] or None,
                    (actor_role or "")[:40] or None,
                    (action or "unknown")[:80],
                    (entity_type or "")[:80] or None,
                    str(entity_id)[:80] if entity_id is not None else None,
                    json.dumps(detail or {}, ensure_ascii=False)[:4000] if detail else None,
                    (request_id or "")[:40] or None,
                    (ip or "")[:120] or None,
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        logger.exception("audit_log write failed action=%s", action)


def recent_audit(limit: int = 50, database_path: str | None = None) -> list[dict[str, Any]]:
    path = database_path or DATABASE_PATH
    try:
        conn = sqlite3.connect(path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        try:
            ensure_audit_table(conn)
            rows = conn.execute(
                """
                SELECT id, created_at, actor, actor_role, action, entity_type, entity_id, detail_json, request_id
                FROM audit_log
                ORDER BY id DESC
                LIMIT ?
                """,
                (max(1, min(limit, 200)),),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()
    except Exception as e:
        logger.warning("recent_audit failed: %s", e)
        return []
