"""Per-source HTTP conditional GET cache (ETag / Last-Modified)."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_source_http_cache_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS source_http_cache (
            source_name TEXT PRIMARY KEY,
            feed_url TEXT,
            etag TEXT,
            last_modified TEXT,
            last_status INTEGER,
            last_fetched_at TEXT,
            last_success_at TEXT,
            not_modified_count INTEGER NOT NULL DEFAULT 0,
            fetch_count INTEGER NOT NULL DEFAULT 0,
            last_error TEXT
        )
        """
    )


def get_source_cache(db_path: str, source_name: str) -> dict[str, Any] | None:
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        ensure_source_http_cache_table(conn)
        row = conn.execute(
            "SELECT * FROM source_http_cache WHERE source_name = ?",
            (source_name,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def record_source_fetch(
    db_path: str,
    *,
    source_name: str,
    feed_url: str,
    status: int | None,
    etag: str | None = None,
    last_modified: str | None = None,
    not_modified: bool = False,
    error: str | None = None,
) -> None:
    conn = sqlite3.connect(db_path, timeout=30.0)
    try:
        ensure_source_http_cache_table(conn)
        prev = conn.execute(
            "SELECT etag, last_modified, not_modified_count, fetch_count FROM source_http_cache WHERE source_name = ?",
            (source_name,),
        ).fetchone()
        nm = int(prev[2] if prev else 0) + (1 if not_modified else 0)
        fc = int(prev[3] if prev else 0) + 1
        keep_etag = etag if etag is not None else (prev[0] if prev else None)
        keep_lm = last_modified if last_modified is not None else (prev[1] if prev else None)
        now = _now()
        success_at = now if (status and 200 <= int(status) < 400) or not_modified else None
        conn.execute(
            """
            INSERT INTO source_http_cache (
                source_name, feed_url, etag, last_modified, last_status,
                last_fetched_at, last_success_at, not_modified_count, fetch_count, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_name) DO UPDATE SET
                feed_url = excluded.feed_url,
                etag = COALESCE(excluded.etag, source_http_cache.etag),
                last_modified = COALESCE(excluded.last_modified, source_http_cache.last_modified),
                last_status = excluded.last_status,
                last_fetched_at = excluded.last_fetched_at,
                last_success_at = COALESCE(excluded.last_success_at, source_http_cache.last_success_at),
                not_modified_count = excluded.not_modified_count,
                fetch_count = excluded.fetch_count,
                last_error = excluded.last_error
            """,
            (
                source_name,
                feed_url,
                keep_etag,
                keep_lm,
                status,
                now,
                success_at,
                nm,
                fc,
                (error or "")[:500] if error else None,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def source_http_cache_summary(db_path: str) -> dict[str, Any]:
    conn = sqlite3.connect(db_path, timeout=10.0)
    conn.row_factory = sqlite3.Row
    try:
        ensure_source_http_cache_table(conn)
        rows = conn.execute(
            """
            SELECT source_name, last_status, last_success_at, not_modified_count, fetch_count, last_error
            FROM source_http_cache
            ORDER BY source_name
            """
        ).fetchall()
        total = len(rows)
        fetches = sum(int(r["fetch_count"] or 0) for r in rows)
        nm = sum(int(r["not_modified_count"] or 0) for r in rows)
        return {
            "sources_tracked": total,
            "fetch_count": fetches,
            "not_modified_count": nm,
            "not_modified_rate": round(nm / fetches, 3) if fetches else 0.0,
            "sample": [dict(r) for r in rows[:12]],
        }
    except Exception as e:
        return {"sources_tracked": 0, "error": str(e)}
    finally:
        conn.close()
