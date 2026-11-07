"""
Dual-dialect SQL helpers (SQLite ↔ Postgres).

Legacy `app.models` still uses sqlite3 + `?` placeholders. New / migrated
code paths should go through these helpers so Postgres cutover is incremental.
"""
from __future__ import annotations

from typing import Any

from app.db import database_url, dialect_name, get_engine, is_postgres, is_sqlite


def active_dialect(url: str | None = None) -> str:
    return dialect_name(url or database_url())


def adapt_placeholders(sql: str, *, dialect: str | None = None) -> str:
    """
    Convert SQLite-style `?` placeholders to psycopg `%s` when dialect is postgresql.
    Leaves `?` alone for sqlite. Does not rewrite SQL functions (datetime(), etc.).
    """
    d = dialect or active_dialect()
    if d == "postgresql":
        # Naive but safe for our hand-written SQL that only uses ? for params
        return sql.replace("?", "%s")
    return sql


def connect_raw(url: str | None = None):
    """
    Return a DB-API connection.
    - sqlite → sqlite3 connection to file path
    - postgres → psycopg connection via SQLAlchemy engine.raw_connection()
    """
    target = url or database_url()
    if is_sqlite(target):
        import sqlite3

        path = target.replace("sqlite:///", "", 1)
        conn = sqlite3.connect(path, timeout=60.0)
        conn.row_factory = sqlite3.Row
        return conn
    if is_postgres(target):
        eng = get_engine(target)
        return eng.raw_connection()
    raise RuntimeError(f"Unsupported dialect for connect_raw: {dialect_name(target)}")


def execute(conn: Any, sql: str, params: tuple | list | None = None, *, dialect: str | None = None):
    """Execute SQL with dialect-adapted placeholders."""
    adapted = adapt_placeholders(sql, dialect=dialect)
    cur = conn.cursor()
    if params is None:
        cur.execute(adapted)
    else:
        cur.execute(adapted, params)
    return cur


def cutover_status() -> dict[str, Any]:
    url = database_url()
    return {
        "dialect": dialect_name(url),
        "is_sqlite": is_sqlite(url),
        "is_postgres": is_postgres(url),
        "ready_for_pg_runtime": False,  # flips true after models dual-dialect rewrite
        "note": "URL + Alembic + Compose ready; models.py still SQLite-native.",
    }
