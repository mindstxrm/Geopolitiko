"""
Database URL resolution and optional SQLAlchemy engine.

Local/dev default remains SQLite file paths (`DATABASE_PATH` /
`ANALYST_DESK_DATABASE_PATH`). Production can set:

  DATABASE_URL=postgresql+psycopg://user:pass@host:5432/geopolitiko
  ANALYST_DESK_DATABASE_URL=postgresql+psycopg://user:pass@host:5432/analyst_desk

Full cutover of the legacy `models.py` SQLite SQL is incremental — see
`docs/P4_PLATFORM.md` and `scripts/migrate_sqlite_to_postgres.py`.
"""
from __future__ import annotations

import os
from functools import lru_cache
from typing import Any
from urllib.parse import urlparse

from config import ANALYST_DESK_DATABASE_PATH, BASE_DIR, DATABASE_PATH


def database_url() -> str:
    """Primary app DB URL. Prefers DATABASE_URL; else sqlite file URL."""
    raw = (os.environ.get("DATABASE_URL") or "").strip()
    if raw:
        return _normalize_url(raw)
    path = (os.environ.get("DATABASE_PATH") or DATABASE_PATH).strip()
    return f"sqlite:///{path}"


def analyst_desk_database_url() -> str:
    raw = (os.environ.get("ANALYST_DESK_DATABASE_URL") or "").strip()
    if raw:
        return _normalize_url(raw)
    path = (os.environ.get("ANALYST_DESK_DATABASE_PATH") or ANALYST_DESK_DATABASE_PATH).strip()
    return f"sqlite:///{path}"


def _normalize_url(url: str) -> str:
    """Render/Heroku often give postgres:// — SQLAlchemy wants postgresql+psycopg://."""
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://") :]
    if url.startswith("postgresql://") and "+psycopg" not in url and "+psycopg2" not in url:
        url = "postgresql+psycopg://" + url[len("postgresql://") :]
    return url


def dialect_name(url: str | None = None) -> str:
    u = url or database_url()
    scheme = urlparse(u).scheme.split("+")[0].lower()
    if scheme in ("postgres", "postgresql"):
        return "postgresql"
    if scheme.startswith("sqlite"):
        return "sqlite"
    return scheme or "unknown"


def is_postgres(url: str | None = None) -> bool:
    return dialect_name(url) == "postgresql"


def is_sqlite(url: str | None = None) -> bool:
    return dialect_name(url) == "sqlite"


@lru_cache(maxsize=4)
def get_engine(url: str | None = None):
    """SQLAlchemy engine for migrations / platform tables / health probes."""
    from sqlalchemy import create_engine

    target = url or database_url()
    kwargs: dict[str, Any] = {"pool_pre_ping": True}
    if target.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False, "timeout": 60}
    return create_engine(target, **kwargs)


def probe_database(url: str | None = None) -> dict[str, Any]:
    """Lightweight connectivity check for Ops/health."""
    target = url or database_url()
    out: dict[str, Any] = {
        "url_scheme": dialect_name(target),
        "ok": False,
        "error": None,
        "backend": dialect_name(target),
    }
    # Never echo credentials
    parsed = urlparse(target)
    if parsed.hostname:
        out["host"] = parsed.hostname
        out["database"] = (parsed.path or "").lstrip("/") or None
    elif target.startswith("sqlite"):
        out["path"] = target.replace("sqlite:///", "", 1)
    try:
        from sqlalchemy import text

        eng = get_engine(target)
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        out["ok"] = True
    except Exception as e:
        out["error"] = str(e)
    return out


def ensure_data_dirs() -> None:
    """Create local data/ for SQLite paths when not using remote Postgres."""
    if is_sqlite(database_url()):
        path = database_url().replace("sqlite:///", "", 1)
        os.makedirs(os.path.dirname(path) or str(BASE_DIR / "data"), exist_ok=True)
    if is_sqlite(analyst_desk_database_url()):
        path = analyst_desk_database_url().replace("sqlite:///", "", 1)
        os.makedirs(os.path.dirname(path) or str(BASE_DIR / "data"), exist_ok=True)
