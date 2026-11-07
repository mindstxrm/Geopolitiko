"""Lightweight durable ops status for scrape/job health (JSON file beside the DB)."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import BASE_DIR, DATABASE_PATH

_DEFAULT_PATH = Path(DATABASE_PATH).resolve().parent / "platform_ops_status.json"


def ops_status_path() -> Path:
    override = (os.environ.get("PLATFORM_OPS_STATUS_PATH") or "").strip()
    if override:
        return Path(override)
    return _DEFAULT_PATH


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def read_ops_status() -> dict[str, Any]:
    path = ops_status_path()
    if not path.is_file():
        return {}
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw) if raw.strip() else {}
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def write_ops_status(patch: dict[str, Any]) -> dict[str, Any]:
    """Merge patch into ops status and persist. Best-effort; never raises to callers."""
    path = ops_status_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        current = read_ops_status()
        current.update(patch)
        current["updated_at"] = _now_iso()
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(current, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        tmp.replace(path)
        return current
    except OSError:
        return patch


def record_refresh_start() -> None:
    write_ops_status({"last_refresh_started_at": _now_iso(), "last_refresh_status": "running"})


def record_refresh_success(*, articles_scraped: int, sources: int) -> None:
    write_ops_status(
        {
            "last_refresh_finished_at": _now_iso(),
            "last_refresh_status": "ok",
            "last_refresh_error": None,
            "last_scrape_articles": int(articles_scraped),
            "last_scrape_sources": int(sources),
        }
    )


def record_refresh_failure(error: str) -> None:
    write_ops_status(
        {
            "last_refresh_finished_at": _now_iso(),
            "last_refresh_status": "error",
            "last_refresh_error": (error or "")[:2000],
        }
    )


def record_macro_success() -> None:
    write_ops_status({"last_macro_ingest_at": _now_iso(), "last_macro_status": "ok"})


def record_macro_failure(error: str) -> None:
    write_ops_status(
        {
            "last_macro_ingest_at": _now_iso(),
            "last_macro_status": "error",
            "last_macro_error": (error or "")[:2000],
        }
    )


def record_backup(path: str, bytes_copied: int) -> None:
    write_ops_status(
        {
            "last_backup_at": _now_iso(),
            "last_backup_path": path,
            "last_backup_bytes": int(bytes_copied),
        }
    )


def record_desk_worker_heartbeat(
    *,
    status: str = "ok",
    interval_seconds: int | None = None,
    stats: dict[str, Any] | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    patch: dict[str, Any] = {
        "desk_worker_last_heartbeat_at": _now_iso(),
        "desk_worker_last_status": status,
    }
    if interval_seconds is not None:
        patch["desk_worker_interval_seconds"] = int(interval_seconds)
    if stats:
        patch["desk_worker_last_stats"] = {
            k: stats.get(k)
            for k in (
                "ingested",
                "enriched",
                "metrics_docs",
                "events",
                "alerts_created",
                "briefing_id",
            )
            if k in stats
        }
    if error:
        patch["desk_worker_last_error"] = (error or "")[:2000]
    elif status == "ok":
        patch["desk_worker_last_error"] = None
    return write_ops_status(patch)


def default_backup_dir() -> Path:
    return Path(BASE_DIR) / "data" / "backups"
