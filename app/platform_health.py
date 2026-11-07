"""Platform health snapshot for /api/health and the Ops board."""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import (
    ANALYST_DESK_ADMIN_PASSWORD,
    ANALYST_DESK_DATABASE_PATH,
    DATABASE_PATH,
    SECRET_KEY,
    analyst_desk_publish_on_approve,
)
from app.platform_ops import read_ops_status


_DEV_SECRETS = {
    "dev-secret-key-change-in-production",
    "analyst-desk-dev-secret-change-in-production",
}


def is_production_runtime() -> bool:
    """True on Render or when GEOPOLITIKO_ENV/FLASK_ENV is production."""
    if (os.environ.get("RENDER") or "").strip():
        return True
    env = (os.environ.get("GEOPOLITIKO_ENV") or os.environ.get("FLASK_ENV") or "").strip().lower()
    return env in ("production", "prod")


def _file_db_stats(path: str) -> dict[str, Any]:
    p = Path(path)
    out: dict[str, Any] = {
        "path": str(p),
        "exists": p.is_file(),
        "size_bytes": p.stat().st_size if p.is_file() else 0,
        "writable_dir": False,
        "ok": False,
        "error": None,
    }
    try:
        out["writable_dir"] = os.access(p.parent, os.W_OK)
    except OSError as e:
        out["error"] = str(e)
        return out
    if not p.is_file():
        out["error"] = "database file missing"
        return out
    try:
        conn = sqlite3.connect(str(p), timeout=5.0)
        try:
            conn.execute("SELECT 1").fetchone()
            out["ok"] = True
        finally:
            conn.close()
    except sqlite3.Error as e:
        out["error"] = str(e)
    return out


def _hours_since(iso: str | None) -> float | None:
    if not iso:
        return None
    try:
        raw = iso.replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() / 3600.0
    except ValueError:
        return None


def _desk_counts() -> dict[str, Any]:
    out: dict[str, Any] = {
        "pending_proposals": None,
        "approved_unpublished": None,
        "recent_briefings": None,
        "last_task_at": None,
        "error": None,
    }
    path = ANALYST_DESK_DATABASE_PATH
    if not Path(path).is_file():
        out["error"] = "desk database missing"
        return out
    try:
        conn = sqlite3.connect(path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        try:
            out["pending_proposals"] = conn.execute(
                "SELECT COUNT(*) AS n FROM proposals WHERE status = 'pending'"
            ).fetchone()["n"]
            out["approved_unpublished"] = conn.execute(
                """
                SELECT COUNT(*) AS n FROM proposals
                WHERE status = 'approved'
                  AND (published_to_terminal_at IS NULL OR published_to_terminal_at = '')
                """
            ).fetchone()["n"]
            try:
                out["recent_briefings"] = conn.execute(
                    "SELECT COUNT(*) AS n FROM briefings"
                ).fetchone()["n"]
            except sqlite3.OperationalError:
                out["recent_briefings"] = None
            try:
                row = conn.execute(
                    "SELECT MAX(started_at) AS t FROM agent_tasks"
                ).fetchone()
                out["last_task_at"] = row["t"] if row else None
            except sqlite3.OperationalError:
                out["last_task_at"] = None
        finally:
            conn.close()
    except sqlite3.Error as e:
        out["error"] = str(e)
    return out


def _terminal_intel_stats() -> dict[str, Any]:
    out: dict[str, Any] = {"count": 0, "last_published_at": None, "error": None}
    if not Path(DATABASE_PATH).is_file():
        out["error"] = "news database missing"
        return out
    try:
        from app.models import get_recent_desk_terminal_intel

        rows = get_recent_desk_terminal_intel(limit=1)
        out["count"] = _count_desk_intel()
        out["last_published_at"] = rows[0].get("published_at") if rows else None
    except Exception as e:
        out["error"] = str(e)
    return out


def _count_desk_intel() -> int:
    try:
        conn = sqlite3.connect(DATABASE_PATH, timeout=5.0)
        try:
            row = conn.execute("SELECT COUNT(*) AS n FROM desk_terminal_intel").fetchone()
            return int(row[0] if row else 0)
        except sqlite3.OperationalError:
            return 0
        finally:
            conn.close()
    except sqlite3.Error:
        return 0


def _secrets_status() -> dict[str, Any]:
    sk = (SECRET_KEY or "").strip()
    desk_pw = (ANALYST_DESK_ADMIN_PASSWORD or "").strip()
    openai = bool((os.environ.get("OPENAI_API_KEY") or "").strip())
    return {
        "secret_key_set": bool(sk) and sk not in _DEV_SECRETS,
        "using_dev_secret": sk in _DEV_SECRETS or not sk,
        "desk_password_set": bool(desk_pw),
        "publish_on_approve": analyst_desk_publish_on_approve(),
        "openai_configured": openai,
        "production_runtime": is_production_runtime(),
    }


def build_health_report() -> dict[str, Any]:
    """Full health document used by API + Ops UI."""
    from app.models import get_articles_total_count, get_last_scrape_time

    ops = read_ops_status()
    news_db = _file_db_stats(DATABASE_PATH)
    desk_db = _file_db_stats(ANALYST_DESK_DATABASE_PATH)
    last_scrape = get_last_scrape_time()
    article_count = 0
    try:
        article_count = get_articles_total_count() if news_db["ok"] else 0
    except Exception:
        article_count = 0

    scrape_hours = _hours_since(last_scrape)
    refresh_hours = _hours_since(ops.get("last_refresh_finished_at"))
    desk = _desk_counts()
    intel = _terminal_intel_stats()
    secrets = _secrets_status()
    worker_hb = ops.get("desk_worker_last_heartbeat_at")
    worker_hours = _hours_since(worker_hb)
    interval = ops.get("desk_worker_interval_seconds") or 300
    try:
        interval_f = float(interval)
    except (TypeError, ValueError):
        interval_f = 300.0
    # Stale if no heartbeat within 3x configured interval (min 15 min grace for first boot)
    stale_after_h = max(0.25, (interval_f * 3) / 3600.0)
    worker_fresh = worker_hours is not None and worker_hours <= stale_after_h

    checks: list[dict[str, Any]] = []

    def add(name: str, ok: bool, detail: str, severity: str = "error") -> None:
        checks.append({"name": name, "ok": ok, "detail": detail, "severity": severity if not ok else "ok"})

    add("news_db", bool(news_db["ok"]), news_db.get("error") or f"{news_db['size_bytes']} bytes")
    add(
        "articles_present",
        article_count > 0,
        f"{article_count} articles",
        severity="warn",
    )
    add(
        "scrape_freshness",
        scrape_hours is not None and scrape_hours < 24,
        f"last article scrape {last_scrape or 'never'}; hours_ago={scrape_hours}",
        severity="warn",
    )
    refresh_ok = (ops.get("last_refresh_status") == "ok") or not ops
    add(
        "last_refresh",
        refresh_ok and (refresh_hours is None or refresh_hours < 24),
        ops.get("last_refresh_error")
        or f"status={ops.get('last_refresh_status') or 'unknown'}; finished={ops.get('last_refresh_finished_at')}",
        severity="warn",
    )
    add(
        "desk_db",
        bool(desk_db["ok"]) or not Path(ANALYST_DESK_DATABASE_PATH).exists(),
        desk_db.get("error") or ("optional / not created yet" if not desk_db["exists"] else "ok"),
        severity="warn",
    )
    if secrets["production_runtime"]:
        add("secret_key", not secrets["using_dev_secret"], "set SECRET_KEY in production")
        add(
            "desk_password",
            secrets["desk_password_set"],
            "set ANALYST_DESK_ADMIN_PASSWORD in production",
            severity="warn",
        )
    add(
        "desk_worker",
        worker_fresh or worker_hb is None,
        (
            f"heartbeat={worker_hb or 'never'}; status={ops.get('desk_worker_last_status') or '—'}; "
            f"hours_ago={worker_hours}"
        ),
        severity="warn",
    )

    hard_fail = any(c["severity"] == "error" and not c["ok"] for c in checks)
    warn = any(c["severity"] == "warn" and not c["ok"] for c in checks)
    status = "error" if hard_fail else ("degraded" if warn else "ok")

    return {
        "status": status,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "checks": checks,
        "news": {
            "database": news_db,
            "article_count": article_count,
            "last_scrape_time": last_scrape,
            "hours_since_scrape": scrape_hours,
        },
        "refresh": {
            "ops": ops,
            "hours_since_refresh": refresh_hours,
        },
        "desk": {
            "database": desk_db,
            **desk,
            "publish_on_approve": analyst_desk_publish_on_approve(),
        },
        "desk_worker": {
            "last_heartbeat_at": worker_hb,
            "hours_since_heartbeat": worker_hours,
            "last_status": ops.get("desk_worker_last_status"),
            "interval_seconds": ops.get("desk_worker_interval_seconds"),
            "last_error": ops.get("desk_worker_last_error"),
            "last_stats": ops.get("desk_worker_last_stats"),
            "fresh": worker_fresh,
        },
        "terminal_intel": intel,
        "secrets": secrets,
        "sources": _sources_block(),
        "indicators": _indicators_block(),
        "scheduled_briefs": {
            "last_at": ops.get("last_scheduled_briefs_at"),
            "last_count": ops.get("last_scheduled_briefs_count"),
            "last_errors": ops.get("last_scheduled_briefs_errors"),
            "last_agents": ops.get("last_scheduled_briefs_agents"),
        },
        "database_url": _database_url_block(),
        "audit": _audit_block(),
        "metrics": _metrics_block(),
        "celery_preferred": _celery_preferred(),
        "cutover": _cutover_block(),
        "feature_flags": _feature_flags_block(),
    }


def _cutover_block() -> dict[str, Any]:
    try:
        from app.sql_dialect import cutover_status

        return cutover_status()
    except Exception as e:
        return {"error": str(e)}


def _feature_flags_block() -> dict[str, Any]:
    try:
        from app.feature_flags import all_flags

        return all_flags()
    except Exception as e:
        return {"error": str(e)}


def _database_url_block() -> dict[str, Any]:
    try:
        from app.db import database_url, dialect_name, probe_database

        url = database_url()
        probe = probe_database(url)
        return {
            "backend": dialect_name(url),
            "ok": bool(probe.get("ok")),
            "host": probe.get("host"),
            "database": probe.get("database"),
            "path": probe.get("path"),
            "error": probe.get("error"),
        }
    except Exception as e:
        return {"backend": "unknown", "ok": False, "error": str(e)}


def _audit_block() -> dict[str, Any]:
    try:
        from app.audit import recent_audit

        rows = recent_audit(limit=5)
        return {"ok": True, "recent_count": len(rows), "latest": rows[0] if rows else None}
    except Exception as e:
        return {"ok": False, "error": str(e), "recent_count": 0}


def _metrics_block() -> dict[str, Any]:
    try:
        from app.sentry_init import get_metrics_snapshot

        return {"ok": True, "counters": get_metrics_snapshot()}
    except Exception as e:
        return {"ok": False, "error": str(e), "counters": {}}


def _celery_preferred() -> bool:
    try:
        from config import analyst_desk_use_celery

        return bool(analyst_desk_use_celery())
    except Exception:
        return False


def _sources_block() -> dict[str, Any]:
    try:
        from app.scrapers.sources import sources_summary

        return sources_summary()
    except Exception as e:
        return {"count": 0, "by_tier": {}, "error": str(e)}


def _indicators_block() -> dict[str, Any]:
    try:
        from app.indicator_coverage import build_indicator_coverage

        return build_indicator_coverage()
    except Exception as e:
        return {"ok": False, "error": str(e)}
