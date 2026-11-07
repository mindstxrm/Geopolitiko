"""Lightweight global status ticker metrics (safe for every request)."""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from config import ANALYST_DESK_DATABASE_PATH, DATABASE_PATH


def _pending_desk() -> int | None:
    path = ANALYST_DESK_DATABASE_PATH
    if not Path(path).is_file():
        return None
    try:
        conn = sqlite3.connect(path, timeout=2.0)
        try:
            return int(
                conn.execute(
                    "SELECT COUNT(*) FROM proposals WHERE status = 'pending'"
                ).fetchone()[0]
            )
        finally:
            conn.close()
    except Exception:
        return None


def _high_risk_count() -> int | None:
    """Count high-risk countries from snapshots or integration fragility."""
    path = DATABASE_PATH
    if not Path(path).is_file():
        return None
    try:
        conn = sqlite3.connect(path, timeout=2.0)
        try:
            # Prefer snapshots.risk_score when populated
            try:
                n = conn.execute(
                    """
                    SELECT COUNT(*) FROM country_risk_snapshots
                    WHERE COALESCE(risk_score, 0) >= 60
                    """
                ).fetchone()[0]
                if int(n) > 0:
                    return int(n)
            except sqlite3.OperationalError:
                pass
            # Fallback: geopolitical fragility on integration table
            return int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM country_risk_integration
                    WHERE COALESCE(geopolitical_fragility_score, 0) >= 60
                    """
                ).fetchone()[0]
            )
        finally:
            conn.close()
    except Exception:
        return None


def _articles_24h() -> int | None:
    path = DATABASE_PATH
    if not Path(path).is_file():
        return None
    try:
        conn = sqlite3.connect(path, timeout=2.0)
        try:
            row = conn.execute(
                """
                SELECT COUNT(*) FROM articles
                WHERE COALESCE(NULLIF(TRIM(published_utc), ''), scraped_at)
                      >= datetime('now', '-1 day')
                """
            ).fetchone()
            return int(row[0]) if row else 0
        finally:
            conn.close()
    except Exception:
        return None


def build_status_ticker() -> dict[str, Any]:
    from app.freshness import build_freshness_snapshot

    freshness = build_freshness_snapshot()
    pending = _pending_desk()
    high_risk = _high_risk_count()
    articles = _articles_24h()
    news_stale = bool(freshness.get("news_stale"))
    status = "crit" if news_stale else "ok"
    if freshness.get("macro_stale") and not news_stale:
        status = "warn"
    return {
        "status": status,
        "status_label": "STALE" if news_stale else ("WARN" if status == "warn" else "OK"),
        "news_as_of": freshness.get("news_as_of"),
        "news_minutes_ago": freshness.get("news_minutes_ago"),
        "high_risk_count": high_risk,
        "pending_proposals": pending,
        "articles_24h": articles,
        "freshness": freshness,
    }
