"""Live-data freshness helpers for health checks and UI banners."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from config import live_macro_stale_minutes, live_news_stale_minutes


def _parse_iso(iso: str | None) -> datetime | None:
    if not iso:
        return None
    try:
        s = iso.strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def minutes_since(iso: str | None) -> float | None:
    dt = _parse_iso(iso)
    if not dt:
        return None
    return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds() / 60.0)


def build_freshness_snapshot() -> dict[str, Any]:
    """
    Compute news/macro freshness for banners + health.
    news_stale if scrape lag > LIVE_NEWS_STALE_MINUTES (default 30).
    macro_stale if last macro ingest > LIVE_MACRO_STALE_MINUTES (default 120).
    """
    from app.models import get_last_scrape_time
    from app.platform_ops import read_ops_status

    ops = read_ops_status()
    last_scrape = get_last_scrape_time()
    # Prefer ops refresh timestamp when present (ingest worker)
    refresh_at = ops.get("last_refresh_finished_at") or ops.get("ingest_worker_last_scrape_at")
    news_as_of = refresh_at or last_scrape
    news_mins = minutes_since(news_as_of)
    news_limit = float(live_news_stale_minutes())
    news_stale = news_mins is None or news_mins > news_limit

    macro_as_of = (
        ops.get("ingest_worker_last_macro_fast_at")
        or ops.get("last_macro_ingest_at")
    )
    macro_mins = minutes_since(macro_as_of)
    macro_limit = float(live_macro_stale_minutes())
    # Only mark macro stale when we have ever ingested (avoid banner on brand-new empty installs)
    macro_stale = bool(macro_as_of) and (macro_mins is None or macro_mins > macro_limit)

    ingest_hb = ops.get("ingest_worker_last_heartbeat_at")
    ingest_mins = minutes_since(ingest_hb)
    ingest_alive = ingest_mins is not None and ingest_mins <= max(news_limit, 15)

    any_stale = news_stale or macro_stale
    return {
        "news_as_of": news_as_of,
        "news_minutes_ago": round(news_mins, 1) if news_mins is not None else None,
        "news_stale_after_minutes": news_limit,
        "news_stale": news_stale,
        "macro_as_of": macro_as_of,
        "macro_minutes_ago": round(macro_mins, 1) if macro_mins is not None else None,
        "macro_stale_after_minutes": macro_limit,
        "macro_stale": macro_stale,
        "ingest_worker_heartbeat_at": ingest_hb,
        "ingest_worker_alive": ingest_alive,
        "any_stale": any_stale,
        "banner": _banner_text(
            news_stale=news_stale,
            macro_stale=macro_stale,
            news_as_of=news_as_of,
            news_mins=news_mins,
            macro_as_of=macro_as_of,
            macro_mins=macro_mins,
            ingest_alive=ingest_alive,
        ),
    }


def _banner_text(
    *,
    news_stale: bool,
    macro_stale: bool,
    news_as_of: str | None,
    news_mins: float | None,
    macro_as_of: str | None,
    macro_mins: float | None,
    ingest_alive: bool,
) -> str | None:
    if not news_stale and not macro_stale:
        return None
    parts = []
    if news_stale:
        if news_as_of and news_mins is not None:
            parts.append(f"News data as of {news_as_of} ({news_mins:.0f}m ago) — STALE")
        else:
            parts.append("News data missing or never scraped — STALE")
    if macro_stale:
        if macro_as_of and macro_mins is not None:
            parts.append(f"Macro/FX as of {macro_as_of} ({macro_mins:.0f}m ago) — STALE")
        else:
            parts.append("Macro/FX ingest stale")
    if not ingest_alive:
        parts.append("Start the ingest worker (`python run_ingest_worker.py` or Compose `ingest`)")
    return " · ".join(parts)
