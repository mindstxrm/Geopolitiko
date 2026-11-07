"""Multi-region scheduled daily briefs for the Desk worker (P3)."""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from app.platform_ops import read_ops_status, write_ops_status

logger = logging.getLogger(__name__)

# Default desks spanning the SOP families (one agent per major theater).
_DEFAULT_SCHEDULED_AGENTS = [
    "southeast_asia_i",
    "east_asia_i",
    "east_asia_ii",
    "oceania_i",
    "south_asia_i",
    "central_asia_i",
    "gulf_i",
    "levant_i",
    "eastern_europe_i",
    "southern_europe_i",
    "nordics_i",
]


def scheduled_briefs_enabled() -> bool:
    v = (os.environ.get("ANALYST_DESK_SCHEDULED_BRIEFS") or "1").strip().lower()
    return v in ("1", "true", "yes", "on")


def scheduled_brief_interval_hours() -> int:
    raw = (os.environ.get("ANALYST_DESK_SCHEDULED_BRIEF_HOURS") or "24").strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return 24


def scheduled_agent_ids() -> list[str]:
    raw = (os.environ.get("ANALYST_DESK_SCHEDULED_AGENTS") or "").strip()
    if not raw:
        return list(_DEFAULT_SCHEDULED_AGENTS)
    return [a.strip() for a in raw.split(",") if a.strip()]


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


def should_run_scheduled_briefs(ops: dict[str, Any] | None = None) -> bool:
    if not scheduled_briefs_enabled():
        return False
    ops = ops if ops is not None else read_ops_status()
    hours = _hours_since(ops.get("last_scheduled_briefs_at"))
    if hours is None:
        return True
    return hours >= float(scheduled_brief_interval_hours())


def run_scheduled_regional_briefs(*, force: bool = False) -> dict[str, Any]:
    """
    Generate daily_volume proposals for configured regional agents.
    Returns summary stats; safe to call from the worker.
    """
    if not force and not should_run_scheduled_briefs():
        return {"skipped": True, "reason": "interval_not_elapsed"}

    from app.analyst_desk.agents import AGENTS
    from app.analyst_desk.generate import run_daily_volume

    agents = [a for a in scheduled_agent_ids() if a in AGENTS]
    created: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for aid in agents:
        try:
            pid = run_daily_volume(aid, days=7)
            if pid:
                created.append({"agent_id": aid, "proposal_id": pid})
            else:
                errors.append({"agent_id": aid, "error": "no_proposal"})
        except Exception as e:
            logger.exception("scheduled brief failed for %s", aid)
            errors.append({"agent_id": aid, "error": str(e)[:300]})

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    write_ops_status(
        {
            "last_scheduled_briefs_at": now,
            "last_scheduled_briefs_count": len(created),
            "last_scheduled_briefs_errors": len(errors),
            "last_scheduled_briefs_agents": [c["agent_id"] for c in created],
        }
    )
    return {
        "skipped": False,
        "created": created,
        "errors": errors,
        "ran_at": now,
        "agent_count": len(agents),
    }
