"""Simple env-backed feature flags (no external service)."""
from __future__ import annotations

import os
from typing import Any


def _truthy(raw: str | None, default: bool = False) -> bool:
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def flag(name: str, default: bool = False) -> bool:
    """
    Read GEOPOLITIKO_FF_<NAME> (uppercased, dashes→underscores).
    Example: flag("labs_nav") → GEOPOLITIKO_FF_LABS_NAV
    """
    key = "GEOPOLITIKO_FF_" + name.strip().upper().replace("-", "_").replace(".", "_")
    return _truthy(os.environ.get(key), default=default)


def all_flags() -> dict[str, Any]:
    """Known product flags with defaults."""
    return {
        "labs_nav": flag("labs_nav", True),  # Scenario/Workspace under More labs
        "terminal_events_api": flag("terminal_events_api", True),
        "multi_source_gate": flag("multi_source_gate", True),
        "celery_preferred": flag("celery_preferred", False),  # informational; real switch is analyst_desk_use_celery
        "strict_csrf": not _truthy(os.environ.get("GEOPOLITIKO_DISABLE_CSRF"), default=False),
    }
