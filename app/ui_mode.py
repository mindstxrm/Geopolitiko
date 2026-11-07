"""Soft operator modes: Monitor / Investigate / Brief (no AuthZ).

Persisted via cookie ``gpk_mode``; overridable with ``?mode=``.
"""
from __future__ import annotations

from typing import Optional

from flask import request

COOKIE_NAME = "gpk_mode"
MODES = ("monitor", "investigate", "brief")
DEFAULT_MODE = "monitor"

# Endpoints that imply a mode when cookie is missing / invalid.
_ENDPOINT_MODE = {
    "main.situation_room": "monitor",
    "main.risk_dashboard": "monitor",
    "main.macro_dashboard": "monitor",
    "main.world_monitor": "monitor",
    "main.index": "investigate",
    "main.insights": "investigate",
    "main.alerts_list": "investigate",
    "main.integration_dashboard": "investigate",
    "main.country_dashboard": "investigate",
    "main.relationship_mapper": "investigate",
    "main.conflict_dashboard": "investigate",
    "main.diplomacy_dashboard": "investigate",
    "main.supply_chain_dashboard": "investigate",
    "main.approval_protests": "investigate",
    "main.stability_dashboard": "investigate",
    "main.brief_home": "brief",
    "main.app_home": "brief",
    "main.reports_hub": "brief",
    "main.export_briefing": "brief",
    "main.ops_health_board": "brief",
    "main.launch_analyst_desk": "brief",
}

_MODE_LANDING_ENDPOINT = {
    "monitor": "main.situation_room",
    "investigate": "main.index",
    "brief": "main.brief_home",
}


def normalize_mode(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    v = str(value).strip().lower()
    return v if v in MODES else None


def mode_from_endpoint(endpoint: Optional[str]) -> str:
    if not endpoint:
        return DEFAULT_MODE
    if endpoint in _ENDPOINT_MODE:
        return _ENDPOINT_MODE[endpoint]
    # Prefix matches for diplomacy_* etc.
    if endpoint.startswith("main.diplomacy"):
        return "investigate"
    if endpoint.startswith("main.scenario"):
        return "brief"
    return DEFAULT_MODE


def resolve_ui_mode(endpoint: Optional[str] = None) -> str:
    """Resolve current UI mode: query ?mode= → cookie → endpoint → default."""
    q = normalize_mode(request.args.get("mode") if request else None)
    if q:
        return q
    cookie = normalize_mode(request.cookies.get(COOKIE_NAME) if request else None)
    if cookie:
        return cookie
    return mode_from_endpoint(endpoint or (request.endpoint if request else None))


def mode_landing_endpoint(mode: str) -> str:
    return _MODE_LANDING_ENDPOINT.get(normalize_mode(mode) or DEFAULT_MODE, "main.situation_room")


def mode_should_persist(mode: str, endpoint: Optional[str] = None) -> bool:
    """True when query or switcher sets an explicit mode worth writing to cookie."""
    q = normalize_mode(request.args.get("mode") if request else None)
    return bool(q and q == mode)
