"""
Role-based access control for Terminal + Desk.

Roles (ascending privilege):
  viewer   — read-only
  analyst  — draft / generate
  approver — approve/reject/publish
  admin    — ops, user role changes, everything

Env bootstrap:
  GEOPOLITIKO_DEFAULT_USER_ROLE — role assigned to new users (default viewer)
  GEOPOLITIKO_ADMIN_USERNAMES — comma-separated usernames always treated as admin
"""
from __future__ import annotations

import os
from functools import wraps
from typing import Callable, Iterable

from flask import abort, flash, g, redirect, request, session, url_for
from flask_login import current_user

ROLES = ("viewer", "analyst", "approver", "admin")
_ROLE_RANK = {r: i for i, r in enumerate(ROLES)}


def normalize_role(role: str | None) -> str:
    r = (role or "").strip().lower()
    if r in _ROLE_RANK:
        return r
    return default_user_role()


def default_user_role() -> str:
    r = (os.environ.get("GEOPOLITIKO_DEFAULT_USER_ROLE") or "viewer").strip().lower()
    return r if r in _ROLE_RANK else "viewer"


def admin_usernames() -> set[str]:
    raw = (os.environ.get("GEOPOLITIKO_ADMIN_USERNAMES") or "").strip()
    if not raw:
        return set()
    return {p.strip().lower() for p in raw.split(",") if p.strip()}


def role_at_least(have: str | None, need: str) -> bool:
    h = normalize_role(have)
    n = normalize_role(need)
    return _ROLE_RANK[h] >= _ROLE_RANK[n]


def resolve_user_role(user: dict | None = None, username: str | None = None) -> str:
    """Effective role for a DB user dict or username."""
    uname = (username or (user or {}).get("username") or "").strip().lower()
    if uname and uname in admin_usernames():
        return "admin"
    if user and user.get("role"):
        return normalize_role(str(user.get("role")))
    return default_user_role()


def current_role() -> str:
    """Role for the active Terminal session, API key user, or Desk session."""
    # Desk SSO / password session
    desk_user = session.get("analyst_desk_user")
    if isinstance(desk_user, dict) and desk_user.get("role"):
        return normalize_role(str(desk_user.get("role")))
    if session.get("analyst_desk_role"):
        return normalize_role(str(session.get("analyst_desk_role")))

    api_user = getattr(g, "api_user", None)
    if api_user:
        return resolve_user_role(api_user)

    try:
        if current_user.is_authenticated:
            row = None
            try:
                from app.models import get_user_by_id

                row = get_user_by_id(int(current_user.id))
            except Exception:
                row = None
            uname = getattr(current_user, "username", None)
            if row:
                return resolve_user_role(row, uname)
            return resolve_user_role({"username": uname, "role": getattr(current_user, "role", None)}, uname)
    except Exception:
        pass

    # Open desk (no password) → admin for local dev convenience
    if not (os.environ.get("ANALYST_DESK_ADMIN_PASSWORD") or "").strip():
        if request.blueprint == "analyst_desk":
            return "admin"

    return "viewer"


def require_role(minimum: str, *, api: bool = False) -> Callable:
    """Decorator: require role >= minimum. Redirects HTML; 403 for API."""

    need = normalize_role(minimum)

    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapped(*args, **kwargs):
            if role_at_least(current_role(), need):
                return fn(*args, **kwargs)
            if api or request.path.startswith("/api/"):
                abort(403)
            flash(f"Insufficient role (need {need}+).", "error")
            if request.blueprint == "analyst_desk":
                return redirect(url_for("analyst_desk.index"))
            return redirect(url_for("main.situation_room"))

        return wrapped

    return decorator


def roles_meeting(minimum: str) -> Iterable[str]:
    need = _ROLE_RANK[normalize_role(minimum)]
    return [r for r, i in _ROLE_RANK.items() if i >= need]
