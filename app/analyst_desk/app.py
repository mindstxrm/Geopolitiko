"""Standalone Flask app for Analyst Desk (separate process/URL from main Geopolitiko)."""
from __future__ import annotations

from pathlib import Path

from flask import Flask, redirect, request, session, url_for

# Repo root (contains templates/analyst_desk/); app lives at app/analyst_desk/app.py
_ANALYST_DESK_ROOT = Path(__file__).resolve().parents[2]

import config as _config
from config import (
    analyst_desk_heuristic_only,
    assert_production_secrets,
    load_app_dotenv,
)
from app.analyst_desk.agents import AGENTS
from app.analyst_desk.store import init_analyst_desk_db, sync_agents_registry
from app.models import init_db
from app.observability import install_request_observability


def create_analyst_desk_app() -> Flask:
    load_app_dotenv()
    assert_production_secrets()
    # Read paths from config module (not import-time bindings) so tests can monkeypatch.
    news_db = _config.DATABASE_PATH
    desk_db = _config.ANALYST_DESK_DATABASE_PATH
    desk_secret = _config.ANALYST_DESK_SECRET_KEY
    desk_password = _config.ANALYST_DESK_ADMIN_PASSWORD
    # News DB global path for read-only article access
    init_db(news_db)
    init_analyst_desk_db(desk_db)
    sync_agents_registry(AGENTS)

    app = Flask(__name__, template_folder=str(_ANALYST_DESK_ROOT / "templates"))

    app.config["SECRET_KEY"] = desk_secret
    app.config["ANALYST_DESK_ADMIN_PASSWORD"] = desk_password or ""
    app.config["SESSION_COOKIE_NAME"] = "analyst_desk_session"
    app.config["PERMANENT_SESSION_LIFETIME"] = 86400 * 7

    from app.analyst_desk.routes import bp as analyst_desk_bp
    from app.security import install_security
    from app.sentry_init import init_sentry

    app.register_blueprint(analyst_desk_bp)
    install_request_observability(app, service="analyst_desk")
    install_security(app, service="analyst_desk")
    init_sentry(service="analyst_desk")

    @app.template_filter("brief_html")
    def _brief_html_filter(text: str) -> str:
        from app.analyst_desk.brief_render import render_brief_html
        from markupsafe import Markup

        return Markup(render_brief_html(text or ""))

    @app.template_filter("brief_preview")
    def _brief_preview_filter(text: str, limit: int = 180) -> str:
        from app.analyst_desk.brief_render import brief_preview_plain

        return brief_preview_plain(text or "", limit=limit)

    @app.context_processor
    def _analyst_desk_template_globals():
        from app.analyst_desk.auth_bridge import terminal_public_url
        from app.analyst_desk.brief_render import brief_toc
        from app.authz import current_role

        return {
            "analyst_desk_auth_required": bool(
                (app.config.get("ANALYST_DESK_ADMIN_PASSWORD") or "").strip()
            ),
            "analyst_desk_heuristic_only": analyst_desk_heuristic_only(),
            "terminal_public_url": terminal_public_url(),
            "desk_user": session.get("analyst_desk_user"),
            "current_role": current_role(),
            "brief_toc": brief_toc,
        }

    def _password_configured() -> bool:
        return bool((app.config.get("ANALYST_DESK_ADMIN_PASSWORD") or "").strip())

    def _is_logged_in() -> bool:
        # Open desk when no desk password is set (dev)
        if not _password_configured():
            return True
        return bool(session.get("analyst_desk_ok"))

    _PUBLIC_ENDPOINTS = {
        "analyst_desk.login",
        "analyst_desk.auth_bridge",
        "analyst_desk.health",
    }

    @app.before_request
    def _analyst_desk_auth_gate():
        if request.endpoint in (None, "static"):
            return
        if request.blueprint != "analyst_desk":
            return
        if request.endpoint in _PUBLIC_ENDPOINTS:
            return
        if not _is_logged_in():
            return redirect(url_for("analyst_desk.login", next=request.path))
        # Admin-only product boundary: customers never use Desk UI.
        from flask import flash

        from app.authz import current_role, role_at_least
        from app.analyst_desk.auth_bridge import terminal_public_url

        if not role_at_least(current_role(), "admin"):
            session.pop("analyst_desk_ok", None)
            session.pop("analyst_desk_user", None)
            session.pop("analyst_desk_role", None)
            flash("Analyst Desk is admin-only.", "error")
            term = (terminal_public_url() or "").rstrip("/") or ""
            if term:
                return redirect(f"{term}/app")
            return redirect(url_for("analyst_desk.login"))

    return app
