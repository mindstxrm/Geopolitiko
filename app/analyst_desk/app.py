"""Standalone Flask app for Analyst Desk (separate process/URL from main Geopolitiko)."""
from __future__ import annotations

from pathlib import Path

from flask import Flask, redirect, request, session, url_for

# Repo root (contains templates/analyst_desk/); app lives at app/analyst_desk/app.py
_ANALYST_DESK_ROOT = Path(__file__).resolve().parents[2]

from config import (
    ANALYST_DESK_ADMIN_PASSWORD,
    ANALYST_DESK_DATABASE_PATH,
    ANALYST_DESK_SECRET_KEY,
    DATABASE_PATH,
    analyst_desk_heuristic_only,
    load_app_dotenv,
)
from app.analyst_desk.agents import AGENTS
from app.analyst_desk.store import init_analyst_desk_db, sync_agents_registry
from app.models import init_db


def create_analyst_desk_app() -> Flask:
    load_app_dotenv()
    # News DB global path for read-only article access
    init_db(DATABASE_PATH)
    init_analyst_desk_db(ANALYST_DESK_DATABASE_PATH)
    sync_agents_registry(AGENTS)

    app = Flask(__name__, template_folder=str(_ANALYST_DESK_ROOT / "templates"))

    app.config["SECRET_KEY"] = ANALYST_DESK_SECRET_KEY
    app.config["ANALYST_DESK_ADMIN_PASSWORD"] = ANALYST_DESK_ADMIN_PASSWORD or ""
    app.config["SESSION_COOKIE_NAME"] = "analyst_desk_session"
    app.config["PERMANENT_SESSION_LIFETIME"] = 86400 * 7

    from app.analyst_desk.routes import bp as analyst_desk_bp

    app.register_blueprint(analyst_desk_bp)

    @app.context_processor
    def _analyst_desk_template_globals():
        return {
            "analyst_desk_auth_required": bool(
                (app.config.get("ANALYST_DESK_ADMIN_PASSWORD") or "").strip()
            ),
            "analyst_desk_heuristic_only": analyst_desk_heuristic_only(),
        }

    def _password_configured() -> bool:
        return bool((app.config.get("ANALYST_DESK_ADMIN_PASSWORD") or "").strip())

    def _is_logged_in() -> bool:
        if not _password_configured():
            return True
        return bool(session.get("analyst_desk_ok"))

    @app.before_request
    def _analyst_desk_auth_gate():
        if request.endpoint in (None, "static"):
            return
        if request.blueprint != "analyst_desk":
            return
        if request.endpoint == "analyst_desk.login":
            return
        if not _is_logged_in():
            return redirect(url_for("analyst_desk.login", next=request.path))

    return app
