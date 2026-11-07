"""Geopolitical News aggregator application."""
from flask import Flask, g, request

from app.auth import User
from app.models import get_user_by_api_key, init_db

# Legacy boilerplate to strip from stored analysis
_BOILERPLATE_PHRASES = [
    " It may have implications for regional stability and policy.",
    "It may have implications for regional stability and policy.",
]


def create_app():
    from config import DATABASE_PATH, SECRET_KEY, assert_production_secrets, load_app_dotenv

    load_app_dotenv()
    assert_production_secrets()

    app = Flask(__name__)
    app.config["DATABASE_PATH"] = DATABASE_PATH
    app.config["SECRET_KEY"] = SECRET_KEY

    from flask_login import LoginManager
    login_manager = LoginManager(app)
    login_manager.login_view = "main.login"
    login_manager.login_message = "Please log in to continue."
    login_manager.login_message_category = "info"

    @login_manager.user_loader
    def load_user(user_id):
        try:
            uid = int(user_id)
        except (TypeError, ValueError):
            return None
        return User.from_id(uid)

    @app.before_request
    def init_database():
        init_db(app.config["DATABASE_PATH"])

    @app.before_request
    def api_key_auth():
        """For /api/* requests, set g.api_user if X-API-Key or Authorization: Bearer is valid."""
        if not request.path.startswith("/api/"):
            return
        key = request.headers.get("X-API-Key")
        if not key and request.headers.get("Authorization", "").startswith("Bearer "):
            key = request.headers.get("Authorization")[7:].strip()
        if not key:
            return
        user = get_user_by_api_key(key)
        if user:
            g.api_user = user

    from app.messaging import simple_markdown
    app.jinja_env.filters["message_markdown"] = simple_markdown

    @app.template_filter("strip_boilerplate")
    def strip_boilerplate(text):
        """Remove legacy 'regional stability' boilerplate from why_it_matters."""
        if not text or not isinstance(text, str):
            return text
        out = text
        for phrase in _BOILERPLATE_PHRASES:
            out = out.replace(phrase, "")
        return out.strip()

    @app.template_filter("why_matters_only")
    def why_matters_only(text):
        """Show only the topic line from why_it_matters; drop any repeated summary sentence."""
        if not text or not isinstance(text, str):
            return ""
        out = text
        for phrase in _BOILERPLATE_PHRASES:
            out = out.replace(phrase, "")
        out = out.strip()
        if not out:
            return ""
        # If it contains the topic line, show only that first sentence to avoid duplicating key takeaways
        if "This story relates to:" in out:
            idx = out.find("This story relates to:")
            end = out.find(".", idx)
            if end != -1:
                return out[idx : end + 1].strip()
            return out[idx:].strip()
        return out

    from app.observability import install_request_observability
    from app.security import install_security
    from app.sentry_init import init_sentry

    install_request_observability(app, service="terminal")
    install_security(app, service="terminal")
    init_sentry(service="terminal")

    from app.routes import bp
    from app.api_routes import api
    app.register_blueprint(bp)
    app.register_blueprint(api)

    @app.context_processor
    def inject_authz_context():
        try:
            from app.authz import current_role

            return {"current_role": current_role()}
        except Exception:
            return {"current_role": "viewer"}

    @app.context_processor
    def inject_terminal_policy_context():
        """As-of time + feed freshness for policy / think-tank UX (all templates)."""
        from datetime import datetime, timezone

        from flask import request as flask_request
        from flask import url_for as flask_url_for

        from app.models import get_last_scrape_time
        from app.ui_mode import mode_landing_endpoint, resolve_ui_mode

        now = datetime.now(timezone.utc)
        freshness = {}
        ticker = {}
        try:
            from app.freshness import build_freshness_snapshot

            freshness = build_freshness_snapshot()
        except Exception:
            freshness = {"any_stale": False, "banner": None}
        try:
            from app.status_ticker import build_status_ticker

            ticker = build_status_ticker()
        except Exception:
            ticker = {
                "status": "warn",
                "status_label": "—",
                "news_as_of": freshness.get("news_as_of"),
                "news_minutes_ago": freshness.get("news_minutes_ago"),
                "high_risk_count": None,
                "pending_proposals": None,
                "articles_24h": None,
            }
        ep = flask_request.endpoint if flask_request else None
        ui_mode = resolve_ui_mode(ep)
        try:
            mode_home_url = flask_url_for(mode_landing_endpoint(ui_mode))
        except Exception:
            mode_home_url = "/"
        return {
            "policy_now_utc": now.strftime("%Y-%m-%d %H:%M UTC"),
            "policy_now_iso": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "last_scrape_time": get_last_scrape_time(),
            "data_freshness": freshness,
            "status_ticker": ticker,
            "ui_mode": ui_mode,
            "mode_home_url": mode_home_url,
        }

    @app.after_request
    def persist_ui_mode_cookie(response):
        """Write gpk_mode when ?mode= is present on a Terminal navigation."""
        try:
            from flask import request as flask_request

            from app.ui_mode import COOKIE_NAME, normalize_mode

            mode = normalize_mode(flask_request.args.get("mode"))
            if mode:
                response.set_cookie(
                    COOKIE_NAME,
                    mode,
                    max_age=60 * 60 * 24 * 365,
                    samesite="Lax",
                    path="/",
                )
        except Exception:
            pass
        return response

    # Pre-warm world GeoJSON cache in background so Situation Room map loads fast
    def _prewarm_geojson():
        try:
            from app.api_routes import _fetch_world_geojson
            _fetch_world_geojson()
        except Exception:
            pass

    import threading
    threading.Thread(target=_prewarm_geojson, daemon=True).start()

    return app
