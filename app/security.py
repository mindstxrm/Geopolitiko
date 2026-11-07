"""CSRF protection and HTTP security headers."""
from __future__ import annotations

import logging
import secrets
from typing import Iterable

from flask import Flask, g, request, session

logger = logging.getLogger(__name__)

# Paths that skip CSRF (health, SSO bridge consume, API key clients).
_CSRF_EXEMPT_PREFIXES = (
    "/api/",
    "/static/",
)
_CSRF_EXEMPT_ENDPOINTS = {
    "main.health",  # if any
    "analyst_desk.health",
    "analyst_desk.auth_bridge",
}


def _csrf_disabled(app: Flask) -> bool:
    import os

    if app.config.get("TESTING"):
        return True
    if app.config.get("WTF_CSRF_ENABLED") is False:
        return True
    return (os.environ.get("GEOPOLITIKO_DISABLE_CSRF") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def install_security(app: Flask, *, service: str = "terminal") -> None:
    """Attach CSRF checks + security headers."""

    @app.before_request
    def _csrf_prepare():
        if "_csrf_token" not in session:
            session["_csrf_token"] = secrets.token_urlsafe(32)
        g.csrf_token = session["_csrf_token"]

    @app.before_request
    def _csrf_validate():
        if _csrf_disabled(app):
            return
        if request.method in ("GET", "HEAD", "OPTIONS", "TRACE"):
            return
        if _is_csrf_exempt(app):
            return
        # API key auth bypasses form CSRF
        if getattr(g, "api_user", None):
            return
        sent = (
            request.form.get("csrf_token")
            or request.headers.get("X-CSRF-Token")
            or request.headers.get("X-CSRFToken")
            or ""
        ).strip()
        expected = (session.get("_csrf_token") or "").strip()
        if not expected or not sent or not secrets.compare_digest(sent, expected):
            logger.warning(
                "csrf_reject service=%s path=%s method=%s",
                service,
                request.path,
                request.method,
            )
            from flask import abort

            abort(400, description="CSRF validation failed")

    @app.after_request
    def _security_headers(response):
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
        response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        response.headers.setdefault(
            "Permissions-Policy",
            "geolocation=(), microphone=(), camera=()",
        )
        # Conservative CSP — allow CDNs used by Terminal
        if "Content-Security-Policy" not in response.headers:
            response.headers["Content-Security-Policy"] = (
                "default-src 'self'; "
                "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://cdn.plot.ly; "
                "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
                "font-src 'self' https://fonts.gstatic.com data:; "
                "img-src 'self' data: https:; "
                "connect-src 'self' https:; "
                "frame-ancestors 'self'"
            )
        if request.is_secure:
            response.headers.setdefault(
                "Strict-Transport-Security", "max-age=31536000; includeSubDomains"
            )
        # Auto-inject CSRF hidden field into HTML forms that lack one
        try:
            ctype = (response.content_type or "").lower()
            if "text/html" in ctype and not _csrf_disabled(app):
                token = (session.get("_csrf_token") or "").strip()
                if token and response.get_data(as_text=True):
                    html = response.get_data(as_text=True)
                    if 'name="csrf_token"' not in html and "<form" in html.lower():
                        inject = (
                            f'<input type="hidden" name="csrf_token" value="{token}">'
                        )
                        import re

                        html2, n = re.subn(
                            r"(<form\b[^>]*>)",
                            r"\1" + inject,
                            html,
                            flags=re.IGNORECASE,
                        )
                        if n:
                            response.set_data(html2)
        except Exception:
            logger.debug("csrf auto-inject skipped", exc_info=True)
        return response

    @app.context_processor
    def _csrf_context():
        token = session.get("_csrf_token") or ""
        return {"csrf_token": token, "csrf_token_value": token}


def _is_csrf_exempt(app: Flask) -> bool:
    if request.endpoint in _CSRF_EXEMPT_ENDPOINTS:
        return True
    path = request.path or ""
    for prefix in _CSRF_EXEMPT_PREFIXES:
        if path.startswith(prefix):
            return True
    # Explicit exempt list via config
    extra: Iterable[str] = app.config.get("CSRF_EXEMPT_ENDPOINTS") or ()
    if request.endpoint in set(extra):
        return True
    return False
