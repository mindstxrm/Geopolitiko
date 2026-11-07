"""Soft mode homes + terminal homage smoke tests."""
from __future__ import annotations


def test_resolve_ui_mode_defaults_and_query(app_client):
    from app.ui_mode import COOKIE_NAME, normalize_mode, resolve_ui_mode

    assert normalize_mode("MONITOR") == "monitor"
    assert normalize_mode("nope") is None

    # Default landing → monitor
    home = app_client.get("/")
    assert home.status_code == 200
    html = home.get_data(as_text=True)
    assert 'data-ui-mode="monitor"' in html
    assert 'class="mode-strip"' in html or "mode-strip" in html
    assert "fkey-row" in html
    assert "F2" in html and "F4" in html

    # Query mode sets cookie
    brief = app_client.get("/brief?mode=brief")
    assert brief.status_code == 200
    assert 'data-ui-mode="brief"' in brief.get_data(as_text=True)
    cookie = brief.headers.getlist("Set-Cookie")
    assert any(COOKIE_NAME in c and "brief" in c for c in cookie)

    # Cookie persists on next request without query
    again = app_client.get("/help")
    assert again.status_code == 200
    assert 'data-ui-mode="brief"' in again.get_data(as_text=True)


def test_brief_home_and_feed_mode(app_client):
    brief = app_client.get("/brief")
    assert brief.status_code == 200
    html = brief.get_data(as_text=True)
    # Customer Brief hub — no Desk CTAs for anonymous/viewer
    assert "Recently published" in html
    assert "Reports hub" in html or "Export" in html
    assert "Open Desk Approvals" not in html
    assert "metrics-rail" in html or "status-ticker" in html

    feed = app_client.get("/feed?mode=investigate")
    assert feed.status_code == 200
    feed_html = feed.get_data(as_text=True)
    assert 'data-ui-mode="investigate"' in feed_html
    assert 'data-mode="investigate"' in feed_html


def test_help_documents_fkeys(app_client):
    help_page = app_client.get("/help")
    assert help_page.status_code == 200
    html = help_page.get_data(as_text=True)
    assert "F1" in html and "F6" in html
    assert "Monitor" in html and "Brief" in html


def test_ui_mode_module_endpoint_map():
    from app.ui_mode import mode_from_endpoint, mode_landing_endpoint

    assert mode_from_endpoint("main.situation_room") == "monitor"
    assert mode_from_endpoint("main.index") == "investigate"
    assert mode_from_endpoint("main.brief_home") == "brief"
    assert mode_landing_endpoint("investigate") == "main.index"
    assert mode_landing_endpoint("brief") == "main.brief_home"


def test_css_has_mode_and_fkey_rules():
    from pathlib import Path

    css = Path("app/static/css/terminal.css").read_text(encoding="utf-8")
    assert ".mode-strip" in css
    assert ".fkey-row" in css
    assert 'data-ui-mode="investigate"' in css or "[data-ui-mode=\"investigate\"]" in css
    assert ".metrics-rail" in css
