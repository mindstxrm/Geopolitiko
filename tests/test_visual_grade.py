"""Visual Grade-1: status ticker + ops wall chrome smoke."""
from __future__ import annotations


def test_status_ticker_builder(tmp_env, monkeypatch):
    import config
    from app.models import init_db
    from app.platform_ops import write_ops_status
    from app.status_ticker import build_status_ticker

    monkeypatch.setattr(config, "DATABASE_PATH", str(tmp_env["news_db"]))
    monkeypatch.setattr(config, "ANALYST_DESK_DATABASE_PATH", str(tmp_env["desk_db"]))
    monkeypatch.setenv("PLATFORM_OPS_STATUS_PATH", str(tmp_env["ops"]))
    init_db(str(tmp_env["news_db"]))
    write_ops_status({"last_refresh_finished_at": "2000-01-01T00:00:00Z"})

    ticker = build_status_ticker()
    assert ticker["status"] in ("ok", "warn", "crit")
    assert ticker["status_label"]
    assert "news_as_of" in ticker
    assert "high_risk_count" in ticker
    assert "pending_proposals" in ticker


def test_home_and_ops_render_visual_classes(app_client):
    home = app_client.get("/")
    assert home.status_code == 200
    html = home.get_data(as_text=True)
    assert "status-ticker" in html
    assert "metrics-rail" in html or "status-pill" in html
    assert "status-pill" in html
    assert "situation-top-strip" in html
    assert "mode-strip" in html
    assert "fkey-row" in html

    # Ops is admin-only — anonymous should redirect to login
    ops_anon = app_client.get("/ops", follow_redirects=False)
    assert ops_anon.status_code in (302, 303)


def test_terminal_css_has_risk_tokens():
    from pathlib import Path

    css = Path("app/static/css/terminal.css").read_text(encoding="utf-8")
    assert "--t-risk-1" in css
    assert "--t-status-ok" in css
    assert ".status-ticker" in css
    assert ".ops-wall" in css
