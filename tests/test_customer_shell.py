"""Customer Terminal shell: admin-only Desk/Ops, /app home, Brief hub."""
from __future__ import annotations


def _make_user(tmp_env, monkeypatch, *, username: str, role: str = "viewer"):
    from app.models import create_user, init_db
    import config

    monkeypatch.setattr(config, "DATABASE_PATH", str(tmp_env["news_db"]))
    init_db(str(tmp_env["news_db"]))
    create_user(
        username,
        "password123",
        email=f"{username}@example.com",
        name=username.title(),
        title="Analyst",
        organization="Acme Desk",
        role=role,
    )


def _login(client, username: str, password: str = "password123"):
    return client.post(
        "/login",
        data={"username": username, "password": password},
        follow_redirects=False,
    )


def test_ops_and_desk_require_admin(app_client, tmp_env, monkeypatch):
    _make_user(tmp_env, monkeypatch, username="viewer1", role="viewer")
    # Anonymous → login
    r = app_client.get("/ops", follow_redirects=False)
    assert r.status_code in (302, 303)
    assert "/login" in (r.headers.get("Location") or "")

    r = app_client.get("/desk", follow_redirects=False)
    assert r.status_code in (302, 303)
    assert "/login" in (r.headers.get("Location") or "")

    _login(app_client, "viewer1")
    r = app_client.get("/ops", follow_redirects=False)
    assert r.status_code in (302, 303)
    loc = r.headers.get("Location") or ""
    assert "/ops" not in loc or loc.endswith("/app") or "/app" in loc

    r = app_client.get("/desk", follow_redirects=False)
    assert r.status_code in (302, 303)
    assert "/app" in (r.headers.get("Location") or "")


def test_admin_can_open_ops(app_client, tmp_env, monkeypatch):
    monkeypatch.setenv("GEOPOLITIKO_ADMIN_USERNAMES", "boss")
    _make_user(tmp_env, monkeypatch, username="boss", role="viewer")
    _login(app_client, "boss")
    r = app_client.get("/ops")
    assert r.status_code == 200
    html = r.get_data(as_text=True)
    assert "Ops health" in html
    assert "Analyst Desk" in html or "ops-wall" in html


def test_viewer_nav_hides_desk_and_ops(app_client, tmp_env, monkeypatch):
    _make_user(tmp_env, monkeypatch, username="cust", role="viewer")
    _login(app_client, "cust")
    home = app_client.get("/app")
    assert home.status_code == 200
    html = home.get_data(as_text=True)
    assert "App home" in html or "Welcome" in html
    assert "Analyst Desk" not in html
    assert ">Ops health<" not in html and "Ops health</a>" not in html
    assert "Getting started" in html or "app-onboarding" in html
    assert "Acme Desk" in html


def test_customer_brief_hides_desk_cta(app_client, tmp_env, monkeypatch):
    _make_user(tmp_env, monkeypatch, username="cust2", role="viewer")
    _login(app_client, "cust2")
    brief = app_client.get("/brief")
    assert brief.status_code == 200
    html = brief.get_data(as_text=True)
    assert "Open Desk Approvals" not in html
    assert "Reports hub" in html
    assert "Export briefing" in html or "Build briefing" in html


def test_admin_brief_shows_admin_panel(app_client, tmp_env, monkeypatch):
    monkeypatch.setenv("GEOPOLITIKO_ADMIN_USERNAMES", "admin1")
    _make_user(tmp_env, monkeypatch, username="admin1", role="admin")
    _login(app_client, "admin1")
    brief = app_client.get("/brief")
    assert brief.status_code == 200
    html = brief.get_data(as_text=True)
    assert "Open Desk Approvals" in html
    assert "Desk queue" in html


def test_desk_blocks_non_admin_session(tmp_env, monkeypatch):
    monkeypatch.setenv("ANALYST_DESK_ADMIN_PASSWORD", "desk-pass")
    monkeypatch.setenv("SECRET_KEY", "cust-shell-secret")
    from app.models import init_db
    from app.analyst_desk.app import create_analyst_desk_app
    from app.analyst_desk.store import init_analyst_desk_db, sync_agents_registry
    from app.analyst_desk.agents import AGENTS
    import config

    monkeypatch.setattr(config, "DATABASE_PATH", str(tmp_env["news_db"]))
    monkeypatch.setattr(config, "ANALYST_DESK_DATABASE_PATH", str(tmp_env["desk_db"]))
    monkeypatch.setattr(config, "ANALYST_DESK_ADMIN_PASSWORD", "desk-pass")
    init_db(str(tmp_env["news_db"]))
    init_analyst_desk_db(str(tmp_env["desk_db"]))
    sync_agents_registry(AGENTS)
    app = create_analyst_desk_app()
    app.config["TESTING"] = True
    app.config["ANALYST_DESK_ADMIN_PASSWORD"] = "desk-pass"
    c = app.test_client()
    with c.session_transaction() as sess:
        sess["analyst_desk_ok"] = True
        sess["analyst_desk_user"] = {"username": "viewer", "role": "viewer"}
        sess["analyst_desk_role"] = "viewer"
    r = c.get("/", follow_redirects=False)
    assert r.status_code in (302, 303)
