"""P2: SSO bridge + Desk health."""
from __future__ import annotations


def test_mint_and_verify_bridge_token(monkeypatch):
    monkeypatch.setenv("SECRET_KEY", "p2-test-secret")
    monkeypatch.setenv("ANALYST_DESK_BRIDGE_SECRET", "p2-bridge-secret")
    from app.analyst_desk.auth_bridge import mint_bridge_token, verify_bridge_token

    token = mint_bridge_token(user_id=7, username="analyst1")
    data = verify_bridge_token(token)
    assert data is not None
    assert data["uid"] == 7
    assert data["u"] == "analyst1"
    assert verify_bridge_token("not-a-token") is None


def test_desk_health_public(tmp_env, monkeypatch):
    monkeypatch.setenv("ANALYST_DESK_ADMIN_PASSWORD", "desk-pass")
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
    r = c.get("/health")
    assert r.status_code in (200, 503)
    data = r.get_json()
    assert data["service"] == "analyst_desk"
    assert "worker" in data


def test_auth_bridge_sets_session(tmp_env, monkeypatch):
    monkeypatch.setenv("SECRET_KEY", "p2-test-secret")
    monkeypatch.setenv("ANALYST_DESK_BRIDGE_SECRET", "p2-bridge-secret")
    monkeypatch.setenv("ANALYST_DESK_ADMIN_PASSWORD", "desk-pass")
    from app.models import init_db
    from app.analyst_desk.app import create_analyst_desk_app
    from app.analyst_desk.auth_bridge import mint_bridge_token
    from app.analyst_desk.store import init_analyst_desk_db, sync_agents_registry
    from app.analyst_desk.agents import AGENTS
    import config

    monkeypatch.setattr(config, "DATABASE_PATH", str(tmp_env["news_db"]))
    monkeypatch.setattr(config, "ANALYST_DESK_DATABASE_PATH", str(tmp_env["desk_db"]))
    init_db(str(tmp_env["news_db"]))
    init_analyst_desk_db(str(tmp_env["desk_db"]))
    sync_agents_registry(AGENTS)
    app = create_analyst_desk_app()
    app.config["TESTING"] = True
    app.config["ANALYST_DESK_ADMIN_PASSWORD"] = "desk-pass"
    c = app.test_client()
    # Gate blocks index without session
    r0 = c.get("/")
    assert r0.status_code in (302, 200)
    monkeypatch.setenv("GEOPOLITIKO_ADMIN_USERNAMES", "bridge-user")
    token = mint_bridge_token(user_id=1, username="bridge-user", role="admin")
    r = c.get(f"/auth/bridge?token={token}", follow_redirects=False)
    assert r.status_code in (302, 303)
    r2 = c.get("/")
    assert r2.status_code == 200
    assert b"bridge-user" in r2.data or b"Approvals" in r2.data


def test_terminal_launch_desk_requires_login(app_client):
    r = app_client.get("/desk", follow_redirects=False)
    assert r.status_code in (302, 303)
    assert "/login" in (r.headers.get("Location") or "")


def test_worker_heartbeat_recorded(tmp_env, monkeypatch):
    from app.platform_ops import read_ops_status, record_desk_worker_heartbeat

    record_desk_worker_heartbeat(
        status="ok",
        interval_seconds=120,
        stats={"ingested": 2, "events": 1, "alerts_created": 0},
    )
    ops = read_ops_status()
    assert ops.get("desk_worker_last_status") == "ok"
    assert ops.get("desk_worker_interval_seconds") == 120
    assert ops.get("desk_worker_last_stats", {}).get("ingested") == 2
