"""P4: AuthZ, audit, DB URL helpers, security headers."""
from __future__ import annotations


def test_role_ranking():
    from app.authz import role_at_least, normalize_role

    assert normalize_role("APPROVER") == "approver"
    assert role_at_least("admin", "approver")
    assert role_at_least("approver", "analyst")
    assert not role_at_least("viewer", "approver")


def test_database_url_defaults_to_sqlite(tmp_env, monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    import importlib
    import app.db as db

    importlib.reload(db)
    url = db.database_url()
    assert url.startswith("sqlite:///")
    assert db.is_sqlite(url)
    assert not db.is_postgres(url)


def test_normalize_postgres_url():
    from app.db import _normalize_url, dialect_name

    u = _normalize_url("postgres://u:p@localhost:5432/db")
    assert u.startswith("postgresql+psycopg://")
    assert dialect_name(u) == "postgresql"


def test_audit_and_role_column(tmp_env):
    from app.models import create_user, get_user_by_id, init_db, set_user_role
    from app.audit import record_audit, recent_audit

    init_db(str(tmp_env["news_db"]))
    uid = create_user("alice", "secret-pass", role="analyst")
    row = get_user_by_id(uid)
    assert row["role"] == "analyst"
    assert set_user_role(uid, "approver")
    assert get_user_by_id(uid)["role"] == "approver"

    record_audit(
        "proposal.approve",
        actor="alice",
        actor_role="approver",
        entity_type="proposal",
        entity_id=42,
        detail={"test": True},
        database_path=str(tmp_env["news_db"]),
    )
    rows = recent_audit(limit=5, database_path=str(tmp_env["news_db"]))
    assert rows
    assert rows[0]["action"] == "proposal.approve"
    assert rows[0]["entity_id"] == "42"


def test_security_headers_on_health(app_client):
    r = app_client.get("/api/health")
    assert r.status_code in (200, 503)
    assert r.headers.get("X-Content-Type-Options") == "nosniff"
    assert r.headers.get("X-Frame-Options") == "SAMEORIGIN"
    assert "Content-Security-Policy" in r.headers


def test_csrf_rejects_post_without_token(tmp_env, monkeypatch):
    monkeypatch.delenv("GEOPOLITIKO_DISABLE_CSRF", raising=False)
    from app.models import init_db
    from app import create_app

    init_db(str(tmp_env["news_db"]))
    application = create_app()
    application.config["TESTING"] = False  # enable CSRF
    client = application.test_client()
    # login POST without token should 400
    r = client.post("/login", data={"username": "x", "password": "y"}, follow_redirects=False)
    assert r.status_code == 400


def test_health_includes_p4_blocks(app_client):
    r = app_client.get("/api/health")
    data = r.get_json()
    assert "database_url" in data
    assert "audit" in data
    assert "metrics" in data
    assert "celery_preferred" in data


def test_approver_gate_on_desk_proposal(tmp_env, monkeypatch):
    monkeypatch.setenv("ANALYST_DESK_ADMIN_PASSWORD", "desk-pass")
    import config
    from app.models import init_db
    from app.analyst_desk.store import init_analyst_desk_db, insert_proposal
    from app.analyst_desk.app import create_analyst_desk_app

    monkeypatch.setattr(config, "ANALYST_DESK_ADMIN_PASSWORD", "desk-pass")
    monkeypatch.setattr(config, "DATABASE_PATH", str(tmp_env["news_db"]))
    monkeypatch.setattr(config, "ANALYST_DESK_DATABASE_PATH", str(tmp_env["desk_db"]))

    init_db(str(tmp_env["news_db"]))
    init_analyst_desk_db(str(tmp_env["desk_db"]))
    pid = insert_proposal(
        agent_id="asean",
        run_type="daily_volume",
        title="T",
        body="Body text for test",
        source_context={"test": True},
        countries=["SGP"],
        focus_country="SGP",
    )
    app = create_analyst_desk_app()
    app.config["TESTING"] = True
    app.config["ANALYST_DESK_ADMIN_PASSWORD"] = "desk-pass"
    client = app.test_client()
    # Log in as desk admin (admin role) via password — should allow approve
    with client.session_transaction() as sess:
        sess["analyst_desk_ok"] = True
        sess["analyst_desk_user"] = {"username": "viewer-only", "via": "test", "role": "viewer"}
        sess["analyst_desk_role"] = "viewer"
    r = client.post(
        f"/proposal/{pid}",
        data={"action": "approve", "reviewed_body": "Body text for test"},
        follow_redirects=True,
    )
    assert r.status_code == 200
    assert b"Approver role required" in r.data
