"""Health endpoint and ops board."""


def test_api_health_ok(app_client):
    r = app_client.get("/api/health")
    assert r.status_code in (200, 503)
    data = r.get_json()
    assert data["status"] in ("ok", "degraded", "error")
    assert "checks" in data
    assert "news" in data


def test_ops_board_renders(app_client, tmp_env, monkeypatch):
    monkeypatch.setenv("GEOPOLITIKO_ADMIN_USERNAMES", "opsadmin")
    from app.models import create_user

    create_user(
        "opsadmin",
        "password123",
        email="opsadmin@example.com",
        name="Ops",
        title="Admin",
        organization="Ops",
        role="admin",
    )
    app_client.post(
        "/login",
        data={"username": "opsadmin", "password": "password123"},
        follow_redirects=False,
    )
    r = app_client.get("/ops")
    assert r.status_code == 200
    body = r.get_data(as_text=True)
    assert "Ops health" in body
    assert "/api/health" in body
