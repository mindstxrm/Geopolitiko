"""Health endpoint and ops board."""


def test_api_health_ok(app_client):
    r = app_client.get("/api/health")
    assert r.status_code in (200, 503)
    data = r.get_json()
    assert data["status"] in ("ok", "degraded", "error")
    assert "checks" in data
    assert "news" in data


def test_ops_board_renders(app_client):
    r = app_client.get("/ops")
    assert r.status_code == 200
    body = r.get_data(as_text=True)
    assert "Ops health" in body
    assert "/api/health" in body
