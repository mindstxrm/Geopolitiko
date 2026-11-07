"""P3: sources tiers, scheduled briefs gating, delivery payload, indicator coverage."""
from __future__ import annotations


def test_sources_have_tiers():
    from app.scrapers.sources import SOURCES, source_tier_for_name, sources_summary

    assert len(SOURCES) >= 20
    assert all(s.get("tier") in ("A", "B", "C") for s in SOURCES)
    assert source_tier_for_name("Reuters - World") == "A"
    assert source_tier_for_name("The Diplomat") == "B"
    summary = sources_summary()
    assert summary["count"] == len(SOURCES)
    assert summary["by_tier"]["A"] >= 1


def test_numbered_sources_include_tier():
    from app.analyst_desk.desk_sops import build_numbered_sources

    articles = [
        {
            "id": 1,
            "title": "Test",
            "source_name": "Reuters - World",
            "url": "http://example.com",
            "summary": "Hello",
        }
    ]
    block, structured = build_numbered_sources(articles)
    assert "Tier A" in block
    assert structured[0]["tier"] == "A"


def test_scheduled_briefs_interval_gate(tmp_env, monkeypatch):
    monkeypatch.setenv("ANALYST_DESK_SCHEDULED_BRIEFS", "1")
    monkeypatch.setenv("ANALYST_DESK_SCHEDULED_BRIEF_HOURS", "24")
    from app.analyst_desk.scheduled_briefs import should_run_scheduled_briefs
    from app.platform_ops import write_ops_status

    assert should_run_scheduled_briefs({}) is True
    write_ops_status({"last_scheduled_briefs_at": "2099-01-01T00:00:00Z"})
    # Far-future timestamp → hours_since negative → treat as not elapsed? 
    # Negative hours means "in the future" — should_run uses hours >= interval.
    # Fix: use a recent timestamp instead.
    from datetime import datetime, timezone, timedelta

    recent = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    write_ops_status({"last_scheduled_briefs_at": recent})
    assert should_run_scheduled_briefs() is False
    old = (datetime.now(timezone.utc) - timedelta(hours=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    write_ops_status({"last_scheduled_briefs_at": old})
    assert should_run_scheduled_briefs() is True


def test_deliver_approved_brief_disabled(monkeypatch):
    monkeypatch.setenv("ANALYST_DESK_DELIVER_ON_APPROVE", "0")
    from app.analyst_desk.delivery import deliver_approved_brief

    ok, channel, detail = deliver_approved_brief(
        {"id": 1, "title": "T", "agent_id": "x", "body": "hello", "countries": ["SGP"]}
    )
    assert ok is False
    assert channel == "none"
    assert "disabled" in detail


def test_api_sources_and_indicators(app_client):
    r = app_client.get("/api/sources")
    assert r.status_code == 200
    data = r.get_json()
    assert data["summary"]["count"] >= 1
    r2 = app_client.get("/api/indicators/coverage")
    assert r2.status_code == 200
    cov = r2.get_json()
    assert "macroeconomic_stress_countries" in cov
