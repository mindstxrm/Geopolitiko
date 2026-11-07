"""Approve → publish → Terminal visibility."""
from __future__ import annotations


def test_publish_approved_proposal_to_terminal(tmp_env, monkeypatch):
    from app.models import get_desk_terminal_intel_for_country, get_recent_desk_terminal_intel, init_db
    from app.analyst_desk.store import (
        insert_proposal,
        init_analyst_desk_db,
        sync_agents_registry,
        update_proposal_review,
    )
    from app.analyst_desk.agents import AGENTS
    from app.analyst_desk.publish import publish_approved_proposal_to_terminal
    import config

    monkeypatch.setattr(config, "DATABASE_PATH", str(tmp_env["news_db"]))
    init_db(str(tmp_env["news_db"]))
    init_analyst_desk_db(str(tmp_env["desk_db"]))
    sync_agents_registry(AGENTS)

    agent_id = next(iter(AGENTS.keys()))
    countries = list(AGENTS[agent_id].get("countries") or ["SGP"])[:2]
    pid = insert_proposal(
        agent_id=agent_id,
        run_type="daily_volume",
        title="Test daily volume",
        body="## Test brief\n\nThis is publishable content for P0 tests.",
        countries=countries,
        focus_country=countries[0],
        source_context={"article_count": 3, "days": 1},
    )
    update_proposal_review(pid, "approved", None, "pytest")
    # re-set body as reviewed
    update_proposal_review(
        pid,
        "approved",
        "## Test brief\n\nThis is publishable content for P0 tests.",
        "pytest",
    )

    result = publish_approved_proposal_to_terminal(pid)
    assert result.get("ok") is True, result
    assert not result.get("skipped")

    recent = get_recent_desk_terminal_intel(limit=5)
    assert any(r.get("proposal_id") == pid for r in recent)

    country_rows = get_desk_terminal_intel_for_country(countries[0], limit=5)
    assert any(r.get("proposal_id") == pid for r in country_rows)
    assert "publishable content" in (country_rows[0].get("body_markdown") or "")
