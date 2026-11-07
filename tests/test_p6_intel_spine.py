"""P6: entities, confirmation gates, golden briefs, terminal events."""
from __future__ import annotations
from pathlib import Path


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "golden_briefs"


def test_entity_resolve_org_and_country():
    from app.entities import resolve_entity, country_entity_id, extract_entities_from_text

    assert country_entity_id("sgp") == "country:SGP"
    nato = resolve_entity("NATO")
    assert nato and nato["id"] == "org:nato"
    found = extract_entities_from_text("Xi Jinping met ASEAN leaders", countries=["CHN"])
    ids = {e["id"] for e in found}
    assert "country:CHN" in ids
    assert "person:xi-jinping" in ids
    assert "org:asean" in ids


def test_confirmation_gate_sensitive_single_source():
    from app.analyst_desk.confirmation import assess_confirmation, gate_brief_for_confirmation

    a = assess_confirmation(
        text="Dozens killed in overnight missile strike",
        sources=[{"name": "Blog", "tier": "C"}],
    )
    assert a["needs_confirmation"]
    assert a["status"] == "single_source"

    ok = assess_confirmation(
        text="Dozens killed in overnight missile strike",
        sources=[
            {"name": "Reuters", "tier": "A"},
            {"name": "AP", "tier": "A"},
        ],
    )
    assert ok["status"] == "confirmed"

    gate = gate_brief_for_confirmation(
        "Routine diplomatic talks resumed",
        sources=[{"name": "Reuters", "tier": "A"}],
    )
    assert gate["ok"] is True


def test_golden_briefs_eval_regression():
    from app.analyst_desk.eval_brief import evaluate_brief

    pass_body = (FIXTURES / "asean_daily_pass.md").read_text(encoding="utf-8")
    sources = [
        {"name": "Reuters - World", "tier": "A"},
        {"name": "AP - Top News", "tier": "A"},
    ]
    good = evaluate_brief(pass_body, agent_id="asean", sources=sources)
    assert good["pass"] is True
    assert good["grade"] in ("A", "B", "C")

    fail_body = (FIXTURES / "asean_daily_fail_sensitive.md").read_text(encoding="utf-8")
    bad = evaluate_brief(
        fail_body,
        agent_id="asean",
        sources=[{"name": "Blog", "tier": "C"}],
    )
    conf_checks = [c for c in bad["checks"] if c["id"] == "multi_source_confirmation"]
    assert conf_checks
    assert conf_checks[0]["ok"] is False


def test_terminal_events_sync(tmp_env, monkeypatch):
    import config
    from app.models import init_db
    from app.analyst_desk.store import init_analyst_desk_db, create_standalone_event
    from app.terminal_events import sync_recent_desk_events_to_terminal, list_terminal_events

    monkeypatch.setattr(config, "DATABASE_PATH", str(tmp_env["news_db"]))
    monkeypatch.setattr(config, "ANALYST_DESK_DATABASE_PATH", str(tmp_env["desk_db"]))
    init_db(str(tmp_env["news_db"]))
    init_analyst_desk_db(str(tmp_env["desk_db"]))

    eid = create_standalone_event(
        event_title="NATO exercise near Baltic",
        event_summary="Allied naval exercise announced.",
        event_type="military",
        primary_country_code="POL",
        region="europe",
        importance_score=55,
        confidence_score=0.7,
        event_signature="test-sig-nato-baltic",
    )
    assert eid
    out = sync_recent_desk_events_to_terminal(limit=10, database_path=str(tmp_env["news_db"]))
    assert out["synced"] >= 1
    rows = list_terminal_events(country="POL", database_path=str(tmp_env["news_db"]))
    assert rows
    assert rows[0]["desk_event_id"] == eid
    assert any(e.get("id") == "org:nato" for e in (rows[0].get("entities") or []))
