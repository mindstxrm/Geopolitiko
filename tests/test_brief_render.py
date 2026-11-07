"""Readable brief rendering for Approvals."""
from __future__ import annotations


SAMPLE = """## Situation snapshot
FACT shipping advisories cite two wires [S1][S2].

## Key developments
- CLAIM coastal strike remains single-source [S1]
- ASSESS miscalculation risk is elevated

## Sources
- [S1] Reuters
- [S2] AP
"""


def test_split_and_render_sections():
    from app.analyst_desk.brief_render import split_brief_sections, render_brief_html, brief_preview_plain

    secs = split_brief_sections(SAMPLE)
    assert len(secs) >= 3
    assert secs[0]["heading"] == "Situation snapshot"
    html = render_brief_html(SAMPLE)
    assert "brief-section" in html
    assert "brief-cite" in html
    assert "brief-label-fact" in html
    assert "<ul>" in html
    preview = brief_preview_plain(SAMPLE, limit=80)
    assert "Situation snapshot" not in preview
    assert "shipping" in preview.lower() or "advisories" in preview.lower()


def test_proposal_page_renders_readable(tmp_env, monkeypatch):
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
        title="Readable test",
        body=SAMPLE,
        source_context={"article_count": 2, "days": 3},
        countries=["SGP"],
        focus_country="SGP",
    )
    app = create_analyst_desk_app()
    app.config["TESTING"] = True
    client = app.test_client()
    with client.session_transaction() as sess:
        sess["analyst_desk_ok"] = True
        sess["analyst_desk_user"] = {"username": "a", "role": "admin"}
        sess["analyst_desk_role"] = "admin"
    r = client.get(f"/proposal/{pid}")
    assert r.status_code == 200
    assert b"brief-section" in r.data
    assert b"brief-cite" in r.data
    assert b"Edit raw draft" in r.data
    # Should not dump the whole draft as the only visible surface in a giant bare pre
    assert b"<pre style=\"white-space: pre-wrap" not in r.data
