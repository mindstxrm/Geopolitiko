"""P5: reject taxonomy + DLQ helpers."""
from __future__ import annotations


def test_reject_requires_reason(tmp_env, monkeypatch):
    monkeypatch.setenv("ANALYST_DESK_ADMIN_PASSWORD", "desk-pass")
    import config
    from app.models import init_db
    from app.analyst_desk.store import init_analyst_desk_db, insert_proposal, get_proposal
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
        body="Body",
        source_context={"test": True},
        countries=["SGP"],
        focus_country="SGP",
    )
    app = create_analyst_desk_app()
    app.config["TESTING"] = True
    app.config["ANALYST_DESK_ADMIN_PASSWORD"] = "desk-pass"
    client = app.test_client()
    with client.session_transaction() as sess:
        sess["analyst_desk_ok"] = True
        sess["analyst_desk_user"] = {"username": "approver", "via": "test", "role": "admin"}
        sess["analyst_desk_role"] = "admin"

    r = client.post(
        f"/proposal/{pid}",
        data={"action": "reject", "reviewer_note": "nope"},
        follow_redirects=True,
    )
    assert r.status_code == 200
    assert b"Select a reject reason" in r.data
    assert get_proposal(pid)["status"] == "pending"

    r2 = client.post(
        f"/proposal/{pid}",
        data={"action": "reject", "reject_reason": "duplicate", "reviewer_note": "dupe"},
        follow_redirects=True,
    )
    assert r2.status_code == 200
    row = get_proposal(pid)
    assert row["status"] == "rejected"
    assert "duplicate" in (row.get("reviewer_note") or "")


def test_dlq_delete_helper(tmp_env, monkeypatch):
    import config
    from app.analyst_desk.store import (
        init_analyst_desk_db,
        create_dead_letter_task,
        get_dead_letter_task,
        delete_dead_letter_task,
    )

    monkeypatch.setattr(config, "ANALYST_DESK_DATABASE_PATH", str(tmp_env["desk_db"]))
    init_analyst_desk_db(str(tmp_env["desk_db"]))
    dlq_id = create_dead_letter_task(
        task_name="app.analyst_desk.tasks.run_full_pipeline",
        task_id="t1",
        payload={},
        error_message="boom",
        retries=2,
    )
    assert get_dead_letter_task(dlq_id)
    assert delete_dead_letter_task(dlq_id)
    assert get_dead_letter_task(dlq_id) is None
