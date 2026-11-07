"""P7: dialect helpers, feature flags, domain desk_intel."""
from __future__ import annotations


def test_adapt_placeholders():
    from app.sql_dialect import adapt_placeholders

    sql = "SELECT * FROM users WHERE id = ? AND role = ?"
    assert adapt_placeholders(sql, dialect="sqlite") == sql
    assert adapt_placeholders(sql, dialect="postgresql") == "SELECT * FROM users WHERE id = %s AND role = %s"


def test_cutover_status_sqlite(tmp_env, monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    import importlib
    import app.db as db
    import app.sql_dialect as sd

    importlib.reload(db)
    importlib.reload(sd)
    st = sd.cutover_status()
    assert st["is_sqlite"] is True
    assert st["ready_for_pg_runtime"] is False


def test_feature_flags(monkeypatch):
    from app.feature_flags import flag, all_flags

    monkeypatch.setenv("GEOPOLITIKO_FF_LABS_NAV", "0")
    assert flag("labs_nav", True) is False
    flags = all_flags()
    assert "multi_source_gate" in flags


def test_domain_desk_intel_upsert(tmp_env):
    from app.domain.desk_intel import upsert_desk_terminal_intel, ensure_desk_terminal_intel_table
    import sqlite3

    path = str(tmp_env["news_db"])
    conn = sqlite3.connect(path)
    ensure_desk_terminal_intel_table(conn)
    conn.commit()
    conn.close()

    upsert_desk_terminal_intel(
        proposal_id=99,
        agent_id="asean",
        run_type="daily_volume",
        title="Hello",
        body_markdown="Body",
        countries_csv="SGP",
        focus_country="SGP",
        metrics_json="[]",
        reviewer_note=None,
        database_path=path,
    )
    conn = sqlite3.connect(path)
    row = conn.execute("SELECT title FROM desk_terminal_intel WHERE proposal_id = 99").fetchone()
    conn.close()
    assert row[0] == "Hello"


def test_health_includes_cutover(app_client):
    r = app_client.get("/api/health")
    data = r.get_json()
    assert "cutover" in data
    assert "feature_flags" in data
