"""Live data freshness: scrape/jobs split, ETag cache, Celery briefs hook, stale thresholds."""
from __future__ import annotations

from unittest.mock import MagicMock, patch  # MagicMock used in ETag test


def test_scrape_and_jobs_are_separable(tmp_env, monkeypatch):
    from app.models import init_db
    import config
    from app import scheduler as sched

    monkeypatch.setattr(config, "DATABASE_PATH", str(tmp_env["news_db"]))
    init_db(str(tmp_env["news_db"]))

    with patch.object(sched, "scrape_all_sources", create=True):
        pass
    with patch("app.scrapers.rss_scraper.scrape_all_sources", return_value={"A": 2, "B": 0}) as scrape:
        with patch("app.platform_ops.record_refresh_start"), patch(
            "app.platform_ops.record_refresh_success"
        ), patch("app.platform_ops.write_ops_status"):
            out = sched.run_scrape_only()
    assert out["ok"] is True
    assert out["articles"] == 2
    scrape.assert_called_once()

    with patch("app.jobs.topics.extract_topics_for_all", return_value=1), patch(
        "app.jobs.impact.score_all", return_value=1
    ), patch("app.jobs.analysis.generate_analysis_for_all", return_value=1), patch(
        "app.jobs.digest.generate_daily_digest"
    ), patch("app.jobs.clustering.cluster_articles"), patch(
        "app.risk_engine.compute_country_risk_from_articles"
    ), patch("app.risk_engine.compute_forward_risk_index"), patch(
        "app.jobs.institutional.run_institutional_models"
    ), patch.object(sched, "_fire_alert_webhooks"), patch.object(
        sched, "_emit_conflict_events"
    ), patch("app.platform_ops.write_ops_status"):
        jobs = sched.run_analysis_jobs()
    assert jobs.get("ok") is True
    assert jobs.get("topics") == 1


def test_etag_304_records_zero_articles(tmp_env, monkeypatch):
    import config
    from app.models import init_db
    from app.scrapers.http_cache import get_source_cache, record_source_fetch
    from app.scrapers import rss_scraper

    monkeypatch.setattr(config, "DATABASE_PATH", str(tmp_env["news_db"]))
    init_db(str(tmp_env["news_db"]))
    db = str(tmp_env["news_db"])
    record_source_fetch(
        db,
        source_name="Test Feed",
        feed_url="https://example.com/rss",
        status=200,
        etag='"abc"',
        last_modified="Wed, 01 Jan 2020 00:00:00 GMT",
    )

    fake_resp = MagicMock()
    fake_resp.status_code = 304
    fake_resp.headers = {"ETag": '"abc"', "Last-Modified": "Wed, 01 Jan 2020 00:00:00 GMT"}
    fake_resp.raise_for_status = MagicMock()

    with patch("app.scrapers.rss_scraper.get", return_value=fake_resp):
        n = rss_scraper.scrape_source(
            {"name": "Test Feed", "url": "https://example.com/rss", "homepage": "https://example.com"},
            db,
        )
    assert n == 0
    cached = get_source_cache(db, "Test Feed")
    assert cached
    assert int(cached["not_modified_count"]) >= 1


def test_celery_task_source_includes_scheduled_briefs_and_heartbeat():
    """Guardrail: Celery full pipeline must stay parity with loop worker."""
    import inspect
    from app.analyst_desk import tasks as tasks_mod

    src = inspect.getsource(tasks_mod.run_full_pipeline)
    assert "run_scheduled_regional_briefs" in src
    assert "record_desk_worker_heartbeat" in src
    assert "scheduled_briefs" in src


def test_freshness_stale_threshold(tmp_env, monkeypatch):
    import config
    from app.models import init_db
    from app.platform_ops import write_ops_status
    from app.freshness import build_freshness_snapshot

    monkeypatch.setattr(config, "DATABASE_PATH", str(tmp_env["news_db"]))
    monkeypatch.setenv("LIVE_NEWS_STALE_MINUTES", "30")
    monkeypatch.setenv("PLATFORM_OPS_STATUS_PATH", str(tmp_env["ops"]))
    init_db(str(tmp_env["news_db"]))
    write_ops_status({"last_refresh_finished_at": "2000-01-01T00:00:00Z"})
    snap = build_freshness_snapshot()
    assert snap["news_stale"] is True
    assert snap["banner"]
    assert "STALE" in snap["banner"]


def test_health_and_banner_context(app_client):
    r = app_client.get("/api/health")
    data = r.get_json()
    assert "freshness" in data
    assert "ingest_worker" in data
    # Ops is admin-only; anonymous visitors are sent to login
    page = app_client.get("/ops", follow_redirects=False)
    assert page.status_code in (302, 303)
    assert "/login" in (page.headers.get("Location") or "")
