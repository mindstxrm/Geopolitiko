"""Single entry point for one full Analyst Desk pipeline tick (shared by threaded worker, Celery, tests)."""
from __future__ import annotations

from typing import Any

from config import analyst_desk_phase2_enabled, analyst_desk_phase3_enabled


def run_full_pipeline_tick() -> dict[str, Any]:
    """
    Run ingest → enrich → metrics → events → alerts → brief (+ optional phase 2/3).
    Returns a flat dict of counters suitable for logging and Celery results.
    """
    from app.analyst_desk.pipeline import (
        generate_daily_brief,
        ingest_from_news_articles,
        process_alerts,
        process_country_risk_snapshots,
        process_document_enrichment,
        process_event_builder,
        process_metric_extraction,
        process_regional_synthesis,
        process_topic_synthesis,
        process_verification_checks,
        process_watchlist_movers,
        process_scenarios,
    )

    i = ingest_from_news_articles(limit=120, days=2)
    e = process_document_enrichment(limit=200)
    mx = process_metric_extraction(limit=180)
    b = process_event_builder(limit=200)
    v = process_verification_checks(limit=200)
    a = process_alerts(limit=200)
    d = generate_daily_brief(limit_events=20)
    t = r = c = m = {}
    if analyst_desk_phase2_enabled():
        t = process_topic_synthesis(limit_events=180)
        r = process_regional_synthesis(limit_events=180)
        c = process_country_risk_snapshots(limit_events=220)
        m = process_watchlist_movers(delta_threshold=8.0)
    s: dict[str, Any] = {}
    if analyst_desk_phase3_enabled():
        s = process_scenarios()

    return {
        "ok": True,
        "ingested": i.get("ingested", 0),
        "enriched": e.get("processed", 0),
        "metrics_docs": mx.get("docs_metrics_processed", 0),
        "metrics_rows": mx.get("metrics_written", 0),
        "events": b.get("events_written", 0),
        "verified": v.get("verified", 0),
        "contradicted": v.get("contradicted", 0),
        "alerts_created": a.get("alerts_created", 0),
        "briefing_id": d.get("briefing_id"),
        "topic_briefings": t.get("topic_briefings_created", 0),
        "regional_snaps": r.get("regional_snapshots_written", 0),
        "country_snaps": c.get("country_snapshots_written", 0),
        "movers": m.get("movers_created", 0),
        "scenarios": s.get("scenarios_created", 0),
    }
