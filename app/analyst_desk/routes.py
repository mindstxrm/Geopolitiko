"""Analyst Desk HTTP routes (separate Flask app — not main blueprint)."""
from __future__ import annotations

import os

from config import (
    CELERY_BROKER_URL,
    analyst_desk_celery_beat_seconds,
    analyst_desk_metric_llm_enabled,
)

from flask import (
    Blueprint,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from app.analyst_desk.agents import AGENTS, list_agent_ids
from app.analyst_desk.delivery import delivery_status, send_test_delivery
from app.analyst_desk.generate import (
    run_all_daily_volume,
    run_daily_volume,
    run_weekly_country_deep_dive,
)
from app.analyst_desk.pipeline import (
    generate_daily_brief,
    ingest_from_news_articles,
    process_alerts,
    process_country_risk_snapshots,
    process_document_enrichment,
    process_event_builder,
    process_metric_extraction,
    process_verification_checks,
    process_regional_synthesis,
    process_topic_synthesis,
    process_watchlist_movers,
    process_scenarios,
)
from app.analyst_desk.store import (
    count_proposals_by_status,
    create_alert,
    get_briefing,
    get_proposal,
    get_scenario,
    list_recent_scenarios,
    list_recent_briefings,
    list_risk_traces_for_country,
    get_events_by_ids,
    list_published_proposals,
    list_recent_alerts,
    list_recent_dead_letter_tasks,
    list_recent_extracted_metrics,
    list_recent_country_risk_snapshots,
    list_recent_agent_messages,
    list_proposals,
    list_recent_regional_risk_snapshots,
    list_recent_events,
    list_recent_verification_checks,
    list_stuck_agent_messages,
    list_recent_watchlist_movers,
    list_recent_agent_tasks,
    mark_alert_failed,
    mark_alert_sent,
    prune_operational_data,
    query_events_text,
    update_proposal_review,
)

bp = Blueprint("analyst_desk", __name__)


def _celery_desk_available() -> bool:
    try:
        import celery  # noqa: F401

        from app.analyst_desk.celery_app import celery_app

        return bool(celery_app)
    except Exception:
        return False


def _password_configured(app) -> bool:
    pwd = (app.config.get("ANALYST_DESK_ADMIN_PASSWORD") or "").strip()
    return bool(pwd)


@bp.route("/login", methods=["GET", "POST"])
def login():
    from flask import current_app

    app = current_app
    if not _password_configured(app):
        return redirect(url_for("analyst_desk.index"))
    if request.method == "POST":
        pwd = (request.form.get("password") or "").strip()
        expected = (app.config.get("ANALYST_DESK_ADMIN_PASSWORD") or "").strip()
        if pwd and pwd == expected:
            session["analyst_desk_ok"] = True
            session.permanent = True
            nxt = request.args.get("next") or url_for("analyst_desk.index")
            return redirect(nxt)
        flash("Invalid password.", "error")
    return render_template("analyst_desk/login.html")


@bp.route("/logout")
def logout():
    session.pop("analyst_desk_ok", None)
    return redirect(url_for("analyst_desk.login"))


@bp.route("/")
def index():
    st = request.args.get("status") or None
    props = list_proposals(status=st, limit=150)
    return render_template(
        "analyst_desk/index.html",
        proposals=props,
        status_filter=st,
        agents=AGENTS,
        recent_tasks=list_recent_agent_tasks(limit=20),
        recent_events=list_recent_events(limit=20),
        recent_alerts=list_recent_alerts(limit=20),
        recent_country_risk=list_recent_country_risk_snapshots(limit=20),
        recent_regional_risk=list_recent_regional_risk_snapshots(limit=20),
        recent_watchlist_movers=list_recent_watchlist_movers(limit=20),
        recent_briefings=list_recent_briefings(limit=15),
        recent_scenarios=list_recent_scenarios(limit=25),
        pending_messages=list_recent_agent_messages(limit=20, status="pending"),
        delivery=delivery_status(),
        openai_set=bool(os.environ.get("OPENAI_API_KEY")),
        celery_available=_celery_desk_available(),
        metric_llm_enabled=analyst_desk_metric_llm_enabled(),
        celery_broker_preview=(CELERY_BROKER_URL.split("@")[-1] if "@" in CELERY_BROKER_URL else CELERY_BROKER_URL)[:80],
        celery_beat_seconds=analyst_desk_celery_beat_seconds(),
    )


@bp.route("/operations")
def operations():
    return render_template(
        "analyst_desk/operations.html",
        proposal_counts=count_proposals_by_status(),
        recent_tasks=list_recent_agent_tasks(limit=40),
        pending_messages=list_recent_agent_messages(limit=40, status="pending"),
        stuck_messages=list_stuck_agent_messages(older_than_minutes=60, limit=40),
        recent_alerts=list_recent_alerts(limit=30),
        recent_events=list_recent_events(limit=30),
        recent_verifications=list_recent_verification_checks(limit=30),
        dead_letters=list_recent_dead_letter_tasks(limit=30),
        delivery=delivery_status(),
        celery_available=_celery_desk_available(),
    )


@bp.route("/operations/prune", methods=["POST"])
def operations_prune():
    out = prune_operational_data(keep_tasks_days=30, keep_messages_days=14, keep_metrics_days=30)
    flash(
        f"Prune complete: tasks {out.get('tasks_deleted', 0)}, messages {out.get('messages_deleted', 0)}, metrics {out.get('metrics_deleted', 0)}.",
        "info",
    )
    return redirect(url_for("analyst_desk.operations"))


@bp.route("/approvals")
def approvals():
    return render_template(
        "analyst_desk/approvals.html",
        pending=list_proposals(status="pending", limit=300),
        approved=list_proposals(status="approved", limit=120),
        rejected=list_proposals(status="rejected", limit=120),
    )


@bp.route("/approvals/bulk", methods=["POST"])
def approvals_bulk():
    action = (request.form.get("action") or "").strip().lower()
    confirm_publish = (request.form.get("confirm_publish") or "").strip() == "1"
    ids_raw = request.form.getlist("proposal_ids")
    ids: list[int] = []
    for v in ids_raw:
        try:
            ids.append(int(v))
        except (TypeError, ValueError):
            continue
    if not ids:
        flash("No proposals selected.", "error")
        return redirect(url_for("analyst_desk.approvals"))
    if action == "approve" and not confirm_publish:
        flash("Bulk approve blocked: tick 'Confirm publish to Terminal' first.", "error")
        return redirect(url_for("analyst_desk.approvals"))
    changed = 0
    published = 0
    failed_pub = 0
    from app.analyst_desk.publish import publish_approved_proposal_to_terminal

    for pid in ids:
        row = get_proposal(pid)
        if not row or (row.get("status") or "").strip().lower() != "pending":
            continue
        if action == "approve":
            update_proposal_review(pid, "approved", row.get("body"), "Bulk-approved")
            changed += 1
            pub = publish_approved_proposal_to_terminal(pid)
            if pub.get("ok"):
                published += 1
            else:
                failed_pub += 1
        elif action == "reject":
            update_proposal_review(pid, "rejected", None, "Bulk-rejected")
            changed += 1
    if action == "approve":
        flash(
            f"Bulk approve done: {changed} proposals approved, {published} published, {failed_pub} publish failures.",
            "info",
        )
    elif action == "reject":
        flash(f"Bulk reject done: {changed} proposals rejected.", "info")
    else:
        flash("Unknown bulk action.", "error")
    return redirect(url_for("analyst_desk.approvals"))


@bp.route("/intel")
def intel_feed():
    return render_template(
        "analyst_desk/intel.html",
        published=list_published_proposals(limit=200),
        recent_briefings=list_recent_briefings(limit=40),
        recent_scenarios=list_recent_scenarios(limit=40),
    )


@bp.route("/extraction")
def extraction_qc():
    cc = (request.args.get("country") or "").strip().upper() or None
    return render_template(
        "analyst_desk/extraction.html",
        country_filter=cc,
        metrics=list_recent_extracted_metrics(limit=300, country_code=cc),
    )


@bp.route("/proposal/<int:pid>", methods=["GET", "POST"])
def proposal_detail(pid: int):
    row = get_proposal(pid)
    if not row:
        flash("Proposal not found.", "error")
        return redirect(url_for("analyst_desk.index"))
    if request.method == "POST":
        action = (request.form.get("action") or "").strip()
        note = (request.form.get("reviewer_note") or "").strip() or None
        edited = (request.form.get("reviewed_body") or "").strip()
        if action == "approve":
            final = edited if edited else row["body"]
            update_proposal_review(pid, "approved", final, note)
            from app.analyst_desk.publish import publish_approved_proposal_to_terminal

            pub = publish_approved_proposal_to_terminal(pid)
            if pub.get("ok") and pub.get("skipped"):
                flash(
                    "Approved. (Terminal publish disabled — set ANALYST_DESK_PUBLISH_ON_APPROVE=1 to push to the Terminal.)",
                    "info",
                )
            elif pub.get("ok"):
                n = pub.get("metrics_attached", 0)
                flash(
                    f"Approved and published to Geopolitical Terminal ({n} quantitative signals attached).",
                    "info",
                )
            else:
                flash(f"Approved, but Terminal publish failed: {pub.get('error')}", "error")
        elif action == "reject":
            update_proposal_review(pid, "rejected", None, note)
            flash("Rejected.", "info")
        else:
            flash("Unknown action.", "error")
        return redirect(url_for("analyst_desk.proposal_detail", pid=pid))
    # Editable draft defaults to last reviewed text or original model output
    display_body = (row.get("reviewed_body") or row["body"] or "").strip() or row["body"]
    return render_template(
        "analyst_desk/proposal.html",
        p=row,
        display_body=display_body,
        agents=AGENTS,
    )


@bp.route("/run/daily", methods=["POST"])
def run_daily():
    agent_id = (request.form.get("agent_id") or "").strip()
    if agent_id not in list_agent_ids():
        flash("Invalid agent.", "error")
        return redirect(url_for("analyst_desk.index"))
    pid = run_daily_volume(agent_id)
    if pid:
        flash(f"Daily volume draft created (#{pid}).", "info")
        return redirect(url_for("analyst_desk.proposal_detail", pid=pid))
    flash("Run failed.", "error")
    return redirect(url_for("analyst_desk.index"))


@bp.route("/run/weekly", methods=["POST"])
def run_weekly():
    agent_id = (request.form.get("agent_id") or "").strip()
    cc = (request.form.get("country") or "").strip().upper()
    if agent_id not in list_agent_ids():
        flash("Invalid agent.", "error")
        return redirect(url_for("analyst_desk.index"))
    pid = run_weekly_country_deep_dive(agent_id, cc)
    if pid:
        flash(f"Weekly draft created (#{pid}).", "info")
        return redirect(url_for("analyst_desk.proposal_detail", pid=pid))
    flash("Invalid country for this agent or run failed.", "error")
    return redirect(url_for("analyst_desk.index"))


@bp.route("/run/daily-all", methods=["POST"])
def run_daily_all():
    results = run_all_daily_volume()
    ok = len([1 for _, pid in results if pid])
    flash(f"Triggered daily drafts for {ok}/{len(results)} agents.", "info")
    return redirect(url_for("analyst_desk.index"))


@bp.route("/pipeline/ingest", methods=["POST"])
def pipeline_ingest():
    out = ingest_from_news_articles(limit=160, days=2)
    flash(f"Ingested {out.get('ingested', 0)} documents; queued {out.get('queued', 0)} messages.", "info")
    return redirect(url_for("analyst_desk.index"))


@bp.route("/pipeline/enrich", methods=["POST"])
def pipeline_enrich():
    out = process_document_enrichment(limit=200)
    flash(f"Enriched {out.get('processed', 0)} documents.", "info")
    return redirect(url_for("analyst_desk.index"))


@bp.route("/pipeline/metrics", methods=["POST"])
def pipeline_metrics():
    out = process_metric_extraction(limit=160)
    flash(
        f"Metric extraction: processed {out.get('docs_metrics_processed', 0)} docs, "
        f"wrote {out.get('metrics_written', 0)} metric rows.",
        "info",
    )
    return redirect(url_for("analyst_desk.index"))


@bp.route("/pipeline/celery-full", methods=["POST"])
def pipeline_celery_full():
    if not _celery_desk_available():
        flash("Celery/Redis not available (install celery[redis] and redis, set CELERY_BROKER_URL).", "error")
        return redirect(url_for("analyst_desk.index"))
    from app.analyst_desk.tasks import run_full_pipeline

    res = run_full_pipeline.delay()
    flash(f"Queued full pipeline on Celery (task id {res.id}). Check worker logs for results.", "info")
    return redirect(url_for("analyst_desk.index"))


@bp.route("/pipeline/celery-metrics", methods=["POST"])
def pipeline_celery_metrics():
    if not _celery_desk_available():
        flash("Celery/Redis not available.", "error")
        return redirect(url_for("analyst_desk.index"))
    from app.analyst_desk.tasks import run_metric_extraction_only

    res = run_metric_extraction_only.delay(200)
    flash(f"Queued metric extraction on Celery queue desk_metrics (task id {res.id}).", "info")
    return redirect(url_for("analyst_desk.index"))


@bp.route("/pipeline/events", methods=["POST"])
def pipeline_events():
    out = process_event_builder(limit=200)
    flash(
        f"Event builder consumed {out.get('messages_consumed', 0)} messages; wrote {out.get('events_written', 0)} event updates.",
        "info",
    )
    return redirect(url_for("analyst_desk.index"))


@bp.route("/pipeline/verify", methods=["POST"])
def pipeline_verify():
    out = process_verification_checks(limit=220)
    flash(
        f"Verification checks: verified {out.get('verified', 0)}, contradicted {out.get('contradicted', 0)}, pending {out.get('pending', 0)}.",
        "info",
    )
    return redirect(url_for("analyst_desk.index"))


@bp.route("/pipeline/alerts", methods=["POST"])
def pipeline_alerts():
    out = process_alerts(limit=200)
    flash(
        (
            f"Alerting lane created {out.get('alerts_created', 0)} alerts "
            f"(delivered {out.get('alerts_delivered', 0)}, failed {out.get('alerts_failed', 0)})."
        ),
        "info",
    )
    return redirect(url_for("analyst_desk.index"))


@bp.route("/pipeline/run-all", methods=["POST"])
def pipeline_run_all():
    i = ingest_from_news_articles(limit=160, days=2)
    e = process_document_enrichment(limit=240)
    mx = process_metric_extraction(limit=200)
    b = process_event_builder(limit=240)
    v = process_verification_checks(limit=240)
    a = process_alerts(limit=240)
    d = generate_daily_brief(limit_events=20)
    t = process_topic_synthesis(limit_events=180)
    r = process_regional_synthesis(limit_events=180)
    c = process_country_risk_snapshots(limit_events=220)
    m = process_watchlist_movers(delta_threshold=8.0)
    s = process_scenarios()
    flash(
        (
            f"Pipeline complete: ingested {i.get('ingested', 0)}, enriched {e.get('processed', 0)}, "
            f"metrics docs {mx.get('docs_metrics_processed', 0)} / rows {mx.get('metrics_written', 0)}, "
            f"verify ok {v.get('verified', 0)} contradicted {v.get('contradicted', 0)}, "
            f"events {b.get('events_written', 0)}, alerts {a.get('alerts_created', 0)} "
            f"(delivered {a.get('alerts_delivered', 0)}, failed {a.get('alerts_failed', 0)}), "
            f"briefing #{d.get('briefing_id')}, topics {t.get('topic_briefings_created', 0)}, "
            f"regional snaps {r.get('regional_snapshots_written', 0)}, country snaps {c.get('country_snapshots_written', 0)}, "
            f"movers {m.get('movers_created', 0)}, scenarios {s.get('scenarios_created', 0)}."
        ),
        "info",
    )
    return redirect(url_for("analyst_desk.index"))


@bp.route("/pipeline/scenarios", methods=["POST"])
def pipeline_scenarios():
    out = process_scenarios()
    flash(f"Scenario agent created {out.get('scenarios_created', 0)} scenarios.", "info")
    return redirect(url_for("analyst_desk.index"))


@bp.route("/scenario/<int:sid>")
def scenario_detail(sid: int):
    s = get_scenario(sid)
    if not s:
        flash("Scenario not found.", "error")
        return redirect(url_for("analyst_desk.index"))
    return render_template("analyst_desk/scenario.html", s=s)


@bp.route("/risk_traces/<country_code>")
def risk_traces_detail(country_code: str):
    cc = (country_code or "").strip().upper()
    # Collect top contributors per dimension using latest snapshot for this country.
    dims = [
        ("political", "Political risk"),
        ("conflict", "Conflict risk"),
        ("sanctions", "Sanctions risk"),
        ("macro", "Macro risk"),
        ("supply", "Supply-chain risk"),
    ]
    trace_blocks: list[dict[str, Any]] = []
    all_event_ids: list[int] = []
    for dim_key, dim_label in dims:
        rows = list_risk_traces_for_country(country_code=cc, dimension=dim_key, limit=6)
        all_event_ids.extend([int(r.get("event_id") or 0) for r in rows if r.get("event_id")])
        trace_blocks.append({"dimension": dim_key, "label": dim_label, "rows": rows})

    events = get_events_by_ids(sorted(set(all_event_ids)), limit=50)
    by_id = {e["id"]: e for e in events}
    for blk in trace_blocks:
        for r in blk["rows"]:
            eid = int(r.get("event_id") or 0)
            ev = by_id.get(eid)
            r["event_title"] = ev.get("event_title") if ev else None
            r["event_type"] = ev.get("event_type") if ev else None

    return render_template("analyst_desk/risk_traces.html", country_code=cc, trace_blocks=trace_blocks)


@bp.route("/delivery/test", methods=["POST"])
def delivery_test():
    # Create auditable test alert record, then deliver it.
    alert_id = create_alert(
        event_id=None,
        country_code="TEST",
        region=None,
        alert_type="test",
        severity="info",
        headline="Analyst Desk delivery test",
        body="Synthetic test alert triggered from Delivery Settings panel.",
        channel="manual_test",
    )
    ok, channel, detail = send_test_delivery()
    if ok:
        mark_alert_sent(alert_id)
        flash(f"Test delivery sent via {channel} ({detail}).", "info")
    else:
        mark_alert_failed(alert_id, detail)
        flash(f"Test delivery failed: {detail}", "error")
    return redirect(url_for("analyst_desk.index"))


@bp.route("/pipeline/brief", methods=["POST"])
def pipeline_brief():
    out = generate_daily_brief(limit_events=25)
    flash(f"Daily brief generated (#{out.get('briefing_id')}).", "info")
    return redirect(url_for("analyst_desk.briefing_detail", bid=int(out.get("briefing_id"))))


@bp.route("/pipeline/topics", methods=["POST"])
def pipeline_topics():
    out = process_topic_synthesis(limit_events=220)
    flash(f"Topic synthesis created {out.get('topic_briefings_created', 0)} topic briefings.", "info")
    return redirect(url_for("analyst_desk.index"))


@bp.route("/pipeline/regional", methods=["POST"])
def pipeline_regional():
    out = process_regional_synthesis(limit_events=220)
    flash(f"Regional synthesis wrote {out.get('regional_snapshots_written', 0)} snapshots.", "info")
    return redirect(url_for("analyst_desk.index"))


@bp.route("/pipeline/risk", methods=["POST"])
def pipeline_country_risk():
    out = process_country_risk_snapshots(limit_events=260)
    flash(f"Country risk agent wrote {out.get('country_snapshots_written', 0)} snapshots.", "info")
    return redirect(url_for("analyst_desk.index"))


@bp.route("/pipeline/movers", methods=["POST"])
def pipeline_movers():
    out = process_watchlist_movers(delta_threshold=8.0)
    flash(f"Watchlist movers created {out.get('movers_created', 0)} rows.", "info")
    return redirect(url_for("analyst_desk.index"))


@bp.route("/briefing/<int:bid>")
def briefing_detail(bid: int):
    b = get_briefing(bid)
    if not b:
        flash("Briefing not found.", "error")
        return redirect(url_for("analyst_desk.index"))
    return render_template("analyst_desk/briefing.html", b=b)


@bp.route("/api/query")
def api_query():
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify({"ok": True, "query": q, "results": [], "count": 0})
    rows = query_events_text(q, limit=40)
    return jsonify({"ok": True, "query": q, "results": rows, "count": len(rows)})
