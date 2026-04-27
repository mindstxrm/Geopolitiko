"""Minimal Phase-1 pipeline for analyst desk multi-agent flow."""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Any

from app.analyst_desk.store import (
    create_alert,
    create_briefing,
    create_country_risk_snapshot,
    create_scenario,
    create_regional_risk_snapshot,
    delete_risk_traces,
    insert_risk_trace,
    create_watchlist_mover,
    get_country_recent_snapshots,
    create_agent_task,
    dequeue_agent_messages,
    emit_agent_message,
    ensure_source,
    fail_agent_task,
    get_raw_document,
    insert_extracted_metric,
    insert_raw_document,
    list_document_country_codes,
    list_enriched_documents_pending_metrics,
    mark_document_metrics_extracted,
    mark_message_processed,
    mark_alert_sent,
    mark_alert_failed,
    set_document_enrichment,
    list_recent_alerts,
    list_events_for_verification,
    list_recent_events,
    set_event_verification,
    latest_country_snapshot_map,
    upsert_event_from_document,
)
from app.analyst_desk.delivery import deliver_alert
from app.analyst_desk.metric_extract import extract_quantitative_signals
from app.analyst_desk.metric_llm_extract import llm_extract_metrics_from_news
from config import (
    analyst_desk_metric_llm_enabled,
    analyst_desk_metric_llm_max_docs,
    analyst_desk_metric_llm_merge_heuristic,
)


def _extract_countries(text: str, known_iso3: set[str]) -> list[tuple[str, float, str]]:
    hits: list[tuple[str, float, str]] = []
    upper = (text or "").upper()
    for cc in sorted(known_iso3):
        if cc in upper:
            hits.append((cc, 0.8, "mentioned"))
    return hits[:8]


def _extract_topics(text: str) -> list[tuple[str, float]]:
    base = (text or "").lower()
    mapping = {
        "military": ["military", "exercise", "naval", "troop", "missile", "strike"],
        "diplomacy": ["summit", "minister", "diplom", "talks", "agreement"],
        "trade": ["tariff", "trade", "export", "import", "supply chain"],
        "sanctions": ["sanction", "blacklist", "entity list", "export control"],
        "energy": ["oil", "gas", "lng", "pipeline", "opec"],
        "politics": ["election", "cabinet", "parliament", "protest", "party"],
        "finance": ["inflation", "debt", "bond", "rate", "currency"],
        "technology": ["chip", "semiconductor", "ai", "telecom", "cyber"],
    }
    out: list[tuple[str, float]] = []
    for topic, keys in mapping.items():
        if any(k in base for k in keys):
            out.append((topic, 0.72))
    if not out:
        out.append(("general", 0.55))
    return out[:6]


def _normalize_title_for_signature(title: str) -> str:
    t = re.sub(r"\s+", " ", (title or "").strip().lower())
    t = re.sub(r"[^a-z0-9 ]+", "", t)
    return t[:120]


def _event_signature(title: str, primary_country: str | None, event_type: str) -> str:
    base = f"{(primary_country or '').upper()}|{event_type}|{_normalize_title_for_signature(title)}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def ingest_from_news_articles(limit: int = 120, days: int = 2) -> dict[str, int]:
    """Ingestion agent: mirror recent main-db articles into desk raw_documents + queue."""
    task_id = create_agent_task("ingestion_news_scraper", "ingest_news", {"limit": limit, "days": days})
    try:
        from app.models import get_articles

        docs = get_articles(limit=limit, days=days)
        created = 0
        queued = 0
        for d in docs:
            source_name = (d.get("source_name") or "unknown").strip() or "unknown"
            source_id = ensure_source(source_name, "rss", d.get("source_url"))
            body = "\n".join(
                x for x in [d.get("title") or "", d.get("summary") or "", d.get("key_takeaways") or ""] if x
            )
            doc_id = insert_raw_document(
                source_id=source_id,
                external_ref=str(d.get("id")) if d.get("id") is not None else None,
                title=(d.get("title") or "").strip(),
                url=d.get("url"),
                published_at=d.get("published_utc") or d.get("scraped_at"),
                fetched_at=d.get("scraped_at"),
                raw_text=body,
                language="en",
                document_type="news",
            )
            if not doc_id:
                continue
            created += 1
            emit_agent_message(
                "document_ingested",
                from_agent_id="ingestion_news_scraper",
                entity_type="document",
                entity_id=doc_id,
                payload={"source": source_name},
            )
            queued += 1
        from app.analyst_desk.store import complete_agent_task

        complete_agent_task(task_id)
        return {"ingested": created, "queued": queued}
    except Exception as e:
        fail_agent_task(task_id, str(e))
        raise


def process_document_enrichment(limit: int = 150) -> dict[str, int]:
    """Preprocessing agents: translation/entity/topic placeholders -> document_enriched."""
    task_id = create_agent_task("preprocess_enrichment", "document_enrichment", {"limit": limit})
    try:
        from app.country_data import ALL_COUNTRIES

        known = {row[0] for row in (ALL_COUNTRIES or [])}
        msgs = dequeue_agent_messages(message_type="document_ingested", status="pending", limit=limit)
        processed = 0
        for m in msgs:
            doc_id = int(m.get("entity_id") or 0)
            if not doc_id:
                mark_message_processed(int(m["id"]), success=False)
                continue
            doc = get_raw_document(doc_id)
            if not doc:
                mark_message_processed(int(m["id"]), success=False)
                continue
            raw = (doc.get("raw_text") or "") + " " + (doc.get("title") or "")
            countries = _extract_countries(raw, known)
            topics = _extract_topics(raw)
            set_document_enrichment(doc_id, countries, topics)
            emit_agent_message(
                "document_enriched",
                from_agent_id="preprocess_enrichment",
                entity_type="document",
                entity_id=doc_id,
                payload={"countries": [c[0] for c in countries], "topics": [t[0] for t in topics]},
            )
            mark_message_processed(int(m["id"]), success=True)
            processed += 1
        from app.analyst_desk.store import complete_agent_task

        complete_agent_task(task_id)
        return {"processed": processed}
    except Exception as e:
        fail_agent_task(task_id, str(e))
        raise


def _dedupe_metric_hits(hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[Any, ...]] = set()
    out: list[dict[str, Any]] = []
    for h in hits:
        key = (
            h.get("metric_kind"),
            h.get("label"),
            h.get("value_numeric"),
            (h.get("snippet") or "")[:60],
            h.get("iso3_country"),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(h)
    return out


def _insert_metrics_for_doc(
    doc_id: int,
    hits: list[dict[str, Any]],
    targets: list[str | None],
) -> int:
    """Insert metric rows; LLM rows may carry iso3_country, heuristic rows fan out to targets."""
    n = 0
    for h in hits:
        llm_cc = (h.get("iso3_country") or "").strip().upper() or None
        if llm_cc:
            insert_extracted_metric(
                doc_id=doc_id,
                country_code=llm_cc,
                metric_kind=str(h.get("metric_kind") or "other"),
                label=h.get("label"),
                value_numeric=h.get("value_numeric"),
                value_text=h.get("value_text"),
                unit=h.get("unit"),
                snippet=h.get("snippet"),
                confidence=float(h.get("confidence") or 0.5),
            )
            n += 1
            continue
        for cc in targets:
            insert_extracted_metric(
                doc_id=doc_id,
                country_code=cc,
                metric_kind=str(h.get("metric_kind") or "other"),
                label=h.get("label"),
                value_numeric=h.get("value_numeric"),
                value_text=h.get("value_text"),
                unit=h.get("unit"),
                snippet=h.get("snippet"),
                confidence=float(h.get("confidence") or 0.5),
            )
            n += 1
    return n


def process_metric_extraction(limit: int = 120) -> dict[str, int]:
    """Extract quantitative signals from enriched documents (regex + optional LLM JSON schema)."""
    task_id = create_agent_task("metric_extractor", "metric_extraction", {"limit": limit})
    try:
        docs = list_enriched_documents_pending_metrics(limit=limit)
        metrics_written = 0
        llm_cap = analyst_desk_metric_llm_max_docs()
        use_llm = analyst_desk_metric_llm_enabled()
        merge_h = analyst_desk_metric_llm_merge_heuristic()
        for idx, doc in enumerate(docs):
            doc_id = int(doc["id"])
            text = doc.get("raw_text") or ""
            title = doc.get("title") or ""
            countries = list_document_country_codes(doc_id)
            # Avoid row explosion: attach each signal to top countries by relevance only.
            targets = countries[:4] if countries else [None]

            heuristic_hits = extract_quantitative_signals(text, title)
            for h in heuristic_hits:
                h["iso3_country"] = None

            llm_hits: list[dict[str, Any]] = []
            if use_llm and idx < llm_cap:
                llm_hits, _err = llm_extract_metrics_from_news(
                    title, text, suggested_iso3=countries
                )

            if llm_hits and not merge_h:
                combined = _dedupe_metric_hits(llm_hits)
            elif llm_hits and merge_h:
                combined = _dedupe_metric_hits(llm_hits + heuristic_hits)
            else:
                combined = heuristic_hits

            metrics_written += _insert_metrics_for_doc(doc_id, combined, targets)
            mark_document_metrics_extracted(doc_id)
            emit_agent_message(
                "score_updated",
                from_agent_id="metric_extractor",
                entity_type="document",
                entity_id=doc_id,
                payload={
                    "metrics": len(combined),
                    "llm": bool(llm_hits),
                    "countries": countries,
                },
            )
        from app.analyst_desk.store import complete_agent_task

        complete_agent_task(task_id)
        return {"docs_metrics_processed": len(docs), "metrics_written": metrics_written}
    except Exception as e:
        fail_agent_task(task_id, str(e))
        raise


def process_event_builder(limit: int = 150) -> dict[str, int]:
    """Country/regional monitor lane: convert enriched docs into normalized events + scores."""
    task_id = create_agent_task("country_monitor", "event_builder", {"limit": limit})
    try:
        msgs = dequeue_agent_messages(message_type="document_enriched", status="pending", limit=limit)
        created = 0
        updated = 0
        for m in msgs:
            doc_id = int(m.get("entity_id") or 0)
            if not doc_id:
                mark_message_processed(int(m["id"]), success=False)
                continue
            doc = get_raw_document(doc_id)
            if not doc:
                mark_message_processed(int(m["id"]), success=False)
                continue
            payload = m.get("payload") or {}
            countries = [str(c).upper() for c in (payload.get("countries") or [])]
            topics = [str(t) for t in (payload.get("topics") or [])]
            primary_cc = countries[0] if countries else None
            event_type = topics[0] if topics else "general"
            title = (doc.get("title") or "").strip() or f"Event from doc {doc_id}"
            summary = (doc.get("raw_text") or "")[:500]
            importance = 60.0 if primary_cc else 40.0
            if "military" in topics or "sanctions" in topics:
                importance = 75.0
            event_id = upsert_event_from_document(
                doc_id=doc_id,
                event_title=title[:220],
                event_summary=summary,
                event_type=event_type,
                primary_country_code=primary_cc,
                region=None,
                importance_score=importance,
                confidence_score=0.68,
                topics=topics,
                countries=[(c, "affected") for c in countries[:6]],
                created_by_agent="country_monitor",
                event_signature=_event_signature(title, primary_cc, event_type),
            )
            emit_agent_message(
                "event_updated",
                from_agent_id="country_monitor",
                entity_type="event",
                entity_id=event_id,
                priority="high" if importance >= 75 else "normal",
                payload={"importance_score": importance, "primary_country": primary_cc},
            )
            emit_agent_message(
                "score_updated",
                from_agent_id="event_scoring",
                entity_type="event",
                entity_id=event_id,
                payload={"importance_score": importance, "confidence_score": 0.68},
            )
            if importance >= 75:
                emit_agent_message(
                    "alert_required",
                    from_agent_id="event_scoring",
                    entity_type="event",
                    entity_id=event_id,
                    priority="critical",
                    payload={"reason": "importance_score_threshold", "score": importance},
                )
            mark_message_processed(int(m["id"]), success=True)
            created += 1
            updated += 1
        from app.analyst_desk.store import complete_agent_task

        complete_agent_task(task_id)
        return {"events_written": updated, "messages_consumed": created}
    except Exception as e:
        fail_agent_task(task_id, str(e))
        raise


def process_verification_checks(limit: int = 120) -> dict[str, int]:
    """Contradiction/verification agent: flag likely conflicting event narratives."""
    task_id = create_agent_task("verification_agent", "event_verification", {"limit": limit})
    try:
        rows = list_events_for_verification(limit=limit)
        verified = 0
        contradicted = 0
        pending = 0
        for e in rows:
            title = (e.get("event_title") or "").lower()
            summary = (e.get("event_summary") or "").lower()
            text = f"{title} {summary}"
            has_escalation = any(k in text for k in ("strike", "attack", "sanction", "exercise", "deployment"))
            has_deescalation = any(k in text for k in ("ceasefire", "talks", "resume", "agreement", "rollback"))
            has_uncertain = any(k in text for k in ("reportedly", "unconfirmed", "alleged", "unclear"))
            if has_escalation and has_deescalation:
                set_event_verification(
                    event_id=int(e["id"]),
                    status="contradicted",
                    confidence=0.45,
                    reason="Escalatory and de-escalatory cues co-exist in event text.",
                    checked_by_agent="verification_agent",
                )
                contradicted += 1
            elif has_uncertain:
                set_event_verification(
                    event_id=int(e["id"]),
                    status="pending",
                    confidence=0.4,
                    reason="Event contains uncertainty cues; hold for additional evidence.",
                    checked_by_agent="verification_agent",
                )
                pending += 1
            else:
                set_event_verification(
                    event_id=int(e["id"]),
                    status="verified",
                    confidence=0.68,
                    reason="No obvious contradiction pattern in title/summary heuristics.",
                    checked_by_agent="verification_agent",
                )
                verified += 1
        from app.analyst_desk.store import complete_agent_task

        complete_agent_task(task_id)
        return {"verified": verified, "contradicted": contradicted, "pending": pending}
    except Exception as e:
        fail_agent_task(task_id, str(e))
        raise


def process_alerts(limit: int = 200) -> dict[str, int]:
    """Output agent lane: materialize alert records for high-priority alert_required messages."""
    task_id = create_agent_task("alerting_agent", "alert_dispatch", {"limit": limit})
    try:
        msgs = dequeue_agent_messages(message_type="alert_required", status="pending", limit=limit)
        made = 0
        delivered = 0
        failed = 0
        for m in msgs:
            payload = m.get("payload") or {}
            event_id = int(m.get("entity_id") or 0) if m.get("entity_id") else None
            score = float(payload.get("score") or 0.0)
            sev = "critical" if score >= 80 else "high" if score >= 70 else "medium"
            headline = f"Alert: event {event_id or 'N/A'} crossed threshold ({score:.1f})"
            body = (
                f"Reason: {payload.get('reason') or 'threshold'}; score={score:.1f}. "
                "Generated by Analyst Desk alerting lane."
            )
            alert_id = create_alert(
                event_id=event_id,
                country_code=payload.get("primary_country"),
                region=None,
                alert_type="risk_threshold",
                severity=sev,
                headline=headline,
                body=body,
                channel="auto",
            )
            alert_row = {
                "id": alert_id,
                "event_id": event_id,
                "country_code": payload.get("primary_country"),
                "severity": sev,
                "alert_type": "risk_threshold",
                "headline": headline,
                "body": body,
                "created_at": None,
            }
            ok, channel, detail = deliver_alert(alert_row)
            if ok:
                mark_alert_sent(alert_id)
                delivered += 1
            else:
                mark_alert_failed(alert_id, detail)
                failed += 1
            mark_message_processed(int(m["id"]), success=True)
            made += 1
        from app.analyst_desk.store import complete_agent_task

        complete_agent_task(task_id)
        return {"alerts_created": made, "alerts_delivered": delivered, "alerts_failed": failed}
    except Exception as e:
        fail_agent_task(task_id, str(e))
        raise


def generate_daily_brief(limit_events: int = 20) -> dict[str, int | str]:
    """Daily brief agent: compile latest normalized events/alerts into one persisted briefing."""
    task_id = create_agent_task("daily_brief_agent", "daily_brief", {"limit_events": limit_events})
    try:
        events = list_recent_events(limit=limit_events)
        alerts = list_recent_alerts(limit=20)
        high = [e for e in events if float(e.get("importance_score") or 0) >= 75]
        summary = (
            f"{len(events)} tracked events in scope. "
            f"{len(high)} high-importance events. "
            f"{len([a for a in alerts if a.get('delivery_status') == 'sent'])} alerts delivered."
        )
        lines = [
            "# Daily Geopolitical Brief",
            "",
            "## Snapshot",
            f"- Events tracked: **{len(events)}**",
            f"- High-importance events (>=75): **{len(high)}**",
            f"- Alerts generated: **{len(alerts)}**",
            "",
            "## Top developments",
        ]
        if not events:
            lines.append("- No normalized events yet. Run pipeline stages first.")
        else:
            for e in events[:10]:
                lines.append(
                    f"- [{e.get('event_type') or 'general'}] "
                    f"{e.get('event_title') or 'Untitled'} "
                    f"(country={e.get('primary_country_code') or '—'}, score={float(e.get('importance_score') or 0):.1f})"
                )
        lines.extend(["", "## Alert status"])
        if not alerts:
            lines.append("- No alerts in current window.")
        else:
            for a in alerts[:8]:
                lines.append(
                    f"- {a.get('severity') or 'info'}: {a.get('headline') or '—'} "
                    f"[{a.get('delivery_status') or 'pending'}]"
                )
        body = "\n".join(lines)
        bid = create_briefing(
            briefing_type="daily",
            scope_type="global",
            scope_id="global",
            title="Daily Geopolitical Brief",
            summary=summary,
            body_markdown=body,
            generated_by_agent="daily_brief_agent",
        )
        emit_agent_message(
            "briefing_required",
            from_agent_id="daily_brief_agent",
            entity_type="briefing",
            entity_id=bid,
            payload={"briefing_type": "daily"},
        )
        from app.analyst_desk.store import complete_agent_task

        complete_agent_task(task_id)
        return {"briefing_id": bid, "events_used": len(events)}
    except Exception as e:
        fail_agent_task(task_id, str(e))
        raise


def process_topic_synthesis(limit_events: int = 120) -> dict[str, int]:
    """Topic agents: synthesize cross-region themes from normalized events."""
    task_id = create_agent_task("topic_synthesis_agent", "topic_synthesis", {"limit_events": limit_events})
    try:
        events = list_recent_events(limit=limit_events)
        buckets: dict[str, list[dict[str, Any]]] = {}
        for e in events:
            t = (e.get("event_type") or "general").strip().lower() or "general"
            buckets.setdefault(t, []).append(e)
        created = 0
        for topic, rows in buckets.items():
            avg = sum(float(r.get("importance_score") or 0.0) for r in rows) / max(1, len(rows))
            top = sorted(rows, key=lambda x: float(x.get("importance_score") or 0.0), reverse=True)[:6]
            lines = [f"# Topic brief: {topic}", "", f"- Events in window: **{len(rows)}**", f"- Avg importance: **{avg:.1f}**", "", "## Highlights"]
            for r in top:
                lines.append(
                    f"- {r.get('event_title') or '—'} (country={r.get('primary_country_code') or '—'}, score={float(r.get('importance_score') or 0):.1f})"
                )
            bid = create_briefing(
                briefing_type="topic",
                scope_type="topic",
                scope_id=topic,
                title=f"Topic brief — {topic}",
                summary=f"{len(rows)} events, avg importance {avg:.1f}",
                body_markdown="\n".join(lines),
                generated_by_agent="topic_synthesis_agent",
            )
            emit_agent_message(
                "briefing_required",
                from_agent_id="topic_synthesis_agent",
                entity_type="briefing",
                entity_id=bid,
                payload={"briefing_type": "topic", "topic": topic},
            )
            created += 1
        from app.analyst_desk.store import complete_agent_task

        complete_agent_task(task_id)
        return {"topic_briefings_created": created}
    except Exception as e:
        fail_agent_task(task_id, str(e))
        raise


def process_regional_synthesis(limit_events: int = 200) -> dict[str, int]:
    """Regional agents: compute regional risk snapshots from country events."""
    task_id = create_agent_task("regional_synthesis_agent", "regional_synthesis", {"limit_events": limit_events})
    try:
        from app.country_data import ALL_COUNTRIES

        cc_to_region = {row[0]: row[2] for row in (ALL_COUNTRIES or []) if row and len(row) > 2}
        events = list_recent_events(limit=limit_events)
        regions: dict[str, list[dict[str, Any]]] = {}
        for e in events:
            cc = (e.get("primary_country_code") or "").upper()
            region = cc_to_region.get(cc) or "Unknown"
            regions.setdefault(region, []).append(e)
        snapshot_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        written = 0
        for region, rows in regions.items():
            overall = sum(float(r.get("importance_score") or 0.0) for r in rows) / max(1, len(rows))
            type_counts: dict[str, int] = {}
            for r in rows:
                t = (r.get("event_type") or "general").lower()
                type_counts[t] = type_counts.get(t, 0) + 1
            driver = max(type_counts.items(), key=lambda x: x[1])[0] if type_counts else "general"
            summary = f"{len(rows)} events; avg importance {overall:.1f}; dominant driver {driver}."
            create_regional_risk_snapshot(
                region=region,
                snapshot_date=snapshot_date,
                overall_risk=overall,
                summary=summary,
                top_risk_driver=driver,
                generated_by_agent="regional_synthesis_agent",
            )
            emit_agent_message(
                "score_updated",
                from_agent_id="regional_synthesis_agent",
                entity_type="region",
                entity_id=region,
                payload={"overall_risk": overall, "top_driver": driver},
            )
            written += 1
        from app.analyst_desk.store import complete_agent_task

        complete_agent_task(task_id)
        return {"regional_snapshots_written": written}
    except Exception as e:
        fail_agent_task(task_id, str(e))
        raise


def process_country_risk_snapshots(limit_events: int = 250) -> dict[str, int]:
    """Country risk agent: compute per-country risk dimensions from normalized events."""
    task_id = create_agent_task("country_risk_agent", "country_risk_snapshots", {"limit_events": limit_events})
    try:
        events = list_recent_events(limit=limit_events)
        by_country: dict[str, list[dict[str, Any]]] = {}
        for e in events:
            cc = (e.get("primary_country_code") or "").upper()
            if not cc:
                continue
            by_country.setdefault(cc, []).append(e)
        snapshot_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        written = 0
        for cc, rows in by_country.items():
            scores = [float(r.get("importance_score") or 0.0) for r in rows]
            base = sum(scores) / max(1, len(scores))
            type_set = {(r.get("event_type") or "").lower() for r in rows}
            political_add = 8 if "politics" in type_set else 0
            conflict_add = 12 if ("military" in type_set or "security" in type_set) else 0
            sanctions_add = 12 if "sanctions" in type_set else 0
            macro_add = 7 if "finance" in type_set else 0
            supply_add = 7 if ("trade" in type_set or "energy" in type_set) else 0

            political = base + political_add
            conflict = base + conflict_add
            sanctions = base + sanctions_add
            macro = base + macro_add
            supply = base + supply_add
            overall = max(0.0, min(100.0, (political + conflict + sanctions + macro + supply) / 5.0))
            watch = "critical" if overall >= 80 else "high" if overall >= 65 else "watch" if overall >= 45 else "stable"

            # Explainability traces: record how much each normalized event contributed
            # to the five risk dimensions for this snapshot.
            delete_risk_traces(cc, snapshot_date)
            n = max(1, len(scores))
            base_share = base / n

            def _dim_for_event_type(et: str) -> str | None:
                et = (et or "").lower()
                if et == "politics":
                    return "political"
                if et in ("military", "security"):
                    return "conflict"
                if et == "sanctions":
                    return "sanctions"
                if et == "finance":
                    return "macro"
                if et in ("trade", "energy"):
                    return "supply"
                return None

            political_events = [r for r in rows if _dim_for_event_type(r.get("event_type")) == "political"]
            conflict_events = [r for r in rows if _dim_for_event_type(r.get("event_type")) == "conflict"]
            sanctions_events = [r for r in rows if _dim_for_event_type(r.get("event_type")) == "sanctions"]
            macro_events = [r for r in rows if _dim_for_event_type(r.get("event_type")) == "macro"]
            supply_events = [r for r in rows if _dim_for_event_type(r.get("event_type")) == "supply"]

            political_add_share = (political_add / max(1, len(political_events))) if political_add else 0.0
            conflict_add_share = (conflict_add / max(1, len(conflict_events))) if conflict_add else 0.0
            sanctions_add_share = (sanctions_add / max(1, len(sanctions_events))) if sanctions_add else 0.0
            macro_add_share = (macro_add / max(1, len(macro_events))) if macro_add else 0.0
            supply_add_share = (supply_add / max(1, len(supply_events))) if supply_add else 0.0

            top_events = sorted(rows, key=lambda x: float(x.get("importance_score") or 0.0), reverse=True)[:12]
            for er in top_events:
                eid = int(er.get("id") or 0)
                et = er.get("event_type") or ""
                dim = _dim_for_event_type(et)

                contrib_p = base_share + (political_add_share if dim == "political" else 0.0)
                contrib_c = base_share + (conflict_add_share if dim == "conflict" else 0.0)
                contrib_s = base_share + (sanctions_add_share if dim == "sanctions" else 0.0)
                contrib_m = base_share + (macro_add_share if dim == "macro" else 0.0)
                contrib_sup = base_share + (supply_add_share if dim == "supply" else 0.0)

                insert_risk_trace(
                    country_code=cc,
                    snapshot_date=snapshot_date,
                    event_id=eid,
                    dimension="political",
                    contribution=contrib_p,
                )
                insert_risk_trace(
                    country_code=cc,
                    snapshot_date=snapshot_date,
                    event_id=eid,
                    dimension="conflict",
                    contribution=contrib_c,
                )
                insert_risk_trace(
                    country_code=cc,
                    snapshot_date=snapshot_date,
                    event_id=eid,
                    dimension="sanctions",
                    contribution=contrib_s,
                )
                insert_risk_trace(
                    country_code=cc,
                    snapshot_date=snapshot_date,
                    event_id=eid,
                    dimension="macro",
                    contribution=contrib_m,
                )
                insert_risk_trace(
                    country_code=cc,
                    snapshot_date=snapshot_date,
                    event_id=eid,
                    dimension="supply",
                    contribution=contrib_sup,
                )

            create_country_risk_snapshot(
                country_code=cc,
                snapshot_date=snapshot_date,
                political_risk=min(100.0, political),
                conflict_risk=min(100.0, conflict),
                sanctions_risk=min(100.0, sanctions),
                macro_risk=min(100.0, macro),
                supply_chain_risk=min(100.0, supply),
                overall_risk=overall,
                watch_level=watch,
                generated_by_agent="country_risk_agent",
            )
            emit_agent_message(
                "score_updated",
                from_agent_id="country_risk_agent",
                entity_type="country",
                entity_id=cc,
                payload={"overall_risk": overall, "watch_level": watch},
            )
            written += 1
        from app.analyst_desk.store import complete_agent_task

        complete_agent_task(task_id)
        return {"country_snapshots_written": written}
    except Exception as e:
        fail_agent_task(task_id, str(e))
        raise


def process_watchlist_movers(delta_threshold: float = 8.0) -> dict[str, int]:
    """Track top movers by comparing latest and previous country risk snapshots."""
    task_id = create_agent_task("watchlist_movers_agent", "watchlist_movers", {"delta_threshold": delta_threshold})
    try:
        latest = latest_country_snapshot_map()
        movers = 0
        for cc, cur in latest.items():
            rows = get_country_recent_snapshots(cc, limit=2)
            if len(rows) < 2:
                continue
            current = float(rows[0].get("overall_risk") or 0.0)
            prev = float(rows[1].get("overall_risk") or 0.0)
            delta = current - prev
            if abs(delta) < delta_threshold:
                continue
            direction = "up" if delta > 0 else "down"
            reason = f"Overall risk moved {delta:+.1f} vs prior snapshot."
            create_watchlist_mover(
                country_code=cc,
                previous_overall_risk=prev,
                current_overall_risk=current,
                delta=abs(delta),
                direction=direction,
                reason=reason,
                snapshot_date=rows[0].get("snapshot_date") or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
            emit_agent_message(
                "risk_threshold_crossed",
                from_agent_id="watchlist_movers_agent",
                entity_type="country",
                entity_id=cc,
                priority="high" if abs(delta) >= 15 else "normal",
                payload={"metric": "overall_risk_delta", "old_value": prev, "new_value": current},
            )
            movers += 1
        from app.analyst_desk.store import complete_agent_task

        complete_agent_task(task_id)
        return {"movers_created": movers}
    except Exception as e:
        fail_agent_task(task_id, str(e))
        raise


def process_scenarios() -> dict[str, int]:
    """Scenario agent: generate base/upside/downside for each latest country snapshot."""
    task_id = create_agent_task("scenario_agent", "scenario_generation", {"scope": "latest_countries"})
    try:
        snapshot_map = latest_country_snapshot_map()
        events = list_recent_events(limit=600)
        by_country: dict[str, list[dict[str, Any]]] = {}
        for e in events:
            cc = (e.get("primary_country_code") or "").upper()
            if cc:
                by_country.setdefault(cc, []).append(e)

        created = 0
        for cc, snap in snapshot_map.items():
            cc_events = by_country.get(cc, [])
            watch = (snap.get("watch_level") or "stable").lower()
            political = float(snap.get("political_risk") or 0.0)
            conflict = float(snap.get("conflict_risk") or 0.0)
            sanctions = float(snap.get("sanctions_risk") or 0.0)
            macro = float(snap.get("macro_risk") or 0.0)
            supply = float(snap.get("supply_chain_risk") or 0.0)

            dim_pairs = [
                ("political", political),
                ("conflict", conflict),
                ("sanctions", sanctions),
                ("macro", macro),
                ("supply", supply),
            ]
            dim_pairs = sorted(dim_pairs, key=lambda x: x[1], reverse=True)
            dominant_dims = [d for d, _ in dim_pairs[:2]]

            type_scores: dict[str, float] = {}
            for e in cc_events:
                et = (e.get("event_type") or "general").lower()
                type_scores[et] = type_scores.get(et, 0.0) + float(e.get("importance_score") or 0.0)
            top_types = [t for t, _ in sorted(type_scores.items(), key=lambda x: -x[1])[:4]]

            if watch == "critical":
                prob_base, prob_up, prob_down = 0.35, 0.20, 0.45
            elif watch == "high":
                prob_base, prob_up, prob_down = 0.45, 0.25, 0.30
            elif watch == "watch":
                prob_base, prob_up, prob_down = 0.55, 0.25, 0.20
            else:
                prob_base, prob_up, prob_down = 0.60, 0.20, 0.20

            def _triggers_for_scenario(s_type: str) -> list[str]:
                triggers: list[str] = []
                joined = " ".join(top_types).lower()
                if "sanctions" in joined or "export" in joined:
                    if s_type == "upside":
                        triggers.append("Sanctions/export-control enforcement eases or faces carve-outs.")
                    elif s_type == "downside":
                        triggers.append("New sanctions/export controls tighten compliance requirements.")
                    else:
                        triggers.append("Sanctions enforcement remains broadly stable (watch for rule changes).")
                if any(t in joined for t in ("military", "security")):
                    if s_type == "upside":
                        triggers.append("Fewer military/security incidents and reduced operational tempo.")
                    elif s_type == "downside":
                        triggers.append("Military/security incident frequency increases or escalates in scope.")
                    else:
                        triggers.append("Military/security activity stays within current escalation band.")
                if "politics" in joined:
                    if s_type == "upside":
                        triggers.append("Improved domestic political stability and policy coherence.")
                    elif s_type == "downside":
                        triggers.append("Political friction or governance shocks raise uncertainty and risk tolerance declines.")
                    else:
                        triggers.append("Domestic politics remains stable-to-mixed; monitor leadership decisions.")
                if any(t in joined for t in ("trade", "energy", "finance")):
                    if s_type == "upside":
                        triggers.append("Trade/financial friction moderates and macro channels normalize.")
                    elif s_type == "downside":
                        triggers.append("Trade/financial friction worsens; macro stress transmits to markets.")
                    else:
                        triggers.append("Macro/market channels remain sensitive; watch debt, currency, and trade controls.")
                if not triggers:
                    triggers = [
                        "No major shift in the dominant risk drivers; watch for discrete catalysts.",
                        "Re-check event stream for sudden changes in event_type distribution.",
                        "Cross-validate risk traces against protests/sanctions/conflict indicators.",
                    ]
                return triggers[:5]

            def _scenario_text(s_type: str) -> str:
                dominant = ", ".join(dominant_dims) if dominant_dims else "general"
                ev_title = cc_events[0].get("event_title") if cc_events else None
                ev_hint = f"Latest notable event: {ev_title}." if ev_title else "Event stream indicates ongoing monitoring."
                if s_type == "base":
                    return (
                        f"BASE CASE for {cc}: Risk remains anchored by {dominant}. "
                        f"Expectation: most indicators change gradually rather than abruptly. {ev_hint}\n\n"
                        f"Key drivers: dominant themes in the last window are: {', '.join(top_types) or 'general'}.\n"
                        f"Net effect: watch level stays near current band unless a catalyst emerges."
                    )
                if s_type == "upside":
                    return (
                        f"UPSIDE CASE for {cc}: Conditions improve and downside tail risk recedes. "
                        f"Risk factors related to {dominant} soften via implementation, compliance relief, or de-escalation.\n\n"
                        f"{ev_hint}\n"
                        f"Main upside mechanism: favorable movement in {', '.join(top_types) or 'risk drivers'}."
                    )
                return (
                    f"DOWNside CASE for {cc}: Risk compounds and escalation pathways strengthen. "
                    f"Expect faster-than-normal transmission from {dominant} into market and policy channels.\n\n"
                    f"{ev_hint}\n"
                    f"Main downside mechanism: unfavorable movement in {', '.join(top_types) or 'risk drivers'}."
                )

            base_triggers = _triggers_for_scenario("base")
            up_triggers = _triggers_for_scenario("upside")
            down_triggers = _triggers_for_scenario("downside")

            s_base = create_scenario(
                scope_type="country",
                scope_id=cc,
                scenario_type="base",
                probability_estimate=prob_base,
                scenario_text=_scenario_text("base"),
                trigger_conditions=base_triggers,
                generated_by_agent="scenario_agent",
            )
            s_up = create_scenario(
                scope_type="country",
                scope_id=cc,
                scenario_type="upside",
                probability_estimate=prob_up,
                scenario_text=_scenario_text("upside"),
                trigger_conditions=up_triggers,
                generated_by_agent="scenario_agent",
            )
            s_down = create_scenario(
                scope_type="country",
                scope_id=cc,
                scenario_type="downside",
                probability_estimate=prob_down,
                scenario_text=_scenario_text("downside"),
                trigger_conditions=down_triggers,
                generated_by_agent="scenario_agent",
            )

            # Notify output lane/audit systems (optional for MVP)
            emit_agent_message(
                "scenario_required",
                from_agent_id="scenario_agent",
                entity_type="scenario",
                entity_id=s_base,
                priority="normal",
                payload={"scope_type": "country", "scope_id": cc, "watch_level": watch},
            )
            created += 3

        from app.analyst_desk.store import complete_agent_task

        complete_agent_task(task_id)
        return {"scenarios_created": created}
    except Exception as e:
        fail_agent_task(task_id, str(e))
        raise

