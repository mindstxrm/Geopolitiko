"""Push human-approved Analyst Desk proposals into the main Terminal database (news.db)."""
from __future__ import annotations

import json
import logging
from typing import Any

from config import DATABASE_PATH, analyst_desk_publish_on_approve

logger = logging.getLogger(__name__)


def publish_approved_proposal_to_terminal(proposal_id: int) -> dict[str, Any]:
    """
    Insert/update desk_terminal_intel in news.db. Call only after status is 'approved'.
    Returns {ok, proposal_id, error?}.
    """
    if not analyst_desk_publish_on_approve():
        return {"ok": True, "proposal_id": proposal_id, "skipped": True, "reason": "publish disabled"}

    from app.analyst_desk.store import (
        get_proposal,
        list_extracted_metrics_for_countries,
        mark_proposal_published_to_terminal,
    )
    from app.models import upsert_desk_terminal_intel

    prop = get_proposal(proposal_id)
    if not prop:
        return {"ok": False, "proposal_id": proposal_id, "error": "proposal not found"}
    if (prop.get("status") or "").strip().lower() != "approved":
        return {"ok": False, "proposal_id": proposal_id, "error": "not approved"}

    countries = prop.get("countries") or []
    if not isinstance(countries, list):
        countries = []
    countries = [str(c).strip().upper() for c in countries if c]
    focus = (prop.get("focus_country") or "").strip().upper() or None
    if not countries and focus:
        countries = [focus]

    body = (prop.get("reviewed_body") or prop.get("body") or "").strip()
    metrics = list_extracted_metrics_for_countries(countries, days=14, limit=100) if countries else []
    metrics_compact = [
        {
            "kind": m.get("metric_kind"),
            "label": m.get("label"),
            "value": m.get("value_numeric"),
            "text": m.get("value_text"),
            "unit": m.get("unit"),
            "country": m.get("country_code"),
            "snippet": (m.get("snippet") or "")[:180],
            "doc_title": m.get("doc_title"),
            "doc_url": m.get("doc_url"),
        }
        for m in metrics
    ]

    countries_csv = ",".join(sorted(set(countries)))

    try:
        upsert_desk_terminal_intel(
            proposal_id=proposal_id,
            agent_id=str(prop.get("agent_id") or ""),
            run_type=str(prop.get("run_type") or ""),
            title=(prop.get("title") or "")[:500] or None,
            body_markdown=body,
            countries_csv=countries_csv or None,
            focus_country=focus,
            metrics_json=json.dumps(metrics_compact, ensure_ascii=False),
            reviewer_note=(prop.get("reviewer_note") or "")[:2000] or None,
            database_path=DATABASE_PATH,
        )
        mark_proposal_published_to_terminal(proposal_id)
        return {"ok": True, "proposal_id": proposal_id, "metrics_attached": len(metrics_compact)}
    except Exception as e:
        logger.exception("publish_approved_proposal_to_terminal failed")
        return {"ok": False, "proposal_id": proposal_id, "error": str(e)}
