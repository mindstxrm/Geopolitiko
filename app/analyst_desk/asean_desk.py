"""ASEAN Desk — regional political–economic–security products for Analyst Desk.

Workspace scope is the union of ``southeast_asia_i`` / ``ii`` / ``iii`` plus an
institutional ASEAN tag. Daily / weekly / monthly drafts are proposals
(``pending``) until a human approves them for Terminal publish.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

ASEAN_10: tuple[str, ...] = (
    "BRN",
    "KHM",
    "IDN",
    "LAO",
    "MYS",
    "MMR",
    "PHL",
    "SGP",
    "THA",
    "VNM",
)
ASEAN_PLUS: tuple[str, ...] = ASEAN_10 + ("TLS",)
SEA_DESK_IDS: tuple[str, ...] = (
    "southeast_asia_i",
    "southeast_asia_ii",
    "southeast_asia_iii",
)
ASEAN_AGENT_ID = "asean"
DAILY_CUTOFF_HOUR_UTC = 8  # 16:00 SGT
DAILY_WINDOW_HOURS = 36

DAILY_SYSTEM_PROMPT = """You are the ASEAN Desk Analyst (regional political–economic–security intelligence).
You are an evidence-bound desk officer, not a pundit or advocate.

Write an internal Daily ASEAN Brief (600–1000 words or tight bullet equivalent).
Use ONLY the evidence supplied. If evidence is thin, say so. Label FACT / CLAIM / ASSESS.
Every material claim needs a source class and date when available.
Prefer change vs the prior window. Cap priority developments at 5–7 items.

Mandatory markdown structure:
# ASEAN Daily Brief — YYYY-MM-DD (UTC)
**Coverage window:** <start> → <end> UTC
**Desk confidence overall:** H/M/L
**Top line (3 bullets max):** ...

## 1. Priority developments (ranked)
For each item: What happened; Why it matters for ASEAN; Actors & geography (ISO3);
Confidence / sourcing (H/M/L + tier); Watch next.

## 2. ASEAN institutional / Chair track
## 3. Country notes (only movers)
## 4. Agreements & negotiations tracker (deltas only)
## 5. Macro & markets (if material)
## 6. Risk board (markdown table: Risk | Direction | Horizon | Confidence | Trigger to escalate)
## 7. Open questions / collection needs
## 8. Sources appendix

Do not invent quotations, URLs, or casualty figures. Do not write policy advocacy.
Mark the product as a draft for human review.
"""


def asean_scope_countries() -> list[str]:
    """ISO3 union of SEA desk agents (ASEAN-10 + TLS)."""
    from app.analyst_desk.agents import AGENTS

    seen: list[str] = []
    for aid in SEA_DESK_IDS:
        agent = AGENTS.get(aid) or {}
        for code in agent.get("countries") or []:
            cc = str(code).upper()
            if cc and cc not in seen:
                seen.append(cc)
    for cc in ASEAN_PLUS:
        if cc not in seen:
            seen.append(cc)
    return seen


def is_asean_country(code: str | None) -> bool:
    return str(code or "").upper() in set(asean_scope_countries())


def default_daily_window(
    now: datetime | None = None,
) -> tuple[datetime, datetime]:
    """Coverage window ending at the 08:00 UTC / 16:00 SGT daily cutoff when that has passed."""
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    cutoff = now.replace(hour=DAILY_CUTOFF_HOUR_UTC, minute=0, second=0, microsecond=0)
    if now < cutoff:
        cutoff = cutoff - timedelta(days=1)
    start = cutoff - timedelta(hours=DAILY_WINDOW_HOURS)
    return start, cutoff


def filter_asean_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    allowed = set(asean_scope_countries())
    out: list[dict[str, Any]] = []
    for ev in events or []:
        cc = str(ev.get("primary_country_code") or "").upper()
        region = str(ev.get("region") or "").lower()
        title = str(ev.get("event_title") or "").lower()
        if cc in allowed:
            out.append(ev)
            continue
        if "asean" in region or "southeast asia" in region:
            out.append(ev)
            continue
        if "asean" in title:
            out.append(ev)
    return out


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def heuristic_daily_brief(
    *,
    events: list[dict[str, Any]],
    articles: list[dict[str, Any]],
    window_start: datetime,
    window_end: datetime,
    llm_note: str | None = None,
) -> str:
    """SOP-shaped roll-up when the LLM is unavailable."""
    date_label = window_end.strftime("%Y-%m-%d")
    asean_events = filter_asean_events(events)
    lines = [
        f"# ASEAN Daily Brief — {date_label} (UTC)",
        f"**Coverage window:** {_iso(window_start)} → {_iso(window_end)} UTC",
        "**Desk confidence overall:** L",
        "**Status:** `pending_review` (heuristic roll-up from news DB / events; not a narrative assessment)",
        "",
        "**Top line (3 bullets max):**",
        f"- {len(asean_events)} ASEAN-scoped events and {len(articles)} scoped articles in the desk window.",
        "- Narrative synthesis requires human review or LLM mode; this is an evidence list.",
        "- Escalate only if a FLASH criterion is met (coup, kinetic SCS with casualties, capital-control shock, catastrophic disaster, leader assassination, emergency ASEAN meeting).",
        "",
        "## 1. Priority developments (ranked)",
    ]
    ranked = sorted(
        asean_events,
        key=lambda e: float(e.get("importance_score") or 0),
        reverse=True,
    )[:7]
    if not ranked:
        lines.append("_No normalized ASEAN events in window._")
    else:
        for i, e in enumerate(ranked, 1):
            lines.append(f"### {i}. {e.get('event_title') or 'Untitled'}")
            lines.append(
                f"- **What happened:** `{e.get('event_type') or 'general'}` "
                f"(score={float(e.get('importance_score') or 0):.1f})."
            )
            lines.append("- **Why it matters for ASEAN:** _Not assessed in heuristic mode._")
            lines.append(
                f"- **Actors & geography:** `{e.get('primary_country_code') or '—'}` "
                f"/ `{e.get('region') or '—'}`"
            )
            conf = e.get("confidence_score")
            lines.append(f"- **Confidence / sourcing:** M (event store; confidence_score={conf})")
            lines.append("- **Watch next:** corroborating official release or second independent outlet.")
            lines.append("")
    lines.extend(
        [
            "## 2. ASEAN institutional / Chair track",
            "_No institutional items extracted in heuristic mode unless titled in events above._",
            "",
            "## 3. Country notes (only movers)",
        ]
    )
    by_cc: dict[str, int] = {}
    for e in asean_events:
        cc = str(e.get("primary_country_code") or "UNK").upper()
        by_cc[cc] = by_cc.get(cc, 0) + 1
    if not by_cc:
        lines.append("_Quiet in event store._")
    else:
        for cc in sorted(by_cc):
            lines.append(f"- **{cc}:** {by_cc[cc]} event(s).")
    lines.extend(
        [
            "",
            "## 4. Agreements & negotiations tracker (deltas only)",
            "_Heuristic pass does not classify instruments; scan article trail below._",
            "",
            "## 5. Macro & markets (if material)",
            "_No macro strip in heuristic mode._",
            "",
            "## 6. Risk board",
            "| Risk | Direction | Horizon | Confidence | Trigger to escalate |",
            "|---|---|---|---|---|",
            "| Collection gap (empty or thin ASEAN event set) | → | 24h | L | FLASH criteria in SOP §8 |",
            "",
            "## 7. Open questions / collection needs",
            "- Pull ASEAN.org / Chair / MFA primaries for any item elevated after review.",
            "- Confirm FX/CPI prints from national statistical agencies before citing levels.",
            "",
            "## 8. Sources appendix",
        ]
    )
    if articles:
        for i, a in enumerate(articles[:25], 1):
            title = (a.get("title") or "(no title)").strip()
            src = (a.get("source_name") or "").strip()
            url = (a.get("url") or a.get("link") or "").strip()
            pub = (a.get("published_at") or a.get("published") or "").strip()
            extra = f" — {url}" if url else ""
            lines.append(f"{i}. [{src}] {title} ({pub}){extra}")
    else:
        lines.append("_No articles in Geopolitiko news DB for this scope/window._")
    if llm_note:
        lines.extend(["", "## Mode", llm_note])
    return "\n".join(lines)


def empty_sidecar(
    *,
    window_start: datetime,
    window_end: datetime,
    generated_at: datetime | None = None,
    countries: list[str] | None = None,
    overall_confidence: str = "L",
) -> dict[str, Any]:
    generated_at = generated_at or datetime.now(timezone.utc)
    return {
        "product": "daily",
        "desk": "asean",
        "window_start_utc": _iso(window_start),
        "window_end_utc": _iso(window_end),
        "generated_at_utc": _iso(generated_at),
        "overall_confidence": overall_confidence,
        "countries_touched": countries or [],
        "asean_institutional": True,
        "pending_review": True,
        "status": "pending_review",
        "priority_items": [],
        "agreements_delta": [],
        "risks": [],
        "open_questions": [],
        "sources": [],
    }


def sidecar_from_events(
    events: list[dict[str, Any]],
    articles: list[dict[str, Any]],
    window_start: datetime,
    window_end: datetime,
) -> dict[str, Any]:
    asean_events = filter_asean_events(events)
    ranked = sorted(
        asean_events,
        key=lambda e: float(e.get("importance_score") or 0),
        reverse=True,
    )[:7]
    countries = []
    for e in asean_events:
        cc = str(e.get("primary_country_code") or "").upper()
        if cc and cc not in countries:
            countries.append(cc)
    payload = empty_sidecar(
        window_start=window_start,
        window_end=window_end,
        countries=countries,
        overall_confidence="L" if not ranked else "M",
    )
    payload["priority_items"] = [
        {
            "title": e.get("event_title") or "",
            "summary": e.get("event_type") or "",
            "countries": [e.get("primary_country_code")] if e.get("primary_country_code") else [],
            "importance": min(5, max(1, round(float(e.get("importance_score") or 0) / 20))),
            "confidence": "M",
            "sources": [],
        }
        for e in ranked
    ]
    payload["sources"] = [
        {
            "title": a.get("title"),
            "publisher": a.get("source_name"),
            "url": a.get("url") or a.get("link"),
            "published_at": a.get("published_at") or a.get("published"),
        }
        for a in articles[:40]
    ]
    payload["open_questions"] = [
        "Heuristic ASEAN brief — human desk should replace with sourced narrative before approve."
    ]
    return payload


def run_asean_daily_brief(limit_events: int = 80, days: int = 2) -> dict[str, Any]:
    """Build an ASEAN-scoped daily briefing + pending proposal. Safe if DBs are empty."""
    from app.analyst_desk.generate import _article_brief, _call_llm
    from app.analyst_desk.store import (
        complete_agent_task,
        create_agent_task,
        create_briefing,
        emit_agent_message,
        fail_agent_task,
        insert_proposal,
        list_recent_events,
    )

    window_start, window_end = default_daily_window()
    task_id = create_agent_task(
        ASEAN_AGENT_ID,
        "asean.daily_brief",
        {"limit_events": limit_events, "days": days},
    )
    try:
        events = filter_asean_events(list_recent_events(limit=limit_events))
        articles: list[dict[str, Any]] = []
        try:
            from app.models import get_articles

            articles = get_articles(
                limit=50,
                countries_list=list(asean_scope_countries()),
                days=days,
            )
        except Exception:
            articles = []

        briefs = "\n".join(_article_brief(a) for a in articles[:35])
        event_lines = "\n".join(
            f"- [{e.get('event_type')}] {e.get('event_title')} "
            f"({e.get('primary_country_code')}, score={float(e.get('importance_score') or 0):.1f})"
            for e in events[:20]
        )
        user = (
            f"Window: {_iso(window_start)} → {_iso(window_end)} UTC\n"
            f"Countries: {', '.join(asean_scope_countries())}\n\n"
            f"Normalized events:\n{event_lines or '(none)'}\n\n"
            f"Article evidence:\n{briefs or '(none)'}\n"
        )
        body, llm_err = _call_llm(DAILY_SYSTEM_PROMPT, user, max_tokens=2800)
        if not body:
            body = heuristic_daily_brief(
                events=events,
                articles=articles,
                window_start=window_start,
                window_end=window_end,
                llm_note=llm_err,
            )

        date_label = window_end.strftime("%Y-%m-%d")
        summary = (
            f"ASEAN daily {date_label}: {len(events)} scoped events, "
            f"{len(articles)} articles. pending_review=true."
        )
        bid = create_briefing(
            briefing_type="daily",
            scope_type="region",
            scope_id="asean",
            title=f"ASEAN Daily Brief — {date_label}",
            summary=summary,
            body_markdown=body,
            generated_by_agent=ASEAN_AGENT_ID,
        )
        sidecar = sidecar_from_events(events, articles, window_start, window_end)
        pid = insert_proposal(
            agent_id=ASEAN_AGENT_ID,
            run_type="asean.daily_brief",
            title=f"ASEAN Daily Brief — {date_label}",
            body=body,
            source_context={
                "window_start_utc": _iso(window_start),
                "window_end_utc": _iso(window_end),
                "briefing_id": bid,
                "article_ids": [a.get("id") for a in articles if a.get("id")],
                "event_ids": [e.get("id") for e in events if e.get("id")],
                "sidecar": sidecar,
                "pending_review": True,
            },
            countries=list(asean_scope_countries()),
        )
        emit_agent_message(
            "briefing_required",
            from_agent_id=ASEAN_AGENT_ID,
            entity_type="proposal",
            entity_id=pid,
            payload={
                "run_type": "asean.daily_brief",
                "briefing_id": bid,
                "article_count": len(articles),
                "event_count": len(events),
            },
        )
        complete_agent_task(task_id)
        return {
            "briefing_id": bid,
            "proposal_id": pid,
            "events_used": len(events),
            "articles_used": len(articles),
            "pending_review": True,
            "sidecar": sidecar,
        }
    except Exception as e:
        fail_agent_task(task_id, str(e))
        raise


def dump_sidecar_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)
