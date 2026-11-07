"""Ground regional analyst output on Geopolitiko articles (read-only via models)."""
from __future__ import annotations

import logging
import os
from typing import Any

from config import analyst_desk_heuristic_only

from app.analyst_desk.agents import AGENTS, get_agent
from app.analyst_desk.desk_sops import (
    append_sources_appendix,
    build_numbered_sources,
    build_system_prompt,
    get_sop_for_agent,
)
from app.analyst_desk.eval_brief import evaluate_brief
from app.analyst_desk.store import (
    complete_agent_task,
    create_agent_task,
    emit_agent_message,
    fail_agent_task,
    insert_proposal,
)

logger = logging.getLogger(__name__)


def _call_llm(system: str, user: str, max_tokens: int = 2500) -> tuple[str | None, str | None]:
    """Returns (text, error_message). ``error_message`` is None on success."""
    if analyst_desk_heuristic_only():
        return None, (
            "**Heuristic-only mode** (`ANALYST_DESK_HEURISTIC_ONLY=1`): drafts are built from your "
            "news database only—no external AI."
        )
    api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return None, (
            "**No API key:** set `OPENAI_API_KEY` in your shell or in a `.env` file in the "
            "project root (next to `config.py`), then restart `run_analyst_desk.py`."
        )
    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user[:14000]},
            ],
            max_tokens=max_tokens,
        )
        text = (response.choices[0].message.content or "").strip()
        return (text if text else None), (None if text else "Model returned empty content.")
    except Exception as e:
        logger.warning("Analyst desk LLM call failed: %s", e)
        return None, f"**API error:** `{e}`"


def _fallback_daily_volume(
    agent: dict[str, Any],
    agent_id: str,
    articles: list[dict[str, Any]],
    sources: list[dict[str, Any]],
    days: int,
    llm_note: str | None,
) -> str:
    """Structured draft from the article list when the LLM is unavailable."""
    sop = get_sop_for_agent(agent_id)
    lines = [
        f"## Top line",
        f"- Region: **{agent['label']}** ({sop['label']}) — ISO3: {', '.join(agent['countries'])}.",
        f"- **{len(articles)}** articles in the last **{days}** days matched this scope.",
        f"- Overall confidence: **{'L' if len(articles) < 5 else 'M'}** (heuristic roll-up).",
        "",
        "## Priority developments",
    ]
    if not articles:
        lines.append("_No articles in window._")
    else:
        for i, a in enumerate(articles[:20], 1):
            sid = f"S{i}"
            title = (a.get("title") or "(no title)").strip()
            src = (a.get("source_name") or "").strip()
            summ = ((a.get("summary") or "").replace("\n", " ").strip())[:300]
            lines.append(f"{i}. **[{src}]** {title} [{sid}]")
            lines.append("   - CLAIM/FACT pending human review (automated roll-up).")
            if summ:
                lines.append(f"   - _Summary:_ {summ}{'…' if len(summ) == 300 else ''}")
    lines.extend(
        [
            "",
            "## Country notes",
            "_See evidence list; deepen on approve._",
            "",
            "## Risk board",
            "| Risk | Direction | Horizon | Confidence | Trigger |",
            "|---|---|---|---|---|",
            "| Evidence thin / coverage gaps | → | 7d | L | Spike in conflict or alliance news |",
            "",
            "## Open questions",
            "- Which items deserve multi-source confirmation before publish?",
            "",
        ]
    )
    if analyst_desk_heuristic_only():
        lines.extend(
            [
                "## Gaps / uncertainty",
                "_**Heuristic-only.** Aggregated from Geopolitiko DB—no LLM narrative._",
            ]
        )
    else:
        lines.extend(["## Gaps / uncertainty", llm_note or "_LLM unavailable._", ""])
    body = "\n".join(lines)
    return append_sources_appendix(body, sources)


def _fallback_weekly_country(
    agent: dict[str, Any],
    agent_id: str,
    country_code: str,
    articles: list[dict[str, Any]],
    sources: list[dict[str, Any]],
    days: int,
    llm_note: str | None,
) -> str:
    lines = [
        "## Executive summary",
        f"- Focus: **{country_code}** under {agent['label']}.",
        f"- **{len(articles)}** articles in the last **{days}** days.",
        "",
        "## Situation",
    ]
    if not articles:
        lines.append("_No articles in window._")
    else:
        for i, a in enumerate(articles[:18], 1):
            title = (a.get("title") or "(no title)").strip()
            src = (a.get("source_name") or "").strip()
            lines.append(f"- **[{src}]** {title} [S{i}]")
    lines.extend(
        [
            "",
            "## Drivers",
            "_Pending synthesis — see Sources._",
            "",
            "## Scenarios",
            "- **Base:** continued pattern of reported items (ASSESS).",
            "- **Upside / Downside:** insufficient evidence in heuristic mode.",
            "",
            "## What would change our view",
            "- Multi-source confirmation of any kinetic or elite-politics claim.",
            "",
        ]
    )
    if llm_note and not analyst_desk_heuristic_only():
        lines.extend(["## Gaps / uncertainty", llm_note, ""])
    body = "\n".join(lines)
    return append_sources_appendix(body, sources)


def _fetch_context(agent_id: str, days: int = 7, limit: int = 40) -> tuple[list[dict], dict[str, Any]]:
    from app.models import get_articles

    agent = get_agent(agent_id)
    if not agent:
        raise ValueError(f"Unknown agent: {agent_id}")
    countries = agent["countries"]
    articles = get_articles(
        limit=limit,
        countries_list=countries,
        days=days,
    )
    meta: dict[str, Any] = {
        "agent_id": agent_id,
        "countries": countries,
        "article_ids": [a.get("id") for a in articles if a.get("id")],
        "days": days,
        "article_count": len(articles),
        "desk_family": get_sop_for_agent(agent_id).get("family"),
    }
    return articles, meta


def _finalize_body(
    body: str,
    *,
    agent_id: str,
    sources: list[dict[str, Any]],
    weekly: bool,
    meta: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    body = append_sources_appendix(body, sources)
    evaluation = evaluate_brief(body, agent_id=agent_id, sources=sources, weekly=weekly)
    meta = dict(meta)
    meta["sources"] = sources
    meta["eval"] = evaluation
    meta["desk_family"] = evaluation.get("desk_family")
    return body, meta


def run_daily_volume(agent_id: str, days: int = 7) -> int | None:
    """Generate a regional daily-style volume brief; returns proposal id or None."""
    agent = get_agent(agent_id)
    if not agent:
        return None
    task_id = create_agent_task(agent_id, "daily_volume", {"days": days})
    try:
        articles, meta = _fetch_context(agent_id, days=days, limit=50)
        evidence_block, sources = build_numbered_sources(articles, limit=35)

        system = build_system_prompt(agent, agent_id, weekly=False)
        user = f"""Produce today's daily brief for {agent['label']}.

Numbered evidence (cite as [S1], [S2], …):
{evidence_block}
"""
        body, llm_err = _call_llm(system, user, max_tokens=2400)
        if not body:
            body = _fallback_daily_volume(agent, agent_id, articles, sources, days, llm_err)

        body, meta = _finalize_body(body, agent_id=agent_id, sources=sources, weekly=False, meta=meta)

        title = f"Daily volume — {agent['label']}"
        pid = insert_proposal(
            agent_id=agent_id,
            run_type="daily_volume",
            title=title,
            body=body,
            source_context=meta,
            countries=agent["countries"],
        )
        complete_agent_task(task_id)
        emit_agent_message(
            "briefing_required",
            from_agent_id=agent_id,
            entity_type="proposal",
            entity_id=pid,
            payload={
                "run_type": "daily_volume",
                "article_count": meta.get("article_count", 0),
                "eval_score": (meta.get("eval") or {}).get("score"),
                "desk_family": meta.get("desk_family"),
            },
        )
        return pid
    except Exception as e:
        fail_agent_task(task_id, str(e))
        raise


def run_weekly_country_deep_dive(agent_id: str, country_code: str, days: int = 14) -> int | None:
    """One-country weekly-style note within the agent's remit."""
    agent = get_agent(agent_id)
    if not agent or country_code.upper() not in agent["countries"]:
        return None
    cc = country_code.upper()
    task_id = create_agent_task(agent_id, "weekly_country", {"country_code": cc, "days": days})
    try:
        from app.models import get_articles

        articles = get_articles(limit=45, country=cc, days=days)
        evidence_block, sources = build_numbered_sources(articles, limit=30)
        meta: dict[str, Any] = {
            "agent_id": agent_id,
            "countries": agent["countries"],
            "focus_country": cc,
            "article_ids": [a.get("id") for a in articles if a.get("id")],
            "days": days,
            "article_count": len(articles),
            "desk_family": get_sop_for_agent(agent_id).get("family"),
        }

        system = build_system_prompt(agent, agent_id, weekly=True)
        user = f"""Produce a weekly-style country note.

Focus country: {cc}

Numbered evidence (cite as [S1], [S2], …):
{evidence_block}
"""
        body, llm_err = _call_llm(system, user, max_tokens=3000)
        if not body:
            body = _fallback_weekly_country(agent, agent_id, cc, articles, sources, days, llm_err)

        body, meta = _finalize_body(body, agent_id=agent_id, sources=sources, weekly=True, meta=meta)

        title = f"Weekly deep dive — {cc} ({agent['label']})"
        pid = insert_proposal(
            agent_id=agent_id,
            run_type="weekly_country",
            title=title,
            body=body,
            source_context=meta,
            countries=agent["countries"],
            focus_country=cc,
        )
        complete_agent_task(task_id)
        emit_agent_message(
            "briefing_required",
            from_agent_id=agent_id,
            entity_type="proposal",
            entity_id=pid,
            payload={
                "run_type": "weekly_country",
                "country_code": cc,
                "article_count": meta.get("article_count", 0),
                "eval_score": (meta.get("eval") or {}).get("score"),
            },
        )
        return pid
    except Exception as e:
        fail_agent_task(task_id, str(e))
        raise


def run_all_daily_volume() -> list[tuple[str, int | None]]:
    out: list[tuple[str, int | None]] = []
    for aid in AGENTS:
        pid = run_daily_volume(aid)
        out.append((aid, pid))
    return out
