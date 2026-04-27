"""Ground regional analyst output on Geopolitiko articles (read-only via models)."""
from __future__ import annotations

import logging
import os
from typing import Any

from config import analyst_desk_heuristic_only

from app.analyst_desk.agents import AGENTS, get_agent
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
                {"role": "user", "content": user[:12000]},
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
    articles: list[dict[str, Any]],
    days: int,
    llm_note: str | None,
) -> str:
    """Structured draft from the article list when the LLM is unavailable."""
    lines = [
        "## Executive snapshot",
        f"- Region: **{agent['label']}** (ISO3: {', '.join(agent['countries'])}).",
        f"- **{len(articles)}** articles in the last **{days}** days matched this scope in the news DB.",
        "",
        "## Headlines & evidence (automated roll-up, not LLM prose)",
    ]
    if not articles:
        lines.append("_No articles in window._")
    else:
        for i, a in enumerate(articles[:30], 1):
            title = (a.get("title") or "(no title)").strip()
            src = (a.get("source_name") or "").strip()
            summ = ((a.get("summary") or "").replace("\n", " ").strip())[:300]
            kt = ((a.get("key_takeaways") or "").replace("\n", " ").strip())[:200]
            lines.append(f"{i}. **[{src}]** {title}")
            if summ:
                lines.append(f"   - _Summary:_ {summ}{'…' if len(summ) == 300 else ''}")
            if kt:
                lines.append(f"   - _Takeaways:_ {kt}{'…' if len(kt) == 200 else ''}")
    lines.append("")
    if analyst_desk_heuristic_only():
        lines.extend(
            [
                "## Mode",
                "_**Heuristic-only.** These regional agents aggregate headlines, summaries, and takeaways "
                "from Geopolitiko's database—no LLM, no API key._",
            ]
        )
    else:
        lines.append("## Why there is no narrative brief")
        if llm_note:
            lines.append(llm_note)
        else:
            lines.append("_Unknown reason._")
        lines.append("")
        lines.append(
            "Re-run **Daily volume** after fixing the issue above to get LLM-synthesized sections "
            "(Executive snapshot, Key developments, Watch items, Gaps)."
        )
    return "\n".join(lines)


def _fallback_weekly_country(
    agent: dict[str, Any],
    country_code: str,
    articles: list[dict[str, Any]],
    days: int,
    llm_note: str | None,
) -> str:
    lines = [
        "## Situation (evidence list)",
        f"- Focus: **{country_code}** under {agent['label']}.",
        f"- **{len(articles)}** articles in the last **{days}** days.",
        "",
        "## Article trail",
    ]
    if not articles:
        lines.append("_No articles in window._")
    else:
        for i, a in enumerate(articles[:28], 1):
            title = (a.get("title") or "(no title)").strip()
            src = (a.get("source_name") or "").strip()
            summ = ((a.get("summary") or "").replace("\n", " ").strip())[:280]
            lines.append(f"{i}. **[{src}]** {title}")
            if summ:
                lines.append(f"   - {summ}{'…' if len(summ) == 280 else ''}")
    lines.append("")
    if analyst_desk_heuristic_only():
        lines.extend(
            [
                "## Mode",
                "_**Heuristic-only** — article trail from the database; no LLM synthesis._",
            ]
        )
    else:
        lines.extend(["## LLM note", llm_note or "_Unknown._", ""])
        lines.append(
            "Re-run **Weekly** after setting `OPENAI_API_KEY` for Drivers / Scenarios / External angles prose."
        )
    return "\n".join(lines)


def _article_brief(a: dict[str, Any]) -> str:
    title = (a.get("title") or "").strip()
    src = (a.get("source_name") or "").strip()
    summ = (a.get("summary") or "")[:400]
    kt = (a.get("key_takeaways") or "")[:300]
    return f"- [{src}] {title}\n  {summ}\n  Takeaways: {kt}\n"


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
    }
    return articles, meta


def run_daily_volume(agent_id: str, days: int = 7) -> int | None:
    """Generate a regional daily-style volume brief; returns proposal id or None."""
    agent = get_agent(agent_id)
    if not agent:
        return None
    task_id = create_agent_task(agent_id, "daily_volume", {"days": days})
    try:
        articles, meta = _fetch_context(agent_id, days=days, limit=50)
        briefs = "\n".join(_article_brief(a) for a in articles[:35])
        if not briefs.strip():
            briefs = "(No articles matched this region in the selected window.)"

        system = f"""You are a senior geopolitical analyst for {agent['label']}.
Write a concise daily-style regional brief for internal clients.
Use only the evidence in the article list; if evidence is thin, say so.
Use clear markdown: ## Executive snapshot, ## Key developments (bullets), ## Watch items, ## Gaps / uncertainty.
Stay neutral and analytic; no policy prescriptions unless framed as options others are debating."""
        user = f"""Region: {agent['label']}
Countries (ISO3): {', '.join(agent['countries'])}

Recent headlines and summaries:
{briefs}
"""
        body, llm_err = _call_llm(system, user, max_tokens=2200)
        if not body:
            body = _fallback_daily_volume(agent, articles, days, llm_err)

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
            payload={"run_type": "daily_volume", "article_count": meta.get("article_count", 0)},
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
        meta: dict[str, Any] = {
            "agent_id": agent_id,
            "countries": agent["countries"],
            "focus_country": cc,
            "article_ids": [a.get("id") for a in articles if a.get("id")],
            "days": days,
            "article_count": len(articles),
        }
        briefs = "\n".join(_article_brief(a) for a in articles[:30])
        if not briefs.strip():
            briefs = "(No articles for this country in the window.)"

        system = f"""You are a senior geopolitical analyst covering {agent['label']}.
Produce a weekly-style country note (internal) with markdown sections:
## Situation, ## Drivers, ## External angles, ## Scenarios (2–3 short), ## What would change our view.
Ground claims in the provided clips only; flag uncertainty."""
        user = f"Focus country: {cc}\n\nArticle evidence:\n{briefs}"
        body, llm_err = _call_llm(system, user, max_tokens=2800)
        if not body:
            body = _fallback_weekly_country(agent, cc, articles, days, llm_err)

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
            payload={"run_type": "weekly_country", "country_code": cc, "article_count": meta.get("article_count", 0)},
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
