"""
Heuristic fills for country dashboard Operating Picture when LLM output is missing.

Uses the same regional scopes as Analyst Desk agents so "agent" countries get an explicit label.
No API keys required — built from aggregate_country_data (articles, scores, protests, etc.).
"""
from __future__ import annotations

from typing import Any, Optional

from app.analyst_desk.agents import AGENTS

# Union of all desk agent country ISO3 codes
_AGENT_COUNTRY_TO_LABEL: dict[str, str] = {}
for _aid, _cfg in AGENTS.items():
    _lbl = _cfg.get("label") or _aid
    for _cc in _cfg.get("countries") or []:
        _AGENT_COUNTRY_TO_LABEL[_cc.upper()] = _lbl


def agent_desk_label_for_country(country_code: str) -> Optional[str]:
    """Human-readable desk name if this ISO3 is covered by a fixed regional agent."""
    return _AGENT_COUNTRY_TO_LABEL.get((country_code or "").upper())


def build_heuristic_operating_picture(data: dict[str, Any], country_code: str) -> dict[str, Any]:
    """
    Build situation summary + bullets from structured dashboard data (no LLM).
    Returns keys: summary, bullets, fill_source, optional agent_desk.
    """
    name = (data.get("country_name") or country_code or "Country").strip()
    region = data.get("region") or "—"
    scores = data.get("scores") or {}
    cr = scores.get("combined_risk")
    ef = scores.get("economic_fragility")
    gf = scores.get("geopolitical_fragility")
    gepi = scores.get("gepi")
    cdei = scores.get("cdei")
    sfi = scores.get("sfi")
    frag = scores.get("fragility")

    summary_parts = [
        f"{name} ({region}) — operating snapshot from Geopolitiko indicators and news feed matches (no LLM).",
        f"Scores on file: combined risk {cr if cr is not None else '—'}, "
        f"economic fragility {ef if ef is not None else '—'}, geopolitical fragility {gf if gf is not None else '—'}.",
        f"Institutional lenses: GEPI {gepi if gepi is not None else '—'}, CDEI {cdei if cdei is not None else '—'}, "
        f"SFI {sfi if sfi is not None else '—'}, fragility index {frag if frag is not None else '—'}.",
        f"Sanctions records: {data.get('sanctions_count', 0)} · Treaties: {data.get('treaties_count', 0)}.",
    ]

    arts = data.get("articles") or []
    if arts:
        summary_parts.append(
            f"Recent {len(arts)} high-impact story line(s) in the feed are listed in the bullets below."
        )
    else:
        summary_parts.append(
            "_No high-impact articles matched this country in the current window; expand the feed or check entity tagging._"
        )

    el = data.get("elections") or []
    if el:
        summary_parts.append(
            "Elections on calendar: "
            + "; ".join(
                f"{e.get('type') or 'Election'} ({e.get('date') or '—'}, {e.get('status') or '—'})" for e in el[:3]
            )
            + "."
        )

    pr = data.get("protests") or []
    if pr:
        summary_parts.append(
            "Protest activity logged: "
            + "; ".join(f"{p.get('date') or '—'}: {(p.get('summary') or '')[:60]}" for p in pr[:2])
            + ("…" if len(pr) > 2 else "")
            + "."
        )

    ce = data.get("conflict_events") or []
    if ce:
        summary_parts.append(
            "Conflict / event imports: "
            + ", ".join(f"[{c.get('source')}] {c.get('date')} ({c.get('type')})" for c in ce[:3])
            + "."
        )

    cf = data.get("chokepoint_flows") or []
    if cf:
        summary_parts.append(
            "Chokepoint exposure: "
            + ", ".join(
                f"{f.get('chokepoint')} ({f.get('sector')}, {f.get('exposure_pct')}%)" for f in cf[:2]
            )
            + "."
        )

    bullets: list[str] = []
    for a in arts[:6]:
        title = (a.get("title") or "").strip()
        if not title:
            continue
        d = a.get("date") or ""
        imp = a.get("impact")
        bullets.append(f"{d} [impact {imp}] {title[:120]}{'…' if len(title) > 120 else ''}")

    if not bullets and data.get("sanctions_count"):
        bullets.append(f"{data['sanctions_count']} sanction(s) / restrictive measures tied to {name} — see Treaties & Diplomacy tab.")

    if not bullets and pr:
        bullets.append(f"Domestic: {len(pr)} protest event(s) on file — see Stability tab.")

    if not bullets and el:
        bullets.append("Electoral calendar entries present — see Stability tab for dates and status.")

    if not bullets:
        bullets.append("Use the tabbed indicators below; add news coverage or run jobs to improve feed linkage for this country.")

    agent_lbl = agent_desk_label_for_country(country_code)
    return {
        "summary": "\n\n".join(summary_parts),
        "bullets": bullets[:8],
        "fill_source": "heuristic_articles",
        "agent_desk": agent_lbl,
    }
