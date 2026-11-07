"""Lightweight brief quality eval (structure, citations, confidence hygiene)."""
from __future__ import annotations

import re
from typing import Any

from app.analyst_desk.desk_sops import get_sop_for_agent

_CITATION_RE = re.compile(r"\[S(\d+)\]", re.IGNORECASE)
_SECTION_RE = re.compile(r"^##\s+(.+)$", re.MULTILINE)


def _norm_heading(h: str) -> str:
    return re.sub(r"\s+", " ", (h or "").strip().lower())


def evaluate_brief(
    body: str,
    *,
    agent_id: str,
    sources: list[dict[str, Any]] | None = None,
    weekly: bool = False,
) -> dict[str, Any]:
    """
    Return {score 0-100, grade A-F, checks[], citation_ids[], missing_sections[]}.
    Deterministic heuristics only — no LLM.
    """
    text = body or ""
    sop = get_sop_for_agent(agent_id)
    required = sop["weekly_sections"] if weekly else sop["daily_sections"]
    found_headings = [_norm_heading(m) for m in _SECTION_RE.findall(text)]
    missing = []
    for sec in required:
        key = _norm_heading(sec)
        # allow partial match (e.g. "Sources appendix" vs "Sources")
        if not any(key in h or h in key for h in found_headings):
            missing.append(sec)

    citations = sorted({int(n) for n in _CITATION_RE.findall(text)})
    source_count = len(sources or [])
    cited_ok = bool(citations) if source_count else True
    # If there was evidence, require at least one citation when body is long enough
    if source_count >= 3 and len(text) > 400 and not citations:
        cited_ok = False

    has_sources_section = any("source" in h for h in found_headings)
    has_labeling = bool(
        re.search(r"\b(FACT|CLAIM|ASSESS)\b", text)
        or re.search(r"\b(confidence|uncertain|single-source|unverified)\b", text, re.I)
    )
    has_gaps = any(
        x in " ".join(found_headings)
        for x in ("gap", "open question", "uncertainty", "what would change")
    )

    checks = [
        {
            "id": "required_sections",
            "ok": len(missing) == 0,
            "detail": "all present" if not missing else f"missing: {', '.join(missing)}",
        },
        {
            "id": "citations",
            "ok": cited_ok,
            "detail": f"{len(citations)} citation marker(s); {source_count} evidence items",
        },
        {
            "id": "sources_section",
            "ok": has_sources_section or source_count == 0,
            "detail": "Sources heading present" if has_sources_section else "no ## Sources heading",
        },
        {
            "id": "uncertainty_hygiene",
            "ok": has_labeling or has_gaps or source_count < 3,
            "detail": (
                "FACT/CLAIM/ASSESS or uncertainty language found"
                if (has_labeling or has_gaps)
                else "no explicit uncertainty labeling"
            ),
        },
        {
            "id": "non_empty",
            "ok": len(text.strip()) >= 120,
            "detail": f"{len(text.strip())} chars",
        },
    ]

    try:
        from app.analyst_desk.confirmation import gate_brief_for_confirmation

        conf_check = gate_brief_for_confirmation(text, sources)
        checks.append(
            {
                "id": conf_check["id"],
                "ok": conf_check["ok"],
                "detail": conf_check["detail"],
            }
        )
        confirmation = conf_check.get("assessment")
    except Exception:
        confirmation = None

    # Score (partial credit for section coverage)
    present_ratio = 1.0
    if required:
        present_ratio = max(0.0, 1.0 - (len(missing) / len(required)))
    score = int(28 * present_ratio)
    if cited_ok:
        score += 28
    if has_sources_section or source_count == 0:
        score += 14
    if has_labeling or has_gaps or source_count < 3:
        score += 14
    if len(text.strip()) >= 120:
        score += 8
    if confirmation is None or confirmation.get("status") in ("not_applicable", "confirmed"):
        score += 8
    score = max(0, min(100, int(score)))
    if score >= 90:
        grade = "A"
    elif score >= 80:
        grade = "B"
    elif score >= 70:
        grade = "C"
    elif score >= 55:
        grade = "D"
    else:
        grade = "F"

    return {
        "score": score,
        "grade": grade,
        "desk_family": sop.get("family"),
        "checks": checks,
        "missing_sections": missing,
        "citation_ids": citations,
        "source_count": source_count,
        "confirmation": confirmation,
        "pass": score >= 70,
    }
