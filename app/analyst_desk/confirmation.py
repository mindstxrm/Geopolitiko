"""
Multi-source confirmation rules before conflict/casualty/security claims
are treated as brief-ready.
"""
from __future__ import annotations

import re
from typing import Any

# Claims that require ≥2 distinct sources (or ≥1 Tier-A + 1 other) before "confirmed"
_SENSITIVE_PATTERNS = (
    r"\bcasualt(?:y|ies)\b",
    r"\bkilled\b",
    r"\bdead\b",
    r"\bstrike(?:s)?\b",
    r"\bmissile\b",
    r"\binvasion\b",
    r"\bcoup\b",
    r"\bnuclear\b",
    r"\bchemical\b",
    r"\bgenocide\b",
    r"\bwar crime",
)

_SENSITIVE_RE = re.compile("|".join(_SENSITIVE_PATTERNS), re.IGNORECASE)


def text_needs_confirmation(text: str) -> bool:
    return bool(_SENSITIVE_RE.search(text or ""))


def assess_confirmation(
    *,
    text: str,
    sources: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Return confirmation assessment:
      status: confirmed | single_source | unconfirmed | not_applicable
      needs_confirmation, source_count, tier_a_count, distinct_names, reason
    """
    sources = sources or []
    needs = text_needs_confirmation(text)
    names = []
    tiers = []
    for s in sources:
        name = (s.get("name") or s.get("source_name") or s.get("title") or "").strip()
        if name:
            names.append(name.lower())
        tier = (s.get("tier") or "").strip().upper()
        if tier:
            tiers.append(tier)

    distinct = len(set(names))
    tier_a = sum(1 for t in tiers if t == "A")
    out: dict[str, Any] = {
        "needs_confirmation": needs,
        "source_count": len(sources),
        "distinct_sources": distinct,
        "tier_a_count": tier_a,
        "status": "not_applicable",
        "reason": "No sensitive claim patterns detected.",
    }
    if not needs:
        return out

    if distinct >= 2 or (tier_a >= 1 and distinct >= 1 and len(sources) >= 2):
        out["status"] = "confirmed"
        out["reason"] = f"{distinct} distinct sources (Tier-A={tier_a})."
    elif distinct == 1 or len(sources) == 1:
        out["status"] = "single_source"
        out["reason"] = "Sensitive claim with only one source — hold for corroboration."
    else:
        out["status"] = "unconfirmed"
        out["reason"] = "Sensitive claim without adequate multi-source backing."
    return out


def gate_brief_for_confirmation(
    body: str,
    sources: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Attach confirmation gate result suitable for eval / review UI."""
    assessment = assess_confirmation(text=body, sources=sources)
    ok = assessment["status"] in ("not_applicable", "confirmed")
    return {
        "id": "multi_source_confirmation",
        "ok": ok,
        "detail": assessment["reason"],
        "assessment": assessment,
    }
