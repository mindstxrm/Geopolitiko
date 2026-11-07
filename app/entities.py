"""
Lightweight entity registry beyond ISO3 countries.

Stable IDs:
  country:<ISO3>     — e.g. country:SGP
  org:<slug>         — e.g. org:asean, org:nato
  person:<slug>      — e.g. person:xi-jinping
  group:<slug>       — e.g. group:houthis

Resolution is alias-based (deterministic). Expand aliases over time.
"""
from __future__ import annotations

import re
from typing import Any

# Seed org / group / person aliases → canonical id
_ORG_ALIASES: dict[str, str] = {
    "asean": "org:asean",
    "association of southeast asian nations": "org:asean",
    "nato": "org:nato",
    "north atlantic treaty organization": "org:nato",
    "un": "org:un",
    "united nations": "org:un",
    "eu": "org:eu",
    "european union": "org:eu",
    "imf": "org:imf",
    "world bank": "org:world-bank",
    "opec": "org:opec",
    "pla": "org:pla",
    "people's liberation army": "org:pla",
    "idf": "org:idf",
    "pentagon": "org:us-dod",
    "us department of defense": "org:us-dod",
    "department of defense": "org:us-dod",
}

_GROUP_ALIASES: dict[str, str] = {
    "houthis": "group:houthis",
    "houthi": "group:houthis",
    "ansar allah": "group:houthis",
    "hamas": "group:hamas",
    "hezbollah": "group:hezbollah",
    "wagner": "group:wagner",
}

_PERSON_ALIASES: dict[str, str] = {
    "xi jinping": "person:xi-jinping",
    "vladimir putin": "person:vladimir-putin",
    "putin": "person:vladimir-putin",
    "joe biden": "person:joe-biden",
    "donald trump": "person:donald-trump",
    "benjamin netanyahu": "person:benjamin-netanyahu",
    "netanyahu": "person:benjamin-netanyahu",
}


def _slug(s: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", (s or "").strip().lower())
    return s.strip("-") or "unknown"


def country_entity_id(iso3: str | None) -> str | None:
    cc = (iso3 or "").strip().upper()
    if len(cc) == 3 and cc.isalpha():
        return f"country:{cc}"
    return None


def resolve_entity(name: str | None) -> dict[str, Any] | None:
    """Resolve a free-text name to a stable entity id + kind."""
    raw = (name or "").strip()
    if not raw:
        return None
    # ISO3 short-circuit
    if len(raw) == 3 and raw.isalpha():
        eid = country_entity_id(raw)
        return {"id": eid, "kind": "country", "label": raw.upper(), "matched": raw}

    key = re.sub(r"\s+", " ", raw.lower())
    if key in _ORG_ALIASES:
        eid = _ORG_ALIASES[key]
        return {"id": eid, "kind": "org", "label": raw, "matched": key}
    if key in _GROUP_ALIASES:
        eid = _GROUP_ALIASES[key]
        return {"id": eid, "kind": "group", "label": raw, "matched": key}
    if key in _PERSON_ALIASES:
        eid = _PERSON_ALIASES[key]
        return {"id": eid, "kind": "person", "label": raw, "matched": key}

    # Soft: "President Xi" etc — check contained aliases
    for alias, eid in _PERSON_ALIASES.items():
        if alias in key:
            return {"id": eid, "kind": "person", "label": raw, "matched": alias}
    for alias, eid in _ORG_ALIASES.items():
        if alias in key:
            return {"id": eid, "kind": "org", "label": raw, "matched": alias}
    for alias, eid in _GROUP_ALIASES.items():
        if alias in key:
            return {"id": eid, "kind": "group", "label": raw, "matched": alias}

    return None


def extract_entities_from_text(text: str, *, countries: list[str] | None = None) -> list[dict[str, Any]]:
    """Best-effort entity mentions from text + explicit country codes."""
    found: dict[str, dict[str, Any]] = {}
    for cc in countries or []:
        eid = country_entity_id(cc)
        if eid:
            found[eid] = {"id": eid, "kind": "country", "label": cc.upper(), "matched": cc}
    blob = text or ""
    # Scan known aliases (longest first)
    aliases = sorted(
        list(_ORG_ALIASES.items()) + list(_GROUP_ALIASES.items()) + list(_PERSON_ALIASES.items()),
        key=lambda x: -len(x[0]),
    )
    lower = blob.lower()
    for alias, eid in aliases:
        if alias in lower and eid not in found:
            kind = eid.split(":", 1)[0]
            found[eid] = {"id": eid, "kind": kind, "label": alias.title(), "matched": alias}
    return list(found.values())


def entity_catalog_summary() -> dict[str, Any]:
    return {
        "orgs": len(set(_ORG_ALIASES.values())),
        "groups": len(set(_GROUP_ALIASES.values())),
        "people": len(set(_PERSON_ALIASES.values())),
        "note": "Alias tables; expand via app/entities.py",
    }
