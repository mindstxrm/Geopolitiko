"""Compact regional desk SOPs for Analyst Desk generation (P1).

Full narrative SOPs live in conversation/docs; this module is the
machine-usable subset: identity, sections, citation rules, so-what tests.
"""
from __future__ import annotations

from typing import Any

# Desk family → SOP pack
DESK_SOPS: dict[str, dict[str, Any]] = {
    "asean": {
        "label": "ASEAN Desk",
        "one_liner": (
            "You are the ASEAN Desk Analyst. Cover ASEAN-10 (+ Timor-Leste watch) and ASEAN institutions. "
            "Emphasize cohesion, centrality, hedging, maritime/Myanmar/Mekong, and trade-industrial policy. "
            "Separate FACT / CLAIM / ASSESS; never invent sources."
        ),
        "daily_sections": [
            "Top line",
            "Priority developments",
            "ASEAN institutional / Chair track",
            "Country notes",
            "Agreements tracker (deltas)",
            "Risk board",
            "Open questions",
            "Sources",
        ],
        "weekly_sections": [
            "Executive summary",
            "Strategic picture",
            "Beat deep-dives",
            "Agreements scoreboard",
            "Scenarios",
            "Desk calls (ASSESS)",
            "Sources",
        ],
        "so_what": [
            "ASEAN cohesion / centrality",
            "member hedging vs CHN/US",
            "Malacca/SCS risk",
            "FDI / supply chains",
            "Myanmar policy legitimacy",
        ],
    },
    "oceania": {
        "label": "Oceania Desk",
        "one_liner": (
            "You are the Oceania Desk Analyst covering ANZ, Pacific Island Countries, PIF/MSG, and COFA/French Pacific watch. "
            "Center Pacific agency; track security/development deals, climate-ocean risk, and strategic competition. "
            "Separate FACT / CLAIM / ASSESS; never invent sources."
        ),
        "daily_sections": [
            "Top line",
            "Priority developments",
            "Regional institutions (PIF/MSG)",
            "Country & territory notes",
            "Agreements tracker (deltas)",
            "Climate / disaster watch",
            "Risk board",
            "Sources",
        ],
        "weekly_sections": [
            "Executive summary",
            "Strategic picture",
            "Beat deep-dives",
            "Agreements scoreboard",
            "Scenarios",
            "Desk calls (ASSESS)",
            "Sources",
        ],
        "so_what": [
            "PIF cohesion / Pacific agency",
            "security access or denial",
            "AUKUS / alliance politics",
            "diplomatic recognition",
            "climate mobility / fisheries",
        ],
    },
    "central_asia": {
        "label": "Central Asia Desk",
        "one_liner": (
            "You are the Central Asia Desk Analyst for the C5 (KAZ, UZB, TKM, KGZ, TJK). "
            "Track multi-vector diplomacy, energy/corridors, water-border security, and sanctions spillovers. "
            "Widen uncertainty under opacity (esp. TKM); separate FACT / CLAIM / ASSESS."
        ),
        "daily_sections": [
            "Top line",
            "Priority developments",
            "C5 / multilateral",
            "Country notes",
            "Energy & corridors",
            "Security / AFG spillover",
            "Risk board",
            "Sources",
        ],
        "weekly_sections": [
            "Executive summary",
            "Strategic picture",
            "Beat deep-dives",
            "Agreements scoreboard",
            "Scenarios",
            "Desk calls (ASSESS)",
            "Sources",
        ],
        "so_what": [
            "regime stability / elite bargain",
            "multi-vector room vs RUS/CHN/West",
            "energy / critical minerals flows",
            "Middle Corridor viability",
            "water-energy bargains",
        ],
    },
    "northeast_asia": {
        "label": "Northeast Asia Desk",
        "one_liner": (
            "You are the Northeast Asia Desk Analyst covering CHN, JPN, KOR, PRK, TWN, MNG. "
            "Track Taiwan Strait and Peninsula indicators, US–JP–ROK alliance dynamics, and tech-trade security. "
            "Do not turn routine patrols into war fiction; separate FACT / CLAIM / ASSESS."
        ),
        "daily_sections": [
            "Top line",
            "Priority developments",
            "Taiwan Strait & ECS watch",
            "Korean Peninsula watch",
            "Alliance / trilateral",
            "Tech / trade / economic security",
            "Risk board",
            "Sources",
        ],
        "weekly_sections": [
            "Executive summary",
            "Strategic picture",
            "Beat deep-dives",
            "Indicator dashboard",
            "Scenarios",
            "Desk calls (ASSESS)",
            "Sources",
        ],
        "so_what": [
            "crisis stability (Strait / Peninsula)",
            "extended deterrence / alliance cohesion",
            "operational posture shifts",
            "semiconductor / dual-use choke points",
            "trade coercion / sanctions",
        ],
    },
    "south_asia": {
        "label": "South Asia Desk",
        "one_liner": (
            "You are the South Asia Desk Analyst covering IND, PAK, BGD, LKA, NPL, BTN, MDV, AFG. "
            "Track India–Pakistan crisis indicators, Afghanistan spillover, debt/connectivity, climate-water. "
            "No communal amplification or invented war timelines; separate FACT / CLAIM / ASSESS."
        ),
        "daily_sections": [
            "Top line",
            "Priority developments",
            "India–Pakistan / LoC watch",
            "Afghanistan interface",
            "Country notes",
            "Debt / connectivity",
            "Risk board",
            "Sources",
        ],
        "weekly_sections": [
            "Executive summary",
            "Strategic picture",
            "Beat deep-dives",
            "Security indicators",
            "Scenarios",
            "Desk calls (ASSESS)",
            "Sources",
        ],
        "so_what": [
            "IND–PAK crisis stability",
            "civil–military balance",
            "AFG spillover",
            "smaller-state bargains",
            "IMF / debt trajectory",
        ],
    },
    "middle_east": {
        "label": "Middle East Desk",
        "one_liner": (
            "You are the Middle East Desk Analyst covering Gulf, Levant, Iran–Iraq–Yemen, and Turkey interfaces. "
            "Prioritize escalation ladders, mediation, Iran network, Gulf hedging, energy-maritime risk. "
            "No sectarian essentialism or targeting guidance; separate FACT / CLAIM / ASSESS."
        ),
        "daily_sections": [
            "Top line",
            "Priority developments",
            "Active conflict tracks",
            "Gulf & Iran diplomacy",
            "Energy & maritime risk",
            "Agreements / mediation",
            "Risk board",
            "Sources",
        ],
        "weekly_sections": [
            "Executive summary",
            "Strategic picture",
            "Conflict trajectories",
            "Diplomacy scoreboard",
            "Scenarios",
            "Desk calls (ASSESS)",
            "Sources",
        ],
        "so_what": [
            "escalation / de-escalation ladder",
            "ceasefire durability",
            "Hormuz / Red Sea / Suez risk",
            "Iran network / Gulf hedging",
            "OPEC+ / fiscal stability",
        ],
    },
    "eastern_europe": {
        "label": "Eastern Europe Desk",
        "one_liner": (
            "You are the Eastern Europe Desk Analyst covering RUS/UKR/BLR/MDA and the eastern EU/NATO flank. "
            "Track war trajectory, strikes, sanctions/energy, alliance politics without laundering MoD propaganda. "
            "No targeting guidance; separate FACT / CLAIM / ASSESS."
        ),
        "daily_sections": [
            "Top line",
            "Priority developments",
            "Battlefield & strike watch",
            "Diplomacy / aid / NATO-EU",
            "Belarus / Moldova / flank",
            "Sanctions & energy",
            "Risk board",
            "Sources",
        ],
        "weekly_sections": [
            "Executive summary",
            "Strategic picture",
            "War trajectory dashboard",
            "Aid / alliance scoreboard",
            "Scenarios",
            "Desk calls (ASSESS)",
            "Sources",
        ],
        "so_what": [
            "operational initiative / endurance",
            "escalation risk",
            "aid / industry / manpower",
            "sanctions effectiveness",
            "Moldova/Belarus stability",
        ],
    },
    "southern_europe": {
        "label": "Southern Europe Desk",
        "one_liner": (
            "You are the Southern Europe Desk Analyst covering ITA, ESP, PRT, MLT (GRC watch). "
            "Track EU Med bargaining, migration-maritime, energy corridors, strategic FDI/defense. "
            "No tabloid tone; separate FACT / CLAIM / ASSESS."
        ),
        "daily_sections": [
            "Top line",
            "Priority developments",
            "EU / Eurogroup interface",
            "Migration & maritime Med",
            "Energy / climate",
            "Country notes",
            "Risk board",
            "Sources",
        ],
        "weekly_sections": [
            "Executive summary",
            "Strategic picture",
            "Beat deep-dives",
            "Agreements scoreboard",
            "Scenarios",
            "Desk calls (ASSESS)",
            "Sources",
        ],
        "so_what": [
            "government / EU bargaining power",
            "migration diplomacy",
            "energy security",
            "NATO southern flank",
            "strategic FDI control",
        ],
    },
    "northern_europe": {
        "label": "Northern Europe Desk",
        "one_liner": (
            "You are the Northern Europe Desk Analyst covering Nordics + Baltics (NB8) and Arctic realm issues. "
            "Track NATO integration, Baltic Sea critical infrastructure, High North competition, energy resilience. "
            "No reckless hybrid attribution; separate FACT / CLAIM / ASSESS."
        ),
        "daily_sections": [
            "Top line",
            "Priority developments",
            "NATO / NB8 posture",
            "Baltic Sea / critical infrastructure",
            "High North / Arctic",
            "Energy",
            "Risk board",
            "Sources",
        ],
        "weekly_sections": [
            "Executive summary",
            "Strategic picture",
            "Beat deep-dives",
            "Defense scoreboard",
            "Scenarios",
            "Desk calls (ASSESS)",
            "Sources",
        ],
        "so_what": [
            "NB8 deterrence",
            "NATO integration",
            "Baltic infra / energy security",
            "High North / Greenland competition",
            "defense industry / munitions",
        ],
    },
    "generic": {
        "label": "Regional Desk",
        "one_liner": (
            "You are a senior regional geopolitical analyst. Aggregate open-source evidence, "
            "separate FACT / CLAIM / ASSESS, cite sources, and flag gaps. Never invent sources or dates."
        ),
        "daily_sections": [
            "Top line",
            "Priority developments",
            "Country notes",
            "Watch items",
            "Gaps / uncertainty",
            "Sources",
        ],
        "weekly_sections": [
            "Executive summary",
            "Situation",
            "Drivers",
            "Scenarios",
            "What would change our view",
            "Sources",
        ],
        "so_what": [
            "regional stability",
            "external alignment shifts",
            "economic / security transmission",
        ],
    },
}


_AGENT_PREFIX_TO_DESK: list[tuple[str, str]] = [
    ("southeast_asia", "asean"),
    ("oceania", "oceania"),
    ("central_asia", "central_asia"),
    ("east_asia", "northeast_asia"),
    ("south_asia", "south_asia"),
    ("gulf", "middle_east"),
    ("levant", "middle_east"),
    ("iran_iraq", "middle_east"),
    ("turkey_east_med", "middle_east"),
    ("eastern_europe", "eastern_europe"),
    ("southern_europe", "southern_europe"),
    ("nordics", "northern_europe"),
    ("baltics", "northern_europe"),
]


def desk_family_for_agent(agent_id: str) -> str:
    aid = (agent_id or "").strip().lower()
    for prefix, family in _AGENT_PREFIX_TO_DESK:
        if aid.startswith(prefix):
            return family
    return "generic"


def get_sop_for_agent(agent_id: str) -> dict[str, Any]:
    family = desk_family_for_agent(agent_id)
    pack = dict(DESK_SOPS.get(family) or DESK_SOPS["generic"])
    pack["family"] = family
    return pack


CITATION_RULES = """
Citation & evidence rules (mandatory):
- Evidence items are numbered [S1], [S2], … in the user message.
- Every material factual claim must cite at least one [Sn].
- Label uncertain or single-source items as CLAIM; judgments as ASSESS; verified multi-source as FACT where possible.
- If evidence is thin, say so under Gaps / Open questions — do not invent URLs, quotes, or events.
- End with ## Sources listing each [Sn] used (title, publisher, and id/url if provided).
- No policy advocacy; use "options include…" if discussing choices.
""".strip()


def build_system_prompt(agent: dict[str, Any], agent_id: str, *, weekly: bool = False) -> str:
    sop = get_sop_for_agent(agent_id)
    sections = sop["weekly_sections"] if weekly else sop["daily_sections"]
    section_line = ", ".join(f"## {s}" for s in sections)
    so_what = "; ".join(sop.get("so_what") or [])
    return f"""{sop['one_liner']}

Desk family: {sop['family']} ({sop['label']})
Assigned coverage label: {agent.get('label')}
Countries (ISO3): {', '.join(agent.get('countries') or [])}

Required markdown sections (use these headings):
{section_line}

"So what" tests to apply when ranking items: {so_what}

{CITATION_RULES}

Write for internal intelligence consumers. Be concise, neutral, and change-focused.
""".strip()


def build_numbered_sources(articles: list[dict[str, Any]], limit: int = 35) -> tuple[str, list[dict[str, Any]]]:
    """Return prompt block + structured source list for eval/context."""
    from app.scrapers.sources import source_tier_for_name

    lines: list[str] = []
    structured: list[dict[str, Any]] = []
    for i, a in enumerate(articles[:limit], 1):
        sid = f"S{i}"
        title = (a.get("title") or "(no title)").strip()
        src = (a.get("source_name") or "").strip()
        url = (a.get("url") or "").strip()
        aid = a.get("id")
        tier = source_tier_for_name(src)
        summ = ((a.get("summary") or "").replace("\n", " ").strip())[:400]
        kt = ((a.get("key_takeaways") or "").replace("\n", " ").strip())[:300]
        lines.append(f"[{sid}] (Tier {tier} · {src}) {title}")
        if aid:
            lines.append(f"    article_id={aid}")
        if url:
            lines.append(f"    url={url}")
        if summ:
            lines.append(f"    summary: {summ}")
        if kt:
            lines.append(f"    takeaways: {kt}")
        structured.append(
            {
                "sid": sid,
                "article_id": aid,
                "title": title,
                "source_name": src,
                "url": url,
                "tier": tier,
            }
        )
    if not lines:
        return "(No articles matched this scope in the selected window.)", []
    return "\n".join(lines), structured


def append_sources_appendix(body: str, sources: list[dict[str, Any]]) -> str:
    """Ensure a ## Sources appendix exists with the numbered evidence list."""
    text = (body or "").rstrip()
    if not sources:
        return text
    if "## Sources" in text or "## sources" in text:
        return text
    lines = ["", "## Sources", ""]
    for s in sources:
        bit = f"- [{s['sid']}] **{s.get('source_name') or 'source'}** — {s.get('title') or ''}"
        if s.get("article_id"):
            bit += f" (article_id={s['article_id']})"
        if s.get("url"):
            bit += f" — {s['url']}"
        lines.append(bit)
    return text + "\n".join(lines)
