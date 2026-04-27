"""Higher-order synthesis: country operating picture, thematic briefings, signal divergence detection."""
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Topic -> country names / entities for thematic aggregation
THEMATIC_TOPICS = {
    "US-China": {
        "topics": ["US-China", "Asia-Pacific"],
        "countries": ["USA", "CHN", "TWN", "JPN", "KOR"],
        "country_names": ["United States", "China", "Taiwan", "Japan", "South Korea", "US", "USA"],
        "regions": ["East Asia", "Southeast Asia"],
    },
    "Russia-Ukraine": {
        "topics": ["Russia-Ukraine", "NATO", "Europe"],
        "countries": ["RUS", "UKR", "POL", "LTU", "LVA", "EST"],
        "country_names": ["Russia", "Ukraine", "Poland"],
        "regions": ["Europe", "Eastern Europe"],
    },
    "Middle East": {
        "topics": ["Middle East"],
        "countries": ["ISR", "PSE", "IRN", "SAU", "SYR", "LBN", "EGY", "YEM", "IRQ"],
        "country_names": ["Israel", "Iran", "Saudi Arabia", "Syria", "Lebanon", "Egypt", "Yemen", "Iraq", "Gaza"],
        "regions": ["Middle East"],
    },
}


def _get_connection():
    from app.models import _connection
    return _connection


def _call_llm(system: str, user: str, max_tokens: int = 1500) -> str | None:
    """Call OpenAI for synthesis. Returns None on failure or missing key."""
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return None
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user[:8000]},
            ],
            max_tokens=max_tokens,
        )
        return (response.choices[0].message.content or "").strip()
    except Exception as e:
        logger.warning("Synthesis LLM call failed: %s", e)
        return None


def aggregate_country_data(country_code: str, region: Optional[str] = None) -> dict:
    """Aggregate all indicator data for a country/region for synthesis."""
    from app.models import (
        get_integration_country,
        get_articles,
        get_election_calendar,
        get_protest_tracking,
        get_conflict_event_imports,
        get_flows_for_country,
        get_macroeconomic_stress,
        get_capital_flows,
        get_sanctions,
        get_treaties,
    )
    from app.institutional_models.readers import (
        get_gepi_latest,
        get_cdei_by_country,
        get_sfi_by_country,
        get_fragility_by_country,
    )
    from app.country_data import ISO3_TO_2

    country = get_integration_country(country_code)
    if not country:
        return {}

    code_2 = ISO3_TO_2.get(country_code.upper(), "") if ISO3_TO_2 else ""
    name = (country.get("country_name") or "").strip() or country_code

    # Risk heat map - get from country_risk_snapshots (uses code_2 or code_3)
    from app.models import get_country_risk_snapshots
    snapshots = get_country_risk_snapshots()
    risk_row = None
    for s in snapshots:
        if (s.get("country_code") or "").upper() in (country_code.upper(), code_2.upper()):
            risk_row = s
            break

    # Articles - filter by country name/code in topics/entities
    articles = get_articles(limit=15, days=14, min_impact=5, country=name)
    if not articles:
        articles = get_articles(limit=15, days=14, min_impact=5, countries_list=[name, country_code])

    # Institutional scores
    gepi = get_gepi_latest()
    cdei_rows = get_cdei_by_country(country_code=country_code)
    sfi_rows = get_sfi_by_country(country_code=country_code)
    frag_rows = get_fragility_by_country(country_code=country_code)

    # Economic
    macro = get_macroeconomic_stress(country_code=country_code)
    capital = get_capital_flows(country_code=country_code)

    # Sanctions (target)
    sanctions = get_sanctions(target=name, limit=20)

    # Elections, protests, conflict events
    elections = get_election_calendar(country_code=country_code, limit=10)
    protests = get_protest_tracking(country_code=country_code, limit=15)
    conflict = get_conflict_event_imports(country_code=country_code, limit=15)

    # Trade / chokepoints
    flows = get_flows_for_country(country_code=country_code, limit=20)
    treaties = get_treaties(party=name, limit=20)

    return {
        "country_code": country_code,
        "country_name": name,
        "region": country.get("region") or region,
        "scores": {
            "combined_risk": country.get("combined_systemic_risk_score"),
            "economic_fragility": country.get("economic_fragility_score"),
            "geopolitical_fragility": country.get("geopolitical_fragility_score"),
            "gepi": round(float(gepi.get("gepi_score") or 0), 2) if gepi else None,
            "cdei": round(float(cdei_rows[0]["cdei_total"] or 0), 2) if cdei_rows else None,
            "sfi": round(float(sfi_rows[0]["sfi_score"] or 0), 2) if sfi_rows else None,
            "fragility": round(float(frag_rows[0]["fragility_level"] or 0) * 100, 0) if frag_rows else None,
            "heat_map_risk": risk_row.get("risk_score") if risk_row else None,
        },
        "macro": macro[:3] if macro else [],
        "capital": capital[:3] if capital else [],
        "sanctions_count": len(sanctions) if sanctions else 0,
        "articles": [{"title": a.get("title"), "impact": a.get("impact_score"), "date": (a.get("published_utc") or a.get("scraped_at") or "")[:10]} for a in (articles or [])[:10]],
        "elections": [{"type": e.get("election_type"), "date": (e.get("date_planned") or "")[:10], "status": e.get("status")} for e in (elections or [])[:5]],
        "protests": [{"date": (p.get("event_date") or "")[:10], "summary": (p.get("summary") or "")[:80]} for p in (protests or [])[:5]],
        "conflict_events": [{"source": c.get("source"), "date": (c.get("event_date") or "")[:10], "type": c.get("event_type")} for c in (conflict or [])[:5]],
        "chokepoint_flows": [{"chokepoint": f.get("chokepoint_name"), "sector": f.get("sector"), "exposure_pct": f.get("exposure_pct")} for f in (flows or [])[:5]],
        "treaties_count": len(treaties) if treaties else 0,
    }


def generate_country_operating_picture(country_code: str, region: Optional[str] = None) -> dict:
    """Generate 1–2 paragraph situation summary + bullet points via LLM."""
    data = aggregate_country_data(country_code, region)
    if not data:
        return {"summary": "", "bullets": [], "error": "Country not found", "data": {}}

    # Build context string for LLM
    lines = [
        f"Country: {data['country_name']} ({data['country_code']})",
        f"Region: {data.get('region') or '—'}",
        "",
        "Scores:",
        f"  Combined risk: {data['scores'].get('combined_risk')} | Economic fragility: {data['scores'].get('economic_fragility')} | GEPI: {data['scores'].get('gepi')} | CDEI: {data['scores'].get('cdei')} | SFI: {data['scores'].get('sfi')} | Fragility: {data['scores'].get('fragility')} | Heat map risk: {data['scores'].get('heat_map_risk')}",
        "",
        f"Macro stress (debt, inflation): {len(data.get('macro') or [])} data points",
        f"Capital flows: {len(data.get('capital') or [])} entries",
        f"Sanctions targeting: {data.get('sanctions_count', 0)}",
        f"Treaties: {data.get('treaties_count', 0)}",
        "",
        f"Recent high-impact articles ({len(data.get('articles') or [])}):",
    ]
    for a in (data.get("articles") or [])[:8]:
        lines.append(f"  - [{a.get('impact')}] {a.get('date')}: {a.get('title', '')[:70]}")

    lines.extend([
        "",
        "Elections:", *[f"  - {e.get('type')} {e.get('date')} ({e.get('status')})" for e in (data.get("elections") or [])],
        "",
        "Protests:", *[f"  - {p.get('date')}: {p.get('summary', '')[:60]}" for p in (data.get("protests") or [])[:5]],
        "",
        "Conflict events:", *[f"  - [{c.get('source')}] {c.get('date')}: {c.get('type')}" for c in (data.get("conflict_events") or [])],
        "",
        "Chokepoint exposure:", *[f"  - {f.get('chokepoint')} ({f.get('sector')}): {f.get('exposure_pct')}%" for f in (data.get("chokepoint_flows") or [])],
    ])

    context = "\n".join(lines)
    system = """You are a geopolitical analyst. Given structured data about a country, write:
1. SITUATION SUMMARY: 1–2 paragraphs that synthesize the key risk factors, recent developments, and operating context. Be specific and analytical.
2. BULLET POINTS: 3–5 concise bullets highlighting: main risks, policy/market implications, what to watch.

Reply in this exact format:
SITUATION SUMMARY:
[paragraphs]

BULLET POINTS:
• [bullet 1]
• [bullet 2]
..."""

    out = _call_llm(system, context)
    summary = ""
    bullets = []
    if out:
        if "SITUATION SUMMARY:" in out and "BULLET POINTS:" in out:
            parts = out.split("BULLET POINTS:")
            summary = parts[0].replace("SITUATION SUMMARY:", "").strip()
            bullet_text = parts[1].strip()
            bullets = [b.strip().lstrip("•-–").strip() for b in bullet_text.split("\n") if b.strip()][:6]
        else:
            summary = out[:1500]

    # Fill gaps without LLM (same logic as Analyst Desk regional roll-ups: scores + feed + indicators)
    fill_source = None
    agent_desk = None
    if not (summary or "").strip() or not bullets:
        from app.analyst_desk.dashboard_fill import build_heuristic_operating_picture

        h = build_heuristic_operating_picture(data, country_code)
        if not (summary or "").strip():
            summary = h["summary"]
        if not bullets:
            bullets = h["bullets"]
        fill_source = h.get("fill_source")
        agent_desk = h.get("agent_desk")

    result = {
        "summary": summary,
        "bullets": bullets,
        "data": data,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }
    if fill_source:
        result["fill_source"] = fill_source
    if agent_desk:
        result["agent_desk"] = agent_desk
    return result


def aggregate_thematic_data(topic: str) -> dict:
    """Aggregate data for a thematic briefing (US-China, Russia-Ukraine, Middle East)."""
    cfg = THEMATIC_TOPICS.get(topic)
    if not cfg:
        return {}

    from app.models import (
        get_articles,
        get_treaties,
        get_sanctions,
        get_chokepoints,
        get_flows_for_chokepoint,
        get_export_restrictions,
    )
    from app.institutional_models.readers import get_gepi_latest

    topics_list = cfg.get("topics", [topic])
    countries = cfg.get("country_names", []) + cfg.get("countries", [])
    regions = cfg.get("regions", [])

    # Articles by topic
    articles = get_articles(limit=20, days=14, min_impact=5, topic=topics_list[0])
    if not articles and len(topics_list) > 1:
        articles = get_articles(limit=20, days=14, min_impact=5, topics_list=topics_list)

    # Treaties - party filter (any of the country names)
    treaties_all = []
    for c in countries[:5]:
        t = get_treaties(party=c, limit=10)
        if t:
            treaties_all.extend(t[:5])
    treaties_all = treaties_all[:15]

    # Sanctions - target filter
    sanctions_all = []
    for c in countries[:5]:
        s = get_sanctions(target=c, limit=10)
        if s:
            sanctions_all.extend(s[:5])
    sanctions_all = sanctions_all[:15]

    # Chokepoints in region
    chokepoints = get_chokepoints()
    region_cps = [cp for cp in (chokepoints or []) if (cp.get("region") or "") in regions]
    flows = []
    for cp in region_cps[:5]:
        fl = get_flows_for_chokepoint(cp.get("id"))
        flows.extend((fl or [])[:5])

    # Export restrictions (search by topic keywords)
    export_r = get_export_restrictions(search=topic.replace("-", " "), limit=20)

    gepi = get_gepi_latest()

    return {
        "topic": topic,
        "articles": [{"title": a.get("title"), "impact": a.get("impact_score"), "date": (a.get("published_utc") or "")[:10]} for a in (articles or [])[:12]],
        "treaties_count": len(treaties_all),
        "treaties_sample": [{"name": t.get("name"), "type": t.get("treaty_type")} for t in treaties_all[:5]],
        "sanctions_count": len(sanctions_all),
        "chokepoints": [{"name": cp.get("name"), "region": cp.get("region")} for cp in region_cps[:5]],
        "flows_count": len(flows),
        "export_restrictions_count": len(export_r or []),
        "gepi": round(float(gepi.get("gepi_score") or 0), 2) if gepi else None,
    }


def generate_thematic_briefing(topic: str) -> dict:
    """Generate thematic briefing (e.g. US-China this week) via LLM."""
    data = aggregate_thematic_data(topic)
    if not data:
        return {"summary": "", "bullets": [], "error": f"Unknown topic: {topic}", "data": {}}

    lines = [
        f"Theme: {topic}",
        "",
        f"GEPI (escalation pressure): {data.get('gepi')}",
        f"Articles (last 14 days, impact 5+): {len(data.get('articles') or [])}",
        "",
        "Top articles:",
    ]
    for a in (data.get("articles") or [])[:10]:
        lines.append(f"  - [{a.get('impact')}] {a.get('date')}: {a.get('title', '')[:70]}")

    lines.extend([
        "",
        f"Treaties (relevant parties): {data.get('treaties_count', 0)}",
        f"Sanctions (targeting): {data.get('sanctions_count', 0)}",
        f"Export restrictions: {data.get('export_restrictions_count', 0)}",
        "",
        "Chokepoints in region:",
        *[f"  - {cp.get('name')} ({cp.get('region')})" for cp in (data.get("chokepoints") or [])],
    ])

    context = "\n".join(lines)
    system = f"""You are a geopolitical analyst. Given data on {topic}, write:
1. THIS WEEK: 1–2 paragraphs synthesizing developments across news, treaties, sanctions, and supply chain. What's the trend? Key drivers?
2. BULLET POINTS: 3–5 bullets on implications for policy, markets, or regional stability.

Format:
THIS WEEK:
[paragraphs]

BULLET POINTS:
• [bullet 1]
• [bullet 2]
..."""

    out = _call_llm(system, context)
    summary = ""
    bullets = []
    if out:
        if "THIS WEEK:" in out and "BULLET POINTS:" in out:
            parts = out.split("BULLET POINTS:")
            summary = parts[0].replace("THIS WEEK:", "").strip()
            bullet_text = parts[1].strip()
            bullets = [b.strip().lstrip("•-–").strip() for b in bullet_text.split("\n") if b.strip()][:6]
        else:
            summary = out[:1500]

    return {
        "summary": summary,
        "bullets": bullets,
        "data": data,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }


def get_signal_divergences() -> list:
    """Detect mismatches between models. Returns list of divergence alerts."""
    divergences = []

    from app.institutional_models.readers import get_gepi_latest
    from app.models import _connection, get_country_risk_snapshots, get_protest_counts_by_country

    with _connection() as conn:
        # GEPI high but sanctions activity low
        gepi = get_gepi_latest()
        gepi_score = float(gepi.get("gepi_score") or 0) if gepi else 0
        cur = conn.execute(
            """SELECT COUNT(*) FROM sanctions_registry WHERE substr(start_date, 1, 10) >= date('now', '-30 days')"""
        )
        sanctions_30d = (cur.fetchone() or (0,))[0]

        if gepi_score >= 0.6 and sanctions_30d < 2:
            divergences.append({
                "type": "gepi_sanctions",
                "title": "GEPI elevated but sanctions activity low",
                "detail": f"Global escalation pressure (GEPI) is {gepi_score:.2f} (elevated) yet only {sanctions_30d} new sanctions in last 30 days. Possible lag or different escalation channel.",
                "suggestion": "Monitor military activity and rhetoric; sanctions may follow.",
            })

        # Fragility rising vs protest counts (latest per country)
        cutoff = (datetime.utcnow() - timedelta(days=14)).strftime("%Y-%m-%d")
        cur = conn.execute(
            """SELECT f.country_code, f.fragility_level
               FROM gpi_fragility_daily f
               INNER JOIN (
                 SELECT country_code, MAX(as_of_date) AS max_date
                 FROM gpi_fragility_daily
                 WHERE as_of_date >= ? AND country_code IS NOT NULL
                 GROUP BY country_code
               ) sub ON f.country_code = sub.country_code AND f.as_of_date = sub.max_date
               WHERE f.fragility_level >= 0.6
               ORDER BY f.fragility_level DESC LIMIT 20""",
            (cutoff,),
        )
        high_frag = {row[0]: row[1] for row in cur}

        protest_counts = get_protest_counts_by_country(date_from=cutoff, limit=50)
        protest_by_cc = {p.get("country_code"): p.get("cnt", 0) for p in (protest_counts or []) if p.get("country_code")}

        for cc, frag in list(high_frag.items())[:5]:
            protests = protest_by_cc.get(cc, 0)
            if frag >= 0.6 and protests == 0:
                divergences.append({
                    "type": "fragility_protests",
                    "title": f"Fragility elevated in {cc} but no recent protests",
                    "detail": f"Fragility score {frag:.2f} yet 0 protests in last 14 days. May reflect FX stress, elite instability, or data lag.",
                    "suggestion": "Check currency stress and elite/institutional indicators.",
                })

        # Article impact vs risk heat map
        cur = conn.execute(
            """SELECT AVG(impact_score) as avg_imp, COUNT(*) as cnt FROM articles
               WHERE COALESCE(NULLIF(trim(published_utc), ''), scraped_at) >= date('now', '-7 days')
               AND impact_score >= 7"""
        )
        row = cur.fetchone()
        high_impact_7d = (row[1] or 0) if row else 0
        avg_impact = (row[0] or 0) if row else 0

        snapshots = get_country_risk_snapshots()
        top_risk = max([s.get("risk_score") or 0 for s in (snapshots or [])], default=0)

        if high_impact_7d >= 5 and top_risk < 50:
            divergences.append({
                "type": "articles_heatmap",
                "title": "High-impact articles spiking but risk heat map subdued",
                "detail": f"{high_impact_7d} high-impact articles (7+) in last 7 days, avg impact {avg_impact:.1f}, yet max heat map risk is {top_risk}. Possible regional dispersion or scoring lag.",
                "suggestion": "Run risk engine refresh; check if articles span many regions.",
            })

    return divergences


# --- Executive one-pager ---


def generate_executive_one_pager(country_code: Optional[str] = None, region_code: Optional[str] = None) -> dict:
    """
    Per country or per region: risk, fragility, top 3 drivers, 2–3 implications, links.
    Template filled by structured data + optional LLM for drivers/implications.
    """
    from app.models import (
        get_integration_country,
        get_articles,
        get_country_risk_snapshots,
        get_risk_index,
        get_sanctions,
        get_protest_tracking,
        get_macroeconomic_stress,
    )
    from app.institutional_models.readers import get_gepi_latest, get_fragility_by_country
    from app.country_data import ISO3_TO_2

    if country_code:
        country = get_integration_country(country_code)
        if not country:
            return {"error": "Country not found", "data": {}}
        name = (country.get("country_name") or "").strip() or country_code
        code_2 = ISO3_TO_2.get(country_code.upper(), "") if ISO3_TO_2 else ""
        risk_row = None
        snapshots = get_country_risk_snapshots()
        for s in snapshots or []:
            if (s.get("country_code") or "").upper() in (country_code.upper(), code_2.upper()):
                risk_row = s
                break
        articles = get_articles(limit=20, days=7, min_impact=5, country=name)
        if not articles:
            articles = get_articles(limit=20, days=7, min_impact=5, countries_list=[name, country_code])
        frag_rows = get_fragility_by_country(country_code=country_code)
        sanctions = get_sanctions(target=name, limit=10)
        protests = get_protest_tracking(country_code=country_code, limit=10)
        macro = get_macroeconomic_stress(country_code=country_code)

        risk = country.get("combined_systemic_risk_score") or risk_row.get("risk_score") if risk_row else None
        geopolitical = country.get("geopolitical_fragility_score")
        economic = country.get("economic_fragility_score")
        fragility = round(float(frag_rows[0]["fragility_level"] or 0) * 100, 0) if frag_rows else None
        article_count = len(articles or [])

        # Drivers: from articles (topics, event types) + indicators (sanctions, protests, macro)
        driver_signals = []
        if sanctions:
            driver_signals.append(f"Sanctions pressure: {len(sanctions)} measures targeting {name}")
        if protests:
            driver_signals.append(f"Protest activity: {len(protests)} events in last 14 days")
        if macro:
            driver_signals.append("Macro stress: debt/inflation indicators elevated")
        for a in (articles or [])[:5]:
            et = a.get("event_type") or ""
            top = (a.get("topics") or "").split(",")[0].strip() if a.get("topics") else ""
            if et or top:
                driver_signals.append(f"News: {top or et} — {(a.get('title') or '')[:50]}…")

        # LLM for top 3 drivers + 2-3 implications
        context = f"""Country: {name} ({country_code})
Risk: {risk} | Geopolitical fragility: {geopolitical} | Economic: {economic} | Fragility: {fragility}%
Articles (7d): {article_count}
Sanctions: {len(sanctions or [])} | Protests: {len(protests or [])}

Driver signals:
{chr(10).join('- ' + d for d in driver_signals[:8])}

Provide:
1. TOP 3 DRIVERS: The three most important factors driving risk right now (one line each).
2. IMPLICATIONS: 2–3 implications for policy or markets (one line each).

Format:
TOP 3 DRIVERS:
1. [driver 1]
2. [driver 2]
3. [driver 3]

IMPLICATIONS:
• [implication 1]
• [implication 2]
• [implication 3]"""
        out = _call_llm("You are a geopolitical analyst. Be concise.", context)
        drivers = ["—", "—", "—"]
        implications = ["—", "—"]
        llm_parsed = False
        if out:
            if "TOP 3 DRIVERS:" in out and "IMPLICATIONS:" in out:
                dr_part = out.split("IMPLICATIONS:")[0].replace("TOP 3 DRIVERS:", "").strip()
                imp_part = out.split("IMPLICATIONS:")[1].strip()
                drivers = [ln.strip().lstrip("123.-•").strip() for ln in dr_part.split("\n") if ln.strip()][:3]
                implications = [ln.strip().lstrip("•-–").strip() for ln in imp_part.split("\n") if ln.strip()][:3]
                llm_parsed = True
            while len(drivers) < 3:
                drivers.append(driver_signals[len(drivers)] if len(driver_signals) > len(drivers) else "—")
            while len(implications) < 2:
                implications.append("—")

        # Without API key or failed parse: same signals Analyst Desk uses (articles, sanctions, protests, macro)
        heuristic_fill = not llm_parsed

        def _is_placeholder(s: str) -> bool:
            t = (s or "").strip()
            return not t or t == "—" or t == "-"

        if heuristic_fill or all(_is_placeholder(d) for d in drivers[:3]):
            ds = [d for d in driver_signals if d and not _is_placeholder(d)][:3]
            while len(ds) < 3:
                ds.append(
                    "No extra high-impact headlines in the 7-day window — use dashboard tabs for full indicators."
                )
            drivers = ds[:3]

        if heuristic_fill or all(_is_placeholder(i) for i in implications[:3]):
            implications = []
            if sanctions:
                implications.append(
                    f"Sanctions: {len(sanctions)} measure(s) affecting {name} — map compliance and spillover channels."
                )
            if protests:
                implications.append(
                    f"Domestic stability: {len(protests)} protest event(s) logged — monitor policy response and contagion."
                )
            if articles:
                implications.append(
                    "Active high-impact news flow — expect policy/market attention on covered themes."
                )
            if macro:
                implications.append(
                    "Macro stress data on file — watch debt, inflation, and rating/spread channels."
                )
            while len(implications) < 2:
                implications.append(
                    "Cross-check Operating Picture with GEPI/CDEI/SFI and treaty/sanctions tabs."
                )
            implications = implications[:3]

        from app.analyst_desk.dashboard_fill import agent_desk_label_for_country

        exec_result = {
            "type": "country",
            "name": name,
            "region": country.get("region"),
            "risk": risk,
            "geopolitical": geopolitical,
            "economic": economic,
            "fragility": fragility,
            "article_count": article_count,
            "drivers": drivers[:3],
            "implications": implications[:3],
            "link_keys": {"country_code": country_code},
        }
        if heuristic_fill:
            exec_result["heuristic_fill"] = True
        ad = agent_desk_label_for_country(country_code)
        if ad:
            exec_result["agent_desk"] = ad
        return exec_result
    else:
        # Region: use risk_index + GEPI + top risk countries
        risk_list = get_risk_index()
        region_row = None
        if region_code:
            for r in risk_list or []:
                if (r.get("region_code") or "").upper() == (region_code or "").upper():
                    region_row = r
                    break
        else:
            region_row = (risk_list or [{}])[0] if risk_list else None
        gepi = get_gepi_latest()
        snapshots = get_country_risk_snapshots()
        top_risk_countries = sorted(
            [{"code": s.get("country_code"), "score": s.get("risk_score")} for s in (snapshots or [])[:10]],
            key=lambda x: -(x.get("score") or 0),
        )[:5]

        coup = (region_row or {}).get("coup_likelihood_pct") or 0
        sanc = (region_row or {}).get("sanctions_probability_pct") or 0
        trade = (region_row or {}).get("trade_disruption_pct") or 0
        comp = (coup + sanc + trade) / 3 if (coup or sanc or trade) else None

        context = f"""Region: {region_code or 'Global'}
GEPI: {gepi.get('gepi_score') if gepi else 'N/A'}
Forward Risk: coup {coup}%, sanctions {sanc}%, trade {trade}%
Top risk countries: {', '.join(f"{c['code']}({c['score']})" for c in top_risk_countries)}

Provide:
1. TOP 3 DRIVERS: Main escalation drivers for this region.
2. IMPLICATIONS: 2–3 policy/market implications.

Format:
TOP 3 DRIVERS:
1. [driver]
2. [driver]
3. [driver]

IMPLICATIONS:
• [implication]
• [implication]"""
        out = _call_llm("You are a geopolitical analyst. Be concise.", context)
        drivers = ["—", "—", "—"]
        implications = ["—", "—"]
        if out:
            if "TOP 3 DRIVERS:" in out and "IMPLICATIONS:" in out:
                dr_part = out.split("IMPLICATIONS:")[0].replace("TOP 3 DRIVERS:", "").strip()
                imp_part = out.split("IMPLICATIONS:")[1].strip()
                drivers = [ln.strip().lstrip("123.-•").strip() for ln in dr_part.split("\n") if ln.strip()][:3]
                implications = [ln.strip().lstrip("•-–").strip() for ln in imp_part.split("\n") if ln.strip()][:3]

        return {
            "type": "region",
            "region": region_code or "Global",
            "gepi": round(float(gepi.get("gepi_score") or 0), 2) if gepi else None,
            "top_risk_countries": top_risk_countries,
            "forward_risk": {"coup": coup, "sanctions": sanc, "trade": trade},
            "drivers": drivers[:3],
            "implications": implications[:3],
            "link_keys": {"region_code": region_code},
        }


# --- Escalation trajectory ---


def get_escalation_trajectory(days: int = 30) -> dict:
    """
    Time series: GEPI, risk scores, article impact, Forward Risk Index.
    Compute trend (increasing/stable/decreasing) and LLM 1–2 sentence narrative.
    """
    from app.institutional_models.readers import get_gepi_history
    from app.models import get_risk_index, get_country_risk_snapshots, _connection

    gepi_history = get_gepi_history(days=days)
    gepi_series = [{"date": h["as_of_date"], "score": round(float(h.get("gepi_score") or 0), 3)} for h in (gepi_history or [])]

    # Article impact by day (avg impact_score per date)
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    impact_series = []
    with _connection() as conn:
        cur = conn.execute(
            """SELECT substr(COALESCE(NULLIF(trim(published_utc), ''), scraped_at), 1, 10) AS d, AVG(impact_score) AS avg_imp, COUNT(*) AS cnt
               FROM articles WHERE (COALESCE(NULLIF(trim(published_utc), ''), scraped_at) >= ?) AND impact_score >= 1
               GROUP BY d ORDER BY d""",
            (cutoff,),
        )
        for row in cur:
            impact_series.append({"date": row[0], "avg_impact": round(float(row[1] or 0), 2), "count": row[2]})

    # Forward Risk Index (current snapshot; no history in DB)
    risk_index = get_risk_index()
    fri_composite = 0
    if risk_index:
        for r in risk_index:
            coup = r.get("coup_likelihood_pct") or 0
            sanc = r.get("sanctions_probability_pct") or 0
            trade = r.get("trade_disruption_pct") or 0
            fri_composite += (coup + sanc + trade) / 3
        fri_composite = round(fri_composite / len(risk_index), 1) if risk_index else 0
    snapshots = get_country_risk_snapshots()
    max_risk = max([s.get("risk_score") or 0 for s in (snapshots or [])], default=0)

    # Trend from GEPI + impact: compare last 7 days vs prior 7
    gepi_vals = [x["score"] for x in gepi_series]
    imp_vals = [x["avg_impact"] for x in impact_series]
    n = len(gepi_vals)
    avg_recent_gepi = sum(gepi_vals[-7:]) / 7 if len(gepi_vals) >= 7 else (sum(gepi_vals) / n if n else 0)
    avg_prior_gepi = sum(gepi_vals[-14:-7]) / 7 if len(gepi_vals) >= 14 else (sum(gepi_vals[:7]) / 7 if len(gepi_vals) >= 7 else avg_recent_gepi)
    avg_recent_imp = sum(imp_vals[-7:]) / 7 if len(imp_vals) >= 7 else (sum(imp_vals) / n if n else 0)
    avg_prior_imp = sum(imp_vals[-14:-7]) / 7 if len(imp_vals) >= 14 else (sum(imp_vals[:7]) / 7 if len(imp_vals) >= 7 else avg_recent_imp)

    trend = "stable"
    if avg_recent_gepi > avg_prior_gepi * 1.05 or avg_recent_imp > avg_prior_imp * 1.1:
        trend = "increasing"
    elif avg_recent_gepi < avg_prior_gepi * 0.95 or avg_recent_imp < avg_prior_imp * 0.9:
        trend = "decreasing"

    avg_recent = (avg_recent_gepi * 100 + avg_recent_imp * 5) / 2  # composite for display
    avg_prior = (avg_prior_gepi * 100 + avg_prior_imp * 5) / 2

    context = f"""Escalation trajectory (last {days} days):
- GEPI: recent 7d avg {avg_recent_gepi:.3f}, prior 7d avg {avg_prior_gepi:.3f}
- Article impact: recent 7d avg {avg_recent_imp:.2f}, prior 7d avg {avg_prior_imp:.2f}
- Forward Risk Index (current): {fri_composite}
- Max country risk: {max_risk}
- Computed trend: {trend}

Write 1–2 sentences summarizing whether escalation is increasing, stable, or decreasing, and the key signal(s) driving that view."""
    narrative = _call_llm("You are a geopolitical analyst. Write a brief escalation trend summary.", context)
    if not narrative:
        narrative = f"Escalation pressure is {trend} over the last {days} days. GEPI recent avg {avg_recent_gepi:.3f} vs prior {avg_prior_gepi:.3f}; article impact recent {avg_recent_imp:.2f} vs prior {avg_prior_imp:.2f}."

    return {
        "days": days,
        "trend": trend,
        "narrative": narrative.strip(),
        "avg_recent": avg_recent,
        "avg_prior": avg_prior,
        "gepi_series": gepi_series,
        "impact_series": impact_series,
        "fri_composite": fri_composite,
        "max_risk": max_risk,
    }


# --- Cluster + implications ---


def get_cluster_implications(cluster_id: int) -> Optional[dict]:
    """
    Per-cluster implications: combine GEPI, fragility, etc. across articles in the cluster.
    One LLM call for clusters with size >= 3 to produce "what this story cluster means".
    """
    from app.models import get_articles_by_cluster, get_cluster_label
    from app.institutional_models.readers import get_gepi_latest, get_fragility_by_country
    from app.models import get_country_risk_snapshots

    articles = get_articles_by_cluster(cluster_id)
    if not articles or len(articles) < 3:
        return None

    label = get_cluster_label(cluster_id)

    # Aggregate article data
    topics_set = set()
    entities_set = set()
    impacts = []
    takeaways = []
    why_matters = []
    titles = []
    for a in articles:
        for t in (a.get("topics") or "").split(","):
            t = t.strip()
            if t:
                topics_set.add(t)
        for e in (a.get("entities") or "").split(","):
            e = e.strip()
            if e and len(e) > 2:
                entities_set.add(e)
        imp = a.get("impact_score")
        if imp is not None:
            impacts.append(int(imp))
        if a.get("key_takeaways"):
            takeaways.append((a.get("key_takeaways") or "")[:200])
        if a.get("why_it_matters"):
            why_matters.append((a.get("why_it_matters") or "")[:200])
        titles.append((a.get("title") or "")[:80])

    avg_impact = sum(impacts) / len(impacts) if impacts else None
    gepi = get_gepi_latest()
    gepi_score = round(float(gepi.get("gepi_score") or 0), 2) if gepi else None

    # Try to match entities to countries for fragility (ISO3 codes; or names from ALL_COUNTRIES)
    country_codes = set()
    try:
        from app.country_data import ALL_COUNTRIES
        name_to_iso = {row[1].lower(): row[0] for row in (ALL_COUNTRIES or []) if len(row) >= 2}
    except Exception:
        name_to_iso = {}
    for ent in entities_set:
        ent_upper = ent.upper()
        if len(ent_upper) == 3 and ent_upper.isalpha():
            country_codes.add(ent_upper)
        ent_lower = ent.lower() if ent else ""
        for name, iso in name_to_iso.items():
            if name and ent_lower and name in ent_lower:
                country_codes.add(iso)

    fragility_sample = []
    for cc in list(country_codes)[:5]:
        frag_rows = get_fragility_by_country(country_code=cc)
        if frag_rows:
            f = frag_rows[0].get("fragility_level") or 0
            fragility_sample.append(f"{cc}: {round(float(f) * 100, 0)}")

    snapshots = get_country_risk_snapshots()
    risk_by_cc = {s.get("country_code"): s.get("risk_score") for s in (snapshots or []) if s.get("country_code")}
    risk_sample = []
    for cc in list(country_codes)[:5]:
        r = risk_by_cc.get(cc)
        if r is not None:
            risk_sample.append(f"{cc}: {r}")

    context = f"""Story cluster: "{label}"
{len(articles)} articles. Avg impact: {avg_impact or 'N/A'}. GEPI (global): {gepi_score or 'N/A'}.

Topics: {', '.join(list(topics_set)[:15])}
Entities (sample): {', '.join(list(entities_set)[:20])}
Fragility (where available): {', '.join(fragility_sample) if fragility_sample else '—'}
Risk heat map (where available): {', '.join(risk_sample) if risk_sample else '—'}

Article titles (sample):
{chr(10).join('- ' + t for t in titles[:8])}

Per-article takeaways (sample):
{chr(10).join('- ' + tk for tk in takeaways[:5]) if takeaways else '—'}

Per-article "why it matters" (sample):
{chr(10).join('- ' + w for w in why_matters[:5]) if why_matters else '—'}

Write a short synthesis (2–4 sentences) of "what this story cluster means": the combined significance, key drivers, and implications. Be specific and analytical."""
    synthesis = _call_llm("You are a geopolitical analyst. Synthesize the meaning of a story cluster from multi-source coverage.", context)

    return {
        "cluster_id": cluster_id,
        "label": label,
        "article_count": len(articles),
        "avg_impact": round(avg_impact, 1) if avg_impact is not None else None,
        "gepi": gepi_score,
        "topics": list(topics_set)[:15],
        "fragility_sample": fragility_sample,
        "risk_sample": risk_sample,
        "synthesis": synthesis.strip() if synthesis else None,
    }


# --- Meta-briefing ---


def generate_meta_briefing(days: int = 7) -> dict:
    """
    Weekly "system state" briefing: GEPI, risk heat map, fragility hotspots,
    spike vs declining topics, top scenario runs, digests, clusters.
    ~1-page "where the system thinks things stand".
    """
    from app.models import (
        get_country_risk_snapshots,
        get_spike_topics,
        get_declining_topics,
        get_digests,
        get_clusters_with_counts,
        get_scenario_engine_runs,
        _connection,
    )
    from app.institutional_models.readers import get_gepi_latest as _gepi

    gepi = _gepi()
    gepi_score = round(float(gepi.get("gepi_score") or 0), 2) if gepi else None

    snapshots = get_country_risk_snapshots()
    top_risk = [{"code": s.get("country_code"), "score": s.get("risk_score")} for s in (snapshots or [])[:10]]

    # Fragility hotspots (high fragility countries)
    cutoff = (datetime.utcnow() - timedelta(days=14)).strftime("%Y-%m-%d")
    fragility_hotspots = []
    with _connection() as conn:
        cur = conn.execute(
            """SELECT f.country_code, f.fragility_level
               FROM gpi_fragility_daily f
               INNER JOIN (
                 SELECT country_code, MAX(as_of_date) AS max_date
                 FROM gpi_fragility_daily
                 WHERE as_of_date >= ? AND country_code IS NOT NULL
                 GROUP BY country_code
               ) sub ON f.country_code = sub.country_code AND f.as_of_date = sub.max_date
               WHERE f.fragility_level >= 0.5
               ORDER BY f.fragility_level DESC LIMIT 10""",
            (cutoff,),
        )
        fragility_hotspots = [{"code": row[0], "level": round(float(row[1] or 0) * 100, 0)} for row in cur]

    spike_topics = get_spike_topics(days_recent=min(days, 7), days_prior=min(days, 7), limit=8)
    declining_topics = get_declining_topics(days_recent=min(days, 7), days_prior=min(days, 7), limit=6)

    scenario_runs = get_scenario_engine_runs(limit=5)
    digests = get_digests(limit=5, digest_type=None)
    clusters = get_clusters_with_counts(limit=10)

    divergences = get_signal_divergences()

    context = f"""Weekly system state briefing (last {days} days).

GEPI (escalation pressure): {gepi_score or 'N/A'}
Top risk countries (heat map): {', '.join(f"{r['code']}({r['score']})" for r in top_risk[:8])}
Fragility hotspots (elevated): {', '.join(f"{h['code']}({h['level']})" for h in fragility_hotspots[:6])}

Spiking topics: {', '.join(t[0] for t in spike_topics[:6]) if spike_topics else '—'}
Declining topics: {', '.join(t[0] for t in declining_topics[:4]) if declining_topics else '—'}

Recent scenario runs: {len(scenario_runs)}. Sample: {', '.join(f"{r.get('event_label', r.get('event_type', ''))} ({r.get('region') or r.get('country') or '—'})" for r in scenario_runs[:3])}
Recent digests: {len(digests)}. Sample: {', '.join(d.get('title', '')[:40] for d in digests[:3])}
Story clusters: {len(clusters)}. Top: {', '.join(c.get('label', '')[:30] for c in clusters[:5])}

Signal divergences: {len(divergences)}. {'; '.join(d.get('title', '')[:60] for d in divergences[:3]) if divergences else 'None'}

Write a ~1-page "where the system thinks things stand" briefing. Structure:
1. Opening (2-3 sentences): Overall system state and trend.
2. Key risks: Heat map and fragility hotspots.
3. Topic momentum: What's spiking vs declining and what it suggests.
4. Scenario / outlook: What recent scenario runs and digests add.
5. Clusters and divergence: Major story clusters and any model mismatches to watch.

Be concise, analytical, and actionable. Target ~300-400 words."""
    briefing = _call_llm("You are a geopolitical analyst producing a weekly system state briefing. Synthesize all inputs into a coherent narrative.", context)

    return {
        "days": days,
        "briefing": briefing.strip() if briefing else "No LLM summary available. Set OPENAI_API_KEY for meta-briefing.",
        "gepi": {"gepi_score": gepi_score} if gepi else None,
        "top_risk": top_risk,
        "fragility_hotspots": fragility_hotspots,
        "spike_topics": spike_topics,
        "declining_topics": declining_topics,
        "scenario_runs": scenario_runs,
        "scenario_runs_count": len(scenario_runs),
        "digests": digests,
        "digests_count": len(digests),
        "clusters": clusters,
        "clusters_count": len(clusters),
        "divergences": divergences,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }
