"""REST API for articles, digests, watchlists, alerts."""
import json
from urllib.request import urlopen, Request
from flask import Blueprint, current_app, g, jsonify, request, Response, url_for

from collections import defaultdict
import csv
import io

from app.macro_indicators import get_macro_indicators
from app.models import (
    run_scenario_engine,
    update_approval_rating,
    update_protest,
    delete_approval_rating,
    delete_protest,
    get_articles,
    get_digests,
    get_digest,
    get_watchlists,
    get_watchlist,
    get_articles_for_watchlist,
    get_alerts,
    get_alert_matches,
    get_article,
    search_articles,
    get_country_risk_snapshots,
    get_risk_index,
    get_treaties,
    get_treaties_count,
    get_treaty,
    get_sanctions,
    get_sanction,
    get_sanctions_total_count,
    get_entity_list_alerts,
    get_export_restrictions,
    get_voting_alignment,
    get_un_resolutions,
    get_un_resolution_detail,
    add_un_vote,
    get_integration_countries,
    get_defense_spending,
    get_military_exercises,
    get_border_incidents,
    get_military_movement,
    get_naval_deployments,
    get_arms_trade,
    get_chokepoints_with_geo,
    get_chokepoint,
    get_flows_for_chokepoint,
    get_flows_for_country,
    get_election_calendar,
    get_election_calendar_count,
    get_election,
    add_election,
    get_approval_ratings,
    get_approval_ratings_count,
    get_approval_rating,
    add_approval_rating,
    get_protest_tracking,
    get_protest_tracking_count,
    get_protest,
    add_protest_event,
    get_scenarios,
    macro_get_latest,
    macro_get_series,
    macro_list_countries,
    macro_list_indicators,
)

api = Blueprint("api", __name__, url_prefix="/api")


# --- Command bar typeahead ---
WORKSPACE_PRESETS = [
    ("CHN GOV", "Workspace: China, Supply chain, Risk, Taiwan"),
    ("SGP TRADE", "Workspace: Singapore, Supply chain, Risk, Malaysia"),
    ("ASEAN RISK", "Workspace: Risk, Indonesia, Thailand, Vietnam"),
    ("TWN ESC", "Workspace: Taiwan, Scenario Engine, Risk, Supply chain"),
]


def _command_suggest_presets(q):
    """Return workspace preset suggestions that match q (keyword type)."""
    if not q or len(q) < 2:
        return []
    q_lower = q.lower().strip()
    q_upper = q.upper().replace(" ", "")
    out = []
    for label, desc in WORKSPACE_PRESETS:
        key = label.upper().replace(" ", "")
        if q_upper in key or q_lower in label.lower():
            url = None
            if key == "CHNGOV":
                url = url_for("main.workspace", layout="4", t1="/country/CN", t2="/supply-chain", t3="/risk", t4="/country/TW")
            elif key == "SGPTRADE":
                url = url_for("main.workspace", layout="4", t1="/country/SG", t2="/supply-chain", t3="/risk", t4="/country/MY")
            elif key == "ASEANRISK":
                url = url_for("main.workspace", layout="4", t1="/risk", t2="/country/ID", t3="/country/TH", t4="/country/VN")
            elif key == "TWNESC":
                url = url_for("main.workspace", layout="4", t1="/country/TW", t2="/scenarios/engine", t3="/risk", t4="/supply-chain")
            if url:
                out.append({"label": label, "description": desc, "url": url, "kind": "preset"})
    return out


def _command_suggest_countries(q, limit=12):
    """Return country suggestions (name or code match)."""
    if not q or len(q) < 1:
        return []
    countries = get_integration_countries(limit=300)
    q_lower = q.lower().strip()
    q_upper = q.upper().strip()
    out = []
    for c in countries:
        name = (c.get("country_name") or "").strip()
        code = (c.get("country_code") or "").strip().upper()
        if not name and not code:
            continue
        if q_lower in name.lower() or q_upper == code or (len(q_upper) >= 2 and q_upper in code):
            url = url_for("main.country_dashboard", country_code=c["country_code"])
            out.append({"label": name, "subtitle": code, "url": url, "kind": "country"})
            if len(out) >= limit:
                break
    return out


def _command_suggest_regions(q, limit=12):
    """Return region suggestions from country data."""
    try:
        from app.country_data import ALL_COUNTRIES
        regions = sorted(set(r[2] for r in (ALL_COUNTRIES or []) if len(r) > 2 and r[2]))
    except Exception:
        regions = [
            "East Asia", "South Asia", "Southeast Asia", "Middle East", "Africa",
            "Latin America", "Europe", "North America", "Oceania", "Central Asia", "Caribbean",
        ]
    if not q or len(q) < 1:
        return [{"label": r, "url": url_for("main.integration_dashboard", region=r), "kind": "region"} for r in regions[:limit]]
    q_lower = q.lower().strip()
    out = []
    for r in regions:
        if q_lower in r.lower():
            out.append({"label": r, "url": url_for("main.integration_dashboard", region=r), "kind": "region"})
            if len(out) >= limit:
                break
    return out


def _command_suggest_scenarios(q, limit=10):
    """Return scenario name suggestions."""
    if not q or len(q) < 1:
        return []
    scenarios = get_scenarios(limit=50)
    q_lower = q.lower().strip()
    out = []
    for s in scenarios:
        name = (s.get("name") or "").strip()
        if not name:
            continue
        if q_lower in name.lower():
            out.append({
                "label": name,
                "url": url_for("main.scenario_detail", scenario_id=s["id"]),
                "kind": "scenario",
            })
            if len(out) >= limit:
                break
    return out


@api.route("/command/suggest")
def api_command_suggest():
    """Typeahead for command bar: countries, regions, presets, scenarios. ?q=...&type=country|region|keyword|scenario|..."""
    q = (request.args.get("q") or "").strip()
    search_type = (request.args.get("type") or "keyword").strip().lower()
    suggestions = []

    if search_type == "country":
        suggestions = _command_suggest_countries(q)
    elif search_type == "region":
        suggestions = _command_suggest_regions(q)
    elif search_type == "scenario":
        suggestions = _command_suggest_scenarios(q)
    elif search_type in ("keyword", "sector", "risk", "treaty", "sanctions"):
        # For keyword (and when type might match presets), include workspace presets
        suggestions = _command_suggest_presets(q)
        if search_type == "keyword" and q:
            # Also add quick links for risk/sector
            if any(x in q.lower() for x in ("risk", "sector")):
                suggestions.append({
                    "label": "Risk Engine",
                    "url": url_for("main.risk_dashboard"),
                    "kind": "quick",
                })
            if "feed" in q.lower() or "intel" in q.lower():
                suggestions.append({
                    "label": "Intelligence Feed",
                    "url": url_for("main.index"),
                    "kind": "quick",
                })
    else:
        suggestions = _command_suggest_presets(q)

    return jsonify({"suggestions": suggestions})


@api.route("/macro/indicators")
def api_macro_indicators():
    """Homepage macro indicators (cached server-side)."""
    return jsonify(get_macro_indicators())


@api.route("/macro/catalog")
def api_macro_catalog():
    """Catalog for dashboard selectors: countries + indicators."""
    region = (request.args.get("region") or "").strip() or None
    group = (request.args.get("group") or "").strip().lower() or None
    if group not in (None, "", "asean", "g20", "major"):
        group = None
    category = (request.args.get("category") or "").strip().lower() or None
    countries = macro_list_countries(region=region, group=group)
    indicators = macro_list_indicators(category=category)
    return jsonify({"countries": countries, "indicators": indicators})


@api.route("/macro/latest")
def api_macro_latest():
    """Latest macro values (time-series tables). Filters: region, group=asean|g20|major, indicator, limit."""
    region = (request.args.get("region") or "").strip() or None
    group = (request.args.get("group") or "").strip().lower() or None
    if group not in (None, "", "asean", "g20", "major"):
        group = None
    indicator = (request.args.get("indicator") or "").strip().lower() or None
    limit = min(int(request.args.get("limit", 120) or 120), 800)
    rows = macro_get_latest(region=region, group=group, indicator=indicator, limit=limit)
    return jsonify({"latest": rows, "region": region, "group": group, "indicator": indicator})


@api.route("/macro/country/<country_code>")
def api_macro_country(country_code):
    """Series for a country. Query: indicator (required), start, end."""
    indicator = (request.args.get("indicator") or "").strip().lower()
    if not indicator:
        return jsonify({"error": "indicator is required"}), 400
    start = (request.args.get("start") or "").strip() or None
    end = (request.args.get("end") or "").strip() or None
    data = macro_get_series(country_code=country_code, indicator=indicator, start=start, end=end)

    # On-demand fallback: if DB is empty (e.g. before scheduler runs), fetch from public sources.
    # This also makes range presets behave intuitively even when the ingest job is still warming up.
    if not data:
        cc = (country_code or "").strip().upper()[:6]
        ind = (indicator or "").strip().lower()[:80]
        try:
            from datetime import datetime, timezone
            import requests
            from app.models import _connection

            # --- FX (Frankfurter) ---
            if ind.startswith("fx_usd_"):
                # FX is stored under synthetic USD base in DB, but for API fallback we can compute live too.
                sym = ind.replace("fx_usd_", "").upper()
                if sym and sym != "USD":
                    # Default to 2Y if no range supplied (Frankfurter is daily).
                    now = datetime.now(timezone.utc).date()
                    start_d = start or (now.replace(year=now.year - 2).strftime("%Y-%m-%d"))
                    end_d = end or now.strftime("%Y-%m-%d")
                    resp = requests.get(
                        f"https://api.frankfurter.app/{start_d}..{end_d}",
                        params={"from": "EUR", "to": f"{sym},USD"},
                        headers={"User-Agent": "Geopolitiko/1.0"},
                        timeout=25,
                    )
                    if resp.ok:
                        payload = resp.json() if resp.content else {}
                        rates_by_date = payload.get("rates") if isinstance(payload, dict) else None
                        if isinstance(rates_by_date, dict):
                            out = []
                            for d, rates in sorted(rates_by_date.items()):
                                if not isinstance(rates, dict):
                                    continue
                                try:
                                    eur_sym = float(rates.get(sym))
                                    eur_usd = float(rates.get("USD"))
                                except (TypeError, ValueError):
                                    continue
                                if eur_sym and eur_usd:
                                    out.append({"date": str(d)[:10], "value": eur_sym / eur_usd})
                            data = out

            # --- World Bank (annual indicators) ---
            if not data:
                with _connection() as conn:
                    row = conn.execute(
                        """
                        SELECT s.key, i.external_code
                        FROM indicators i
                        JOIN data_sources s ON s.id = i.source_id
                        WHERE i.name = ?
                        LIMIT 1
                        """,
                        (ind,),
                    ).fetchone()
                if row and row[0] == "world_bank" and row[1]:
                    wb_code = str(row[1]).strip()
                    # Fetch series (World Bank returns most recent first). Keep up to ~60 points.
                    resp = requests.get(
                        f"https://api.worldbank.org/v2/country/{cc}/indicator/{wb_code}",
                        params={"format": "json", "per_page": 100},
                        headers={"User-Agent": "Geopolitiko/1.0"},
                        timeout=25,
                    )
                    if resp.ok:
                        payload = resp.json()
                        if isinstance(payload, list) and len(payload) >= 2 and isinstance(payload[1], list):
                            out = []
                            for r in payload[1]:
                                if not isinstance(r, dict):
                                    continue
                                v = r.get("value")
                                d = r.get("date")
                                if v is None or d is None:
                                    continue
                                try:
                                    fv = float(v)
                                except (TypeError, ValueError):
                                    continue
                                ds = str(d)
                                if len(ds) == 4 and ds.isdigit():
                                    ds = f"{ds}-01-01"
                                out.append({"date": ds[:10], "value": fv})
                            # API returns descending years; normalize to ascending so charts behave.
                            data = list(reversed(out))
        except Exception:
            pass

    return jsonify({"country_code": (country_code or "").upper()[:6], "indicator": indicator, "series": data})


@api.route("/macro/indicator/<indicator_name>")
def api_macro_indicator(indicator_name):
    """Series for an indicator across one country. Query: country (required), start, end."""
    country = (request.args.get("country") or "").strip().upper()
    if not country:
        return jsonify({"error": "country is required"}), 400
    start = (request.args.get("start") or "").strip() or None
    end = (request.args.get("end") or "").strip() or None
    data = macro_get_series(country_code=country, indicator=indicator_name, start=start, end=end)
    return jsonify({"country_code": country[:6], "indicator": (indicator_name or "").lower()[:80], "series": data})


@api.route("/macro/alerts")
def api_macro_alerts():
    """Rule-based macro alerts (MVP). Query: group, region."""
    region = (request.args.get("region") or "").strip() or None
    group = (request.args.get("group") or "").strip().lower() or None
    if group not in (None, "", "asean", "g20", "major"):
        group = None

    # Use latest values from our ingested tables (annual indicators).
    infl = macro_get_latest(region=region, group=group, indicator="inflation_cpi", limit=500)
    gdp = macro_get_latest(region=region, group=group, indicator="gdp_growth_yoy", limit=500)
    debt = macro_get_latest(region=region, group=group, indicator="gov_debt_pct_gdp", limit=500)

    def idx(rows):
        return {r["country_code"]: r for r in (rows or []) if r.get("country_code")}

    infl_i, gdp_i, debt_i = idx(infl), idx(gdp), idx(debt)
    alerts = []

    for code, row in infl_i.items():
        try:
            v = float(row.get("value"))
        except (TypeError, ValueError):
            continue
        if v >= 8:
            alerts.append({
                "type": "inflation_spike",
                "severity": "high" if v >= 12 else "med",
                "country_code": code,
                "country_name": row.get("country_name"),
                "value": v,
                "unit": row.get("unit") or "%",
                "date": row.get("date"),
                "message": f"High inflation ({v:.1f}%).",
            })

    for code, row in gdp_i.items():
        try:
            v = float(row.get("value"))
        except (TypeError, ValueError):
            continue
        if v <= 0:
            alerts.append({
                "type": "growth_negative",
                "severity": "high" if v <= -2 else "med",
                "country_code": code,
                "country_name": row.get("country_name"),
                "value": v,
                "unit": row.get("unit") or "%",
                "date": row.get("date"),
                "message": f"Negative growth ({v:.1f}%).",
            })

    for code, row in debt_i.items():
        try:
            v = float(row.get("value"))
        except (TypeError, ValueError):
            continue
        if v >= 90:
            alerts.append({
                "type": "debt_high",
                "severity": "high" if v >= 120 else "med",
                "country_code": code,
                "country_name": row.get("country_name"),
                "value": v,
                "unit": row.get("unit") or "%",
                "date": row.get("date"),
                "message": f"High government debt ({v:.0f}% of GDP).",
            })

    # Stable sort: severity then country
    sev_rank = {"high": 0, "med": 1, "low": 2}
    alerts.sort(key=lambda a: (sev_rank.get(a.get("severity") or "low", 9), a.get("country_name") or a.get("country_code") or ""))
    return jsonify({"alerts": alerts, "region": region, "group": group})


@api.route("/macro/export.csv")
def api_macro_export_csv():
    """Export macro data as CSV.

    Query:
      scope=latest|series
      For latest: indicator, group, region
      For series: country, indicator, start, end
    """
    scope = (request.args.get("scope") or "latest").strip().lower()
    if scope not in ("latest", "series"):
        scope = "latest"

    output = io.StringIO()
    writer = csv.writer(output)

    if scope == "series":
        country = (request.args.get("country") or "").strip().upper()
        indicator = (request.args.get("indicator") or "").strip().lower()
        if not country or not indicator:
            return jsonify({"error": "country and indicator required for scope=series"}), 400
        start = (request.args.get("start") or "").strip() or None
        end = (request.args.get("end") or "").strip() or None
        rows = macro_get_series(country_code=country, indicator=indicator, start=start, end=end, limit=5000)
        writer.writerow(["country", "indicator", "date", "value"])
        for r in rows:
            writer.writerow([country, indicator, r.get("date"), r.get("value")])
        filename = f"macro_series_{country}_{indicator}.csv"
    else:
        region = (request.args.get("region") or "").strip() or None
        group = (request.args.get("group") or "").strip().lower() or None
        if group not in (None, "", "asean", "g20", "major"):
            group = None
        indicator = (request.args.get("indicator") or "").strip().lower() or None
        rows = macro_get_latest(region=region, group=group, indicator=indicator, limit=2000)
        writer.writerow(["country_code", "country_name", "region", "indicator", "date", "value", "unit"])
        for r in rows:
            writer.writerow([
                r.get("country_code"),
                r.get("country_name"),
                r.get("region"),
                r.get("indicator"),
                r.get("date"),
                r.get("value"),
                r.get("unit"),
            ])
        filename = f"macro_latest_{indicator or 'all'}.csv"

    csv_text = output.getvalue()
    return Response(
        csv_text,
        mimetype="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-store",
        },
    )


@api.route("/macro/series_multi")
def api_macro_series_multi():
    """Fetch multiple indicator series in one call.

    Query:
      country= (required)
      indicators=comma-separated indicator names (required)
      start= (optional)
      end= (optional)
    """
    country = (request.args.get("country") or "").strip().upper()
    if not country:
        return jsonify({"error": "country is required"}), 400
    indicators_raw = (request.args.get("indicators") or "").strip()
    if not indicators_raw:
        return jsonify({"error": "indicators is required"}), 400
    indicators = []
    for x in indicators_raw.replace(";", ",").split(","):
        x = (x or "").strip().lower()
        if x:
            indicators.append(x[:80])
    indicators = list(dict.fromkeys(indicators))[:60]
    start = (request.args.get("start") or "").strip() or None
    end = (request.args.get("end") or "").strip() or None

    out = {}
    for ind in indicators:
        try:
            out[ind] = macro_get_series(country_code=country, indicator=ind, start=start, end=end, limit=2500)
        except Exception:
            out[ind] = []
    return jsonify({"country_code": country[:6], "start": start, "end": end, "series": out})


def _article_to_json(a):
    return {
        "id": a["id"],
        "title": a["title"],
        "url": a["url"],
        "source": a["source_name"],
        "summary": a.get("summary"),
        "published_utc": a.get("published_utc"),
        "topics": a.get("topics_list", []),
        "impact_score": a.get("impact_score"),
        "impact_domains": a.get("impact_domains_list", []),
        "urgency": a.get("urgency"),
        "event_type": a.get("event_type"),
        "image_url": a.get("image_url"),
        "video_url": a.get("video_url"),
    }


@api.route("/articles")
def api_articles():
    limit = min(int(request.args.get("limit", 50) or 50), 200)
    source = request.args.get("source", "").strip() or None
    topic = request.args.get("topic", "").strip() or None
    days = request.args.get("days", type=int) or None
    articles = get_articles(limit=limit, source=source, topic=topic, days=days)
    return jsonify({"articles": [_article_to_json(a) for a in articles]})


@api.route("/articles/<int:article_id>/implications")
def api_article_implications(article_id):
    """Institutional model context for an article (GEPI, CDEI, SFI, fragility)."""
    a = get_article(article_id)
    if not a:
        return jsonify({"error": "Article not found"}), 404
    if a.get("topics"):
        try:
            a["topics_list"] = json.loads(a["topics"]) if isinstance(a["topics"], str) else a["topics"]
        except (json.JSONDecodeError, TypeError):
            a["topics_list"] = []
    else:
        a["topics_list"] = []
    if a.get("entities"):
        try:
            a["entities_list"] = json.loads(a["entities"]) if isinstance(a["entities"], str) else a["entities"]
        except (json.JSONDecodeError, TypeError):
            a["entities_list"] = []
    else:
        a["entities_list"] = []
    try:
        from app.institutional_models.article_implications import get_article_implications
        implications = get_article_implications(a)
        return jsonify(implications)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api.route("/articles/<int:article_id>")
def api_article(article_id):
    a = get_article(article_id)
    if not a:
        return jsonify({"error": "Not found"}), 404
    return jsonify(_article_to_json(a))


@api.route("/search")
def api_search():
    q = request.args.get("q", "").strip()
    limit = min(int(request.args.get("limit", 50) or 50), 100)
    if not q:
        return jsonify({"articles": []})
    articles = search_articles(q, limit=limit)
    return jsonify({"articles": [_article_to_json(a) for a in articles]})


@api.route("/digests")
def api_digests():
    limit = min(int(request.args.get("limit", 20) or 20), 50)
    digest_type = request.args.get("type", "").strip() or None
    digests = get_digests(limit=limit, digest_type=digest_type)
    return jsonify({
        "digests": [
            {"id": d["id"], "title": d["title"], "digest_type": d["digest_type"], "created_at": d["created_at"]}
            for d in digests
        ]
    })


@api.route("/digests/<int:digest_id>")
def api_digest(digest_id):
    d = get_digest(digest_id)
    if not d:
        return jsonify({"error": "Not found"}), 404
    import json
    try:
        content = json.loads(d["content"])
    except (json.JSONDecodeError, TypeError):
        content = {}
    return jsonify({
        "id": d["id"],
        "title": d["title"],
        "digest_type": d["digest_type"],
        "created_at": d["created_at"],
        "content": content,
    })


@api.route("/watchlists")
def api_watchlists():
    wls = get_watchlists()
    return jsonify({
        "watchlists": [
            {"id": w["id"], "name": w["name"], "topics": w.get("topics_list", [])}
            for w in wls
        ]
    })


@api.route("/watchlists/<int:watchlist_id>")
def api_watchlist(watchlist_id):
    wl = get_watchlist(watchlist_id)
    if not wl:
        return jsonify({"error": "Not found"}), 404
    articles = get_articles_for_watchlist(watchlist_id, limit=100)
    return jsonify({
        "id": wl["id"],
        "name": wl["name"],
        "topics": wl.get("topics_list", []),
        "articles": [_article_to_json(a) for a in articles],
    })


@api.route("/alerts")
def api_alerts():
    user_id = g.api_user.get("id") if getattr(g, "api_user", None) else None
    alerts = get_alerts(user_id=user_id)
    return jsonify({
        "alerts": [
            {"id": a["id"], "name": a["name"], "topics": a.get("topics_list", []), "min_impact_score": a["min_impact_score"]}
            for a in alerts
        ]
    })


@api.route("/alerts/<int:alert_id>/matches")
def api_alert_matches(alert_id):
    days = request.args.get("days", type=int) or 1
    limit = min(int(request.args.get("limit", 50) or 50), 200)
    matches = get_alert_matches(alert_id, days=days, limit=limit)
    return jsonify({"matches": [_article_to_json(m) for m in matches]})


def _messaging_user_id():
    """Current user id from API key or session for messaging API."""
    if getattr(g, "api_user", None):
        return g.api_user.get("id")
    from flask_login import current_user
    if current_user.is_authenticated:
        return current_user.id
    return None


@api.route("/messaging/channels")
def api_messaging_channels():
    """List channels the current user is a member of."""
    uid = _messaging_user_id()
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    from app.models import messaging_get_channels_for_user
    channels = messaging_get_channels_for_user(uid, include_muted=True)
    return jsonify({"channels": [{"id": c["id"], "name": c["name"], "slug": c["slug"], "channel_type": c["channel_type"], "role": c["role"]} for c in channels]})


@api.route("/messaging/channels/<int:channel_id>/messages")
def api_messaging_messages(channel_id):
    """GET messages. Query: after_id (polling), before_id (load more), limit (default 50), parent_id (thread)."""
    uid = _messaging_user_id()
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    from app.models import messaging_get_channel_by_id, messaging_is_member
    from app.messaging import get_messages_for_channel
    ch = messaging_get_channel_by_id(channel_id)
    if not ch or not messaging_is_member(channel_id, uid):
        return jsonify({"error": "Forbidden"}), 403
    after_id = request.args.get("after_id", type=int)
    before_id = request.args.get("before_id", type=int)
    parent_id = request.args.get("parent_id", type=int)
    limit = min(int(request.args.get("limit", 50) or 50), 200)
    from flask import current_app
    secret = current_app.config.get("SECRET_KEY", "")
    messages = get_messages_for_channel(channel_id, secret, limit=limit, before_id=before_id, after_id=after_id, parent_id=parent_id)
    return jsonify({"messages": messages})


@api.route("/messaging/channels/<int:channel_id>/messages", methods=["POST"])
def api_messaging_post_message(channel_id):
    """POST new message. JSON: content, parent_id?, attachment_type?, attachment_id?."""
    uid = _messaging_user_id()
    if not uid:
        return jsonify({"error": "Unauthorized"}), 401
    from app.models import messaging_get_channel_by_id, messaging_is_member, messaging_add_message
    from app.messaging import add_message_encrypted, fire_channel_webhook
    ch = messaging_get_channel_by_id(channel_id)
    if not ch or not messaging_is_member(channel_id, uid):
        return jsonify({"error": "Forbidden"}), 403
    data = request.get_json() or {}
    content = (data.get("content") or "").strip()
    if not content or len(content) > 10000:
        return jsonify({"error": "Invalid content"}), 400
    from flask import current_app
    secret = current_app.config.get("SECRET_KEY", "")
    parent_id = data.get("parent_id")
    attachment_type = data.get("attachment_type")
    attachment_id = data.get("attachment_id")
    attachment_extra = (data.get("attachment_extra") or "").strip() or None
    if attachment_type == "risk":
        attachment_id = 0
        attachment_extra = None
    elif attachment_type == "country":
        attachment_extra = (data.get("attachment_country_code") or data.get("attachment_extra") or "").strip().upper()[:3] or None
        attachment_id = None
        if not attachment_extra:
            attachment_type = None
    msg_id = add_message_encrypted(channel_id, uid, content, secret, parent_id=parent_id, attachment_type=attachment_type, attachment_id=attachment_id, attachment_extra=attachment_extra)
    from app.models import get_user_by_id
    user = get_user_by_id(uid)
    from app.messaging import format_user_display
    author_label = format_user_display(user) or user.get("username", "")
    fire_channel_webhook(channel_id, msg_id, author_label, content)
    return jsonify({"id": msg_id})


@api.route("/briefing")
def api_briefing():
    """GET briefing as JSON: ?ids=1,2,3 and optional title=, intro=. Returns title, intro, articles (up to 50)."""
    ids_raw = request.args.get("ids", "").strip()
    title = request.args.get("title", "").strip() or "Briefing"
    intro = request.args.get("intro", "").strip()
    article_ids = []
    for x in ids_raw.replace(",", " ").split():
        x = x.strip()
        if x.isdigit():
            article_ids.append(int(x))
    article_ids = article_ids[:50]
    articles = []
    for aid in article_ids:
        a = get_article(aid)
        if a:
            articles.append(_article_to_json(a))
    return jsonify({"title": title, "intro": intro, "articles": articles})


@api.route("/scenario_engine/run")
def api_scenario_engine_run():
    """GET run Scenario Engine: ?event_type=...&region=...&country=.... Returns same result shape as UI (event_type, event_label, region, country, agents, paths, path_descriptions, run_at)."""
    event_type = (request.args.get("event_type") or "").strip() or "election_upset"
    region = (request.args.get("region") or "").strip()
    country = (request.args.get("country") or "").strip()
    result = run_scenario_engine(event_type, region=region, country=country)
    return jsonify(result)


# --- Real-Time Geopolitical Risk Engine ---
@api.route("/risk/heatmap")
def api_risk_heatmap():
    """Global risk heat map: country/region codes with risk scores and sector exposure."""
    data = get_country_risk_snapshots()
    return jsonify({"heat_map": data})


@api.route("/risk/index")
def api_risk_index():
    """Forward Risk Probability Index: coup %, sanctions %, trade disruption % by region."""
    region = request.args.get("region", "").strip() or None
    if region:
        row = get_risk_index(region_code=region)
        return jsonify({"index": row} if row else {"index": None})
    rows = get_risk_index()
    return jsonify({"index": rows})


@api.route("/risk/sectors")
def api_risk_sectors():
    """Sector-specific risk exposure by country (same as heatmap with sector_* fields)."""
    data = get_country_risk_snapshots()
    return jsonify({"sectors": data})


# --- Diplomacy & Treaty Intelligence ---
@api.route("/diplomacy/treaties")
def api_diplomacy_treaties():
    treaty_type = request.args.get("type", "").strip() or None
    party = request.args.get("party", "").strip() or None
    date_from = request.args.get("date_from", "").strip() or None
    date_to = request.args.get("date_to", "").strip() or None
    search = request.args.get("search", "").strip() or None
    escalation_only = request.args.get("escalation") == "1"
    limit = min(int(request.args.get("limit", 50) or 50), 200)
    offset = int(request.args.get("offset", 0) or 0)
    data = get_treaties(
        treaty_type=treaty_type,
        party=party,
        date_from=date_from,
        date_to=date_to,
        search=search,
        escalation_only=escalation_only,
        limit=limit,
        offset=offset,
    )
    total = get_treaties_count(
        treaty_type=treaty_type,
        party=party,
        date_from=date_from,
        date_to=date_to,
        search=search,
        escalation_only=escalation_only,
    )
    return jsonify({"treaties": data, "total": total})


@api.route("/diplomacy/treaties/<int:treaty_id>")
def api_diplomacy_treaty(treaty_id):
    t = get_treaty(treaty_id)
    if not t:
        return jsonify({"error": "Not found"}), 404
    return jsonify(t)


@api.route("/diplomacy/sanctions")
def api_diplomacy_sanctions():
    imposing = request.args.get("imposing", "").strip() or None
    target = request.args.get("target", "").strip() or None
    source = request.args.get("source", "").strip() or None
    date_from = request.args.get("date_from", "").strip() or None
    date_to = request.args.get("date_to", "").strip() or None
    limit = min(int(request.args.get("limit", 100) or 100), 200)
    offset = int(request.args.get("offset", 0) or 0)
    data = get_sanctions(imposing=imposing, target=target, source=source, date_from=date_from, date_to=date_to, limit=limit, offset=offset)
    total = get_sanctions_total_count(imposing=imposing, target=target, source=source, date_from=date_from, date_to=date_to)
    return jsonify({"sanctions": data, "total": total})


@api.route("/diplomacy/sanctions/<int:sanction_id>")
def api_diplomacy_sanction(sanction_id):
    s = get_sanction(sanction_id)
    if not s:
        return jsonify({"error": "Not found"}), 404
    return jsonify(s)


@api.route("/elections", methods=["GET", "POST"])
def api_elections():
    if request.method == "POST":
        data = request.get_json() or {}
        country_code = (data.get("country_code") or "").strip()
        country_name = (data.get("country_name") or "").strip()
        election_type = (data.get("election_type") or "").strip() or "other"
        date_planned = (data.get("date_planned") or "").strip()
        status = (data.get("status") or "").strip() or None
        notes = (data.get("notes") or "").strip() or None
        if not country_code or not country_name or not date_planned:
            return jsonify({"error": "country_code, country_name, and date_planned required"}), 400
        try:
            eid = add_election(
                country_code=country_code,
                country_name=country_name,
                election_type=election_type,
                date_planned=date_planned,
                status=status,
                notes=notes,
            )
            return jsonify({"id": eid, "message": "Election added"}), 201
        except Exception as err:
            return jsonify({"error": str(err)}), 500
    country_code = request.args.get("country_code", "").strip() or None
    region = request.args.get("region", "").strip() or None
    date_from = request.args.get("date_from", "").strip() or None
    date_to = request.args.get("date_to", "").strip() or None
    status = request.args.get("status", "").strip() or None
    election_type = request.args.get("election_type", "").strip() or None
    search = request.args.get("search", "").strip() or None
    limit = min(int(request.args.get("limit", 100) or 100), 500)
    offset = int(request.args.get("offset", 0) or 0)
    order_by = request.args.get("sort", "").strip() or None
    data = get_election_calendar(
        country_code=country_code,
        region=region,
        date_from=date_from,
        date_to=date_to,
        status=status,
        election_type=election_type,
        search=search,
        limit=limit,
        offset=offset,
        order_by=order_by,
    )
    total = get_election_calendar_count(
        country_code=country_code,
        region=region,
        date_from=date_from,
        date_to=date_to,
        status=status,
        election_type=election_type,
        search=search,
    )
    return jsonify({"elections": data, "total": total})


@api.route("/elections/<int:election_id>")
def api_election(election_id):
    e = get_election(election_id)
    if not e:
        return jsonify({"error": "Not found"}), 404
    return jsonify(e)


@api.route("/approval-ratings", methods=["GET", "POST"])
def api_approval_ratings():
    if request.method == "POST":
        data = request.get_json() or {}
        country_code = (data.get("country_code") or "").strip()
        country_name = (data.get("country_name") or "").strip()
        subject = (data.get("subject") or "").strip()
        approval_pct = data.get("approval_pct")
        if approval_pct is not None:
            try:
                approval_pct = float(approval_pct)
            except (TypeError, ValueError):
                approval_pct = None
        poll_date = (data.get("poll_date") or "").strip() or None
        source = (data.get("source") or "").strip() or None
        poll_url = (data.get("poll_url") or "").strip() or None
        sample_size = data.get("sample_size") if "sample_size" in data else None
        if sample_size is not None:
            try:
                sample_size = int(sample_size)
            except (TypeError, ValueError):
                sample_size = None
        if not country_code or not country_name or not subject or approval_pct is None:
            return jsonify({"error": "country_code, country_name, subject, and approval_pct required"}), 400
        try:
            rid = add_approval_rating(
                country_code=country_code,
                country_name=country_name,
                subject=subject,
                approval_pct=approval_pct,
                poll_date=poll_date,
                source=source,
                poll_url=poll_url,
                sample_size=sample_size,
            )
            return jsonify({"id": rid, "message": "Approval rating added"}), 201
        except Exception as err:
            return jsonify({"error": str(err)}), 500
    country_code = request.args.get("country_code", "").strip() or None
    region = request.args.get("region", "").strip() or None
    date_from = request.args.get("date_from", "").strip() or None
    date_to = request.args.get("date_to", "").strip() or None
    subject = request.args.get("subject", "").strip() or None
    min_approval = request.args.get("min_approval", type=float)
    max_approval = request.args.get("max_approval", type=float)
    source = request.args.get("source", "").strip() or None
    limit = min(int(request.args.get("limit", 100) or 100), 500)
    offset = int(request.args.get("offset", 0) or 0)
    order_by = request.args.get("sort", "").strip() or None
    data = get_approval_ratings(
        country_code=country_code,
        region=region,
        date_from=date_from,
        date_to=date_to,
        subject=subject,
        min_approval=min_approval,
        max_approval=max_approval,
        source=source,
        limit=limit,
        offset=offset,
        order_by=order_by,
    )
    total = get_approval_ratings_count(
        country_code=country_code,
        region=region,
        date_from=date_from,
        date_to=date_to,
        subject=subject,
        min_approval=min_approval,
        max_approval=max_approval,
        source=source,
    )
    return jsonify({"approval": data, "total": total})


@api.route("/approval-ratings/<int:rating_id>", methods=["GET", "PATCH", "DELETE"])
def api_approval_rating(rating_id):
    r = get_approval_rating(rating_id)
    if not r:
        return jsonify({"error": "Not found"}), 404
    if request.method == "DELETE":
        if delete_approval_rating(rating_id):
            return jsonify({"message": "Deleted"}), 200
        return jsonify({"error": "Not found"}), 404
    if request.method == "PATCH":
        data = request.get_json() or {}
        update_approval_rating(
            rating_id,
            country_code=(data.get("country_code") or "").strip() or None,
            country_name=(data.get("country_name") or "").strip() or None,
            subject=(data.get("subject") or "").strip() or None,
            approval_pct=data.get("approval_pct") if "approval_pct" in data else None,
            poll_date=(data.get("poll_date") or "").strip() or None,
            source=(data.get("source") or "").strip() or None,
            poll_url=(data.get("poll_url") or "").strip() or None,
            sample_size=data.get("sample_size") if "sample_size" in data else None,
        )
        r = get_approval_rating(rating_id)
        return jsonify(r)
    return jsonify(r)


@api.route("/protests", methods=["GET", "POST"])
def api_protests():
    if request.method == "POST":
        data = request.get_json() or {}
        country_code = (data.get("country_code") or "").strip()
        country_name = (data.get("country_name") or "").strip()
        event_date = (data.get("event_date") or "").strip()
        summary = (data.get("summary") or "").strip() or None
        estimated_size = (data.get("estimated_size") or "").strip() or None
        trigger_topic = (data.get("trigger_topic") or "").strip() or None
        location = (data.get("location") or "").strip() or None
        severity = (data.get("severity") or "").strip() or None
        source_url = (data.get("source_url") or "").strip() or None
        if not country_code or not country_name or not event_date:
            return jsonify({"error": "country_code, country_name, and event_date required"}), 400
        try:
            pid = add_protest_event(
                country_code=country_code,
                country_name=country_name,
                event_date=event_date,
                summary=summary,
                estimated_size=estimated_size,
                trigger_topic=trigger_topic,
                location=location,
                severity=severity,
                source_url=source_url,
            )
            return jsonify({"id": pid, "message": "Protest event added"}), 201
        except Exception as err:
            return jsonify({"error": str(err)}), 500
    country_code = request.args.get("country_code", "").strip() or None
    region = request.args.get("region", "").strip() or None
    date_from = request.args.get("date_from", "").strip() or None
    date_to = request.args.get("date_to", "").strip() or None
    trigger_topic = request.args.get("trigger_topic", "").strip() or None
    search = request.args.get("search", "").strip() or None
    limit = min(int(request.args.get("limit", 100) or 100), 500)
    offset = int(request.args.get("offset", 0) or 0)
    order_by = request.args.get("sort", "").strip() or None
    data = get_protest_tracking(
        country_code=country_code,
        region=region,
        date_from=date_from,
        date_to=date_to,
        trigger_topic=trigger_topic,
        search=search,
        limit=limit,
        offset=offset,
        order_by=order_by,
    )
    total = get_protest_tracking_count(
        country_code=country_code,
        region=region,
        date_from=date_from,
        date_to=date_to,
        trigger_topic=trigger_topic,
        search=search,
    )
    return jsonify({"protests": data, "total": total})


@api.route("/protests/<int:protest_id>", methods=["GET", "PATCH", "DELETE"])
def api_protest(protest_id):
    p = get_protest(protest_id)
    if not p:
        return jsonify({"error": "Not found"}), 404
    if request.method == "DELETE":
        if delete_protest(protest_id):
            return jsonify({"message": "Deleted"}), 200
        return jsonify({"error": "Not found"}), 404
    if request.method == "PATCH":
        data = request.get_json() or {}
        update_protest(
            protest_id,
            country_code=(data.get("country_code") or "").strip() or None,
            country_name=(data.get("country_name") or "").strip() or None,
            event_date=(data.get("event_date") or "").strip() or None,
            summary=(data.get("summary") or "").strip() or None,
            estimated_size=(data.get("estimated_size") or "").strip() or None,
            trigger_topic=(data.get("trigger_topic") or "").strip() or None,
            location=(data.get("location") or "").strip() or None,
            severity=(data.get("severity") or "").strip() or None,
            source_url=(data.get("source_url") or "").strip() or None,
        )
        p = get_protest(protest_id)
        if request.args.get("format") == "geojson":
            return _protest_to_geojson(p)
        return jsonify(p)
    if request.args.get("format") == "geojson":
        return _protest_to_geojson(p)
    return jsonify(p)


def _protest_to_geojson(p):
    """Return a single protest as GeoJSON Feature (geometry null if no coords)."""
    return jsonify({
        "type": "Feature",
        "id": p.get("id"),
        "geometry": None,
        "properties": {k: v for k, v in p.items() if k != "id"}
    })


@api.route("/sanctions/entity-list")
def api_sanctions_entity_list():
    source = request.args.get("source", "").strip() or None
    search = request.args.get("search", "").strip() or None
    limit = min(int(request.args.get("limit", 100) or 100), 500)
    data = get_entity_list_alerts(source=source, search=search, limit=limit)
    return jsonify({"entities": data})


@api.route("/sanctions/export-restrictions")
def api_sanctions_export_restrictions():
    issuer = request.args.get("issuer", "").strip() or None
    restriction_type = request.args.get("restriction_type", "").strip() or None
    search = request.args.get("search", "").strip() or None
    limit = min(int(request.args.get("limit", 100) or 100), 500)
    data = get_export_restrictions(issuer=issuer, restriction_type=restriction_type, search=search, limit=limit)
    return jsonify({"restrictions": data})


@api.route("/diplomacy/alignment")
def api_diplomacy_alignment():
    """Alignment with sort, bloc, pagination. Params: country, min_votes, sort, bloc, limit, offset, archive."""
    country = request.args.get("country", "").strip() or None
    min_votes = request.args.get("min_votes", type=int)
    sort = (request.args.get("sort") or "score_desc").strip()
    bloc = request.args.get("bloc", "").strip() or None
    limit = min(int(request.args.get("limit", 50) or 50), 200)
    offset = max(0, int(request.args.get("offset", 0) or 0))
    archive = request.args.get("archive", "").strip().lower() in ("1", "true", "yes")
    data = get_voting_alignment(
        country=country,
        min_votes=min_votes,
        limit=limit,
        offset=offset,
        sort=sort,
        bloc=bloc,
        include_defunct=archive,
    )
    return jsonify({"alignment": data})


@api.route("/diplomacy/resolutions")
def api_diplomacy_resolutions():
    """List UN resolutions. Params: limit, offset, search, country (ISO3 - resolutions where country voted)."""
    from app.models import get_un_resolutions_for_country
    limit = min(int(request.args.get("limit", 50) or 50), 200)
    offset = int(request.args.get("offset", 0) or 0)
    search = (request.args.get("search") or "").strip() or None
    country = (request.args.get("country") or "").strip().upper()[:3] or None
    if country:
        resolutions = get_un_resolutions_for_country(country, limit=500)
    else:
        resolutions = get_un_resolutions(limit=500 if search else limit + offset, use_cache=not search)
    if search:
        q = search.lower()
        resolutions = [r for r in resolutions if q in (r.get("resolution_id") or "").lower() or q in (r.get("resolution_title") or "").lower()]
    resolutions = resolutions[offset:offset + limit]
    return jsonify({"resolutions": resolutions})


@api.route("/diplomacy/resolutions/<path:resolution_id>")
def api_diplomacy_resolution_detail(resolution_id):
    """Vote breakdown for a resolution."""
    detail = get_un_resolution_detail(resolution_id)
    if not detail:
        return jsonify({"error": "Resolution not found"}), 404
    return jsonify(detail)


@api.route("/integration/countries")
def api_integration_countries():
    """Country list for integration page autocomplete: [{name, code}, ...]."""
    try:
        countries = get_integration_countries(limit=300)
        return jsonify({
            "countries": [
                {"name": c.get("country_name") or "", "code": c.get("country_code") or ""}
                for c in (countries or []) if c.get("country_code")
            ]
        })
    except Exception as e:
        return jsonify({"error": str(e), "countries": []})


@api.route("/diplomacy/countries")
def api_diplomacy_countries():
    """Country list for autocomplete: [{name, iso3}, ...]."""
    try:
        from app.un_votes.country_map import get_countries_for_autocomplete
        pairs = get_countries_for_autocomplete()
        return jsonify({"countries": [{"name": n, "iso3": c} for n, c in pairs]})
    except Exception as e:
        return jsonify({"error": str(e), "countries": []})


@api.route("/un/country/<iso3>/alignment")
def api_un_country_alignment(iso3):
    """UN voting alignment for a country: USA/CHN alignment, volatility, shocks, by-issue."""
    try:
        from app.un_votes.readers import get_country_alignment_summary
        data = get_country_alignment_summary((iso3 or "").strip().upper()[:3])
        return jsonify({
            "values": data,
            "time_window": "rolling 12m/36m",
            "method_version": "gpi_un_v1",
            "last_updated": None,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api.route("/un/pair/alignment")
def api_un_pair_alignment():
    """Pairwise alignment. Params: country_a, country_b, window (12m/36m)."""
    ca = (request.args.get("country_a") or "").strip().upper()[:3]
    cb = (request.args.get("country_b") or "").strip().upper()[:3]
    window = request.args.get("window", "12m")
    if not ca or not cb:
        return jsonify({"error": "country_a and country_b required"}), 400
    try:
        from app.un_votes.readers import get_rolling_alignment
        data = get_rolling_alignment(country_a=ca, country_b=cb, window=window, limit=10)
        return jsonify({
            "values": data,
            "time_window": window,
            "vote_count_used": data[0].get("vote_count_used") if data else None,
            "method_version": data[0].get("method_version") if data else None,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api.route("/un/pair/alignment-by-issue")
def api_un_pair_alignment_by_issue():
    """Alignment by issue. Params: country_a, country_b, issue_tag (optional)."""
    ca = (request.args.get("country_a") or "").strip().upper()[:3]
    cb = (request.args.get("country_b") or "").strip().upper()[:3]
    issue = (request.args.get("issue_tag") or "").strip() or None
    if not ca or not cb:
        return jsonify({"error": "country_a and country_b required"}), 400
    try:
        from app.un_votes.readers import get_alignment_by_issue
        data = get_alignment_by_issue(country_a=ca, country_b=cb, issue_tag=issue, limit=50)
        return jsonify({
            "values": data,
            "method_version": data[0].get("method_version") if data else None,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api.route("/un/bloc/<name>/cohesion")
def api_un_bloc_cohesion(name):
    """Bloc cohesion for ASEAN, EU, G7, BRICS, etc."""
    try:
        from app.un_votes.readers import get_bloc_cohesion, get_bloc_member_positions
        cohesion = get_bloc_cohesion(bloc_name=name.upper(), limit=24)
        members = get_bloc_member_positions(bloc_name=name.upper()) if cohesion else []
        return jsonify({
            "values": {"cohesion": cohesion, "member_positions": members},
            "method_version": cohesion[0].get("method_version") if cohesion else None,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api.route("/un/polarization")
def api_un_polarization():
    """Global polarization index time series."""
    try:
        from app.un_votes.readers import get_global_polarization
        data = get_global_polarization(limit=int(request.args.get("limit", 24) or 24))
        return jsonify({
            "values": data,
            "method_version": data[0].get("method_version") if data else None,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api.route("/diplomacy/votes", methods=["POST"])
def api_diplomacy_add_vote():
    """Add a UN vote. JSON: resolution_id, country_code, vote (yes/no/abstain/absent), vote_date (YYYY-MM-DD), resolution_title (optional)."""
    data = request.get_json() or {}
    resolution_id = (data.get("resolution_id") or "").strip()
    country_code = (data.get("country_code") or "").strip()
    vote = (data.get("vote") or "").strip().lower()
    vote_date = (data.get("vote_date") or "").strip()
    if not resolution_id or not country_code or not vote or not vote_date:
        return jsonify({"error": "resolution_id, country_code, vote, vote_date required"}), 400
    if vote not in ("yes", "no", "abstain", "absent"):
        return jsonify({"error": "vote must be yes, no, abstain, or absent"}), 400
    add_un_vote(
        resolution_id=resolution_id,
        resolution_title=(data.get("resolution_title") or "").strip() or None,
        country_code=country_code,
        vote=vote,
        vote_date=vote_date,
    )
    return jsonify({"ok": True})


# --- Relationship Mapper graph data ---
def _build_relationship_graph(mode, region_filter=None, min_weight=None, min_degree=None, un_window=None, un_issue=None):
    """Build nodes and edges for economic|military|diplomatic|sanctions network. Node id = country code.
    region_filter: only include nodes in this region (and edges between them).
    min_weight: for diplomatic mode, only include edges with weight >= min_weight (0-1).
    min_degree: optional; only include nodes with at least this many links (1 = drop isolated).
    un_window: for diplomatic, use GPI rolling alignment (12m or 36m) instead of all-time.
    un_issue: for diplomatic, use GPI alignment-by-issue (e.g. human_rights, nuclear).
    """
    integration = get_integration_countries(limit=300)
    name_to_code = {}
    for c in integration:
        code = (c.get("country_code") or "").strip()
        name = (c.get("country_name") or "").strip()
        if code:
            name_to_code[name.lower()] = code
            name_to_code[code.upper()] = code

    def resolve(s):
        if not s:
            return None
        s = (s or "").strip()
        if not s:
            return None
        return name_to_code.get(s.lower()) or name_to_code.get(s.upper()) or (s if len(s) <= 3 else None)

    code_to_info = {}
    for c in integration:
        code = (c.get("country_code") or "").strip()
        if not code:
            continue
        code_to_info[code] = {
            "label": (c.get("country_name") or code),
            "region": (c.get("region") or "Other").strip(),
            "trade": float(c.get("trade_flow_pct_gdp") or 0),
        }

    defense_by_country = defaultdict(float)
    for row in get_defense_spending(limit=500):
        code = (row.get("country_code") or "").strip()
        if code:
            val = float(row.get("spending_usd_billions") or 0)
            if val > defense_by_country[code]:
                defense_by_country[code] = val

    alignment = get_voting_alignment(limit=400)
    treaties = get_treaties(limit=300)
    sanctions_list = get_sanctions(limit=400)

    # GPI UN rolling or by-issue (for diplomatic mode when un_window or un_issue set)
    gpi_align = []
    if mode == "diplomatic" and (un_window or un_issue):
        try:
            from app.un_votes.readers import get_rolling_alignment, get_alignment_by_issue
            if un_issue:
                gpi_align = get_alignment_by_issue(issue_tag=un_issue, limit=500)
                # Take latest year per pair; similarity_score is 0-100
                by_pair = {}
                for row in gpi_align:
                    a, b = (row.get("country_a") or "").strip(), (row.get("country_b") or "").strip()
                    if a and b and a != b:
                        key = (min(a, b), max(a, b))
                        if key not in by_pair or (row.get("year") or "") > (by_pair[key].get("year") or ""):
                            by_pair[key] = row
                gpi_align = list(by_pair.values())
            elif un_window in ("12m", "36m"):
                gpi_align = get_rolling_alignment(window=un_window, limit=500)
                # Take latest end_date per pair; similarity_score is 0-100
                by_pair = {}
                for row in gpi_align:
                    a, b = (row.get("country_a") or "").strip(), (row.get("country_b") or "").strip()
                    if a and b and a != b:
                        key = (min(a, b), max(a, b))
                        if key not in by_pair or (row.get("end_date") or "") > (by_pair[key].get("end_date") or ""):
                            by_pair[key] = row
                gpi_align = list(by_pair.values())
        except Exception:
            gpi_align = []

    all_codes = set(code_to_info.keys())
    edges = []
    latest_updated = None

    if mode == "diplomatic":
        source_rows = gpi_align if gpi_align else alignment
        for row in source_rows:
            a, b = (row.get("country_a") or "").strip(), (row.get("country_b") or "").strip()
            if a and b and a != b:
                all_codes.add(a)
                all_codes.add(b)
                # voting_alignment: alignment_score 0-1; gpi: similarity_score 0-100
                if gpi_align:
                    score = float(row.get("similarity_score") or 0) / 100.0
                    votes_total = row.get("vote_count_used")
                    suffix = " ({})".format(un_issue or un_window) if (un_issue or un_window) else ""
                    label = "UN alignment {:.0f}%{}".format(score * 100, suffix)
                    if votes_total is not None:
                        label += " ({} votes)".format(votes_total)
                else:
                    score = float(row.get("alignment_score") or 0)
                    votes_agreed = row.get("votes_agreed")
                    votes_total = row.get("votes_total")
                    label = "UN alignment {:.0f}%".format(score * 100)
                    if votes_agreed is not None and votes_total is not None:
                        label += " ({}/{} votes)".format(votes_agreed, votes_total)
                if min_weight is not None and score < min_weight:
                    continue
                if score > 0:
                    edges.append({"source": a, "target": b, "weight": score, "label": label})
                u = row.get("updated_at") or row.get("end_date")
                if u and (latest_updated is None or u > latest_updated):
                    latest_updated = u
    elif mode == "military":
        for t in treaties:
            if (t.get("treaty_type") or "").strip() != "defense_pact":
                continue
            pa, pb = (t.get("party_a") or "").strip(), (t.get("party_b") or "").strip()
            ca, cb = resolve(pa), resolve(pb)
            if ca and cb and ca != cb:
                all_codes.add(ca)
                all_codes.add(cb)
                tid = t.get("id")
                edges.append({
                    "source": ca, "target": cb, "weight": 2,
                    "label": (t.get("name") or "Defense pact")[:60],
                    "treaty_id": int(tid) if tid is not None else None,
                })
    elif mode == "economic":
        for t in treaties:
            tt = (t.get("treaty_type") or "").strip()
            if tt not in ("trade_agreement", "investment_treaty"):
                continue
            pa, pb = (t.get("party_a") or "").strip(), (t.get("party_b") or "").strip()
            ca, cb = resolve(pa), resolve(pb)
            if ca and cb and ca != cb:
                all_codes.add(ca)
                all_codes.add(cb)
                tid = t.get("id")
                edges.append({
                    "source": ca, "target": cb, "weight": 1,
                    "label": (t.get("name") or tt.replace("_", " ").title())[:60],
                    "treaty_id": int(tid) if tid is not None else None,
                })
    else:
        mode = "sanctions"
        for s in sanctions_list:
            imp, tgt = (s.get("imposing_country") or "").strip(), (s.get("target_country") or "").strip()
            ci, ct = resolve(imp), resolve(tgt)
            if ci and ct and ci != ct:
                all_codes.add(ci)
                all_codes.add(ct)
                mtype = (s.get("measure_type") or "sanctions")[:20]
                edges.append({
                    "source": ci, "target": ct, "weight": 1,
                    "label": "{} → {} ({})".format(imp or ci, tgt or ct, mtype),
                    "directed": True,
                })

    if region_filter:
        all_codes = {c for c in all_codes if (code_to_info.get(c) or {}).get("region") == region_filter}
        edges = [e for e in edges if e["source"] in all_codes and e["target"] in all_codes]

    for e in edges:
        all_codes.add(e["source"])
        all_codes.add(e["target"])

    if min_degree is not None and min_degree >= 1:
        degree = defaultdict(int)
        for e in edges:
            degree[e["source"]] += 1
            degree[e["target"]] += 1
        all_codes = {c for c in all_codes if degree[c] >= min_degree}
        edges = [e for e in edges if e["source"] in all_codes and e["target"] in all_codes]

    nodes = []
    for code in sorted(all_codes):
        info = code_to_info.get(code) or {}
        label = info.get("label") or code
        reg = info.get("region") or "Other"
        if mode == "military":
            size = max(1, defense_by_country.get(code, 0) * 2)
            tooltip = "{} · {} · Defense: ${:.0f}B".format(label, reg, defense_by_country.get(code, 0))
        elif mode == "economic":
            size = max(1, info.get("trade", 0) * 0.5)
            tooltip = "{} · {} · Trade flow: {:.0f}% GDP".format(label, reg, info.get("trade", 0))
        else:
            size = max(2, defense_by_country.get(code, 0) * 0.5 + 2)
            tooltip = "{} · {}".format(label, reg)
        nodes.append({
            "id": code,
            "label": label,
            "size": round(size, 1),
            "bloc": reg,
            "tooltip": tooltip,
        })

    return {"mode": mode, "nodes": nodes, "edges": edges, "updated_at": latest_updated}


@api.route("/relationship-graph")
def api_relationship_graph():
    """Relationship Mapper: nodes and edges for economic|military|diplomatic|sanctions network."""
    mode = (request.args.get("mode") or "diplomatic").strip().lower()
    if mode not in ("economic", "military", "diplomatic", "sanctions"):
        mode = "diplomatic"
    region_filter = request.args.get("region", "").strip() or None
    min_weight = request.args.get("min_weight", type=float)
    if min_weight is not None and (min_weight < 0 or min_weight > 1):
        min_weight = None
    min_degree = request.args.get("min_degree", type=int)
    if min_degree is not None and min_degree < 0:
        min_degree = None
    un_window = (request.args.get("un_window") or "").strip() or None
    if un_window and un_window not in ("12m", "36m"):
        un_window = None
    un_issue = (request.args.get("un_issue") or "").strip() or None
    data = _build_relationship_graph(
        mode,
        region_filter=region_filter,
        min_weight=min_weight,
        min_degree=min_degree,
        un_window=un_window,
        un_issue=un_issue,
    )
    return jsonify(data)


# --- Supply chain: chokepoints & flows ---
@api.route("/chokepoints")
def api_chokepoints():
    """List chokepoints with lat, lon, pct_global_trade for map."""
    chokepoints = get_chokepoints_with_geo()
    return jsonify({"chokepoints": chokepoints})


@api.route("/chokepoints/<int:chokepoint_id>/flows")
def api_chokepoint_flows(chokepoint_id):
    """Flows for one chokepoint (for optional client-side panel load)."""
    cp = get_chokepoint(chokepoint_id)
    if not cp:
        return jsonify({"error": "Chokepoint not found"}), 404
    flows = get_flows_for_chokepoint(chokepoint_id)
    return jsonify({"chokepoint": cp, "flows": flows})


@api.route("/chokepoints/<int:chokepoint_id>/naval")
def api_chokepoint_naval(chokepoint_id):
    """Naval deployments in the chokepoint's region."""
    cp = get_chokepoint(chokepoint_id)
    if not cp:
        return jsonify({"error": "Chokepoint not found"}), 404
    region = (cp.get("region") or "").strip()
    deployments = get_naval_deployments(region=region, limit=20) if region else []
    return jsonify({"chokepoint_id": chokepoint_id, "region": region, "deployments": deployments})


@api.route("/country/<country_code>/chokepoint-exposure")
def api_country_chokepoint_exposure(country_code):
    """Chokepoint flows for a country (for country dashboard)."""
    flows = get_flows_for_country(country_code=country_code, limit=50)
    return jsonify({"country_code": country_code, "flows": flows})


# World GeoJSON for Situation Room map (server-side fetch to avoid CORS)
# 110m resolution = smaller file (~700KB) so it loads much faster than full-res
_WORLD_GEOJSON_URL = "https://raw.githubusercontent.com/datasets/geo-boundaries-world-110m/master/countries.geojson"
_world_geojson_cache = None


def _fetch_world_geojson():
    """Fetch world countries GeoJSON (cached in memory after first load)."""
    global _world_geojson_cache
    if _world_geojson_cache is not None:
        return _world_geojson_cache
    try:
        req = Request(_WORLD_GEOJSON_URL, headers={"User-Agent": "GeopoliticalNews/1.0"})
        with urlopen(req, timeout=12) as resp:
            data = json.loads(resp.read().decode())
        _world_geojson_cache = data
        return data
    except Exception:
        return {"type": "FeatureCollection", "features": []}


@api.route("/world-geojson")
def api_world_geojson():
    """Serve world countries GeoJSON (proxied to avoid CORS, cached for speed)."""
    data = _fetch_world_geojson()
    return Response(
        json.dumps(data),
        mimetype="application/json",
        headers={"Cache-Control": "public, max-age=3600"},
    )


# --- Conflict / Military & Security API ---

def _conflict_api_filters():
    def _int(v, d=None):
        try:
            return int(v) if v not in (None, "") else d
        except (TypeError, ValueError):
            return d
    return {
        "defense_country": request.args.get("country_code") or request.args.get("defense_country") or None,
        "year_from": _int(request.args.get("year_from")),
        "year_to": _int(request.args.get("year_to")),
        "region": (request.args.get("region") or "").strip() or None,
        "country_code": (request.args.get("country_code") or "").strip() or None,
        "date_from": (request.args.get("date_from") or "").strip() or None,
        "date_to": (request.args.get("date_to") or "").strip() or None,
        "severity": (request.args.get("severity") or "").strip() or None,
        "limit": min(_int(request.args.get("limit"), 100), 500),
    }


@api.route("/conflict/defense")
def api_conflict_defense():
    f = _conflict_api_filters()
    rows = get_defense_spending(country_code=f["defense_country"], year_from=f["year_from"], year_to=f["year_to"], limit=f["limit"])
    return jsonify({"data": rows})


@api.route("/conflict/exercises")
def api_conflict_exercises():
    f = _conflict_api_filters()
    rows = get_military_exercises(region=f["region"], date_from=f["date_from"], date_to=f["date_to"], limit=f["limit"])
    return jsonify({"data": rows})


@api.route("/conflict/incidents")
def api_conflict_incidents():
    f = _conflict_api_filters()
    rows = get_border_incidents(country_code=f["country_code"], date_from=f["date_from"], date_to=f["date_to"], severity=f["severity"], limit=f["limit"])
    return jsonify({"data": rows})


@api.route("/conflict/movement")
def api_conflict_movement():
    f = _conflict_api_filters()
    rows = get_military_movement(country_code=f["country_code"], region=f["region"], date_from=f["date_from"], date_to=f["date_to"], limit=f["limit"])
    return jsonify({"data": rows})


@api.route("/conflict/naval")
def api_conflict_naval():
    f = _conflict_api_filters()
    rows = get_naval_deployments(region=f["region"], country_code=f["country_code"], limit=f["limit"])
    return jsonify({"data": rows})


@api.route("/conflict/arms")
def api_conflict_arms():
    supplier = (request.args.get("supplier") or "").strip() or None
    recipient = (request.args.get("recipient") or "").strip() or None
    year_from = request.args.get("year_from", type=int)
    year_to = request.args.get("year_to", type=int)
    limit = min(int(request.args.get("limit") or 100), 500)
    rows = get_arms_trade(supplier=supplier, recipient=recipient, year_from=year_from, year_to=year_to, limit=limit)
    return jsonify({"data": rows})


# --- Institutional models (GEPI, CDEI, SFI, fragility) ---

@api.route("/gpi/gepi")
def api_gpi_gepi():
    """Latest GEPI score and history."""
    from app.institutional_models.readers import get_gepi_latest, get_gepi_history, get_gepi_channel_scores
    latest = get_gepi_latest()
    days = min(int(request.args.get("days") or 30), 365)
    history = get_gepi_history(days=days)
    channels = get_gepi_channel_scores(latest["as_of_date"] if latest else None) if request.args.get("channels") else []
    return jsonify({"latest": latest, "history": history, "channels": channels})


@api.route("/gpi/cdei")
def api_gpi_cdei():
    """CDEI by country."""
    from app.institutional_models.readers import get_cdei_by_country
    country = (request.args.get("country_code") or "").strip() or None
    as_of = (request.args.get("as_of") or "").strip() or None
    rows = get_cdei_by_country(country_code=country, as_of=as_of)
    return jsonify({"data": rows})


@api.route("/gpi/sfi")
def api_gpi_sfi():
    """SFI by country."""
    from app.institutional_models.readers import get_sfi_by_country
    country = (request.args.get("country_code") or "").strip() or None
    as_of = (request.args.get("as_of") or "").strip() or None
    rows = get_sfi_by_country(country_code=country, as_of=as_of)
    return jsonify({"data": rows})


@api.route("/gpi/fragility")
def api_gpi_fragility():
    """Fragility by country."""
    from app.institutional_models.readers import get_fragility_by_country
    country = (request.args.get("country_code") or "").strip() or None
    as_of = (request.args.get("as_of") or "").strip() or None
    rows = get_fragility_by_country(country_code=country, as_of=as_of)
    return jsonify({"data": rows})


# --- Lightweight UX analytics (logged server-side; no PII stored) ---

@api.route("/ux/event", methods=["POST"])
def ux_event():
    """Record anonymized UI events for product improvement (body JSON: event, path, meta)."""
    if not request.is_json:
        return jsonify({"ok": False, "error": "expected JSON"}), 400
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"ok": False, "error": "invalid body"}), 400
    event = str(data.get("event") or "")[:80]
    path = str(data.get("path") or "")[:512]
    role = str(data.get("role") or "")[:32]
    meta = data.get("meta")
    meta_s = str(meta)[:240] if meta is not None else ""
    if not event:
        return jsonify({"ok": False, "error": "missing event"}), 400
    current_app.logger.info("ux_event event=%s path=%s role=%s meta=%s", event, path, role or "-", meta_s or "-")
    return "", 204
