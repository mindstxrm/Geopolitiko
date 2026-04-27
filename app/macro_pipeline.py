"""Macro data ingestion pipeline.

Called from the background scheduler. Ingests into `indicator_values`:
- World Bank Indicators (annual, no key)
- Frankfurter FX / ECB (daily, no key)
- FRED US series (monthly; requires ``FRED_API_KEY``)
- Eurostat (annual; EU harmonised unemployment, no key)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import requests

from config import fred_api_key

from app.models import (
    macro_insert_value,
    macro_list_countries,
    macro_list_indicators,
    macro_seed_defaults,
)

logger = logging.getLogger(__name__)

WORLD_BANK_BASE = "https://api.worldbank.org/v2"
FX_HOST_BASE = "https://api.frankfurter.app"
FRED_API_BASE = "https://api.stlouisfed.org/fred"
EUROSTAT_API = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"

# Eurostat geo codes (API) -> ISO3 country codes in our `countries` table
EUROSTAT_UNEMP_GEO = [
    ("DE", "DEU"),
    ("FR", "FRA"),
    ("IT", "ITA"),
    ("ES", "ESP"),
    ("NL", "NLD"),
    ("PL", "POL"),
]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_world_bank_latest(payload) -> Tuple[Optional[float], Optional[str]]:
    """Return (value, date) from World Bank indicator payload."""
    if not isinstance(payload, list) or len(payload) < 2:
        return None, None
    rows = payload[1]
    if not isinstance(rows, list):
        return None, None
    for row in rows:
        if not isinstance(row, dict):
            continue
        val = row.get("value")
        if val is None:
            continue
        date = row.get("date")
        try:
            return float(val), str(date) if date is not None else None
        except (TypeError, ValueError):
            continue
    return None, None


def _fetch_world_bank(country_code: str, indicator_code: str) -> Tuple[Optional[float], Optional[str], str]:
    url = f"{WORLD_BANK_BASE}/country/{country_code}/indicator/{indicator_code}"
    try:
        resp = requests.get(
            url,
            params={"format": "json", "per_page": 60},
            headers={"User-Agent": "Geopolitiko/1.0"},
            timeout=20,
        )
        resp.raise_for_status()
        val, as_of = _parse_world_bank_latest(resp.json())
        return val, as_of, "World Bank"
    except Exception as e:
        return None, None, f"World Bank error: {e}"


def _fetch_world_bank_multi(country_codes: List[str], indicator_code: str, per_page: int = 20000) -> List[dict]:
    """Fetch an indicator for multiple countries in one call.

    World Bank supports `country=USA;CHN;SGP` style paths.
    Returns raw row dicts (date, value, country, etc.).
    """
    codes = [c.strip().upper()[:6] for c in (country_codes or []) if c and c.strip()]
    codes = [c for c in codes if c]
    if not codes:
        return []
    # Keep URL length reasonable; World Bank also accepts "all" but is huge.
    url = f"{WORLD_BANK_BASE}/country/{';'.join(codes)}/indicator/{indicator_code}"
    try:
        resp = requests.get(
            url,
            params={"format": "json", "per_page": per_page},
            headers={"User-Agent": "Geopolitiko/1.0"},
            timeout=25,
        )
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, list) or len(payload) < 2 or not isinstance(payload[1], list):
            return []
        return [r for r in payload[1] if isinstance(r, dict)]
    except Exception:
        return []


def _fetch_fx_usd(symbols: List[str]) -> Dict[str, float]:
    """Fetch USD base rates for a list of symbols.

    Frankfurter is EUR-based; we compute USD/CCY from EUR rates:
      USD/CCY = (EUR/CCY) / (EUR/USD)
    """
    syms = [s.strip().upper() for s in (symbols or []) if s and s.strip()]
    if not syms:
        return {}
    try:
        # Ensure USD is included so we can compute cross rates.
        targets = sorted(set([s for s in syms if s != "USD"] + ["USD"]))
        resp = requests.get(
            f"{FX_HOST_BASE}/latest",
            params={"from": "EUR", "to": ",".join(targets)},
            headers={"User-Agent": "Geopolitiko/1.0"},
            timeout=25,
        )
        resp.raise_for_status()
        data = resp.json() if resp.content else {}
        rates = data.get("rates") if isinstance(data, dict) else None
        if not isinstance(rates, dict):
            return {}
        eur_usd = rates.get("USD")
        try:
            eur_usd = float(eur_usd)
        except (TypeError, ValueError):
            eur_usd = None
        if not eur_usd or eur_usd <= 0:
            return {}
        out = {}
        for k, v in rates.items():
            kk = str(k).upper()
            if kk == "USD":
                continue
            try:
                eur_ccy = float(v)
            except (TypeError, ValueError):
                continue
            if eur_ccy and eur_ccy > 0:
                out[kk] = eur_ccy / eur_usd
        # USD/EUR is 1 / (EUR/USD)
        if "EUR" in syms:
            out["EUR"] = 1.0 / eur_usd
        return out
    except Exception:
        return {}


def _fetch_fx_usd_timeseries(symbols: List[str], start: str, end: str) -> Dict[str, Dict[str, float]]:
    """Fetch USD base rates time series for a list of symbols.

    Returns: { "SGD": {"2026-04-01": 1.23, ...}, ... } where values are USD/CCY.
    """
    syms = [s.strip().upper() for s in (symbols or []) if s and s.strip()]
    if not syms:
        return {}
    try:
        targets = sorted(set([s for s in syms if s != "USD"] + ["USD"]))
        resp = requests.get(
            f"{FX_HOST_BASE}/{start}..{end}",
            params={"from": "EUR", "to": ",".join(targets)},
            headers={"User-Agent": "Geopolitiko/1.0"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json() if resp.content else {}
        rates_by_date = data.get("rates") if isinstance(data, dict) else None
        if not isinstance(rates_by_date, dict):
            return {}
        out: Dict[str, Dict[str, float]] = {}
        for d, rates in rates_by_date.items():
            if not isinstance(rates, dict):
                continue
            eur_usd = rates.get("USD")
            try:
                eur_usd = float(eur_usd)
            except (TypeError, ValueError):
                eur_usd = None
            if not eur_usd or eur_usd <= 0:
                continue
            for sym in syms:
                if sym == "USD":
                    continue
                raw = rates.get(sym)
                try:
                    eur_ccy = float(raw)
                except (TypeError, ValueError):
                    continue
                if eur_ccy and eur_ccy > 0:
                    (out.setdefault(sym, {}))[str(d)[:10]] = eur_ccy / eur_usd
            if "EUR" in syms:
                (out.setdefault("EUR", {}))[str(d)[:10]] = 1.0 / eur_usd
        return out
    except Exception:
        return {}


def _fetch_fred_latest(series_id: str, api_key: str) -> Tuple[Optional[float], Optional[str]]:
    """Return (value, observation_date) for latest non-missing FRED observation."""
    if not api_key or not series_id:
        return None, None
    try:
        resp = requests.get(
            f"{FRED_API_BASE}/series/observations",
            params={
                "series_id": series_id.strip().upper(),
                "api_key": api_key,
                "file_type": "json",
                "sort_order": "desc",
                "limit": 100,
            },
            headers={"User-Agent": "Geopolitiko/1.0"},
            timeout=25,
        )
        resp.raise_for_status()
        data = resp.json() if resp.content else {}
        obs = data.get("observations") if isinstance(data, dict) else None
        if not isinstance(obs, list):
            return None, None
        for row in obs:
            if not isinstance(row, dict):
                continue
            raw = row.get("value")
            if raw in (None, ".", ""):
                continue
            try:
                val = float(raw)
            except (TypeError, ValueError):
                continue
            d = row.get("date")
            return val, str(d)[:10] if d else None
    except Exception as e:
        logger.debug("FRED fetch failed for %s: %s", series_id, e)
    return None, None


def _fetch_eurostat_unemployment_prime_age() -> Dict[str, Tuple[float, str]]:
    """Return {iso3: (value, observation_date)} for latest annual prime-age unemployment."""
    try:
        q = [("format", "JSON")]
        for eu_code, _ in EUROSTAT_UNEMP_GEO:
            q.append(("geo", eu_code))
        q.extend(
            [
                ("sex", "T"),
                ("age", "Y25-54"),
                ("unit", "PC_ACT"),
                ("lastTimePeriod", "1"),
            ]
        )
        resp = requests.get(
            f"{EUROSTAT_API}/une_rt_a",
            params=q,
            headers={"User-Agent": "Geopolitiko/1.0"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json() if resp.content else {}
    except Exception as e:
        logger.warning("Eurostat une_rt_a fetch failed: %s", e)
        return {}

    if not isinstance(data, dict):
        return {}
    value_map = data.get("value")
    if not isinstance(value_map, dict):
        return {}
    time_lbl = (data.get("dimension") or {}).get("time") or {}
    tcat = (time_lbl.get("category") or {}).get("label") or {}
    year = next(iter(tcat.keys()), "2024") if tcat else "2024"
    date_str = f"{year}-01-01"

    out: Dict[str, Tuple[float, str]] = {}
    for i, (eu_code, iso3) in enumerate(EUROSTAT_UNEMP_GEO):
        raw = value_map.get(str(i))
        if raw is None:
            continue
        try:
            fv = float(raw)
        except (TypeError, ValueError):
            continue
        out[iso3] = (fv, date_str)
    return out


def ingest_macro_once(country_limit: int = 220) -> Dict[str, object]:
    """Seed + ingest current snapshot into time-series tables."""
    macro_seed_defaults()

    # Pull catalogs
    countries = macro_list_countries(limit=country_limit)
    indicators = macro_list_indicators()

    # Map code->id via API calls? We only have code/name here; indicator_values needs country_id/indicator_id
    # We rely on a lightweight helper query by inserting via models: macro_insert_value takes ids, so we need ids.
    # For MVP, we resolve ids by querying inside models via direct SQL helper endpoints (implemented in api layer).
    # Instead, we keep a small internal resolver using /api/macro/* is unnecessary; use sqlite directly here.
    from app.models import _connection  # local import to avoid widening public API

    with _connection() as conn:
        country_id_by_code = {r[0]: int(r[1]) for r in conn.execute("SELECT code, id FROM countries").fetchall()}
        indicator_id_by_name = {r[0]: int(r[1]) for r in conn.execute("SELECT name, id FROM indicators").fetchall()}
        wb_pairs = conn.execute(
            """
            SELECT i.name, i.external_code
            FROM indicators i
            JOIN data_sources s ON s.id = i.source_id
            WHERE s.key = 'world_bank'
              AND i.external_code IS NOT NULL
              AND i.external_code != ''
              AND (i.frequency = 'annual' OR i.frequency IS NULL)
            ORDER BY i.name
            """
        ).fetchall()
        fx_pairs = conn.execute(
            """
            SELECT i.name, i.external_code
            FROM indicators i
            JOIN data_sources s ON s.id = i.source_id
            WHERE s.key = 'frankfurter'
              AND i.external_code IS NOT NULL
              AND i.external_code != ''
              AND i.frequency = 'daily'
              AND i.category = 'fx'
            ORDER BY i.name
            """
        ).fetchall()
        fred_pairs = conn.execute(
            """
            SELECT i.name, i.external_code
            FROM indicators i
            JOIN data_sources s ON s.id = i.source_id
            WHERE s.key = 'fred'
              AND i.external_code IS NOT NULL
              AND i.external_code != ''
            ORDER BY i.name
            """
        ).fetchall()

    # World Bank: ingest only a focused subset of key indicators (annual).
    # This keeps the job fast (many WB indicators x many countries is expensive),
    # while making the dashboard range presets useful on the most important series.
    WB_INGEST_NAMES = {
        "gdp_growth_yoy",
        "gdp_current_usd",
        "gdp_per_capita_usd",
        "inflation_cpi",
        "gov_debt_pct_gdp",
        "current_account_pct_gdp",
        "trade_balance_pct_gdp",
        "unemployment_rate",
        "broad_money_growth",
        "domestic_credit_private_pct_gdp",
        "reserves_months_imports",
        "real_effective_exchange_rate",
        "official_exchange_rate",
    }
    wb_ok = 0
    wb_attempts = 0
    country_codes = [(c.get("code") or "").strip().upper() for c in countries if (c.get("code") or "").strip()]
    # Fetch per indicator, write latest per country
    for name, wb_code in wb_pairs:
        if (name or "").strip().lower() not in WB_INGEST_NAMES:
            continue
        iid = indicator_id_by_name.get(name)
        if not iid:
            continue
        if not wb_code:
            continue
        wb_attempts += len(country_codes)
        rows = _fetch_world_bank_multi(country_codes, wb_code)

        # These are annual series, so we can safely store a bounded history window.
        if True:
            # Write last ~35 years (bounded) so queries are fast but useful.
            try:
                min_year = datetime.now(timezone.utc).year - 35
            except Exception:
                min_year = 1990
            for r in rows:
                cc = (r.get("countryiso3code") or "").strip().upper()
                if not cc:
                    continue
                cid = country_id_by_code.get(cc)
                if not cid:
                    continue
                val = r.get("value")
                date = r.get("date")
                if val is None or date is None:
                    continue
                d = str(date)[:10]
                # WB annual dates are year strings; store as YYYY-01-01 for consistency with ISO date filtering
                if len(d) == 4 and d.isdigit():
                    y = int(d)
                    if y < min_year:
                        continue
                    d = f"{y}-01-01"
                else:
                    try:
                        y2 = int(d[:4])
                        if y2 < min_year:
                            continue
                    except Exception:
                        pass
                try:
                    fv = float(val)
                except (TypeError, ValueError):
                    continue
                macro_insert_value(cid, iid, d, fv, raw_json=None)
                wb_ok += 1
        # (No latest-only path anymore; we simply ingest a bounded history for the curated set.)

    # FX: backfill a rolling daily history so dashboard ranges work (default: 2 years).
    # Countries table is ISO3-focused; for FX we store under code "USD" as the base currency entity.
    usd_cid = country_id_by_code.get("USD")
    if not usd_cid:
        from app.models import macro_upsert_country
        usd_cid = macro_upsert_country("USD", "United States Dollar", region="Global", is_major=True)

    symbols = [str(ccy).upper() for _name, ccy in (fx_pairs or []) if ccy]
    fx_ok = 0
    end_dt = datetime.now(timezone.utc).date()
    start_dt = end_dt - timedelta(days=730)
    fx_start = start_dt.strftime("%Y-%m-%d")
    fx_end = end_dt.strftime("%Y-%m-%d")
    rates_ts = _fetch_fx_usd_timeseries(symbols, fx_start, fx_end)
    for name, sym0 in fx_pairs:
        iid = indicator_id_by_name.get(name)
        if not iid:
            continue
        sym = str(sym0).upper() if sym0 else None
        if not sym:
            continue
        by_date = rates_ts.get(sym) or {}
        for d, v in by_date.items():
            macro_insert_value(
                usd_cid,
                iid,
                str(d)[:10],
                v,
                raw_json=json.dumps({"base": "USD", "symbol": sym, "source": "frankfurter"}),
            )
            fx_ok += 1

    # FRED — US monthly series (requires FRED_API_KEY)
    fred_ok = 0
    fred_key = fred_api_key()
    if fred_key:
        usa_cid = country_id_by_code.get("USA")
        if usa_cid:
            for fname, series_id in fred_pairs:
                iid = indicator_id_by_name.get(fname)
                if not iid or not series_id:
                    continue
                val, as_of = _fetch_fred_latest(str(series_id), fred_key)
                if val is not None and as_of:
                    macro_insert_value(
                        usa_cid,
                        iid,
                        as_of,
                        val,
                        raw_json=json.dumps({"series": str(series_id).strip(), "source": "fred"}),
                    )
                    fred_ok += 1
    else:
        logger.info("FRED_API_KEY not set; skipping FRED series")

    # Eurostat — harmonised unemployment (EU members in catalog)
    euro_ok = 0
    eu_iid = indicator_id_by_name.get("eurostat_unemployment_prime_age")
    if eu_iid:
        eu_map = _fetch_eurostat_unemployment_prime_age()
        for iso3, (fv, dstr) in eu_map.items():
            cid = country_id_by_code.get(iso3)
            if not cid:
                continue
            macro_insert_value(
                cid,
                eu_iid,
                dstr,
                fv,
                raw_json=json.dumps({"source": "eurostat", "dataset": "une_rt_a"}),
            )
            euro_ok += 1

    return {
        "ok": True,
        "ingested_at": _utc_now_iso(),
        "world_bank_attempts": wb_attempts,
        "world_bank_written": wb_ok,
        "fx_written": fx_ok,
        "fred_written": fred_ok,
        "eurostat_written": euro_ok,
        "fred_skipped_no_key": not bool(fred_key),
    }

