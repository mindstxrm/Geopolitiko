"""Fetch and cache a small set of macro indicators for the homepage.

Data sources are intentionally public/no-auth so the widget works out of the box.
Currently uses the World Bank Indicators API.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests


WORLD_BANK_BASE = "https://api.worldbank.org/v2"


@dataclass(frozen=True)
class IndicatorSpec:
    key: str
    label: str
    country: str
    indicator: str
    unit: str


_DEFAULT_SPECS: List[IndicatorSpec] = [
    IndicatorSpec(
        key="wld_gdp_growth",
        label="World GDP growth",
        country="WLD",
        indicator="NY.GDP.MKTP.KD.ZG",
        unit="%",
    ),
    IndicatorSpec(
        key="wld_inflation",
        label="World inflation (CPI)",
        country="WLD",
        indicator="FP.CPI.TOTL.ZG",
        unit="%",
    ),
    IndicatorSpec(
        key="usa_unemployment",
        label="US unemployment",
        country="USA",
        indicator="SL.UEM.TOTL.ZS",
        unit="%",
    ),
]


_cache: Dict[str, Any] = {"expires_at": None, "payload": None}
_CACHE_TTL_SECONDS = 6 * 60 * 60  # 6 hours


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_world_bank_latest(series: Any) -> Tuple[Optional[float], Optional[str]]:
    """Return (value, year) from a World Bank indicator response payload.

    World Bank returns JSON like: [metadata, [ {date: '2023', value: 1.23, ...}, ... ] ]
    We take the most recent non-null value.
    """
    if not isinstance(series, list) or len(series) < 2:
        return None, None
    rows = series[1]
    if not isinstance(rows, list):
        return None, None
    for row in rows:
        if not isinstance(row, dict):
            continue
        val = row.get("value")
        date = row.get("date")
        if val is None:
            continue
        try:
            return float(val), str(date) if date is not None else None
        except (TypeError, ValueError):
            continue
    return None, None


def _fetch_world_bank_indicator(country: str, indicator: str) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    url = f"{WORLD_BANK_BASE}/country/{country}/indicator/{indicator}"
    params = {"format": "json", "per_page": 60}
    try:
        resp = requests.get(
            url,
            params=params,
            headers={"User-Agent": "GeopoliticalNews/1.0"},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        value, year = _parse_world_bank_latest(data)
        return value, year, "World Bank"
    except Exception:
        return None, None, "World Bank"


def get_macro_indicators() -> Dict[str, Any]:
    """Return cached macro indicators payload for homepage.

    Shape:
      {
        "updated_at": "<server time ISO>",
        "indicators": [
          { "key", "label", "value", "unit", "as_of", "source" }
        ]
      }
    """
    now = datetime.now(timezone.utc)
    expires_at = _cache.get("expires_at")
    if expires_at and isinstance(expires_at, datetime) and expires_at > now and _cache.get("payload"):
        return _cache["payload"]

    indicators: List[Dict[str, Any]] = []
    for spec in _DEFAULT_SPECS:
        val, as_of, source = _fetch_world_bank_indicator(spec.country, spec.indicator)
        indicators.append(
            {
                "key": spec.key,
                "label": spec.label,
                "value": val,
                "unit": spec.unit,
                "as_of": as_of,
                "source": source,
            }
        )

    payload = {"updated_at": _utc_now_iso(), "indicators": indicators}
    _cache["payload"] = payload
    _cache["expires_at"] = now.replace(microsecond=0) + _seconds(_CACHE_TTL_SECONDS)
    return payload


def _seconds(n: int):
    from datetime import timedelta

    return timedelta(seconds=n)

