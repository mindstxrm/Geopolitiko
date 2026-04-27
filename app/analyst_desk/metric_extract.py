"""Heuristic extraction of quantitative signals from news text (percentages, money, bps, counts)."""
from __future__ import annotations

import re
from typing import Any


def _clip(s: str, n: int = 220) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else s[: n - 1] + "…"


def extract_quantitative_signals(text: str, title: str = "") -> list[dict[str, Any]]:
    """
    Return list of dicts with keys: metric_kind, label, value_numeric, value_text, unit, snippet, confidence.
    No LLM — regex / keyword windows; tune patterns over time.
    """
    blob = f"{title}\n{text or ''}"
    if not blob.strip():
        return []
    out: list[dict[str, Any]] = []

    def add(
        kind: str,
        label: str,
        *,
        num: float | None = None,
        vtext: str | None = None,
        unit: str | None = None,
        span: tuple[int, int],
        confidence: float,
    ) -> None:
        a, b = span
        snippet = _clip(blob[max(0, a - 40) : min(len(blob), b + 40)])
        out.append(
            {
                "metric_kind": kind,
                "label": label,
                "value_numeric": num,
                "value_text": vtext,
                "unit": unit,
                "snippet": snippet,
                "confidence": confidence,
            }
        )

    # --- Percentages with economic / policy keywords ---
    pct_patterns: list[tuple[str, str, re.Pattern[str], int, float]] = [
        ("inflation", "Inflation / CPI", re.compile(r"(inflation|consumer\s+prices|cpi)\b[^.%]{0,80}?(\d+(?:\.\d+)?)\s*%", re.I), 2, 0.62),
        ("interest_rate", "Policy / interest rate", re.compile(r"(interest\s+rate|policy\s+rate|benchmark\s+rate)\b[^.%]{0,80}?(\d+(?:\.\d+)?)\s*%", re.I), 2, 0.58),
        ("unemployment", "Unemployment rate", re.compile(r"unemployment\b[^.%]{0,80}?(\d+(?:\.\d+)?)\s*%", re.I), 1, 0.58),
        ("gdp_growth", "GDP growth", re.compile(r"gdp\b[^.%]{0,100}?(\d+(?:\.\d+)?)\s*%", re.I), 1, 0.55),
        ("generic_pct", "Mentioned percentage", re.compile(r"(\d+(?:\.\d+)?)\s*%\s*(?:growth|increase|decrease|cut|hike|rise|fall|jump|drop)", re.I), 1, 0.45),
    ]
    for kind, label, rx, gidx, conf in pct_patterns:
        for m in rx.finditer(blob):
            try:
                val = float(m.group(gidx))
            except (TypeError, ValueError, IndexError):
                continue
            add(kind, label, num=val, unit="%", span=m.span(), confidence=conf)

    # --- Basis points ---
    for m in re.finditer(r"(\d{1,4})\s*basis\s*points?\b", blob, re.I):
        try:
            bps = int(m.group(1))
        except ValueError:
            continue
        add("basis_points", "Basis points move", num=float(bps), unit="bps", vtext=f"{bps} bps", span=m.span(), confidence=0.52)

    # --- USD amounts (billion / million) ---
    money_bn = re.compile(
        r"\$\s*(\d+(?:\.\d+)?)\s*(billion|bn)\b",
        re.I,
    )
    for m in money_bn.finditer(blob):
        try:
            val = float(m.group(1))
        except ValueError:
            continue
        add("usd_billion", "USD (billions)", num=val, unit="USD bn", span=m.span(), confidence=0.55)

    money_m = re.compile(r"\$\s*(\d+(?:\.\d+)?)\s*(million|mn|m)\b", re.I)
    for m in money_m.finditer(blob):
        try:
            val = float(m.group(1))
        except ValueError:
            continue
        add("usd_million", "USD (millions)", num=val, unit="USD m", span=m.span(), confidence=0.5)

    # --- Large troop / personnel counts (security) ---
    troops = re.compile(r"\b(\d{2,6})\s+(troops|soldiers|personnel)\b", re.I)
    for m in troops.finditer(blob):
        try:
            n = float(m.group(1))
        except ValueError:
            continue
        add("troop_count", "Troop / personnel count", num=n, unit="personnel", span=m.span(), confidence=0.48)

    # --- ISO dates (as timeline anchors) ---
    for m in re.finditer(r"\b(20\d{2}-\d{2}-\d{2})\b", blob):
        add("date_anchor", "Date reference", vtext=m.group(1), unit="ISO date", span=m.span(), confidence=0.42)

    # Dedupe near-identical snippets
    seen: set[tuple[Any, ...]] = set()
    deduped: list[dict[str, Any]] = []
    for row in out:
        key = (
            row["metric_kind"],
            row.get("label"),
            row.get("value_numeric"),
            row.get("value_text"),
            (row.get("snippet") or "")[:80],
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped[:40]
