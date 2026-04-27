"""LLM-based quantitative extraction using OpenAI JSON schema (structured outputs)."""
from __future__ import annotations

import json
import logging
import os
from typing import Any

from config import analyst_desk_heuristic_only

logger = logging.getLogger(__name__)

# OpenAI strict JSON schema: all object properties must be in "required", additionalProperties false.
_METRIC_ITEM_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "metric_kind": {
            "type": "string",
            "description": "Category: inflation, gdp_growth, interest_rate, unemployment, fx_rate, budget, debt, trade_volume, military_count, sanction, energy_price, other",
        },
        "label": {"type": "string", "description": "Short human label, e.g. CPI YoY"},
        "value_numeric": {"type": ["number", "null"], "description": "Numeric value if applicable"},
        "value_text": {"type": ["string", "null"], "description": "Text value if not numeric"},
        "unit": {"type": ["string", "null"], "description": "Unit e.g. %, USD bn, bps, index"},
        "iso3_country": {
            "type": ["string", "null"],
            "description": "ISO 3166-1 alpha-3 country the figure applies to, or null if global/unclear",
        },
        "confidence": {"type": "number", "description": "0–1 confidence"},
        "evidence_quote": {
            "type": "string",
            "description": "Short verbatim quote from the article supporting the extraction",
        },
    },
    "required": [
        "metric_kind",
        "label",
        "value_numeric",
        "value_text",
        "unit",
        "iso3_country",
        "confidence",
        "evidence_quote",
    ],
    "additionalProperties": False,
}

METRIC_EXTRACTION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "metrics": {
            "type": "array",
            "description": "Quantitative or semi-quantitative facts explicitly supported by the text",
            "items": _METRIC_ITEM_SCHEMA,
        }
    },
    "required": ["metrics"],
    "additionalProperties": False,
}


def llm_extract_metrics_from_news(
    title: str,
    text: str,
    *,
    suggested_iso3: list[str] | None = None,
) -> tuple[list[dict[str, Any]], str | None]:
    """
    Returns (metrics_as_flat_dicts_for_pipeline, error_message).
    Each dict keys align with heuristic extractor: metric_kind, label, value_numeric, value_text, unit, snippet, confidence.
    """
    if analyst_desk_heuristic_only():
        return [], "heuristic_only_mode"
    api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return [], "missing_openai_api_key"

    model = (os.environ.get("OPENAI_MODEL") or "gpt-4o-mini").strip()
    blob = f"Title: {title or '(none)'}\n\nBody:\n{(text or '')[:10000]}"
    countries_hint = ", ".join(suggested_iso3 or []) or "(none — infer from text if clear)"

    system = (
        "You extract quantitative geopolitical and macroeconomic signals from news text. "
        "Only include figures explicitly stated or clearly implied in the text—do not invent numbers. "
        "Prefer inflation, rates, growth, fiscal figures, trade values, troop counts, sanctions amounts, energy prices. "
        "If unsure, lower confidence or omit. Output must match the JSON schema."
    )
    user = (
        f"Suggested ISO3 tags from a heuristic tagger (may be empty): {countries_hint}\n\n"
        f"Article:\n{blob}"
    )

    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            max_tokens=2000,
            temperature=0.1,
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "news_metric_extraction",
                    "strict": True,
                    "schema": METRIC_EXTRACTION_SCHEMA,
                },
            },
        )
        raw = (response.choices[0].message.content or "").strip()
        if not raw:
            return [], "empty_model_response"
        data = json.loads(raw)
        rows_in = data.get("metrics") if isinstance(data, dict) else None
        if not isinstance(rows_in, list):
            return [], "invalid_metrics_array"
        out: list[dict[str, Any]] = []
        for row in rows_in:
            if not isinstance(row, dict):
                continue
            vt = row.get("value_text")
            if vt is not None and not isinstance(vt, str):
                vt = str(vt)
            vt = (vt or "").strip()[:200] or None
            out.append(
                {
                    "metric_kind": str(row.get("metric_kind") or "other").strip()[:80] or "other",
                    "label": str(row.get("label") or "").strip()[:200] or "Signal",
                    "value_numeric": row.get("value_numeric"),
                    "value_text": vt,
                    "unit": (str(row.get("unit")).strip()[:40] if row.get("unit") else None),
                    "snippet": (str(row.get("evidence_quote") or "").strip()[:500] or None),
                    "confidence": float(row.get("confidence") or 0.5),
                    "iso3_country": (
                        str(row.get("iso3_country")).strip().upper()[:3]
                        if row.get("iso3_country")
                        else None
                    ),
                }
            )
        return out, None
    except json.JSONDecodeError as e:
        logger.warning("metric LLM JSON decode failed: %s", e)
        return [], f"json_decode: {e}"
    except Exception as e:
        logger.warning("metric LLM call failed: %s", e)
        return [], str(e)
