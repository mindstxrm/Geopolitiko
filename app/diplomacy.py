"""
Diplomacy & Treaty Intelligence — tools.

• Treaty clause comparator (AI-powered)
• Escalation clause detection (AI)
"""
import json
import logging
import os
import re

logger = logging.getLogger(__name__)


def compare_treaty_clauses(text_a: str, text_b: str) -> dict:
    """
    AI-powered comparison of two treaty texts. Returns structured comparison
    (dispute resolution, termination, escalation, key differences).
    """
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key or not text_a.strip() or not text_b.strip():
        return {
            "error": "OPENAI_API_KEY not set or empty inputs",
            "comparison": None,
            "clauses_a": [],
            "clauses_b": [],
        }
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        prompt = f"""Compare these two treaty or agreement texts. For each, identify:
1. Dispute resolution (how disputes are resolved)
2. Termination/withdrawal conditions
3. Any escalation or retaliation clauses
4. Key substantive differences

Text A:
{text_a[:6000]}

Text B:
{text_b[:6000]}

Reply with a JSON object with keys: "comparison" (string, 2-4 paragraphs), "clauses_a" (array of {{"type": "...", "summary": "..."}}), "clauses_b" (same), "key_differences" (array of strings). No other text."""

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=1500,
        )
        raw = (response.choices[0].message.content or "").strip()
        raw = raw.strip().strip("`").strip()
        if raw.startswith("json"):
            raw = raw[4:].strip()
        return json.loads(raw)
    except Exception as e:
        logger.exception("Treaty clause comparison failed: %s", e)
        return {
            "error": str(e),
            "comparison": None,
            "clauses_a": [],
            "clauses_b": [],
            "key_differences": [],
        }


def detect_escalation_clause(full_text: str) -> dict:
    """
    Detect if treaty text contains escalation/retaliation clauses. Returns
    { "has_escalation": bool, "excerpts": [...], "confidence": "high"|"medium"|"low" }.
    """
    if not full_text or not full_text.strip():
        return {"has_escalation": False, "excerpts": [], "confidence": "low"}

    # Rule-based first pass: keywords
    text_lower = full_text.lower()
    patterns = [
        r"escalat(e|ion)",
        r"retaliat(e|ion)",
        r"countermeasure",
        r"reciprocal\s+(action|measure)",
        r"in\s+response\s+to\s+(a\s+)?(material\s+)?breach",
        r"suspend\s+(the\s+)?(agreement|obligations)",
        r"terminat(e|ion)\s+if",
        r"material\s+breach",
        r"withdraw(al)?\s+if",
    ]
    excerpts = []
    for pat in patterns:
        for m in re.finditer(pat, text_lower, re.I):
            start = max(0, m.start() - 40)
            end = min(len(full_text), m.end() + 80)
            excerpt = full_text[start:end].replace("\n", " ").strip()
            if excerpt and excerpt not in excerpts:
                excerpts.append(excerpt[:200])
    has_rule = len(excerpts) > 0

    api_key = os.environ.get("OPENAI_API_KEY")
    if api_key and len(full_text) > 200:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=api_key)
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "user", "content": f"""Does this treaty text contain escalation, retaliation, or reciprocal-measure clauses (e.g. response to breach, suspension, countermeasures)? Reply with JSON only: {{"has_escalation": true/false, "confidence": "high"|"medium"|"low", "one_sentence": "explanation"}}.

Text:
{full_text[:4000]}"""}
                ],
                max_tokens=200,
            )
            raw = (response.choices[0].message.content or "").strip().strip("`").strip()
            if raw.startswith("json"):
                raw = raw[4:].strip()
            out = json.loads(raw)
            out.setdefault("excerpts", excerpts)
            if has_rule and out.get("has_escalation") is False:
                out["has_escalation"] = True
                out["confidence"] = "medium"
            return out
        except Exception as e:
            logger.warning("AI escalation detection failed: %s", e)

    return {
        "has_escalation": has_rule,
        "excerpts": excerpts[:5],
        "confidence": "high" if has_rule else "low",
    }
