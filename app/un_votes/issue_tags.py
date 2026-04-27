"""Rule-based issue tagging for UN resolutions from title text."""
import re
from typing import List, Tuple

METHOD_VERSION = "rule_based_v1"

# Keyword -> issue_tag mapping (order matters for first match)
ISSUE_KEYWORDS = [
    ("human_rights", ["human rights", "humanitarian", "torture", "genocide", "refugee", "migration", "asylum"]),
    ("israel_palestine", ["israel", "palestine", "palestinian", "occupied territory", "golan"]),
    ("nuclear", ["nuclear", "npt", "disarmament", "non-proliferation", "atomic"]),
    ("climate", ["climate", "environment", "biodiversity", "sustainable development", "paris agreement"]),
    ("trade", ["trade", "wto", "tariff", "economic cooperation", "investment"]),
    ("security", ["security", "peacekeeping", "sanctions", "arms", "military"]),
    ("china_taiwan", ["taiwan", "taipei", "one china"]),
    ("ukraine", ["ukraine", "russian federation", "crimea", "donbas"]),
    ("cyber", ["cyber", "information", "digital"]),
    ("health", ["health", "who", "pandemic", "covid", "disease"]),
    ("women", ["women", "gender", "sexual violence"]),
    ("development", ["development", "poverty", "aid", "ldc", "sdg"]),
    ("decolonization", ["decolonization", "self-determination", "non-self-governing"]),
    ("un_reform", ["security council reform", "un reform", "general assembly"]),
]


def tag_resolution(resolution_title: str, resolution_id: str = "") -> List[Tuple[str, float]]:
    """
    Return list of (issue_tag, confidence) for a resolution.
    Confidence 0-1; multiple tags possible.
    """
    if not resolution_title:
        return []
    text = (resolution_title or "").lower()
    tags = []
    for issue_tag, keywords in ISSUE_KEYWORDS:
        for kw in keywords:
            if kw.lower() in text:
                # Simple confidence: exact phrase match = 0.9, substring = 0.7
                conf = 0.9 if text.strip() == kw.lower() else 0.7
                if (issue_tag, conf) not in [(t, c) for t, c in tags]:
                    tags.append((issue_tag, conf))
                    break
    if not tags:
        tags.append(("other", 0.5))
    return tags
