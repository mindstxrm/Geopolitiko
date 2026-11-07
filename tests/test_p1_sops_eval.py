"""P1: desk SOP routing + brief eval harness."""
from app.analyst_desk.desk_sops import (
    append_sources_appendix,
    build_numbered_sources,
    desk_family_for_agent,
    get_sop_for_agent,
)
from app.analyst_desk.eval_brief import evaluate_brief


def test_desk_family_routing():
    assert desk_family_for_agent("southeast_asia_i") == "asean"
    assert desk_family_for_agent("east_asia_ii") == "northeast_asia"
    assert desk_family_for_agent("oceania_i") == "oceania"
    assert desk_family_for_agent("nordics_i") == "northern_europe"
    assert desk_family_for_agent("gulf_i") == "middle_east"
    assert desk_family_for_agent("unknown_xyz") == "generic"


def test_sop_pack_has_sections():
    sop = get_sop_for_agent("southeast_asia_ii")
    assert sop["family"] == "asean"
    assert "Sources" in sop["daily_sections"]
    assert "one_liner" in sop


def test_eval_scores_good_brief():
    sources = [{"sid": "S1", "title": "A", "source_name": "Reuters", "article_id": 1, "url": "http://x"}]
    body = """## Top line
FACT: Something happened [S1].

## Priority developments
- CLAIM: more detail [S1]

## ASEAN institutional / Chair track
- none

## Country notes
- SGP quiet

## Agreements tracker (deltas)
- none

## Risk board
- low

## Open questions
- need corroboration

## Sources
- [S1] Reuters — A
"""
    ev = evaluate_brief(body, agent_id="southeast_asia_i", sources=sources, weekly=False)
    assert ev["pass"] is True
    assert ev["score"] >= 70
    assert 1 in ev["citation_ids"]


def test_eval_flags_missing_citations():
    sources = [
        {"sid": "S1", "title": "A", "source_name": "X", "article_id": 1, "url": ""},
        {"sid": "S2", "title": "B", "source_name": "Y", "article_id": 2, "url": ""},
        {"sid": "S3", "title": "C", "source_name": "Z", "article_id": 3, "url": ""},
    ]
    body = "## Executive snapshot\n\n" + ("Long text without citations. " * 40)
    ev = evaluate_brief(body, agent_id="generic_region", sources=sources, weekly=False)
    assert ev["pass"] is False
    cite_check = next(c for c in ev["checks"] if c["id"] == "citations")
    assert cite_check["ok"] is False


def test_numbered_sources_and_appendix():
    articles = [
        {"id": 10, "title": "Hello", "source_name": "BBC", "url": "http://example.com/a", "summary": "Sum"},
    ]
    block, structured = build_numbered_sources(articles)
    assert "[S1]" in block
    assert structured[0]["article_id"] == 10
    out = append_sources_appendix("## Top line\nHi", structured)
    assert "## Sources" in out
    assert "[S1]" in out
