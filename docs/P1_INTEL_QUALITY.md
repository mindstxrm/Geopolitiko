# P1 — Intelligence defensibility

Makes Analyst Desk drafts **SOP-scoped**, **citation-grounded**, and **auto-evaluated** before human approve.

## What changed

1. **Desk SOP packs** (`app/analyst_desk/desk_sops.py`)  
   Maps agent IDs → regional families (ASEAN, Oceania, Northeast Asia, …) with one-liners, required sections, and so-what tests.

2. **Cited generation** (`app/analyst_desk/generate.py`)  
   Evidence is numbered `[S1]…[Sn]` in the prompt; drafts must cite markers; a `## Sources` appendix is appended if missing.

3. **Eval harness** (`app/analyst_desk/eval_brief.py`)  
   Deterministic checks: required sections, citations, Sources heading, uncertainty hygiene, non-empty.  
   Score 0–100, grade A–F, pass ≥ 70. Stored on `proposal.source_context.eval`.

4. **Review UI**  
   Proposal page shows the eval panel; Approvals list shows grade badges.

## Operator flow

1. Run Desk daily/weekly (or worker).  
2. Open Approvals → Review.  
3. Check **Quality eval** before approve.  
4. Edit body if citations/sections are weak; then approve → Terminal.

## Tests

```bash
pytest -q tests/test_p1_sops_eval.py tests/test_publish.py
```

## Not in this slice

- Full multi-thousand-word SOP markdown files in-repo (compact packs only)  
- LLM-as-judge factuality  
- Automatic reject of failing eval (human still decides)
