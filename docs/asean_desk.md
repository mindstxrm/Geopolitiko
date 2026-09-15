# ASEAN Desk (Analyst Desk workspace)

Regional political–economic–security products covering ASEAN member states and ASEAN as an institution.

## Scope

Country coverage is the union of `southeast_asia_i`, `southeast_asia_ii`, and `southeast_asia_iii`:

`BRN, KHM, IDN, LAO, MYS, MMR, PHL, SGP, THA, VNM, TLS`

Plus an institutional ASEAN tag (Summit, AMM, ADMM/ADMM-Plus, ARF, EAS, Secretariat, Chair-year priorities). Extra-regional poles (CHN, USA, JPN, KOR, IND, AUS, EU, RUS, TWN) only when they materially affect ASEAN equities.

Workspace agent id: `asean`.

## Products

| Task | Cadence | Output |
|---|---|---|
| `asean.daily_brief` | Business day; cutoff **16:00 SGT / 08:00 UTC** | SOP daily markdown + JSON sidecar |
| `asean.weekly_brief` | Monday (prior Mon–Sun) | Pattern brief (not yet auto-scheduled in pipeline) |
| `asean.monthly_report` | First 3 business days of month | Country matrix + risk register (not yet auto-scheduled) |
| `asean.flash_update` | Event-driven | ≤400 words if FLASH criteria met |

Daily drafts are **pending review**. Publish path remains proposal → human approve → `publish_approved_proposal_to_terminal`.

## How to run

Analyst Desk UI: **5b) ASEAN daily brief** or the ASEAN Desk panel.

Programmatic:

```python
from app.analyst_desk.pipeline import generate_asean_daily_brief
generate_asean_daily_brief(limit_events=80, days=2)
```

The full pipeline tick also emits an ASEAN daily briefing (`scope_type=region`, `scope_id=asean`) alongside the global daily brief.

File-based review packets (automation / desk officer) live under `desk/asean/briefs/`.

## Macro

ASEAN-10 macro strip can use `/intelligence/macro?group=asean` (FX, CPI, GDP panels) when the news/macro DB is populated.
