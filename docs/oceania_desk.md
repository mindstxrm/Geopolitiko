# Oceania Desk

Regional workspace over Analyst Desk scopes `oceania_i` (AUS, NZL, PNG, FJI) and `oceania_ii` (SLB, VUT, WSM, TON, KIR, TUV, NRU), plus COFA watch ISO3s already in Geopolitiko country data (`PLW`, `FSM`, `MHL`). French Pacific and other territories (`NCL`, `PYF`, `GUM`, `MNP`, `ASM`, `WLF`, `COK`, `NIU`) are **watch tags**, not core agent countries.

## Products

- **Daily** (`oceania.daily_brief`): 36h window ending **07:00 UTC / 17:00 AEST**. SOP markdown + JSON sidecar. Status `pending_review`.
- **Weekly / monthly**: same workspace; not auto-emitted on the hourly cron unless due (see `desk/oceania/STATE.md`).
- **FLASH**: SOP §8 only (coup, recognition switch, confirmed expanded foreign security presence, major cable cut, catastrophic disaster, Honiara-scale unrest, kinetic EEZ incident).

## How to run

Analyst Desk UI: **5b) Oceania daily brief** or the Oceania Desk panel.

Programmatic:

```python
from app.analyst_desk.pipeline import generate_oceania_daily_brief
generate_oceania_daily_brief(limit_events=80, days=2)
```

The full pipeline tick also emits an Oceania daily briefing (`scope_type=region`, `scope_id=oceania`) alongside the global daily brief.

File-based review packets (automation / desk officer) live under `desk/oceania/briefs/`.

Drafts stay pending until a human approves for Terminal.
