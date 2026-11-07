# P3 — Expand the moat

Source provenance, scheduled multi-region briefs, approve→delivery, and indicator coverage visibility.

## 1. Source catalog + tiers

`app/scrapers/sources.py` now carries:

- `tier`: **A** (wires/majors), **B** (strong regional/specialist), **C** (corroborate)
- `regions`: desk-family tags (`asean`, `oceania`, …)

Desk prompts label evidence as `(tier A · Reuters - World)`.

API: `GET /api/sources`

## 2. Scheduled multi-region briefs

Worker (after each pipeline tick) may run daily volumes for a configured agent set.

| Env | Default | Meaning |
|-----|---------|---------|
| `ANALYST_DESK_SCHEDULED_BRIEFS` | `1` | Enable |
| `ANALYST_DESK_SCHEDULED_BRIEF_HOURS` | `24` | Min hours between runs |
| `ANALYST_DESK_SCHEDULED_AGENTS` | (built-in list) | CSV of agent ids |

Manual force: Desk Home → **5b) Scheduled multi-region briefs**

## 3. Deliver approved briefs

On Approve (+ Terminal publish), optionally push to Slack/webhook and/or email.

| Env | Purpose |
|-----|---------|
| `ANALYST_DESK_BRIEF_WEBHOOK_URL` | Prefer for briefs (falls back to alert webhook) |
| `ANALYST_DESK_ALERT_WEBHOOK_URL` | Fallback / alerts |
| `ANALYST_DESK_ALERT_EMAIL_TO` | CSV recipients |
| `ANALYST_DESK_DELIVER_ON_APPROVE` | `1`/`0` (default on) |

Payload includes Slack-friendly `text` plus structured fields.

## 4. Indicator roadmap coverage

Ops board + `GET /api/indicators/coverage` show how many countries have:

- `macroeconomic_stress`
- `energy_commodity_exposure`

(Country dashboards already render these tables when populated.)

## Operator checklist

1. Expand/tune sources → scrape  
2. Run worker with scheduled briefs enabled  
3. Set webhook for approved briefs  
4. Watch `/ops` for source counts, schedule runs, indicator coverage  
