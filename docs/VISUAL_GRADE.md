# Visual Grade-1 — institutional density

Geopolitiko already had dark Terminal chrome. Grade-1 tightens **density + consistency** so Home/Ops/Desk/Country read as one institutional console — not a soft SaaS product.

## What shipped

| Area | Change |
|------|--------|
| Tokens | `--t-risk-1…5`, `--t-status-ok/warn/crit`, radii 6/4px, shorter command bar |
| Global ticker | Always-on strip under command bar: OK/STALE/WARN · news as-of · high-risk · desk pending · Ops link |
| Situation Room | Data-first strip (counts + as-of + window toggle); intro/onboarding collapsed into details |
| Ops | Severity-sorted ops wall (`ops-wall`) — dense rows + compact panels (ingest / desk / macro) |
| Desk | Same radius/status/risk tokens as Terminal (`templates/analyst_desk/base.html`) |
| Country | Back-link soup removed; mono score ticker cells; compact decision bar |

## Explicitly deferred

- Full mobile redesign
- Map/cartography rewrite
- Role-based home skins
- Marketing landing

## Key files

- `app/static/css/terminal.css` — tokens + ticker / ops-wall / situation / score-ticker
- `app/status_ticker.py` — lightweight per-request metrics
- `app/templates/base.html` — `status-ticker` (replaces ad-hoc stale banner)
- `app/templates/situation_room.html`, `ops_health.html`, `country_dashboard.html`
- `templates/analyst_desk/base.html`

## Success look

An exec opening Home should see **data age + severity + map/feed** in under one second — not onboarding copy. Desk Approvals and Terminal Ops should feel like the same institution with different permissions.
