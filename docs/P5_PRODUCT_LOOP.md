# P5 — Product loop polish

Focus: Monitor → Brief → Approve → Publish visibility without adding engines.

## Shipped

| Area | Detail |
|------|--------|
| **Nav quarantine** | Scenario Engine / Workspace tucked under Tools → **More labs** |
| **Approvals More** | Bulk approve/reject collapsed behind `<details>` so skim-first is default |
| **Ops More** | Platform/sources/indicators panels behind **More** on `/ops` |
| **Reject taxonomy** | Required reason codes on proposal reject |
| **Publish landing** | Approved proposals show Terminal country + Ops links |
| **DLQ replay** | Operations → Replay / Dismiss dead-letter tasks |

## Operator notes

- Replay needs Redis/Celery reachable (`celery_app.send_task`).
- Reject without a reason is blocked server-side.
- Core loop remains: Desk draft → Approvals review → Approve → country `#analyst-desk-terminal` + `/ops`.
