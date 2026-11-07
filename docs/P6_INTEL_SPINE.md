# P6 — Intelligence spine

Makes events, confirmation, entities, and golden-brief regression first-class.

## Shipped

| Area | Detail |
|------|--------|
| **Terminal events** | `terminal_events` table in news.db; sync after verification ticks ([`app/terminal_events.py`](../app/terminal_events.py)); API `GET /api/events` |
| **Multi-source gates** | Sensitive casualty/security patterns require corroboration ([`app/analyst_desk/confirmation.py`](../app/analyst_desk/confirmation.py)); wired into verification + brief eval |
| **Entity IDs** | Stable `country:` / `org:` / `person:` / `group:` ids ([`app/entities.py`](../app/entities.py)); API `GET /api/entities/resolve` |
| **Golden briefs** | Fixtures under `tests/fixtures/golden_briefs/`; regression in `tests/test_p6_intel_spine.py` |

## Notes

- Confirmation with empty source lists marks sensitive claims as unconfirmed/pending — intentional.
- Expand entity aliases as desks encounter recurring actors.
- Full clustering precision/recall harness remains a follow-on.
