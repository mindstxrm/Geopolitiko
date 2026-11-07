# P7 — Platform cutover (incremental)

Builds on P4 Compose/Alembic. Goal: make Postgres + Celery the operable prod path while peeling god-modules.

## Shipped in this slice

| Area | Detail |
|------|--------|
| **SQL dialect helpers** | [`app/sql_dialect.py`](../app/sql_dialect.py) — placeholder adapt + `connect_raw` + cutover status on `/api/health` |
| **Feature flags** | [`app/feature_flags.py`](../app/feature_flags.py) — `GEOPOLITIKO_FF_*` env flags |
| **Domain package start** | [`app/domain/desk_intel.py`](../app/domain/desk_intel.py) — desk→Terminal upsert extracted; `models.upsert_desk_terminal_intel` delegates |
| **Celery default** | `analyst_desk_use_celery()` (P4) — prod prefers Celery; loop worker exits when `ANALYST_DESK_USE_CELERY=1` |
| **Compose** | web + desk + worker + beat + Postgres + Redis ([`docker-compose.yml`](../docker-compose.yml)) |

## Cutover checklist

1. `docker compose up postgres redis -d`
2. `python scripts/migrate_sqlite_to_postgres.py --plan`
3. `pgloader data/news.db postgresql://geopolitiko:geopolitiko@localhost:5432/geopolitiko`
4. `DATABASE_URL=postgresql+psycopg://... alembic upgrade head`
5. Point app `DATABASE_URL` — **runtime still SQLite-native in models.py** until dual-dialect rewrite lands
6. Run Celery worker+beat instead of `run_analyst_desk_worker.py`

## Not done yet (follow-on)

- Rewrite `models.py` SQL to dialect-aware queries / SQLAlchemy
- Split `routes.py` / `terminal.css` into domain packages
- Dependabot + staging environment + IaC
