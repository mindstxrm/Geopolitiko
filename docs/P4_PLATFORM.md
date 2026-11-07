# P4 — Platform durability & AuthZ

Builds on P0–P3. Goal: production topology that small teams can operate, plus real authorization.

## What shipped

| Area | Detail |
|------|--------|
| **DB URLs** | `DATABASE_URL` / `ANALYST_DESK_DATABASE_URL` via [`app/db.py`](../app/db.py); local/dev still defaults to SQLite files |
| **Alembic** | [`alembic/`](../alembic/) + first revision for `audit_log` / `users.role` |
| **Cutover helper** | [`scripts/migrate_sqlite_to_postgres.py`](../scripts/migrate_sqlite_to_postgres.py) |
| **Docker Compose** | Postgres + Redis + web + desk + Celery worker + beat ([`docker-compose.yml`](../docker-compose.yml)) |
| **Celery prod default** | `analyst_desk_use_celery()` — on in production unless `ANALYST_DESK_USE_CELERY=0` |
| **AuthZ** | Roles `viewer` / `analyst` / `approver` / `admin` ([`app/authz.py`](../app/authz.py)); approve/reject require `approver+` |
| **CSRF + headers** | [`app/security.py`](../app/security.py); auto-inject token into POST forms |
| **Audit log** | [`app/audit.py`](../app/audit.py) — approve / reject / publish |
| **Sentry** | Optional `SENTRY_DSN` ([`app/sentry_init.py`](../app/sentry_init.py)) |
| **CI** | pytest + ruff; lockfile [`requirements.lock`](../requirements.lock) |

## Topology

```
browser → web(:5003) + desk(:5005)
                ↓
         Redis (Celery broker)
                ↓
         worker + beat
                ↓
         SQLite volume (default)  or  Postgres (cutover)
```

```bash
docker compose up --build
# Terminal http://localhost:5003
# Desk     http://localhost:5005
```

Secrets for compose live in [`.env.docker`](../.env.docker) (change before any shared deploy).

## Roles

| Role | Can |
|------|-----|
| viewer | Read Terminal / Desk |
| analyst | Generate / draft |
| approver | Approve / reject / publish |
| admin | Ops + shared desk password path |

- New users: `GEOPOLITIKO_DEFAULT_USER_ROLE` (default `viewer`)
- Force admins: `GEOPOLITIKO_ADMIN_USERNAMES=alice,bob`
- Desk shared password logs in as **admin**
- Open desk (no password, local) → **admin**

## Celery vs loop worker

Production / `ANALYST_DESK_USE_CELERY=1`:

```bash
celery -A app.analyst_desk.celery_app worker -l INFO -Q celery,desk_metrics
celery -A app.analyst_desk.celery_app beat -l INFO
```

Escape hatch for the old loop:

```bash
ANALYST_DESK_FORCE_LOOP_WORKER=1 python run_analyst_desk_worker.py
# or ANALYST_DESK_USE_CELERY=0
```

## Postgres cutover (incremental)

1. `docker compose up postgres -d`
2. `python scripts/migrate_sqlite_to_postgres.py --plan`
3. `pgloader data/news.db postgresql://geopolitiko:geopolitiko@localhost:5432/geopolitiko`
4. `DATABASE_URL=... alembic upgrade head`
5. Point health/ops at Postgres; keep SQLite backups until proven

**Important:** `app/models.py` remains SQLite-native (`?` placeholders). P4 wires URLs, health probes, Alembic, and Compose so cutover is operable; full dual-dialect query rewrite is a follow-on (P7 module split).

## Sentry / metrics

```bash
export SENTRY_DSN=https://...@sentry.io/...
export SENTRY_ENVIRONMENT=production
export SENTRY_TRACES_SAMPLE_RATE=0.05
```

In-process counters: `app.sentry_init.incr_metric` / `get_metrics_snapshot` (surfaced on Ops).

## CSRF

Disabled when `TESTING=1` or `GEOPOLITIKO_DISABLE_CSRF=1`. Otherwise POST forms need `csrf_token` (auto-injected from base templates).

## Tests

```bash
pytest -q
ruff check app/db.py app/authz.py app/audit.py app/security.py app/sentry_init.py \
  tests/test_p4_platform.py scripts/migrate_sqlite_to_postgres.py
```
