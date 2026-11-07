# P0 Ops runbook — trust & ship reliability

This documents the **P0** reliability layer: health checks, secrets, SQLite backups, CI/tests, and the approve→publish→Terminal loop.

## Quick links

| Surface | URL (local) |
|--------|-------------|
| Terminal | http://127.0.0.1:5003 |
| Ops health board | http://127.0.0.1:5003/ops |
| Health JSON | http://127.0.0.1:5003/api/health |
| Analyst Desk | http://127.0.0.1:5005 |
| Desk Approvals | http://127.0.0.1:5005/approvals |

## Production secrets (required)

On Render / when `GEOPOLITIKO_ENV=production` or `FLASK_ENV=production` (or `RENDER` is set), the app **refuses to start** with default secrets unless you override:

| Variable | Purpose |
|----------|---------|
| `SECRET_KEY` | Flask sessions (must not be the dev default) |
| `ANALYST_DESK_SECRET_KEY` | Desk sessions |
| `ANALYST_DESK_ADMIN_PASSWORD` | Desk login (required by default in prod) |

Escape hatches (emergency only):

- `GEOPOLITIKO_STRICT_SECRETS=0` — warn instead of crash  
- `GEOPOLITIKO_REQUIRE_DESK_PASSWORD=0` — allow empty desk password in prod  

## Backups (SQLite durability until Postgres)

```bash
python scripts/backup_sqlite.py
# optional:
python scripts/backup_sqlite.py --out /var/data/backups --keep 14
```

Copies `news.db` and `analyst_desk.db` into `data/backups/` (or `--out`), records status for the Ops board, prunes old copies.

### Restore

1. Stop web + workers.  
2. Replace `data/news.db` (and/or `data/analyst_desk.db`) with a backup file.  
3. Remove stale `-wal`/`-shm` next to the live DB if you restored a cold copy.  
4. Restart `python run.py` / Desk / worker.  
5. Confirm `/ops` and `/api/health`.

## Approve → publish → visible

1. Run Desk pipeline / worker so proposals exist.  
2. Open Desk **Approvals** → **Review** a proposal → **Approve + publish**.  
3. Flash message includes Terminal country deep links (`#analyst-desk-terminal`).  
4. Confirm on country dashboard and on Terminal **Ops health → Recently published**.

Requires `ANALYST_DESK_PUBLISH_ON_APPROVE=1` (default).

## Health meanings

- **ok** — hard checks passed; freshness warnings may still appear as degraded  
- **degraded** — soft issues (stale scrape, empty articles, missing desk password in prod)  
- **error** / HTTP 503 — news DB unusable or production secret failure mode for monitors  

Ops status file: `data/platform_ops_status.json` (updated by refresh/macro/backup).

## CI / tests

```bash
pip install -r requirements.txt
pytest -q
```

GitHub Actions workflow: `.github/workflows/ci.yml`.

## Postgres (next durability step)

P0 keeps **SQLite + backups** as the durable path. Full Postgres migration is the follow-on:

1. Provision managed Postgres.  
2. Introduce SQLAlchemy/Alembic (or incremental dual-write).  
3. Cut over `DATABASE_PATH` / desk DB.  
4. Keep backup script until cutover is proven.

Do **not** run production on Render free diskless instances without an external disk or managed DB — the DB will empty on redeploy/sleep.
