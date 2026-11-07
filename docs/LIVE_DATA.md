# Live data freshness (local + cloud)

Geopolitiko is only useful when news and fast macro are recent. **Do not rely on `python run.py` threads in production** — gunicorn never starts them.

## Freshness bar (v1)

| Stream | Cadence | Env |
|--------|---------|-----|
| News scrape-only | every **180s** | `INGEST_SCRAPE_SECONDS` |
| Analysis jobs | every **900s** | `INGEST_JOBS_SECONDS` |
| Macro FX/FRED (fast) | every **3600s** | `INGEST_MACRO_SECONDS` |
| Macro full (WB+) | every **86400s** | `INGEST_MACRO_FULL_SECONDS` |
| Desk pipeline | every **300s** | `ANALYST_DESK_CELERY_BEAT_SECONDS` / worker interval |
| UI STALE (news) | **> 30 min** | `LIVE_NEWS_STALE_MINUTES` |
| UI STALE (macro) | **> 120 min** | `LIVE_MACRO_STALE_MINUTES` |

## Local / Docker Compose

```bash
docker compose up --build
# Services: web, desk, ingest, worker, beat, postgres, redis
```

- **`ingest`** runs `python run_ingest_worker.py` (scrape + jobs + macro).
- Desk Celery **worker + beat** keep the Analyst Desk pipeline warm (including scheduled briefs).

Without Compose:

```bash
python run_ingest_worker.py
# + Celery worker/beat OR:
python run_analyst_desk_worker.py
```

`python run.py` still starts in-process scrape/macro for quick local demos.

## Cloud / Render

Required topology for “live”:

1. **Web** — `gunicorn -b 0.0.0.0:$PORT -w 2 run:app`
2. **Desk** — `gunicorn … run_analyst_desk:app` (or same image, different start)
3. **Ingest Background Worker** — `python run_ingest_worker.py` (**always-on**, not free-tier sleep)
4. **Celery worker** — `celery -A app.analyst_desk.celery_app worker -l INFO -Q celery,desk_metrics`
5. **Celery beat** — `celery -A app.analyst_desk.celery_app beat -l INFO`
6. **Redis** — Celery broker
7. **Durable disk or Postgres** — free ephemeral disk wipes “live” on redeploy

**Free-tier sleep:** if the ingest worker sleeps, Home/Feed will show a **STALE DATA** banner within ~30 minutes. Use a paid always-on worker + persistent disk (or managed DB).

## Conditional GET (ETag)

RSS fetches send `If-None-Match` / `If-Modified-Since`. 304 responses count as success with 0 new articles. Stats land on Ops (`source_http_cache`) and in `data/platform_ops_status.json`.

## Ops

Open `/ops` or `/api/health`:

- `freshness` — news/macro stale flags + banner text
- `ingest_worker` — heartbeats and last scrape/jobs/macro
- `source_http_cache` — 304 rate
- `desk_worker` — Celery/loop heartbeat (scheduled briefs included)

## Empty / cold start

If the DB is empty after sleep or first boot:

1. Start **ingest** (Compose or `run_ingest_worker.py`)
2. Wait one scrape cycle (~3 min default)
3. Optionally `python scrape.py` once for an immediate pull

Do not expect gunicorn alone to refresh news.
