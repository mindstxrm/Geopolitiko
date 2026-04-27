# Analyst Desk: Celery + Redis (multi-worker)

## Install

```bash
pip install -r requirements.txt
```

Ensure **Redis** is running (local or managed), e.g. `redis-server` or Docker:

```bash
docker run -d -p 6379:6379 redis:7-alpine
```

## Environment

| Variable | Purpose |
|----------|---------|
| `CELERY_BROKER_URL` | Default `redis://localhost:6379/0` |
| `CELERY_RESULT_BACKEND` | Defaults to same as broker |
| `ANALYST_DESK_CELERY_BEAT_SECONDS` | If `>0`, registers a Beat schedule for the full pipeline (default `300`). Set `0` to disable and use cron/external Beat only. |
| `DATABASE_PATH` / `ANALYST_DESK_DATABASE_PATH` | Same as the rest of the app |

## LLM metric extraction (JSON schema)

| Variable | Purpose |
|----------|---------|
| `ANALYST_DESK_METRIC_LLM` | `1` to call OpenAI structured outputs for quantitative extraction |
| `OPENAI_API_KEY` / `OPENAI_MODEL` | Required for LLM metrics (unless `ANALYST_DESK_HEURISTIC_ONLY=1`) |
| `ANALYST_DESK_METRIC_LLM_MAX_DOCS` | Max documents per run that get an LLM call (default `15`) |
| `ANALYST_DESK_METRIC_LLM_MERGE_HEURISTIC` | `1` to merge regex + LLM; `0` uses LLM-only when the model returns any row |

## Run workers (project root)

**One worker** consuming both default and metrics queues (simplest):

```bash
celery -A app.analyst_desk.celery_app worker -l INFO -Q celery,desk_metrics -c 4
```

**Split pools** (optional): default queue only:

```bash
celery -A app.analyst_desk.celery_app worker -l INFO -Q celery -c 2
```

**Dedicated metrics workers**:

```bash
celery -A app.analyst_desk.celery_app worker -l INFO -Q desk_metrics -c 2
```

## Beat (scheduled full pipeline)

```bash
celery -A app.analyst_desk.celery_app beat -l INFO
```

If `ANALYST_DESK_CELERY_BEAT_SECONDS=0`, this process starts but has no built-in schedule; add entries in `celery_app.py` or use system cron to `celery call app.analyst_desk.tasks.run_full_pipeline`.

## Overlap / SQLite

Full-pipeline tasks use a short **Redis lock** (`analyst_desk:pipeline_tick_lock`) so two workers rarely hammer SQLite at once. If Redis is down, the task runs without a lock (log warning).

## Desk UI

With Celery importable, the Analyst Desk shows buttons to **queue** the full pipeline or **metrics-only** jobs.

## Tasks reference

| Task name | Queue | Role |
|-----------|-------|------|
| `app.analyst_desk.tasks.run_full_pipeline` | `celery` | Full tick (same as `run_analyst_desk_worker`) |
| `app.analyst_desk.tasks.run_metric_extraction_only` | `desk_metrics` | Metric extraction only |
| `app.analyst_desk.tasks.run_enrich_only` | `celery` | Optional stage hooks |
| `app.analyst_desk.tasks.run_events_only` | `celery` | Optional stage hooks |

## Where are the worker logs?

1. **You must run a worker process** — Beat only schedules tasks; it does not execute them. You need **both** (or only a worker if you trigger tasks from the Desk UI):
   - Terminal A: `celery -A app.analyst_desk.celery_app worker -l info -Q celery,desk_metrics`
   - Terminal B (optional): `celery -A app.analyst_desk.celery_app beat -l info`

2. **Run from the project root** (same folder as `config.py`) so `app.analyst_desk` imports work.

3. **Log level**: use **`-l info`** or **`-l debug`**. You should see:
   - `Analyst Desk Celery worker READY — …` when the worker starts
   - `run_full_pipeline START` / `run_full_pipeline OK` with counters when a task runs

4. **Queue mismatch**: the full pipeline task goes to the **`celery`** queue. If you started a worker with **only** `-Q desk_metrics`, it will never run `run_full_pipeline`. Use **`-Q celery,desk_metrics`** (or two workers).

5. **Manual test** (no Beat):
   ```bash
   celery -A app.analyst_desk.celery_app call app.analyst_desk.tasks.run_full_pipeline
   ```
   Watch the worker terminal for `START` / `OK` lines.

6. **Log file** (optional):  
   `celery -A app.analyst_desk.celery_app worker -l info -Q celery,desk_metrics --logfile=logs/celery_worker.log`
