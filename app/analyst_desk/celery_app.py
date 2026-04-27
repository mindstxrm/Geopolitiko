"""
Celery application for Analyst Desk (Redis broker).

Run workers (project root):
  celery -A app.analyst_desk.celery_app worker -l INFO -c 4

Dedicated metrics queue (optional horizontal scale):
  celery -A app.analyst_desk.celery_app worker -l INFO -Q desk_metrics -c 2

Beat (scheduled full pipeline):
  celery -A app.analyst_desk.celery_app beat -l INFO

Env:
  CELERY_BROKER_URL / CELERY_RESULT_BACKEND (default redis://localhost:6379/0)
  ANALYST_DESK_CELERY_BEAT_SECONDS — interval for beat schedule (0 = disable default schedule)
"""
from __future__ import annotations

import logging
from datetime import timedelta

from celery import Celery
from celery.schedules import schedule
from celery.signals import worker_process_init, worker_ready
from celery.utils.log import get_task_logger

from config import (
    ANALYST_DESK_DATABASE_PATH,
    CELERY_BROKER_URL,
    CELERY_RESULT_BACKEND,
    DATABASE_PATH,
    analyst_desk_celery_beat_seconds,
    load_app_dotenv,
)

load_app_dotenv()

logger = logging.getLogger(__name__)
_boot_log = get_task_logger("analyst_desk.boot")

celery_app = Celery(
    "analyst_desk",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    task_track_started=True,
    task_time_limit=3600,
    task_soft_time_limit=3300,
)

celery_app.conf.task_routes = {
    "app.analyst_desk.tasks.run_metric_extraction_only": {"queue": "desk_metrics"},
}

_beat_s = analyst_desk_celery_beat_seconds()
if _beat_s > 0:
    celery_app.conf.beat_schedule = {
        "analyst-desk-full-pipeline": {
            "task": "app.analyst_desk.tasks.run_full_pipeline",
            "schedule": schedule(run_every=timedelta(seconds=_beat_s)),
        },
    }
    logger.info("Celery beat: analyst-desk-full-pipeline every %ss", _beat_s)
else:
    celery_app.conf.beat_schedule = {}


@worker_ready.connect
def _analyst_desk_worker_ready(**_kwargs) -> None:
    """Visible line when the worker is up (helps if you expected logs but only started Beat)."""
    _boot_log.info(
        "Analyst Desk Celery worker READY — task lines appear here when jobs run. "
        "Start with: celery -A app.analyst_desk.celery_app worker -l info -Q celery,desk_metrics"
    )


@worker_process_init.connect
def _desk_worker_process_init(**_kwargs) -> None:
    """Each forked worker process opens DBs once (SQLite + WAL)."""
    load_app_dotenv()
    from app.analyst_desk.agents import AGENTS
    from app.analyst_desk.store import init_analyst_desk_db, sync_agents_registry
    from app.models import init_db

    init_db(DATABASE_PATH)
    init_analyst_desk_db(ANALYST_DESK_DATABASE_PATH)
    sync_agents_registry(AGENTS)


# Register tasks (import side effects: @celery_app.task decorators).
import app.analyst_desk.tasks  # noqa: E402, F401
