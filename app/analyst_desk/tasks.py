"""Celery tasks for Analyst Desk pipeline stages."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

from celery.utils.log import get_task_logger

from config import CELERY_BROKER_URL

from app.analyst_desk.celery_app import celery_app
from app.analyst_desk.store import create_dead_letter_task

# Celery-attached logger so messages show in the worker terminal (plain logging.getLogger often does not).
logger = get_task_logger(__name__)

_LOCK_KEY = "analyst_desk:pipeline_tick_lock"
_LOCK_TTL = 900


@contextmanager
def _redis_pipeline_lock() -> Iterator[bool]:
    """
    If Redis is available, acquire a short lock so overlapping beat/worker ticks
    do not hammer SQLite. Yields True if this process holds the lock (run pipeline).
    """
    try:
        import redis
    except ImportError:
        yield True
        return

    try:
        r = redis.Redis.from_url(CELERY_BROKER_URL, decode_responses=True)
        lock = r.lock(_LOCK_KEY, timeout=_LOCK_TTL, blocking_timeout=5)
        acquired = lock.acquire(blocking=True)
        if not acquired:
            logger.warning("Analyst desk pipeline lock not acquired; skipping tick")
            yield False
            return
        try:
            yield True
        finally:
            try:
                lock.release()
            except Exception:
                pass
    except Exception as e:
        logger.warning("Redis lock unavailable (%s); running pipeline without lock", e)
        yield True


@celery_app.task(name="app.analyst_desk.tasks.run_full_pipeline", bind=True, max_retries=2)
def run_full_pipeline(self) -> dict[str, Any]:
    """Full ingest → … → scenarios tick. Safe to run on multiple workers with Redis lock."""
    logger.info("run_full_pipeline START task_id=%s", self.request.id)
    with _redis_pipeline_lock() as run:
        if not run:
            logger.warning("run_full_pipeline SKIPPED (lock held) task_id=%s", self.request.id)
            return {"ok": False, "skipped": True, "reason": "lock"}
        try:
            from app.analyst_desk.orchestrator import run_full_pipeline_tick

            stats = run_full_pipeline_tick()
            logger.info(
                "run_full_pipeline OK task_id=%s ingested=%s enriched=%s metrics_docs=%s metrics_rows=%s "
                "events=%s alerts=%s briefing=%s topics=%s regional=%s country_risk=%s movers=%s scenarios=%s",
                self.request.id,
                stats.get("ingested", 0),
                stats.get("enriched", 0),
                stats.get("metrics_docs", 0),
                stats.get("metrics_rows", 0),
                stats.get("events", 0),
                stats.get("alerts_created", 0),
                stats.get("briefing_id"),
                stats.get("topic_briefings", 0),
                stats.get("regional_snaps", 0),
                stats.get("country_snaps", 0),
                stats.get("movers", 0),
                stats.get("scenarios", 0),
            )
            return stats
        except Exception as exc:
            logger.exception("run_full_pipeline failed task_id=%s", self.request.id)
            if int(self.request.retries or 0) >= int(getattr(self, "max_retries", 0)):
                create_dead_letter_task(
                    task_name="app.analyst_desk.tasks.run_full_pipeline",
                    task_id=self.request.id,
                    payload={},
                    error_message=str(exc),
                    retries=int(self.request.retries or 0),
                )
            raise self.retry(exc=exc, countdown=60) from exc


@celery_app.task(name="app.analyst_desk.tasks.run_metric_extraction_only", bind=True)
def run_metric_extraction_only(self, limit: int = 200) -> dict[str, Any]:
    """Optional: scale metric extraction on queue ``desk_metrics``."""
    from app.analyst_desk.pipeline import process_metric_extraction

    logger.info("run_metric_extraction_only START task_id=%s limit=%s", self.request.id, limit)
    try:
        out = process_metric_extraction(limit=limit)
        logger.info(
            "run_metric_extraction_only OK task_id=%s docs=%s rows=%s",
            self.request.id,
            out.get("docs_metrics_processed", 0),
            out.get("metrics_written", 0),
        )
        return out
    except Exception as exc:
        logger.exception("run_metric_extraction_only failed task_id=%s", self.request.id)
        create_dead_letter_task(
            task_name="app.analyst_desk.tasks.run_metric_extraction_only",
            task_id=self.request.id,
            payload={"limit": limit},
            error_message=str(exc),
            retries=int(self.request.retries or 0),
        )
        raise


@celery_app.task(name="app.analyst_desk.tasks.run_enrich_only", bind=True)
def run_enrich_only(self, limit: int = 200) -> dict[str, Any]:
    from app.analyst_desk.pipeline import process_document_enrichment

    logger.info("run_enrich_only START task_id=%s", self.request.id)
    try:
        out = process_document_enrichment(limit=limit)
        logger.info("run_enrich_only OK task_id=%s processed=%s", self.request.id, out.get("processed", 0))
        return out
    except Exception as exc:
        logger.exception("run_enrich_only failed task_id=%s", self.request.id)
        create_dead_letter_task(
            task_name="app.analyst_desk.tasks.run_enrich_only",
            task_id=self.request.id,
            payload={"limit": limit},
            error_message=str(exc),
            retries=int(self.request.retries or 0),
        )
        raise


@celery_app.task(name="app.analyst_desk.tasks.run_events_only", bind=True)
def run_events_only(self, limit: int = 200) -> dict[str, Any]:
    from app.analyst_desk.pipeline import process_event_builder

    logger.info("run_events_only START task_id=%s", self.request.id)
    try:
        out = process_event_builder(limit=limit)
        logger.info(
            "run_events_only OK task_id=%s events_written=%s",
            self.request.id,
            out.get("events_written", 0),
        )
        return out
    except Exception as exc:
        logger.exception("run_events_only failed task_id=%s", self.request.id)
        create_dead_letter_task(
            task_name="app.analyst_desk.tasks.run_events_only",
            task_id=self.request.id,
            payload={"limit": limit},
            error_message=str(exc),
            retries=int(self.request.retries or 0),
        )
        raise
