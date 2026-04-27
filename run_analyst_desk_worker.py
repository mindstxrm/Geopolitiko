#!/usr/bin/env python3
"""
Analyst Desk background worker (no Flask UI).

Runs the full pipeline on a fixed interval so ingestion, enrichment, quantitative extraction,
events, alerts, and (optional) Phase 2/3 lanes keep running while you are offline.

Deploy on a server or local machine with:
  nohup python run_analyst_desk_worker.py >> logs/analyst_desk_worker.log 2>&1 &

Or use systemd with Restart=always.

Environment (see also config.py and .env):
  DATABASE_PATH                    — main news DB (default data/news.db)
  ANALYST_DESK_DATABASE_PATH     — desk SQLite (default data/analyst_desk.db)
  ANALYST_DESK_WORKER_INTERVAL_SECONDS — seconds between ticks (default 300, min 30)
  ANALYST_DESK_PHASE2_ENABLED    — topic/regional/risk/movers in worker (default 1)
  ANALYST_DESK_PHASE3_ENABLED    — scenarios in worker (default 1)

Approved proposals are pushed to the Terminal when you click Approve in the Desk UI
(see ANALYST_DESK_PUBLISH_ON_APPROVE); the worker does not require the UI to be open.
"""
from __future__ import annotations

import logging
import signal
import sys
import time

from config import (
    ANALYST_DESK_DATABASE_PATH,
    DATABASE_PATH,
    analyst_desk_worker_interval_seconds,
    load_app_dotenv,
)

load_app_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("analyst_desk_worker")

_stop = False


def _handle_sigterm(*_args) -> None:
    global _stop
    _stop = True
    logger.info("Shutdown signal received; finishing after current tick if any.")


def run_tick() -> None:
    from app.analyst_desk.orchestrator import run_full_pipeline_tick

    stats = run_full_pipeline_tick()
    logger.info(
        "tick ok ingested=%s enriched=%s metrics_docs=%s metrics_rows=%s events=%s alerts=%s briefing=%s "
        "topics=%s regions=%s countries=%s movers=%s scenarios=%s",
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


def main() -> int:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    from app.models import init_db
    from app.analyst_desk.agents import AGENTS
    from app.analyst_desk.store import init_analyst_desk_db, sync_agents_registry

    init_db(DATABASE_PATH)
    init_analyst_desk_db(ANALYST_DESK_DATABASE_PATH)
    sync_agents_registry(AGENTS)

    interval = analyst_desk_worker_interval_seconds()
    logger.info(
        "Analyst Desk worker started; interval=%ss news_db=%s desk_db=%s",
        interval,
        DATABASE_PATH,
        ANALYST_DESK_DATABASE_PATH,
    )

    while not _stop:
        try:
            run_tick()
        except Exception:
            logger.exception("pipeline tick failed")
        for _ in range(interval):
            if _stop:
                break
            time.sleep(1)
    logger.info("Analyst Desk worker stopped.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
