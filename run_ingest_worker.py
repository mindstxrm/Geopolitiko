#!/usr/bin/env python3
"""
Always-on ingest worker: scrape / analysis jobs / macro on separate cadences.

Use this under Docker Compose, Render Background Worker, or systemd — not gunicorn.
Local demo can still use `python run.py` (in-process threads).

Env:
  INGEST_SCRAPE_SECONDS       — scrape-only interval (default 180)
  INGEST_JOBS_SECONDS         — analysis jobs interval (default 900)
  INGEST_MACRO_SECONDS        — fast FX/FRED macro (default 3600)
  INGEST_MACRO_FULL_SECONDS   — full WB+FX batch (default 86400)
"""
from __future__ import annotations

import logging
import signal
import sys
import time

from config import (
    DATABASE_PATH,
    ingest_jobs_seconds,
    ingest_macro_full_seconds,
    ingest_macro_seconds,
    ingest_scrape_seconds,
    load_app_dotenv,
)

load_app_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("ingest_worker")

_stop = False


def _handle_sig(*_args) -> None:
    global _stop
    _stop = True
    logger.info("Shutdown signal received")


def main() -> int:
    signal.signal(signal.SIGTERM, _handle_sig)
    signal.signal(signal.SIGINT, _handle_sig)

    from app.models import init_db
    from app.scheduler import run_analysis_jobs, run_macro_ingest, run_scrape_only
    from app.platform_ops import write_ops_status

    init_db(DATABASE_PATH)

    scrape_every = ingest_scrape_seconds()
    jobs_every = ingest_jobs_seconds()
    macro_every = ingest_macro_seconds()
    macro_full_every = ingest_macro_full_seconds()

    logger.info(
        "Ingest worker started scrape=%ss jobs=%ss macro_fast=%ss macro_full=%ss db=%s",
        scrape_every,
        jobs_every,
        macro_every,
        macro_full_every,
        DATABASE_PATH,
    )
    write_ops_status(
        {
            "ingest_worker_started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "ingest_scrape_seconds": scrape_every,
            "ingest_jobs_seconds": jobs_every,
            "ingest_macro_seconds": macro_every,
            "ingest_macro_full_seconds": macro_full_every,
        }
    )

    next_scrape = 0.0
    next_jobs = 0.0
    next_macro = 0.0
    next_macro_full = 0.0

    while not _stop:
        now = time.time()
        try:
            if now >= next_scrape:
                out = run_scrape_only()
                logger.info("scrape tick ok=%s articles=%s", out.get("ok"), out.get("articles"))
                write_ops_status(
                    {
                        "ingest_worker_last_scrape_at": time.strftime(
                            "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
                        ),
                        "ingest_worker_last_heartbeat_at": time.strftime(
                            "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
                        ),
                    }
                )
                next_scrape = time.time() + scrape_every

            if now >= next_jobs:
                out = run_analysis_jobs()
                logger.info("jobs tick ok=%s", out.get("ok"))
                write_ops_status(
                    {
                        "ingest_worker_last_jobs_at": time.strftime(
                            "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
                        ),
                        "ingest_worker_last_heartbeat_at": time.strftime(
                            "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
                        ),
                    }
                )
                next_jobs = time.time() + jobs_every

            if now >= next_macro:
                out = run_macro_ingest(mode="fast")
                logger.info("macro-fast tick ok=%s fx=%s", out.get("ok"), out.get("fx_written"))
                write_ops_status(
                    {
                        "ingest_worker_last_macro_fast_at": time.strftime(
                            "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
                        ),
                        "ingest_worker_last_heartbeat_at": time.strftime(
                            "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
                        ),
                    }
                )
                next_macro = time.time() + macro_every

            if now >= next_macro_full:
                out = run_macro_ingest(mode="full")
                logger.info("macro-full tick ok=%s wb=%s", out.get("ok"), out.get("world_bank_written"))
                write_ops_status(
                    {
                        "ingest_worker_last_macro_full_at": time.strftime(
                            "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
                        ),
                        "ingest_worker_last_heartbeat_at": time.strftime(
                            "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
                        ),
                    }
                )
                next_macro_full = time.time() + macro_full_every
        except Exception:
            logger.exception("ingest tick failed")
            write_ops_status(
                {
                    "ingest_worker_last_error": "tick_exception",
                    "ingest_worker_last_heartbeat_at": time.strftime(
                        "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
                    ),
                }
            )

        # Sleep until next due work (cap 5s so shutdown is responsive)
        due = min(next_scrape, next_jobs, next_macro, next_macro_full) - time.time()
        time.sleep(max(0.5, min(5.0, due if due > 0 else 1.0)))

    logger.info("Ingest worker stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
