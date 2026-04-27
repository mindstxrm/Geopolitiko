"""Run the Analyst Desk (separate from main Geopolitiko web app).

Uses its own SQLite file for proposals/approval state (data/analyst_desk.db by default).
Reads articles from the main news database (DATABASE_PATH / data/news.db).

Environment:
  ANALYST_DESK_ADMIN_PASSWORD           — if set, required to use the UI
  ANALYST_DESK_SECRET_KEY               — Flask session signing
  OPENAI_API_KEY                        — optional; for LLM narrative (also from project-root `.env`)
  ANALYST_DESK_HEURISTIC_ONLY=1         — never call an LLM; article roll-ups only (no key needed)
  ANALYST_DESK_PIPELINE_INTERVAL_SECONDS — optional autorun cadence (Flask thread; use run_analyst_desk_worker.py for production)
  ANALYST_DESK_PUBLISH_ON_APPROVE          — 1/0 push approved proposals to Terminal (news.db desk_terminal_intel)
  ANALYST_DESK_WORKER_INTERVAL_SECONDS     — for run_analyst_desk_worker.py (default 300)
  ANALYST_DESK_PHASE2_ENABLED           — 1/0 toggle for topic/regional/risk/mover autorun lanes
  ANALYST_DESK_ALERT_WEBHOOK_URL        — optional webhook target for alert delivery
  ANALYST_DESK_ALERT_EMAIL_TO           — optional CSV recipients for SMTP fallback delivery
"""
import logging
import os
import threading
import time

from config import (
    analyst_desk_pipeline_interval_seconds,
    load_app_dotenv,
)

load_app_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

from app.analyst_desk.app import create_analyst_desk_app

app = create_analyst_desk_app()


def _run_pipeline_once() -> None:
    from app.analyst_desk.orchestrator import run_full_pipeline_tick

    st = run_full_pipeline_tick()
    logger.info(
        "Analyst desk pipeline tick: ingested=%s enriched=%s metrics_docs=%s metrics_rows=%s events=%s alerts=%s briefing=%s topics=%s regions=%s countries=%s movers=%s scenarios=%s",
        st.get("ingested", 0),
        st.get("enriched", 0),
        st.get("metrics_docs", 0),
        st.get("metrics_rows", 0),
        st.get("events", 0),
        st.get("alerts_created", 0),
        st.get("briefing_id"),
        st.get("topic_briefings", 0),
        st.get("regional_snaps", 0),
        st.get("country_snaps", 0),
        st.get("movers", 0),
        st.get("scenarios", 0),
    )


def _maybe_start_pipeline_autorun() -> None:
    interval = analyst_desk_pipeline_interval_seconds()
    if interval <= 0:
        return
    logger.info("Starting Analyst Desk pipeline autorun every %ss", interval)

    def _loop():
        while True:
            try:
                _run_pipeline_once()
            except Exception:
                logger.exception("Analyst desk pipeline autorun tick failed")
            time.sleep(interval)

    t = threading.Thread(target=_loop, daemon=True, name="analyst-desk-pipeline-autorun")
    t.start()


if __name__ == "__main__":
    # Start autorun only once: reloader child in debug, or normal process otherwise.
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or "WERKZEUG_RUN_MAIN" not in os.environ:
        _maybe_start_pipeline_autorun()
    # Default 5005 to avoid clashing with run.py (5003)
    app.run(debug=True, port=5005, host="127.0.0.1")
