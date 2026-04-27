"""Application configuration."""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


def load_app_dotenv() -> None:
    """Load `.env` from project root so OPENAI_API_KEY etc. apply to all entrypoints (e.g. run_analyst_desk)."""
    env_path = BASE_DIR / ".env"
    if not env_path.is_file():
        return
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv(env_path)


DATABASE_PATH = os.environ.get("DATABASE_PATH", str(BASE_DIR / "data" / "news.db"))
# Required for Flask session (e.g. Scenario Engine last run). Set SECRET_KEY in production.
SECRET_KEY = os.environ.get("SECRET_KEY", "dev-secret-key-change-in-production")
SCRAPE_USER_AGENT = (
    "GeopoliticalNews/1.0 (News aggregator; +https://github.com/geopolitical-news)"
)

# UN voting data: local UNGA-DM RS-077 folder (All_Votes_RS-077.csv)
# Default: ~/Downloads/UNGA-DM RS-077. Override with UNIGE_UN_VOTES_PATH.
UNIGE_UN_VOTES_PATH = os.environ.get("UNIGE_UN_VOTES_PATH", str(Path.home() / "Downloads" / "UNGA-DM RS-077"))

# UNIGE UNGA-DM MariaDB (UN voting data): https://unvotes.unige.ch/mariadb
UNIGE_DB_HOST = os.environ.get("UNIGE_DB_HOST", "dbaas.unige.ch")
UNIGE_DB_PORT = int(os.environ.get("UNIGE_DB_PORT", "30001"))
UNIGE_DB_NAME = os.environ.get("UNIGE_DB_NAME", "unga_database")
UNIGE_DB_USER = os.environ.get("UNIGE_DB_USER", "")
UNIGE_DB_PASSWORD = os.environ.get("UNIGE_DB_PASSWORD", "")

# Analyst Desk: separate Flask app + SQLite for AI proposals / human approval (not the main platform).
ANALYST_DESK_DATABASE_PATH = os.environ.get(
    "ANALYST_DESK_DATABASE_PATH", str(BASE_DIR / "data" / "analyst_desk.db")
)
ANALYST_DESK_SECRET_KEY = os.environ.get(
    "ANALYST_DESK_SECRET_KEY", "analyst-desk-dev-secret-change-in-production"
)
# If set, login is required for the Analyst Desk UI. If empty, the desk is open (dev only).
ANALYST_DESK_ADMIN_PASSWORD = os.environ.get("ANALYST_DESK_ADMIN_PASSWORD", "")


def analyst_desk_heuristic_only() -> bool:
    """If True, Analyst Desk never calls an LLM—drafts are article roll-ups only (no API key needed)."""
    v = (os.environ.get("ANALYST_DESK_HEURISTIC_ONLY") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def analyst_desk_pipeline_interval_seconds() -> int:
    """Optional background cadence for desk pipeline (0 disables autorun)."""
    raw = (os.environ.get("ANALYST_DESK_PIPELINE_INTERVAL_SECONDS") or "").strip()
    if not raw:
        return 0
    try:
        val = int(raw)
    except ValueError:
        return 0
    return max(0, val)


def analyst_desk_phase2_enabled() -> bool:
    """Enable/disable Phase 2 lanes in autorun (topic/regional/risk/movers)."""
    v = (os.environ.get("ANALYST_DESK_PHASE2_ENABLED") or "1").strip().lower()
    return v in ("1", "true", "yes", "on")


def analyst_desk_phase3_enabled() -> bool:
    """Enable/disable Phase 3 lanes in autorun (scenarios)."""
    v = (os.environ.get("ANALYST_DESK_PHASE3_ENABLED") or "1").strip().lower()
    return v in ("1", "true", "yes", "on")


def analyst_desk_publish_on_approve() -> bool:
    """When True, approving a desk proposal writes to news.db desk_terminal_intel for the Terminal."""
    v = (os.environ.get("ANALYST_DESK_PUBLISH_ON_APPROVE") or "1").strip().lower()
    return v in ("1", "true", "yes", "on")


def analyst_desk_worker_interval_seconds() -> int:
    """Background worker tick interval (seconds). Min 30."""
    raw = (os.environ.get("ANALYST_DESK_WORKER_INTERVAL_SECONDS") or "300").strip()
    try:
        return max(30, int(raw))
    except ValueError:
        return 300


# Celery + Redis (optional multi-worker pipeline). See docs/analyst_desk_celery.md
CELERY_BROKER_URL = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
CELERY_RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND", CELERY_BROKER_URL)


def analyst_desk_celery_beat_seconds() -> int:
    """If >0, Celery Beat schedule runs the full desk pipeline on this interval. 0 = no default beat entry."""
    raw = (os.environ.get("ANALYST_DESK_CELERY_BEAT_SECONDS") or "300").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 300


def analyst_desk_metric_llm_enabled() -> bool:
    """Use OpenAI JSON-schema structured extraction for quantitative signals (when key present and not heuristic-only)."""
    v = (os.environ.get("ANALYST_DESK_METRIC_LLM") or "0").strip().lower()
    return v in ("1", "true", "yes", "on")


def analyst_desk_metric_llm_max_docs() -> int:
    """Max documents per metric-extraction run that receive an LLM call (cost control)."""
    raw = (os.environ.get("ANALYST_DESK_METRIC_LLM_MAX_DOCS") or "15").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 15


def fred_api_key() -> str:
    """St. Louis Fed FRED API key (free registration). Used for US macro series in macro_pipeline."""
    load_app_dotenv()
    return (os.environ.get("FRED_API_KEY") or "").strip()


def analyst_desk_metric_llm_merge_heuristic() -> bool:
    """If True, also run regex heuristics and merge (more coverage, noisier). If False, LLM-only when LLM returns any row."""
    v = (os.environ.get("ANALYST_DESK_METRIC_LLM_MERGE_HEURISTIC") or "0").strip().lower()
    return v in ("1", "true", "yes", "on")
