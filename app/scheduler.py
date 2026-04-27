"""Background scheduler: scrape news and run analysis jobs every minute."""
import json
import logging
import threading
import time

import requests

from config import DATABASE_PATH
from app.models import (
    init_db,
    get_all_alerts,
    get_alert_matches,
    get_alert_webhook_state,
    set_alert_webhook_state,
    get_new_conflict_records,
    add_conflict_event,
    get_conflict_alert_rules_matching,
)

logger = logging.getLogger(__name__)

# Limit how many articles we process per refresh to keep each cycle fast
REFRESH_TOPICS_LIMIT = 2000
REFRESH_ANALYSIS_LIMIT = 100
REFRESH_IMPACT_LIMIT = 2000

_running = False
_lock = threading.Lock()
_macro_running = False
_macro_lock = threading.Lock()


def run_refresh() -> None:
    """Run scraper then all analysis jobs (topics, impact, analysis, digest, cluster)."""
    global _running
    if _lock.acquire(blocking=False):
        try:
            _running = True
            _do_refresh()
        finally:
            _running = False
            _lock.release()
    else:
        logger.info("Refresh skipped (previous run still in progress)")


def _do_refresh() -> None:
    """Actual refresh logic: scrape + jobs."""
    try:
        init_db(DATABASE_PATH)

        # 1. Scrape all RSS sources
        from app.scrapers.rss_scraper import scrape_all_sources
        results = scrape_all_sources(DATABASE_PATH)
        total = sum(results.values())
        logger.info("Scrape done: %s articles from %s sources", total, len(results))

        # 2. Topics & entities
        from app.jobs.topics import extract_topics_for_all
        n = extract_topics_for_all(limit=REFRESH_TOPICS_LIMIT)
        logger.info("Topics: %s articles", n)

        # 3. Impact scoring
        from app.jobs.impact import score_all
        n = score_all(limit=REFRESH_IMPACT_LIMIT)
        logger.info("Impact: %s articles", n)

        # 4. Analysis (key takeaways, why it matters)
        from app.jobs.analysis import generate_analysis_for_all
        n = generate_analysis_for_all(limit=REFRESH_ANALYSIS_LIMIT)
        logger.info("Analysis: %s articles", n)

        # 5. Daily digest
        from app.jobs.digest import generate_daily_digest
        generate_daily_digest()
        logger.info("Daily digest created")

        # 6. Clustering
        from app.jobs.clustering import cluster_articles
        cluster_articles(lookback_days=7)
        logger.info("Clustering done")

        # 7. Risk Engine (country risk + forward probability index)
        try:
            from app.risk_engine import compute_country_risk_from_articles, compute_forward_risk_index
            compute_country_risk_from_articles(days=7)
            compute_forward_risk_index(days=7)
            logger.info("Risk engine updated")
        except Exception as e:
            logger.warning("Risk engine failed: %s", e)

        # 8. Institutional models (GEPI, fragility, CDEI, SFI, etc.)
        try:
            from app.jobs.institutional import run_institutional_models
            run_institutional_models()
            logger.info("Institutional models updated")
        except Exception as e:
            logger.warning("Institutional models failed: %s", e)

        # 9. Alert webhooks (notify when new matches)
        _fire_alert_webhooks()
        # 10. Conflict events: emit new incidents/exercises/movement and fire conflict alert webhooks
        _emit_conflict_events()
    except Exception as e:
        logger.exception("Refresh failed: %s", e)


def _fire_alert_webhooks() -> None:
    """For each alert with webhook_url, if match count increased, POST to webhook."""
    alerts = get_all_alerts()
    for a in alerts:
        url = (a.get("webhook_url") or "").strip()
        if not url:
            continue
        try:
            matches = get_alert_matches(a["id"], days=1, limit=500)
            count = len(matches)
            state = get_alert_webhook_state(a["id"])
            prev = state["last_count"] if state else 0
            set_alert_webhook_state(a["id"], count)
            if count > prev and count > 0:
                payload = {
                    "alert_id": a["id"],
                    "alert_name": a["name"],
                    "match_count": count,
                    "previous_count": prev,
                }
                requests.post(url, json=payload, timeout=10)
                logger.info("Webhook fired for alert %s: %s matches", a["name"], count)
        except Exception as e:
            logger.warning("Webhook failed for alert %s: %s", a.get("name"), e)


def _emit_conflict_events() -> None:
    """Emit new conflict records as events and POST to matching conflict alert webhooks."""
    try:
        records = get_new_conflict_records()
        for rec in records:
            add_conflict_event(
                event_type=rec["event_type"],
                region=rec.get("region"),
                country_code=rec.get("country_code"),
                record_id=rec["record_id"],
                table_name=rec["table_name"],
                summary=rec.get("summary"),
            )
            rules = get_conflict_alert_rules_matching(
                rec["event_type"],
                rec.get("region"),
                rec.get("country_code"),
            )
            payload = {
                "event_type": rec["event_type"],
                "region": rec.get("region"),
                "country_code": rec.get("country_code"),
                "record_id": rec["record_id"],
                "table_name": rec["table_name"],
                "summary": rec.get("summary"),
            }
            for rule in rules:
                url = (rule.get("webhook_url") or "").strip()
                if not url:
                    continue
                try:
                    requests.post(url, json=payload, timeout=10)
                    logger.info("Conflict webhook fired for rule %s: %s", rule.get("name"), rec["event_type"])
                except Exception as e:
                    logger.warning("Conflict webhook failed for rule %s: %s", rule.get("name"), e)
    except Exception as e:
        logger.warning("Conflict events failed: %s", e)


def start_scheduler(interval_seconds: int = 60, first_delay_seconds: int = 15) -> None:
    """Start background thread that runs refresh every interval_seconds."""
    def loop():
        time.sleep(first_delay_seconds)
        while True:
            run_refresh()
            time.sleep(interval_seconds)

    t = threading.Thread(target=loop, daemon=True, name="refresh-scheduler")
    t.start()
    logger.info("Scheduler started: refresh every %ss (first in %ss)", interval_seconds, first_delay_seconds)


def run_macro_ingest() -> None:
    """Run macro ingestion (guarded by a lock)."""
    global _macro_running
    if _macro_lock.acquire(blocking=False):
        try:
            _macro_running = True
            from app.macro_pipeline import ingest_macro_once
            res = ingest_macro_once()
            logger.info(
                "Macro ingest done: WB %s/%s, FX %s, FRED %s, Eurostat %s",
                res.get("world_bank_written"),
                res.get("world_bank_attempts"),
                res.get("fx_written"),
                res.get("fred_written"),
                res.get("eurostat_written"),
            )
        except Exception as e:
            logger.warning("Macro ingest failed: %s", e)
        finally:
            _macro_running = False
            _macro_lock.release()
    else:
        logger.info("Macro ingest skipped (previous run still in progress)")


def start_macro_scheduler(interval_seconds: int = 6 * 60 * 60, first_delay_seconds: int = 30) -> None:
    """Start background thread that runs macro ingest on a slower cadence."""
    def loop():
        time.sleep(first_delay_seconds)
        while True:
            run_macro_ingest()
            time.sleep(interval_seconds)

    t = threading.Thread(target=loop, daemon=True, name="macro-scheduler")
    t.start()
    logger.info("Macro scheduler started: ingest every %ss (first in %ss)", interval_seconds, first_delay_seconds)


def is_refresh_running() -> bool:
    return _running


def is_macro_ingest_running() -> bool:
    return _macro_running
