"""Background scheduler: scrape news and run analysis jobs (separable cadences)."""
from __future__ import annotations

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
_jobs_running = False
_jobs_lock = threading.Lock()
_macro_running = False
_macro_lock = threading.Lock()


def run_scrape_only() -> dict:
    """
    Scrape all RSS sources without running analysis jobs.
    Returns {ok, articles, sources, not_modified?, error?}.
    """
    global _running
    if not _lock.acquire(blocking=False):
        logger.info("Scrape skipped (previous scrape/refresh still in progress)")
        return {"ok": False, "skipped": True, "reason": "lock"}
    try:
        _running = True
        from app.platform_ops import record_refresh_start, record_refresh_success, record_refresh_failure

        record_refresh_start()
        try:
            init_db(DATABASE_PATH)
            from app.scrapers.rss_scraper import scrape_all_sources

            results = scrape_all_sources(DATABASE_PATH)
            total = sum(int(v or 0) for v in results.values())
            not_modified = sum(1 for v in results.values() if v == 0)
            logger.info(
                "Scrape-only done: %s articles from %s sources (%s zero/304)",
                total,
                len(results),
                not_modified,
            )
            record_refresh_success(articles_scraped=total, sources=len(results))
            try:
                from app.platform_ops import write_ops_status
                from app.scrapers.rss_scraper import source_http_cache_summary

                write_ops_status({"source_http_cache": source_http_cache_summary(DATABASE_PATH)})
            except Exception:
                pass
            return {
                "ok": True,
                "articles": total,
                "sources": len(results),
                "zero_or_304": not_modified,
                "by_source": results,
            }
        except Exception as e:
            record_refresh_failure(str(e))
            logger.exception("Scrape-only failed: %s", e)
            return {"ok": False, "error": str(e)}
    finally:
        _running = False
        _lock.release()


def run_analysis_jobs() -> dict:
    """Run topics/impact/analysis/digest/cluster/risk/alerts without scraping."""
    global _jobs_running
    if not _jobs_lock.acquire(blocking=False):
        logger.info("Analysis jobs skipped (previous jobs still in progress)")
        return {"ok": False, "skipped": True, "reason": "lock"}
    try:
        _jobs_running = True
        init_db(DATABASE_PATH)
        out: dict = {"ok": True}

        from app.jobs.topics import extract_topics_for_all

        out["topics"] = extract_topics_for_all(limit=REFRESH_TOPICS_LIMIT)
        logger.info("Topics: %s articles", out["topics"])

        from app.jobs.impact import score_all

        out["impact"] = score_all(limit=REFRESH_IMPACT_LIMIT)
        logger.info("Impact: %s articles", out["impact"])

        from app.jobs.analysis import generate_analysis_for_all

        out["analysis"] = generate_analysis_for_all(limit=REFRESH_ANALYSIS_LIMIT)
        logger.info("Analysis: %s articles", out["analysis"])

        from app.jobs.digest import generate_daily_digest

        generate_daily_digest()
        out["digest"] = True
        logger.info("Daily digest created")

        from app.jobs.clustering import cluster_articles

        cluster_articles(lookback_days=7)
        out["cluster"] = True
        logger.info("Clustering done")

        try:
            from app.risk_engine import compute_country_risk_from_articles, compute_forward_risk_index

            compute_country_risk_from_articles(days=7)
            compute_forward_risk_index(days=7)
            out["risk"] = True
            logger.info("Risk engine updated")
        except Exception as e:
            logger.warning("Risk engine failed: %s", e)
            out["risk"] = False

        try:
            from app.jobs.institutional import run_institutional_models

            run_institutional_models()
            out["institutional"] = True
            logger.info("Institutional models updated")
        except Exception as e:
            logger.warning("Institutional models failed: %s", e)
            out["institutional"] = False

        _fire_alert_webhooks()
        _emit_conflict_events()
        out["alerts"] = True
        try:
            from app.platform_ops import write_ops_status

            write_ops_status({"last_jobs_finished_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "last_jobs_status": "ok"})
        except Exception:
            pass
        return out
    except Exception as e:
        logger.exception("Analysis jobs failed: %s", e)
        try:
            from app.platform_ops import write_ops_status

            write_ops_status({"last_jobs_status": "error", "last_jobs_error": str(e)[:2000]})
        except Exception:
            pass
        return {"ok": False, "error": str(e)}
    finally:
        _jobs_running = False
        _jobs_lock.release()


def run_refresh() -> None:
    """Run scraper then all analysis jobs (topics, impact, analysis, digest, cluster)."""
    scrape = run_scrape_only()
    if scrape.get("skipped"):
        return
    if not scrape.get("ok"):
        return
    run_analysis_jobs()


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
    """Start background thread that runs full refresh every interval_seconds (dev convenience)."""

    def loop():
        time.sleep(first_delay_seconds)
        while True:
            run_refresh()
            time.sleep(interval_seconds)

    t = threading.Thread(target=loop, daemon=True, name="refresh-scheduler")
    t.start()
    logger.info("Scheduler started: refresh every %ss (first in %ss)", interval_seconds, first_delay_seconds)


def run_macro_ingest(*, mode: str = "full") -> dict:
    """
    Run macro ingestion (guarded by a lock).
    mode: 'full' (WB+FX+FRED+Eurostat) or 'fast' (FX+FRED only).
    """
    global _macro_running
    if not _macro_lock.acquire(blocking=False):
        logger.info("Macro ingest skipped (previous run still in progress)")
        return {"ok": False, "skipped": True, "reason": "lock"}
    try:
        _macro_running = True
        from app.macro_pipeline import ingest_macro_once
        from app.platform_ops import record_macro_failure, record_macro_success

        res = ingest_macro_once(mode=mode)
        record_macro_success()
        logger.info(
            "Macro ingest (%s) done: WB %s/%s, FX %s, FRED %s, Eurostat %s",
            mode,
            res.get("world_bank_written"),
            res.get("world_bank_attempts"),
            res.get("fx_written"),
            res.get("fred_written"),
            res.get("eurostat_written"),
        )
        return {"ok": True, **res}
    except Exception as e:
        try:
            from app.platform_ops import record_macro_failure

            record_macro_failure(str(e))
        except Exception:
            pass
        logger.warning("Macro ingest failed: %s", e)
        return {"ok": False, "error": str(e)}
    finally:
        _macro_running = False
        _macro_lock.release()


def start_macro_scheduler(interval_seconds: int = 6 * 60 * 60, first_delay_seconds: int = 30) -> None:
    """Start background thread that runs macro ingest on a slower cadence."""

    def loop():
        time.sleep(first_delay_seconds)
        while True:
            run_macro_ingest(mode="full")
            time.sleep(interval_seconds)

    t = threading.Thread(target=loop, daemon=True, name="macro-scheduler")
    t.start()
    logger.info("Macro scheduler started: ingest every %ss (first in %ss)", interval_seconds, first_delay_seconds)


def is_refresh_running() -> bool:
    return _running or _jobs_running


def is_macro_ingest_running() -> bool:
    return _macro_running
