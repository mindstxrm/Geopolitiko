"""Optional Sentry error reporting + simple metrics counters."""
from __future__ import annotations

import logging
import os
import threading
from typing import Any

logger = logging.getLogger(__name__)

_metrics_lock = threading.Lock()
_metrics: dict[str, int] = {}


def init_sentry(service: str = "terminal") -> bool:
    """
    Initialize Sentry when SENTRY_DSN is set. Returns True if enabled.
    Env: SENTRY_DSN, SENTRY_TRACES_SAMPLE_RATE (default 0.0), SENTRY_ENVIRONMENT
    """
    dsn = (os.environ.get("SENTRY_DSN") or "").strip()
    if not dsn:
        return False
    try:
        import sentry_sdk
        from sentry_sdk.integrations.flask import FlaskIntegration
    except ImportError:
        logger.warning("SENTRY_DSN set but sentry-sdk not installed")
        return False

    traces = 0.0
    raw = (os.environ.get("SENTRY_TRACES_SAMPLE_RATE") or "0").strip()
    try:
        traces = float(raw)
    except ValueError:
        traces = 0.0

    env = (
        (os.environ.get("SENTRY_ENVIRONMENT") or "").strip()
        or (os.environ.get("GEOPOLITIKO_ENV") or os.environ.get("FLASK_ENV") or "development")
    )
    sentry_sdk.init(
        dsn=dsn,
        integrations=[FlaskIntegration()],
        traces_sample_rate=max(0.0, min(1.0, traces)),
        environment=env,
        release=os.environ.get("SENTRY_RELEASE") or None,
    )
    sentry_sdk.set_tag("service", service)
    logger.info("Sentry enabled service=%s env=%s", service, env)
    return True


def incr_metric(name: str, amount: int = 1) -> None:
    with _metrics_lock:
        _metrics[name] = _metrics.get(name, 0) + amount


def get_metrics_snapshot() -> dict[str, Any]:
    with _metrics_lock:
        return dict(_metrics)
