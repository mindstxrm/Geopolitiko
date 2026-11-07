"""Lightweight observability helpers (request ids, structured log lines)."""
from __future__ import annotations

import logging
import time
import uuid
from typing import Any

from flask import Flask, g, request


logger = logging.getLogger("geopolitiko.obs")


def install_request_observability(app: Flask, *, service: str = "terminal") -> None:
    """Attach before/after request hooks for request_id + latency logs."""

    @app.before_request
    def _obs_before():
        g.request_id = (request.headers.get("X-Request-ID") or "").strip() or uuid.uuid4().hex[:12]
        g._obs_t0 = time.perf_counter()

    @app.after_request
    def _obs_after(response):
        rid = getattr(g, "request_id", None)
        if rid:
            response.headers["X-Request-ID"] = rid
        t0 = getattr(g, "_obs_t0", None)
        ms = int((time.perf_counter() - t0) * 1000) if t0 is not None else None
        logger.info(
            "service=%s request_id=%s method=%s path=%s status=%s ms=%s",
            service,
            rid,
            request.method,
            request.path,
            response.status_code,
            ms,
        )
        return response


def log_event(event: str, **fields: Any) -> None:
    parts = [f"event={event}"]
    for k, v in fields.items():
        parts.append(f"{k}={v}")
    logger.info(" ".join(parts))
