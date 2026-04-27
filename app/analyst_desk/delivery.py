"""Alert delivery adapters for Analyst Desk (webhook + email)."""
from __future__ import annotations

import os
from typing import Any

import requests

from app.email_sender import send_email


def _webhook_url() -> str:
    return (os.environ.get("ANALYST_DESK_ALERT_WEBHOOK_URL") or "").strip()


def _email_recipients() -> list[str]:
    raw = (os.environ.get("ANALYST_DESK_ALERT_EMAIL_TO") or "").strip()
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


def delivery_status() -> dict[str, Any]:
    """Non-secret delivery config status for UI diagnostics."""
    webhook = _webhook_url()
    recipients = _email_recipients()
    return {
        "webhook_configured": bool(webhook),
        "webhook_preview": webhook[:70] + ("..." if webhook and len(webhook) > 70 else ""),
        "email_configured": bool(recipients),
        "email_recipients": recipients,
    }


def _post_webhook(payload: dict[str, Any]) -> tuple[bool, str]:
    url = _webhook_url()
    if not url:
        return False, "webhook_not_configured"
    try:
        r = requests.post(url, json=payload, timeout=15)
        if 200 <= r.status_code < 300:
            return True, "ok"
        return False, f"webhook_http_{r.status_code}"
    except Exception as e:
        return False, f"webhook_error:{e}"


def _send_email_alert(subject: str, body: str) -> tuple[bool, str]:
    recipients = _email_recipients()
    if not recipients:
        return False, "email_not_configured"
    sent = 0
    for to_addr in recipients:
        ok = send_email(to_addr, subject, body)
        if ok:
            sent += 1
    if sent == 0:
        return False, "smtp_send_failed"
    return True, f"sent_{sent}"


def deliver_alert(alert: dict[str, Any]) -> tuple[bool, str, str]:
    """
    Deliver one alert through available channels.
    Returns (ok, channel_used, detail).
    """
    payload = {
        "id": alert.get("id"),
        "event_id": alert.get("event_id"),
        "country_code": alert.get("country_code"),
        "severity": alert.get("severity"),
        "alert_type": alert.get("alert_type"),
        "headline": alert.get("headline"),
        "body": alert.get("body"),
        "created_at": alert.get("created_at"),
    }
    # Prefer webhook if configured; fallback to email.
    ok, detail = _post_webhook(payload)
    if ok:
        return True, "webhook", detail
    ok2, detail2 = _send_email_alert(
        subject=f"[{(alert.get('severity') or 'info').upper()}] {alert.get('headline') or 'Analyst Desk alert'}",
        body=(alert.get("body") or ""),
    )
    if ok2:
        return True, "email", detail2
    return False, "none", f"{detail}; {detail2}"


def send_test_delivery() -> tuple[bool, str, str]:
    """Send a synthetic alert through configured channels."""
    test_alert = {
        "id": "test",
        "event_id": None,
        "country_code": "TEST",
        "severity": "info",
        "alert_type": "test",
        "headline": "Analyst Desk test alert",
        "body": "This is a test alert from Analyst Desk delivery settings.",
        "created_at": None,
    }
    return deliver_alert(test_alert)

