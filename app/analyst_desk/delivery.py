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
    brief_webhook = (
        (os.environ.get("ANALYST_DESK_BRIEF_WEBHOOK_URL") or "").strip() or webhook
    )
    recipients = _email_recipients()
    return {
        "webhook_configured": bool(webhook),
        "webhook_preview": webhook[:70] + ("..." if webhook and len(webhook) > 70 else ""),
        "brief_webhook_configured": bool(brief_webhook),
        "email_configured": bool(recipients),
        "email_recipients": recipients,
        "deliver_on_approve": (os.environ.get("ANALYST_DESK_DELIVER_ON_APPROVE") or "1")
        .strip()
        .lower()
        in ("1", "true", "yes", "on"),
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


def _brief_webhook_url() -> str:
    """Prefer dedicated brief webhook; fall back to alert webhook."""
    return (
        (os.environ.get("ANALYST_DESK_BRIEF_WEBHOOK_URL") or "").strip()
        or _webhook_url()
    )


def deliver_approved_brief(proposal: dict[str, Any]) -> tuple[bool, str, str]:
    """
    Deliver a human-approved proposal (Slack-friendly JSON + email fallback).
    Controlled by ANALYST_DESK_DELIVER_ON_APPROVE (default on when a channel is configured).
    """
    flag = (os.environ.get("ANALYST_DESK_DELIVER_ON_APPROVE") or "1").strip().lower()
    if flag not in ("1", "true", "yes", "on"):
        return False, "none", "deliver_on_approve_disabled"

    title = (proposal.get("title") or f"Proposal #{proposal.get('id')}").strip()
    agent = (proposal.get("agent_id") or "").strip()
    body = (proposal.get("reviewed_body") or proposal.get("body") or "").strip()
    excerpt = body[:3500] + ("…" if len(body) > 3500 else "")
    countries = proposal.get("countries") or []
    if not isinstance(countries, list):
        countries = []
    focus = (proposal.get("focus_country") or "").strip().upper()
    codes = []
    if focus:
        codes.append(focus)
    for c in countries:
        cc = str(c).strip().upper()
        if cc and cc not in codes:
            codes.append(cc)
    from app.analyst_desk.auth_bridge import terminal_public_url

    term = terminal_public_url()
    links = [f"{term}/country/{cc}#analyst-desk-terminal" for cc in codes[:5]]
    text = (
        f"*Approved Desk brief*\n"
        f"*{title}*\n"
        f"Agent: `{agent}` · id={proposal.get('id')}\n"
        f"Countries: {', '.join(codes) or '—'}\n"
        + ("Terminal: " + " | ".join(links) + "\n" if links else "")
        + f"\n{excerpt}"
    )
    payload = {
        "type": "approved_brief",
        "text": text,  # Slack incoming webhooks
        "proposal_id": proposal.get("id"),
        "agent_id": agent,
        "title": title,
        "countries": codes,
        "terminal_links": links,
        "body_excerpt": excerpt,
    }

    url = _brief_webhook_url()
    if url:
        try:
            r = requests.post(url, json=payload, timeout=15)
            if 200 <= r.status_code < 300:
                return True, "webhook", "ok"
            webhook_detail = f"webhook_http_{r.status_code}"
        except Exception as e:
            webhook_detail = f"webhook_error:{e}"
    else:
        webhook_detail = "webhook_not_configured"

    ok2, detail2 = _send_email_alert(
        subject=f"[Approved] {title}",
        body=text.replace("*", ""),
    )
    if ok2:
        return True, "email", detail2
    return False, "none", f"{webhook_detail}; {detail2}"

