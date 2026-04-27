"""Send transactional emails via SMTP when SMTP_* env vars are set."""
import html
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def _smtp_config():
    host = os.environ.get("SMTP_HOST")
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USER")
    password = os.environ.get("SMTP_PASS")
    if not host or not user or not password:
        return None
    return {"host": host, "port": port, "user": user, "password": password}


def send_email(to_email: str, subject: str, body_plain: str, body_html: str = None) -> bool:
    """Send one email. Returns True if sent, False if SMTP not configured or send failed."""
    cfg = _smtp_config()
    if not cfg or not (to_email or "").strip():
        return False
    to_email = to_email.strip()
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = cfg["user"]
    msg["To"] = to_email
    msg.attach(MIMEText(body_plain, "plain"))
    if body_html:
        msg.attach(MIMEText(body_html, "html"))
    try:
        with smtplib.SMTP(cfg["host"], cfg["port"]) as s:
            s.starttls()
            s.login(cfg["user"], cfg["password"])
            s.sendmail(cfg["user"], [to_email], msg.as_string())
        return True
    except Exception:
        return False


def _welcome_email_plain(display_name: str) -> str:
    """Plain-text body for the welcome email."""
    greeting = f"Hi {display_name}," if display_name else "Hi,"
    return f"""{greeting}

Welcome to Geopolitical Terminal. Your account is ready.

WHAT YOU CAN DO
• Situation Room — Your command center for real-time geopolitical news and risk.
• Alerts — Get notified when stories match your topics or regions.
• Saved briefings — Build and export custom briefings for your team.
• Intelligence messaging — Secure, invite-only channels for analysis and sharing.

NEXT STEP
Log in with your username and password at the same place you registered. Bookmark the site for quick access.

If you did not create this account, you can safely ignore this email.

—
Geopolitical Terminal
"""


def _welcome_email_html(display_name: str) -> str:
    """HTML body for the welcome email (inline styles for email clients)."""
    safe_name = html.escape(display_name, quote=True) if display_name else ""
    greeting = f"Hi {safe_name}," if safe_name else "Hi,"
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Welcome to Geopolitical Terminal</title></head>
<body style="margin:0; padding:0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; font-size: 15px; line-height: 1.5; color: #1a1a1a; background: #f5f5f5;">
  <div style="max-width: 560px; margin: 0 auto; padding: 24px;">
    <div style="background: #fff; border-radius: 8px; padding: 32px; box-shadow: 0 1px 3px rgba(0,0,0,0.08);">
      <h1 style="margin: 0 0 8px; font-size: 22px; font-weight: 600; color: #0c0e12;">Welcome to Geopolitical Terminal</h1>
      <p style="margin: 0 0 24px; color: #6b7280; font-size: 14px;">Your account is ready.</p>
      <p style="margin: 0 0 20px;">{greeting}</p>
      <p style="margin: 0 0 20px;">You now have access to:</p>
      <ul style="margin: 0 0 20px; padding-left: 20px;">
        <li style="margin-bottom: 8px;"><strong>Situation Room</strong> — Your command center for real-time geopolitical news and risk.</li>
        <li style="margin-bottom: 8px;"><strong>Alerts</strong> — Get notified when stories match your topics or regions.</li>
        <li style="margin-bottom: 8px;"><strong>Saved briefings</strong> — Build and export custom briefings for your team.</li>
        <li style="margin-bottom: 8px;"><strong>Intelligence messaging</strong> — Secure, invite-only channels for analysis and sharing.</li>
      </ul>
      <p style="margin: 0 0 20px;"><strong>Next step:</strong> Log in with your username and password where you registered. Bookmark the site for quick access.</p>
      <p style="margin: 0 0 24px; font-size: 13px; color: #6b7280;">If you did not create this account, you can safely ignore this email.</p>
      <p style="margin: 0; font-size: 13px; color: #6b7280;">— Geopolitical Terminal</p>
    </div>
  </div>
</body>
</html>
"""


def send_welcome_email(to_email: str, display_name: str) -> bool:
    """Send welcome email to a newly registered user. No-op if SMTP not configured."""
    subject = "Welcome to Geopolitical Terminal — Your account is ready"
    plain = _welcome_email_plain(display_name or "there")
    html = _welcome_email_html(display_name or "there")
    return send_email(to_email, subject, plain, html)
