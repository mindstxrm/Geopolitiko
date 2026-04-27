"""Intelligence Messaging: encryption and high-level API for channels and messages."""
import base64
import hashlib
import re
from typing import Optional, List, Dict, Any

from cryptography.fernet import Fernet, InvalidToken

from app.models import (
    messaging_get_messages as _get_messages,
    messaging_get_thread_replies,
    messaging_get_pinned as _get_pinned,
    messaging_add_message as _add_message,
    messaging_get_member_role,
    messaging_get_channel_by_id,
    messaging_get_channels_for_user,
    MESSAGING_CHANNEL_TYPES,
)


def _fernet_key(secret_key: str) -> bytes:
    """Derive a valid Fernet key from Flask SECRET_KEY (32 bytes base64url)."""
    digest = hashlib.sha256(secret_key.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest)


def get_fernet(secret_key: str):
    return Fernet(_fernet_key(secret_key))


def encrypt_message(plain_text: str, secret_key: str) -> bytes:
    """Encrypt message content for at-rest storage."""
    f = get_fernet(secret_key)
    return f.encrypt(plain_text.encode("utf-8"))


def decrypt_message(encrypted_blob: bytes, secret_key: str) -> str:
    """Decrypt stored message content."""
    if not encrypted_blob:
        return ""
    try:
        f = get_fernet(secret_key)
        return f.decrypt(encrypted_blob).decode("utf-8")
    except (InvalidToken, Exception):
        return "[unable to decrypt]"


def _author_display(row: dict) -> str:
    """Build display string from name, title, organization; fallback to username."""
    name = (row.get("name") or "").strip() or None
    title = (row.get("title") or "").strip() or None
    organization = (row.get("organization") or "").strip() or None
    username = (row.get("username") or "").strip() or ""
    parts = []
    if name:
        parts.append(name)
    if title:
        parts.append(title)
    if organization:
        parts.append("at " + organization)
    if parts:
        return ", ".join(parts)
    return username


def format_user_display(row: dict) -> str:
    """Public helper: display string from name, title, organization; fallback to username. For members and message authors."""
    return _author_display(row)


def _row_to_message(row: dict, channel_id: int, secret_key: str) -> Dict[str, Any]:
    content = decrypt_message(row.get("content_encrypted") or b"", secret_key)
    role = messaging_get_member_role(channel_id, row["user_id"])
    return {
        "id": row["id"],
        "channel_id": row["channel_id"],
        "user_id": row["user_id"],
        "username": row.get("username") or "",
        "author_display": _author_display(row),
        "content": content,
        "created_at": row.get("created_at") or "",
        "edited_at": row.get("edited_at"),
        "parent_id": row.get("parent_id"),
        "pinned_at": row.get("pinned_at"),
        "attachment_type": row.get("attachment_type"),
        "attachment_id": row.get("attachment_id"),
        "attachment_extra": (row.get("attachment_extra") or "").strip() or None,
        "is_verified_analyst": role == "verified_analyst",
    }


def get_messages_for_channel(channel_id: int, secret_key: str, limit: int = 200, before_id: Optional[int] = None, after_id: Optional[int] = None, parent_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """Return messages with decrypted content and verified_analyst flag."""
    rows = _get_messages(channel_id, limit=limit, before_id=before_id, after_id=after_id, parent_id=parent_id)
    return [_row_to_message(row if isinstance(row, dict) else dict(row), channel_id, secret_key) for row in rows]


def get_thread_replies_for_channel(channel_id: int, parent_id: int, secret_key: str) -> List[Dict[str, Any]]:
    rows = messaging_get_thread_replies(channel_id, parent_id)
    return [_row_to_message(row if isinstance(row, dict) else dict(row), channel_id, secret_key) for row in rows]


def get_pinned_for_channel(channel_id: int, secret_key: str) -> Optional[Dict[str, Any]]:
    row = _get_pinned(channel_id)
    if not row:
        return None
    return _row_to_message(row if isinstance(row, dict) else dict(row), channel_id, secret_key)


def add_message_encrypted(channel_id: int, user_id: int, plain_content: str, secret_key: str, parent_id: Optional[int] = None, attachment_type: Optional[str] = None, attachment_id: Optional[int] = None, attachment_extra: Optional[str] = None) -> int:
    """Encrypt and store a message. Returns message id."""
    encrypted = encrypt_message(plain_content, secret_key)
    return _add_message(channel_id, user_id, encrypted, parent_id=parent_id, attachment_type=attachment_type, attachment_id=attachment_id, attachment_extra=attachment_extra)


def is_verified_analyst(channel_id: int, user_id: int) -> bool:
    return messaging_get_member_role(channel_id, user_id) == "verified_analyst"


def channel_type_label(ct: str) -> str:
    return {"country_desk": "Country desk", "thematic": "Thematic", "briefing": "Rapid briefing"}.get(ct, ct)


def fire_channel_webhook(channel_id: int, message_id: int, username: str, content_preview: str) -> None:
    """If channel has webhook_url, POST new message event."""
    ch = messaging_get_channel_by_id(channel_id)
    if not ch or not (ch.get("webhook_url") or "").strip():
        return
    try:
        import requests
        requests.post(
            ch["webhook_url"].strip(),
            json={"channel_id": channel_id, "message_id": message_id, "username": username, "preview": content_preview[:200]},
            timeout=5,
        )
    except Exception:
        pass


def render_message_links(text: str) -> str:
    """Turn URLs into clickable links. Call after escaping HTML."""
    if not text:
        return text
    # Match URLs
    url_pattern = re.compile(r"(https?://[^\s<>]+)", re.IGNORECASE)
    return url_pattern.sub(r'<a href="\1" target="_blank" rel="noopener">\1</a>', text)


def simple_markdown(text: str) -> str:
    """Very simple markdown: **bold**, `code`, and links."""
    if not text:
        return ""
    # Escape HTML first
    import html
    text = html.escape(text)
    # **bold**
    text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
    # `code`
    text = re.sub(r"`(.+?)`", r"<code>\1</code>", text)
    # URLs
    text = render_message_links(text)
    return text


def search_messages_global(user_id: int, secret_key: str, query: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Search across all channels the user is in. Returns list of {channel_id, channel_slug, channel_name, message_id, content, created_at, username}."""
    if not (query or query.strip()):
        return []
    q = query.strip().lower()
    channels = messaging_get_channels_for_user(user_id, include_muted=True)
    results = []
    for ch in channels[:20]:  # limit channels
        msgs = get_messages_for_channel(ch["id"], secret_key, limit=100)
        for m in msgs:
            if q in (m.get("content") or "").lower():
                results.append({
                    "channel_id": ch["id"],
                    "channel_slug": ch["slug"],
                    "channel_name": ch["name"],
                    "message_id": m["id"],
                    "content": m.get("content", ""),
                    "created_at": m.get("created_at"),
                    "username": m.get("username", ""),
                    "author_display": m.get("author_display", m.get("username", "")),
                })
                if len(results) >= limit:
                    return results
    return results
