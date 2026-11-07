"""Signed SSO bridge between Geopolitiko Terminal and Analyst Desk."""
from __future__ import annotations

import os
from typing import Any

from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from config import SECRET_KEY


def bridge_secret() -> str:
    return (
        (os.environ.get("ANALYST_DESK_BRIDGE_SECRET") or "").strip()
        or (os.environ.get("SECRET_KEY") or "").strip()
        or (SECRET_KEY or "")
        or "dev-bridge-secret"
    )


def _serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(bridge_secret(), salt="geopolitiko-desk-bridge-v1")


def mint_bridge_token(*, user_id: int | None, username: str, role: str = "analyst") -> str:
    payload = {
        "uid": user_id,
        "u": (username or "").strip() or "terminal-user",
        "role": role,
    }
    return _serializer().dumps(payload)


def verify_bridge_token(token: str, *, max_age_seconds: int = 300) -> dict[str, Any] | None:
    if not (token or "").strip():
        return None
    try:
        data = _serializer().loads(token, max_age=max_age_seconds)
    except (BadSignature, SignatureExpired):
        return None
    if not isinstance(data, dict):
        return None
    return data


def analyst_desk_public_url() -> str:
    return (os.environ.get("ANALYST_DESK_PUBLIC_URL") or "http://127.0.0.1:5005").rstrip("/")


def terminal_public_url() -> str:
    return (os.environ.get("TERMINAL_PUBLIC_URL") or "http://127.0.0.1:5003").rstrip("/")
