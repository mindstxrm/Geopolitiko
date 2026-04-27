"""Flask-Login user and auth helpers."""
from flask import g
from flask_login import current_user, UserMixin
from werkzeug.security import check_password_hash

from app.models import get_user_by_id


def verify_password(user: dict, password: str) -> bool:
    """Return True if password matches user's password_hash."""
    if not user or not user.get("password_hash"):
        return False
    return check_password_hash(user["password_hash"], password)


def get_effective_user_id():
    """Return the current request's user id: from session (Flask-Login) or from API key (g.api_user)."""
    if current_user.is_authenticated:
        return current_user.id
    if getattr(g, "api_user", None):
        return g.api_user.get("id")
    return None


class User(UserMixin):
    """User for Flask-Login; wraps DB user row."""

    def __init__(self, user_id: int, username: str, email: str = None, is_active: bool = True, name: str = None, title: str = None, organization: str = None):
        self.id = user_id
        self.username = username
        self.email = email or ""
        self._is_active = bool(is_active)
        self.name = (name or "").strip() or None
        self.title = (title or "").strip() or None
        self.organization = (organization or "").strip() or None

    @property
    def is_active(self):
        return self._is_active

    @classmethod
    def from_id(cls, user_id: int):
        """Load user from DB by id. Returns None if not found or inactive."""
        if not user_id:
            return None
        row = get_user_by_id(user_id)
        if not row or not row.get("is_active", 1):
            return None
        return cls(
            user_id=row["id"],
            username=row["username"],
            email=row.get("email") or "",
            is_active=bool(row.get("is_active", 1)),
            name=row.get("name"),
            title=row.get("title"),
            organization=row.get("organization"),
        )

    def get_id(self):
        return str(self.id)
