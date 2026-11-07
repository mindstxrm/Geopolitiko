"""Production secret hardening."""
import pytest

from config import assert_production_secrets


def test_assert_production_secrets_passes_in_dev(monkeypatch):
    monkeypatch.delenv("RENDER", raising=False)
    monkeypatch.delenv("GEOPOLITIKO_ENV", raising=False)
    monkeypatch.delenv("FLASK_ENV", raising=False)
    # Non-production: should not raise even with defaults when strict=False path
    assert_production_secrets(strict=False)


def test_assert_production_secrets_fails_on_defaults(monkeypatch):
    monkeypatch.setenv("GEOPOLITIKO_ENV", "production")
    monkeypatch.setenv("GEOPOLITIKO_STRICT_SECRETS", "1")
    monkeypatch.setenv("GEOPOLITIKO_REQUIRE_DESK_PASSWORD", "1")
    monkeypatch.delenv("SECRET_KEY", raising=False)
    monkeypatch.delenv("ANALYST_DESK_SECRET_KEY", raising=False)
    monkeypatch.delenv("ANALYST_DESK_ADMIN_PASSWORD", raising=False)
    with pytest.raises(RuntimeError, match="SECRET_KEY"):
        assert_production_secrets(strict=True)


def test_assert_production_secrets_ok_when_set(monkeypatch):
    monkeypatch.setenv("GEOPOLITIKO_ENV", "production")
    monkeypatch.setenv("SECRET_KEY", "a-real-production-secret")
    monkeypatch.setenv("ANALYST_DESK_SECRET_KEY", "a-real-desk-secret")
    monkeypatch.setenv("ANALYST_DESK_ADMIN_PASSWORD", "desk-pass")
    assert_production_secrets(strict=True)
