"""Pytest fixtures for Geopolitiko P0 tests."""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture()
def tmp_env(tmp_path, monkeypatch):
    news_db = tmp_path / "news.db"
    desk_db = tmp_path / "analyst_desk.db"
    ops = tmp_path / "platform_ops_status.json"
    monkeypatch.setenv("DATABASE_PATH", str(news_db))
    monkeypatch.setenv("ANALYST_DESK_DATABASE_PATH", str(desk_db))
    monkeypatch.setenv("PLATFORM_OPS_STATUS_PATH", str(ops))
    monkeypatch.setenv("SECRET_KEY", "test-secret-key-not-for-prod")
    monkeypatch.setenv("ANALYST_DESK_SECRET_KEY", "test-desk-secret-key")
    monkeypatch.setenv("ANALYST_DESK_PUBLISH_ON_APPROVE", "1")
    monkeypatch.delenv("RENDER", raising=False)
    monkeypatch.delenv("GEOPOLITIKO_ENV", raising=False)
    monkeypatch.delenv("FLASK_ENV", raising=False)
    # Reload config-dependent modules cleanly for this process
    import config

    monkeypatch.setattr(config, "DATABASE_PATH", str(news_db), raising=False)
    monkeypatch.setattr(config, "ANALYST_DESK_DATABASE_PATH", str(desk_db), raising=False)
    monkeypatch.setattr(config, "SECRET_KEY", "test-secret-key-not-for-prod", raising=False)
    return {"news_db": news_db, "desk_db": desk_db, "ops": ops}


@pytest.fixture()
def app_client(tmp_env, monkeypatch):
    from app.models import init_db
    from app import create_app

    init_db(str(tmp_env["news_db"]))
    # Ensure create_app picks up patched paths
    monkeypatch.setenv("SECRET_KEY", "test-secret-key-not-for-prod")
    application = create_app()
    application.config["TESTING"] = True
    return application.test_client()
