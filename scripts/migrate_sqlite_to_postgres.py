#!/usr/bin/env python3
"""
Migrate SQLite → Postgres using pgloader (recommended) or print a cutover checklist.

Usage:
  # Print recommended pgloader command (does not modify data):
  python scripts/migrate_sqlite_to_postgres.py --plan

  # After DATABASE_URL points at Postgres and schema exists:
  alembic upgrade head

Preferred one-shot with pgloader installed:
  pgloader data/news.db postgresql://geopolitiko:geopolitiko@localhost:5432/geopolitiko

Then set:
  DATABASE_URL=postgresql+psycopg://geopolitiko:geopolitiko@localhost:5432/geopolitiko

Note: the legacy Flask data layer in app/models.py still uses sqlite3 placeholders.
Full dual-dialect runtime is incremental — keep SQLite for local/dev until route-level
SQL is abstracted. This script + Alembic + Docker Postgres establish the durability path.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", action="store_true", help="Print cutover plan and exit")
    parser.add_argument("--sqlite", default=None, help="Path to news.db")
    parser.add_argument(
        "--pg-url",
        default=os.environ.get("DATABASE_URL")
        or "postgresql://geopolitiko:geopolitiko@localhost:5432/geopolitiko",
        help="Postgres URL (postgres:// or postgresql://)",
    )
    args = parser.parse_args()

    from config import DATABASE_PATH, load_app_dotenv

    load_app_dotenv()
    sqlite_path = args.sqlite or DATABASE_PATH
    pg = args.pg_url
    if pg.startswith("postgresql+psycopg://"):
        pg = "postgresql://" + pg[len("postgresql+psycopg://") :]

    print("=== Geopolitiko SQLite → Postgres cutover ===")
    print(f"SQLite source : {sqlite_path}")
    print(f"Postgres dest : {pg}")
    print()
    print("1. Bring up Postgres (docker compose up postgres -d)")
    print("2. Install pgloader (https://pgloader.readthedocs.io/)")
    print(f"3. Run:  pgloader {sqlite_path} {pg}")
    print("4. alembic upgrade head   # audit_log / role columns")
    print("5. Set DATABASE_URL and restart web/worker")
    print("6. Keep scripts/backup_sqlite.py until cutover is proven")
    print()
    print(
        "Runtime note: app/models.py remains SQLite-native for now; "
        "health/ops probe DATABASE_URL via SQLAlchemy (app/db.py)."
    )
    if args.plan:
        return 0

    if not Path(sqlite_path).is_file():
        print(f"ERROR: SQLite file missing: {sqlite_path}", file=sys.stderr)
        return 1

    # Soft probe of destination
    try:
        from app.db import _normalize_url, probe_database

        probe = probe_database(_normalize_url(pg if "://" in pg else f"postgresql://{pg}"))
        print("Postgres probe:", probe)
    except Exception as e:
        print("Postgres probe skipped:", e)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
