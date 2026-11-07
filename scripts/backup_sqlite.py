#!/usr/bin/env python3
"""Backup Geopolitiko SQLite databases (news + analyst desk) with retention."""
from __future__ import annotations

import argparse
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import ANALYST_DESK_DATABASE_PATH, DATABASE_PATH, load_app_dotenv  # noqa: E402
from app.platform_ops import default_backup_dir, record_backup  # noqa: E402


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def backup_one(src: Path, dest_dir: Path) -> Path | None:
    if not src.is_file():
        print(f"skip (missing): {src}")
        return None
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{src.stem}_{_stamp()}{src.suffix}"
    shutil.copy2(src, dest)
    # Also copy WAL/SHM if present for a more consistent offline copy
    for suffix in ("-wal", "-shm"):
        side = Path(str(src) + suffix)
        if side.is_file():
            shutil.copy2(side, Path(str(dest) + suffix))
    print(f"backed up {src} -> {dest} ({dest.stat().st_size} bytes)")
    record_backup(str(dest), dest.stat().st_size)
    return dest


def prune(dest_dir: Path, keep: int) -> None:
    if keep <= 0 or not dest_dir.is_dir():
        return
    files = sorted(dest_dir.glob("*.db"), key=lambda p: p.stat().st_mtime, reverse=True)
    for old in files[keep:]:
        old.unlink(missing_ok=True)
        for suffix in ("-wal", "-shm"):
            Path(str(old) + suffix).unlink(missing_ok=True)
        print(f"pruned {old}")


def main() -> int:
    load_app_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        type=Path,
        default=default_backup_dir(),
        help="Backup directory (default: data/backups)",
    )
    parser.add_argument("--keep", type=int, default=14, help="Keep N most recent .db backups")
    args = parser.parse_args()

    backup_one(Path(DATABASE_PATH), args.out)
    backup_one(Path(ANALYST_DESK_DATABASE_PATH), args.out)
    prune(args.out, args.keep)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
