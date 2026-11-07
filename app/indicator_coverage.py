"""Indicator roadmap coverage snapshot (P3) — what stress/energy data exists."""
from __future__ import annotations

import sqlite3
from typing import Any

from config import DATABASE_PATH


def _count(conn: sqlite3.Connection, sql: str) -> int:
    try:
        row = conn.execute(sql).fetchone()
        return int(row[0] if row else 0)
    except sqlite3.Error:
        return 0


def build_indicator_coverage() -> dict[str, Any]:
    """Counts for macroeconomic stress + energy exposure tables (roadmap §1–2)."""
    out: dict[str, Any] = {
        "macroeconomic_stress_rows": 0,
        "macroeconomic_stress_countries": 0,
        "energy_commodity_rows": 0,
        "energy_commodity_countries": 0,
        "sample_macro_countries": [],
        "ok": False,
        "error": None,
    }
    try:
        conn = sqlite3.connect(DATABASE_PATH, timeout=5.0)
        try:
            out["macroeconomic_stress_rows"] = _count(conn, "SELECT COUNT(*) FROM macroeconomic_stress")
            out["macroeconomic_stress_countries"] = _count(
                conn, "SELECT COUNT(DISTINCT country_code) FROM macroeconomic_stress"
            )
            out["energy_commodity_rows"] = _count(conn, "SELECT COUNT(*) FROM energy_commodity_exposure")
            out["energy_commodity_countries"] = _count(
                conn, "SELECT COUNT(DISTINCT country_code) FROM energy_commodity_exposure"
            )
            try:
                rows = conn.execute(
                    """
                    SELECT country_code, MAX(as_of_date) AS d
                    FROM macroeconomic_stress
                    GROUP BY country_code
                    ORDER BY d DESC
                    LIMIT 8
                    """
                ).fetchall()
                out["sample_macro_countries"] = [r[0] for r in rows]
            except sqlite3.Error:
                pass
            out["ok"] = True
        finally:
            conn.close()
    except sqlite3.Error as e:
        out["error"] = str(e)
    return out
