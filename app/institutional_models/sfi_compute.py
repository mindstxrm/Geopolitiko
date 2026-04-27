"""SFI (Sanctions Friction Index) computation.
Aggregates severity × enforceability × centrality by target country.
"""
from datetime import datetime
from typing import Dict, Optional

SEVERITY_BY_MEASURE = {
    "comprehensive": 5.0,
    "asset_freeze": 4.0,
    "financial": 3.5,
    "sectoral": 3.0,
    "trade": 2.5,
    "travel": 1.5,
    "": 2.0,
}
ENFORCEABILITY_BY_SOURCE = {"OFAC": 0.9, "EU": 0.85, "UK": 0.8, "UN": 0.75}


def _get_connection():
    from app.models import _connection
    return _connection


def _country_name_to_code(name: str) -> str:
    """Simple map for common targets. Extend as needed."""
    m = {
        "Russia": "RUS", "Ukraine": "UKR", "China": "CHN", "Iran": "IRN",
        "North Korea": "PRK", "Syria": "SYR", "Belarus": "BLR", "Cuba": "CUB",
        "Venezuela": "VEN", "Myanmar": "MMR", "Afghanistan": "AFG",
    }
    return m.get(name, name[:3].upper() if len(name) >= 3 else name)


def compute_sfi(as_of: Optional[str] = None) -> int:
    """Compute SFI for all sanctioned countries. Returns count of rows written."""
    as_of = as_of or datetime.utcnow().strftime("%Y-%m-%d")
    now = datetime.utcnow().isoformat() + "Z"
    _conn = _get_connection()
    count = 0
    with _conn() as conn:
        cur = conn.execute(
            """SELECT id, target_country, measure_type, source,
                      COALESCE(severity_score, 0), COALESCE(enforceability_score, 0), COALESCE(economic_centrality_score, 0)
               FROM sanctions_registry
               WHERE (end_date IS NULL OR end_date = '' OR end_date >= ?)""",
            (as_of[:10],),
        )
        by_country: Dict[str, list] = {}
        for row in cur:
            sid, target, mtype, source, sev, enf, cen = row
            if not target:
                continue
            code = _country_name_to_code(target.strip())
            severity = sev if sev and sev > 0 else SEVERITY_BY_MEASURE.get((mtype or "").lower(), 2.0)
            enforceability = enf if enf and enf > 0 else ENFORCEABILITY_BY_SOURCE.get((source or "").strip(), 0.6)
            centrality = cen if cen and cen > 0 else 0.5
            impact = severity * enforceability * centrality
            by_country.setdefault(code, []).append(impact)
        for code, impacts in by_country.items():
            sfi_score = sum(impacts)
            conn.execute(
                """INSERT OR REPLACE INTO gpi_sfi_daily
                   (as_of_date, country_code, sfi_score, active_sanction_count, methodology_version,
                    input_data_timestamp, confidence_score, last_updated, model_notes)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (as_of[:10], code, sfi_score, len(impacts), "1.0", now, 0.8, now, "severity×enforceability×centrality"),
            )
            count += 1
    return count
