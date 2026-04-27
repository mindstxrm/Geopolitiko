#!/usr/bin/env python3
"""
Import WTO-recognised Regional Trade Agreements (RTAs) from AllRTAs.xlsx into the treaties database.
Source: WTO RTA Database (https://www.wto.org/english/tratop_e/region_e/region_e.htm)
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from config import DATABASE_PATH
from app.models import init_db, add_treaty, delete_treaties_by_source_contains

WTO_SOURCE_URL = "https://www.wto.org/english/tratop_e/region_e/region_e.htm"


def _date_str(val) -> str:
    """Convert pandas Timestamp/NaT to YYYY-MM-DD string."""
    if val is None or (hasattr(val, "year") and pd.isna(val)):
        return ""
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-%d")[:10]
    s = str(val)
    return s[:10] if len(s) >= 10 else ""


def _parties(signatories) -> tuple[str, str]:
    """Split signatories into party_a and party_b. For plurilateral, party_b describes rest."""
    if signatories is None or (hasattr(signatories, "__float__") and pd.isna(signatories)) or str(signatories).strip() == "":
        return ("", "")
    parts = [p.strip() for p in str(signatories).split(";") if p.strip()]
    if not parts:
        return ("", "")
    if len(parts) == 1:
        return (parts[0], "")
    if len(parts) == 2:
        return (parts[0], parts[1])
    return (parts[0], f"Plurilateral ({len(parts)} parties)")


def run_import(xlsx_path: str | Path) -> int:
    """Import WTO RTAs from Excel file. Returns 0 on success, 1 on error."""
    xlsx_path = Path(xlsx_path)
    if not xlsx_path.exists():
        print(f"Error: Excel file not found: {xlsx_path}")
        print("Usage: python import_wto_rtas.py [path/to/AllRTAs.xlsx]")
        return 1

    init_db(DATABASE_PATH)

    # Remove previous WTO RTA imports so we can refresh
    deleted = delete_treaties_by_source_contains("wto.org")
    print(f"Removed {deleted} existing WTO-sourced treaties.")

    df = pd.read_excel(xlsx_path)
    added = 0
    skipped = 0

    for _, row in df.iterrows():
        name = (row.get("RTA Name") or "").strip()
        if not name:
            skipped += 1
            continue

        signatories = row.get("Current signatories") or row.get("Original signatories")
        party_a, party_b = _parties(signatories)
        if not party_a:
            party_a = name.split(" - ")[0].strip() if " - " in name else "Multiple"

        date_sig_g = row.get("Date of Signature (G)")
        date_sig_s = row.get("Date of Signature (S)")
        date_entry_g = row.get("Date of Entry into Force (G)")
        date_entry_s = row.get("Date of Entry into Force (S)")
        signed_date = (
            _date_str(date_sig_g)
            or _date_str(date_sig_s)
            or _date_str(date_entry_g)
            or _date_str(date_entry_s)
        )

        def _str(v):
            if v is None or (hasattr(v, "__float__") and pd.isna(v)):
                return ""
            return str(v).strip()

        rta_type = _str(row.get("Type"))
        coverage = _str(row.get("Coverage"))
        status = _str(row.get("Status"))
        rta_id = row.get("RTA ID")
        region = _str(row.get("Region"))
        composition = _str(row.get("RTA Composition"))

        summary_parts = []
        if rta_id is not None:
            summary_parts.append(f"WTO RTA ID: {rta_id}")
        if rta_type:
            summary_parts.append(f"Type: {rta_type}")
        if coverage:
            summary_parts.append(f"Coverage: {coverage}")
        if status:
            summary_parts.append(f"Status: {status}")
        if composition:
            summary_parts.append(f"Composition: {composition}")
        if region:
            summary_parts.append(f"Region: {region}")
        if signatories and party_b and "Plurilateral" in party_b:
            summary_parts.append(f"Parties: {str(signatories)[:500]}")
        summary = ". ".join(summary_parts)

        try:
            add_treaty(
                treaty_type="trade_agreement",
                name=name,
                party_a=party_a[:200],
                party_b=party_b[:200] if party_b else "",
                signed_date=signed_date,
                summary=summary[:2000],
                source_url=WTO_SOURCE_URL,
                wto_rta_id=int(rta_id) if rta_id is not None and not (hasattr(rta_id, "__float__") and pd.isna(rta_id)) else None,
            )
            added += 1
        except Exception as e:
            print(f"Skip {name[:50]}: {e}")
            skipped += 1

    print(f"Imported {added} WTO RTAs. Skipped {skipped}.")
    return 0


def main() -> int:
    xlsx_path = Path(__file__).resolve().parent.parent / "data" / "AllRTAs.xlsx"
    if len(sys.argv) > 1:
        xlsx_path = Path(sys.argv[1])
    return run_import(xlsx_path)


if __name__ == "__main__":
    sys.exit(main())
