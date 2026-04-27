#!/usr/bin/env python3
"""
Seed the agreements database with 100+ trade/bilateral agreements from the DESTA database (Design of Trade Agreements).
Source: https://www.designoftradeagreements.org/ (Dür, Baccini, Elsig).
"""
import csv
import io
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests
from config import DATABASE_PATH
from app.models import init_db, add_treaty, get_treaties

DESTA_CSV_URL = "https://www.designoftradeagreements.org/media/filer_public/2a/c7/2ac78d7d-9a73-471f-9693-e2b4a45d2b62/desta_list_of_treaties_02_02_dyads.csv"
MIN_AGREEMENTS = 100


def main():
    init_db(DATABASE_PATH)
    existing = {(t["name"], (t["signed_date"] or "")[:4]) for t in get_treaties(limit=5000)}
    print(f"Existing treaties in DB: {len(existing)}")

    try:
        r = requests.get(DESTA_CSV_URL, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"Failed to fetch DESTA CSV: {e}")
        return 1

    reader = csv.DictReader(io.StringIO(r.text))
    seen = set()
    added = 0
    for row in reader:
        name = (row.get("name") or "").strip()
        country1 = (row.get("country1") or "").strip()
        country2 = (row.get("country2") or "").strip()
        year = (row.get("year") or "").strip()[:4]
        if not name or not year or not country1:
            continue
        key = (name, year)
        if key in seen:
            continue
        seen.add(key)
        if (name, year) in existing:
            continue
        signed_date = f"{year}-01-01" if len(year) == 4 else ""
        party_b = country2 if country2 and country2 != country1 else ""
        try:
            add_treaty(
                treaty_type="trade_agreement",
                name=name,
                party_a=country1,
                party_b=party_b,
                signed_date=signed_date,
                source_url="https://www.designoftradeagreements.org/",
            )
            added += 1
            if added >= MIN_AGREEMENTS:
                break
        except Exception as e:
            print(f"Skip {name}: {e}")

    print(f"Added {added} agreements. Total unique in this run: {len(seen)}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
