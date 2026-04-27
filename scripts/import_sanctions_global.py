#!/usr/bin/env python3
"""
Import sanctions into sanctions_global from:
- Australian Consolidated List (Excel)
- OFAC SDN list (CSV/XML)
- EU consolidated sanctions (data.europa.eu / sanctionsmap)
- UN Security Council (reference from Fact Sheet)

Schema: sanctions_global
- id, jurisdiction, target_type (person/entity/state), name, country
- sanctions_type, effective_date, expiry_date, measures (JSON), source_link, last_updated
"""
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import DATABASE_PATH
from app.models import init_db, add_sanction_global, clear_sanctions_global_by_jurisdiction

OFAC_BASE = "https://ofac.treasury.gov"
OFAC_SDN_CSV = "https://www.treasury.gov/ofac/downloads/sdn.csv"
OFAC_SDN_ALT = "https://ofac.treasury.gov/sites/default/files/sdn.csv"
EU_SANCTIONS_BASE = "https://data.europa.eu"
UN_CONSOLIDATED = "https://www.un.org/securitycouncil/sanctions/information"


def _date_str(val) -> str:
    """Convert to YYYY-MM-DD string."""
    if val is None or (hasattr(val, "__float__") and str(val) == "nan"):
        return ""
    s = str(val).strip()
    if len(s) >= 10 and s[:4].isdigit():
        return s[:10]
    return ""


def _measures_json(*flags) -> str:
    """Build JSON array of measure strings from flags."""
    out = []
    for f in flags:
        if f in (True, "True", "true", "Yes", "YES", 1, "1") or (isinstance(f, str) and f.strip().lower() in ("true", "yes")):
            out.append(str(f) if isinstance(f, str) else "true")
    return json.dumps(out) if out else "[]"


def import_australian_excel(path: str | Path) -> int:
    """Import Australian_Sanctions_Consolidated_List.xlsx into sanctions_global."""
    import pandas as pd

    path = Path(path)
    if not path.exists():
        print(f"Australian file not found: {path}")
        return 0

    init_db(DATABASE_PATH)
    deleted = clear_sanctions_global_by_jurisdiction("Australia")
    print(f"Cleared {deleted} existing Australia sanctions.")

    df = pd.read_excel(path, sheet_name="Consolidated List")
    added = 0

    seen_refs = set()
    for _, row in df.iterrows():
        name = (row.get("Name of Individual or Entity") or "").strip()
        if not name or len(name) < 2:
            continue

        ref = str(row.get("Reference") or "").strip()
        name_type = str(row.get("Name Type") or "").strip()
        # Only import Primary Name to avoid duplicates (Original Script is same person)
        if name_type != "Primary Name":
            continue
        if ref in seen_refs:
            continue
        seen_refs.add(ref)

        typ = str(row.get("Type") or "entity").strip().lower()
        target_type = "person" if "individual" in typ or typ == "person" else "entity"

        country = str(row.get("Citizenship") or row.get("Address") or "").strip()
        if country and len(country) > 100:
            country = country[:100]

        sanctions_type = str(row.get("Instrument of Designation") or "").strip() or "Sanctions"
        effective_date = _date_str(row.get("Control Date"))
        listing_info = str(row.get("Listing Information") or "")

        measures = []
        if row.get("Targeted Financial Sanction") in (True, "True", "Yes", 1):
            measures.append("Targeted Financial Sanction")
        if row.get("Travel Ban") in (True, "True", "Yes", 1):
            measures.append("Travel Ban")
        if row.get("Arms Embargo") in (True, "True", "Yes", 1):
            measures.append("Arms Embargo")
        if row.get("Maritime Restriction") in (True, "True", "Yes", 1):
            measures.append("Maritime Restriction")
        measures_json = json.dumps(measures) if measures else "[]"

        source_link = "https://www.dfat.gov.au/international-relations/security/sanctions/consolidated-list"
        try:
            add_sanction_global(
                jurisdiction="Australia",
                target_type=target_type,
                name=name[:500],
                country=country[:100] if country else "",
                sanctions_type=sanctions_type[:200] or "Sanctions",
                effective_date=effective_date,
                expiry_date="",
                measures=measures_json,
                source_link=source_link,
            )
            added += 1
        except Exception as e:
            print(f"Skip {name[:40]}: {e}")

    print(f"Imported {added} Australian sanctions.")
    return added


def import_ofac_sdn() -> int:
    """Import OFAC SDN list. Tries CSV download."""
    import urllib.request

    init_db(DATABASE_PATH)
    deleted = clear_sanctions_global_by_jurisdiction("OFAC")
    print(f"Cleared {deleted} existing OFAC sanctions.")

    urls = [
        "https://www.treasury.gov/ofac/downloads/sdn.csv",
        "https://ofac.treasury.gov/sites/default/files/sdn.csv",
    ]

    content = None
    for url in urls:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Geopolitiko/1.0"})
            with urllib.request.urlopen(req, timeout=30) as r:
                content = r.read().decode("utf-8", errors="replace")
            break
        except Exception as e:
            print(f"OFAC {url}: {e}")
            continue

    if not content:
        print("OFAC: Could not download SDN list. Use manual CSV or OFAC website.")
        return 0

    import csv
    import io

    reader = csv.DictReader(io.StringIO(content))
    added = 0
    for row in reader:
        name = (row.get("SDN_Name") or row.get("name") or "").strip()
        if not name:
            continue

        typ = (row.get("SDN_Type") or row.get("type") or "individual").strip().lower()
        target_type = "person" if "individual" in typ or "entity" not in typ and "vessel" not in typ else "entity"
        if "vessel" in typ or "ship" in typ:
            target_type = "entity"

        program = (row.get("program") or row.get("Program") or "").strip()
        sanctions_type = program or "SDN"

        addrs = row.get("addresses") or row.get("Address") or ""
        country = ""
        if addrs:
            for part in str(addrs).split(";"):
                if any(c.isalpha() for c in part) and len(part) < 80:
                    country = part.strip()[:100]
                    break

        effective_date = _date_str(row.get("List_Date") or row.get("list_date") or "")

        try:
            add_sanction_global(
                jurisdiction="OFAC",
                target_type=target_type,
                name=name[:500],
                country=country,
                sanctions_type=sanctions_type[:200],
                effective_date=effective_date,
                expiry_date="",
                measures=json.dumps(["asset_blocking", program] if program else ["asset_blocking"]),
                source_link=OFAC_BASE + "/specially-designated-nationals-and-blocked-persons-list-sdn-human-readable-lists",
            )
            added += 1
        except Exception as e:
            print(f"Skip OFAC {name[:30]}: {e}")

    print(f"Imported {added} OFAC sanctions.")
    return added


def import_eu_sanctions() -> int:
    """Import EU consolidated sanctions. Tries data.europa.eu dataset or fallback."""
    import urllib.request

    init_db(DATABASE_PATH)
    deleted = clear_sanctions_global_by_jurisdiction("EU")
    print(f"Cleared {deleted} existing EU sanctions.")

    urls = [
        "https://webgate.ec.europa.eu/fsd/fsf/public/files/xmlFullSanctionsList_1_1/content?token=dG9rZW4tMjAxNw",
        "https://data.europa.eu/api/hub/search/datasets?query=sanctions&limit=5",
    ]

    for url in urls:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Geopolitiko/1.0"})
            with urllib.request.urlopen(req, timeout=20) as r:
                ct = r.headers.get("Content-Type", "")
                data = r.read()
            if "xml" in ct.lower() or b"<" in data[:100]:
                added = _parse_eu_xml(data, url)
                if added > 0:
                    print(f"Imported {added} EU sanctions from XML.")
                    return added
        except Exception as e:
            print(f"EU {url[:50]}: {e}")

    print("EU: No downloadable list found. Add EU XML/CSV manually or use sanctionsmap.eu.")
    return 0


def _parse_eu_xml(data: bytes, source_url: str) -> int:
    """Parse EU sanctions XML if available."""
    try:
        import xml.etree.ElementTree as ET

        root = ET.fromstring(data)
        added = 0
        ns = {"": ""}
        for ent in root.findall(".//{*}entity") or root.findall(".//entity") or root.findall(".//{*}Entity"):
            name_el = ent.find(".//{*}firstName") or ent.find(".//{*}lastName") or ent.find(".//{*}name") or ent.find(".//{*}entity")
            name = (name_el.text if name_el is not None else "").strip() or ""
            if not name and name_el is not None:
                for c in name_el:
                    if c.text:
                        name += c.text
            if not name:
                continue
            country_el = ent.find(".//{*}country") or ent.find(".//{*}nationality")
            country = (country_el.text if country_el is not None else "").strip()[:100] if country_el is not None else ""
            add_sanction_global(
                jurisdiction="EU",
                target_type="person" if "firstName" in str(ent) or "lastName" in str(ent) else "entity",
                name=name[:500],
                country=country,
                sanctions_type="EU Consolidated List",
                effective_date="",
                expiry_date="",
                measures=json.dumps(["asset_freeze", "travel_ban"]),
                source_link=source_url or EU_SANCTIONS_BASE,
            )
            added += 1
        return added
    except Exception as e:
        print(f"EU XML parse: {e}")
    return 0


UN_SANCTIONS_REGIMES = [
    ("1267", "ISIL (Da'esh), Al-Qaida"),
    ("1518", "Iraq"),
    ("1533", "DRC"),
    ("1591", "Sudan (Darfur)"),
    ("1636", "Lebanon"),
    ("1718", "DPRK"),
    ("1737", "Iran"),
    ("1970", "Libya"),
    ("1988", "Taliban"),
    ("2048", "Guinea-Bissau"),
    ("2140", "Yemen"),
    ("2206", "South Sudan"),
    ("2653", "Haiti"),
    ("2713", "Al-Shabaab"),
    ("2745", "2745 Committee"),
]


def import_un_reference(pdf_path: str | Path | None = None) -> int:
    """Add UN Security Council sanctions regime references (built-in list or from Fact Sheet PDF)."""
    init_db(DATABASE_PATH)
    deleted = clear_sanctions_global_by_jurisdiction("UN")
    print(f"Cleared {deleted} existing UN reference sanctions.")

    regimes = list(UN_SANCTIONS_REGIMES)
    if pdf_path and Path(pdf_path).exists():
        try:
            import fitz

            doc = fitz.open(pdf_path)
            text = "".join(p.get_text() for p in doc)
            doc.close()
            found = re.findall(r"(\d{4})\s*\([^)]+\)", text)
            regimes = [(r, "") for r in sorted(set(f for f in found if f.isdigit() and 1000 <= int(f) <= 9999))[:30]]
        except ImportError:
            try:
                from PyPDF2 import PdfReader

                reader = PdfReader(pdf_path)
                text = "".join((p.extract_text() or "") for p in reader.pages)
                found = re.findall(r"(\d{4})\s*\([^)]+\)", text)
                regimes = [(r, "") for r in sorted(set(f for f in found if f.isdigit() and 1000 <= int(f) <= 9999))[:30]]
            except Exception as e:
                print(f"UN PDF parse: {e}, using built-in list")
        except Exception as e:
            print(f"UN PDF: {e}, using built-in list")

    added = 0
    for reg_item in regimes:
        reg = reg_item[0] if isinstance(reg_item, (tuple, list)) else reg_item
        label = reg_item[1] if isinstance(reg_item, (tuple, list)) and len(reg_item) > 1 else ""
        name = f"UNSC Sanctions Regime {reg}" + (f" ({label})" if label else "")
        try:
            add_sanction_global(
                jurisdiction="UN",
                target_type="state",
                name=name,
                country="",
                sanctions_type=f"Resolution {reg}",
                effective_date="",
                expiry_date="",
                measures=json.dumps(["arms_embargo", "asset_freeze", "travel_ban"]),
                source_link=UN_CONSOLIDATED,
            )
            added += 1
        except Exception as e:
            print(f"Skip UN {reg}: {e}")

    print(f"Imported {added} UN sanctions committee references.")
    return added


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Import sanctions into sanctions_global")
    ap.add_argument("--australian", default="/Users/jasminetan/Downloads/Australian_Sanctions_Consolidated_List.xlsx", help="Australian Excel path")
    ap.add_argument("--un-fact-sheet", default="/Users/jasminetan/Downloads/2025 Fact Sheet.pdf", help="UN Fact Sheet PDF path")
    ap.add_argument("--un-only", action="store_true", help="Import UN reference list only (built-in, no PDF)")
    ap.add_argument("--ofac", action="store_true", help="Import OFAC SDN (download)")
    ap.add_argument("--eu", action="store_true", help="Import EU consolidated (download)")
    ap.add_argument("--all", action="store_true", help="Run all imports")
    args = ap.parse_args()

    total = 0
    if args.un_only:
        total += import_un_reference(None)  # Built-in list only
    elif args.all or not (args.ofac or args.eu):
        if Path(args.australian).exists():
            total += import_australian_excel(args.australian)
        total += import_un_reference(args.un_fact_sheet if Path(args.un_fact_sheet).exists() else None)
    if args.ofac or args.all:
        total += import_ofac_sdn()
    if args.eu or args.all:
        total += import_eu_sanctions()

    print(f"Total imported: {total}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
