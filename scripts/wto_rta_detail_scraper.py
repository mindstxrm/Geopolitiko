#!/usr/bin/env python3
"""
Scrape WTO RTA detail pages to enrich treaties with:
- Agreement text links (PDFs, official pages)
- Annexes and related documents
- WTO consideration documents (docsonline.wto.org)
- RTA provisions
Source: https://rtais.wto.org/UI/PublicShowRTAIDCard.aspx?rtaid={id}
"""
import io
import json
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests
from bs4 import BeautifulSoup

from config import DATABASE_PATH
from app.models import init_db, get_treaty_by_wto_rta_id, update_treaty_wto_details

BASE_URL = "https://rtais.wto.org"
DETAIL_URL = f"{BASE_URL}/UI/PublicShowRTAIDCard.aspx"
EXPORT_URL = f"{BASE_URL}/UI/ExportAllRTAList.aspx"
REQUEST_DELAY = 1.5  # seconds between requests to avoid overloading


def _str(v):
    if v is None or (hasattr(v, "__float__") and hasattr(v, "__bool__") and v != v):  # NaN
        return ""
    return str(v).strip()


def fetch_rta_detail(rta_id: int, session: requests.Session | None = None) -> dict | None:
    """Fetch RTA detail page and extract metadata, links, provisions."""
    sess = session or requests.Session()
    sess.headers.update({
        "User-Agent": "GeopoliticalNews/1.0 (RTA research; +https://github.com/geopolitical-news)",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    })
    try:
        r = sess.get(f"{DETAIL_URL}?rtaid={rta_id}", timeout=30)
        r.raise_for_status()
    except Exception as e:
        return {"error": str(e), "rta_id": rta_id}

    soup = BeautifulSoup(r.text, "html.parser")

    # Agreement text links: PDF, official gov pages (exclude docsonline for now - those are WTO docs)
    agreement_links = []
    annex_links = []
    wto_docs = []
    party_links = []

    base_detail = f"{DETAIL_URL}?rtaid={rta_id}"
    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        text = (a.get_text() or "").strip()
        if not href or href.startswith("#") or "javascript:" in href:
            continue
        if href.startswith("../"):
            href = urljoin(f"{BASE_URL}/UI/", href)
        elif href.startswith("/"):
            href = f"{BASE_URL}{href}"
        if "rtais.wto.org" in href and "ExportAllRTAList" in href:
            continue
        if "docsonline.wto.org" in href or "docs.wto.org" in href:
            wto_docs.append({"url": href, "label": text or "WTO document"})
        elif href.lower().endswith(".pdf"):
            if "annex" in href.lower() or "annex" in text.lower():
                annex_links.append({"url": href, "label": text or "Annex"})
            else:
                agreement_links.append({"url": href, "label": text or "Agreement (PDF)"})
        elif any(x in href for x in ["/trade-agreement", "/fta", "/rta", "cecpa", "fta", "trade-agreements"]):
            agreement_links.append({"url": href, "label": text or "Agreement"})
        elif "mauritiustrade" in href or "commerce.gov" in href or "trade.gov" in href or "gov." in href:
            party_links.append({"url": href, "label": text or "Party portal"})

    # Deduplicate by URL
    def _dedupe(items, key="url"):
        seen = set()
        out = []
        for x in items:
            u = x.get(key, "")
            if u and u not in seen:
                seen.add(u)
                out.append(x)
        return out

    agreement_links = _dedupe(agreement_links)
    annex_links = _dedupe(annex_links)
    wto_docs = _dedupe(wto_docs)[:20]  # Cap WTO docs
    party_links = _dedupe(party_links)[:5]

    # If we found PDF but no agreement link, treat PDF as primary
    if not agreement_links and annex_links:
        agreement_links = annex_links[:1]
        annex_links = annex_links[1:]
    elif not agreement_links and any("pdf" in x["url"].lower() for x in party_links):
        for p in party_links:
            if "pdf" in p["url"].lower():
                agreement_links.append(p)
                break

    # Extract provisions (headings that indicate RTA content)
    provisions = []
    for h in soup.find_all(["h4", "h5", "strong"]):
        t = (h.get_text() or "").strip()
        if t and 10 < len(t) < 100 and t not in ("E", "F", "S"):
            # Filter out single-letter language codes and too-short
            if not re.match(r"^[EFS]$", t):
                provisions.append(t)
    provisions = list(dict.fromkeys(provisions))[:50]

    # Primary document URL: prefer PDF (agreement text), else first link
    primary_doc = ""
    for link in agreement_links + annex_links:
        if ".pdf" in link["url"].lower():
            primary_doc = link["url"]
            break
    if not primary_doc and agreement_links:
        primary_doc = agreement_links[0]["url"]

    detail_url = f"{DETAIL_URL}?rtaid={rta_id}"
    return {
        "rta_id": rta_id,
        "detail_url": detail_url,
        "agreement_links": agreement_links,
        "annex_links": annex_links,
        "wto_docs": wto_docs,
        "party_links": party_links,
        "provisions": provisions,
        "primary_document_url": primary_doc,
    }


def run_scrape(rta_ids: list[int] | None = None, limit: int | None = None, xlsx_path: str | Path | None = None) -> dict:
    """
    Scrape detail pages for RTAs. Get RTA IDs from:
    - rta_ids list if provided
    - Else download Export Excel and use all IDs
    - Else xlsx_path if provided
    """
    import pandas as pd

    init_db(DATABASE_PATH)

    if rta_ids is not None:
        ids = list(rta_ids)
    elif xlsx_path and Path(xlsx_path).exists():
        df = pd.read_excel(xlsx_path)
        col = df.get("RTA ID")
        if col is None:
            return {"error": "No RTA ID column in Excel", "updated": 0}
        ids = [int(x) for x in col.dropna().unique() if str(x).replace(".0", "").isdigit()]
    else:
        # Download Export
        try:
            r = requests.get(EXPORT_URL, timeout=60)
            r.raise_for_status()
            df = pd.read_excel(io.BytesIO(r.content))
            col = df.get("RTA ID")
            if col is None:
                return {"error": "No RTA ID in Export", "updated": 0}
            ids = [int(x) for x in col.dropna().unique() if str(x).replace(".0", "").isdigit()]
        except Exception as e:
            return {"error": str(e), "updated": 0}

    if limit:
        ids = ids[:limit]

    sess = requests.Session()
    updated = 0
    errors = []

    for i, rta_id in enumerate(ids):
        treaty = get_treaty_by_wto_rta_id(rta_id)
        if not treaty:
            continue  # Skip if not in DB
        detail = fetch_rta_detail(rta_id, sess)
        if "error" in detail and "rta_id" in detail:
            errors.append(f"RTA {rta_id}: {detail['error']}")
            time.sleep(REQUEST_DELAY)
            continue
        clauses = {
            "wto_rta_id": rta_id,
            "detail_url": detail.get("detail_url"),
            "agreement_links": detail.get("agreement_links", []),
            "annex_links": detail.get("annex_links", []),
            "wto_docs": detail.get("wto_docs", []),
            "party_links": detail.get("party_links", []),
            "provisions": detail.get("provisions", []),
        }
        doc_url = detail.get("primary_document_url") or treaty.get("document_url")
        update_treaty_wto_details(
            treaty["id"],
            document_url=doc_url or None,
            clauses_json=json.dumps(clauses, ensure_ascii=False)[:15000],
        )
        updated += 1
        if (i + 1) % 10 == 0:
            print(f"  Scraped {i + 1}/{len(ids)}...")
        time.sleep(REQUEST_DELAY)

    return {"updated": updated, "total": len(ids), "errors": errors[:20]}


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Scrape WTO RTA detail pages to enrich treaties")
    parser.add_argument("--limit", type=int, help="Limit number of RTAs to scrape")
    parser.add_argument("--xlsx", type=str, help="Path to AllRTAs.xlsx (uses RTA IDs)")
    parser.add_argument("--ids", type=str, help="Comma-separated RTA IDs (e.g. 517,33,159)")
    args = parser.parse_args()

    rta_ids = None
    if args.ids:
        rta_ids = [int(x.strip()) for x in args.ids.split(",") if x.strip().isdigit()]
    xlsx_path = args.xlsx or (Path(DATABASE_PATH).parent / "AllRTAs.xlsx")
    if not Path(xlsx_path).exists():
        xlsx_path = None

    result = run_scrape(rta_ids=rta_ids, limit=args.limit, xlsx_path=xlsx_path)
    if "error" in result:
        print(f"Error: {result['error']}")
        return 1
    print(f"Updated {result['updated']} treaties with detail data (scraped {result['total']} RTAs).")
    if result.get("errors"):
        print(f"Errors: {len(result['errors'])}")
        for e in result["errors"][:5]:
            print(f"  - {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
