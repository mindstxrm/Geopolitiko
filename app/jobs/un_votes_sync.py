"""Sync UN voting data from UNIGE UNGA-DM (MariaDB or CSV fallback)."""
import csv
import io
import re
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import requests

from config import (
    BASE_DIR,
    UNIGE_UN_VOTES_PATH,
    UNIGE_DB_HOST,
    UNIGE_DB_PORT,
    UNIGE_DB_NAME,
    UNIGE_DB_USER,
    UNIGE_DB_PASSWORD,
)
from app.models import init_db, add_un_votes_bulk
from config import DATABASE_PATH

# Google Drive fallback: UNIGE UNGA-DM RS-077 zip (Kilby/Villanova mirror)
UNIGE_CSV_ZIP_URL = "https://drive.google.com/uc?export=download&id=1KethHiAOseUT7b_QjbyfsdM8iAu1SeUU"

# Map UNIGE vote values to our canonical form (yes, no, abstain, absent)
VOTE_MAP = {
    "yes": "yes",
    "no": "no",
    "abstain": "abstain",
    "abstaining": "abstain",
    "absent": "absent",
    "non-member": "absent",
    "non voting": "absent",
    "not voting": "absent",
    "in favor": "yes",
    "against": "no",
}

def _parse_un_date(s: str) -> str:
    """Parse UNGA-DM date like '25nov1949' -> '1949-11-25'."""
    if not s or len(s) < 9:
        return (s or "")[:10] if s else "0000-00-00"
    s = str(s).strip().lower()
    months = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
              "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}
    m = re.match(r"(\d{1,2})([a-z]{3})(\d{4})", s)
    if m:
        day, mon, year = int(m.group(1)), months.get(m.group(2), 1), int(m.group(3))
        try:
            return datetime(year, mon, day).strftime("%Y-%m-%d")
        except ValueError:
            pass
    # Fallback: extract year if YYYY present
    y = re.search(r"(\d{4})", s)
    return f"{y.group(1)}-01-01" if y else (s[:10] if len(s) >= 10 else "0000-00-00")


def _un_name_to_iso3(name: str) -> Optional[str]:
    """Map UN member state name to ISO3. Returns None if unmappable."""
    from app.un_votes.country_map import normalize_country_to_iso3
    return normalize_country_to_iso3(name)


def _normalize_vote(raw: Optional[str]) -> str:
    """Normalize vote value for storage."""
    if not raw:
        return "absent"
    v = str(raw).strip().lower()
    return VOTE_MAP.get(v, v.replace(" ", "-"))


def sync_un_votes_from_csv(filepath_or_file, limit: Optional[int] = None) -> Tuple[int, Optional[str]]:
    """
    Import UN votes from a CSV file (UNIGE All_Votes_RS-077 or similar format).
    Accepts file path (str) or file-like object. Streams rows to avoid loading large files into memory.
    Supports: decision_id, member_state/current_seat_name, amended_vote/original_vote, meeting_date.
    Maps UN member state names to ISO3 for GPI analytics compatibility.
    Returns (count_inserted, error_message).
    """
    init_db(DATABASE_PATH)
    if hasattr(filepath_or_file, "read"):
        f = filepath_or_file
        close_f = False
    else:
        try:
            f = open(filepath_or_file, "r", encoding="utf-8", errors="replace")
        except Exception as e:
            return 0, str(e)
        close_f = True

    try:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        if not fieldnames:
            return 0, "CSV file is empty or has no headers."

        cols = {k.lower().strip(): k for k in fieldnames}
        decision_col = cols.get("decision_id") or cols.get("resolution_id") or cols.get("decisionid")
        member_col = cols.get("current_seat_name") or cols.get("member_state") or cols.get("country_code") or cols.get("memberstate") or cols.get("country")
        vote_col = cols.get("amended_vote") or cols.get("original_vote") or cols.get("vote")
        date_col = cols.get("meeting_date") or cols.get("vote_date") or cols.get("meetingdate") or cols.get("date")
        title_col = cols.get("draft_resolution_id") or cols.get("decision_topic")

        if not decision_col or not member_col or not vote_col:
            return 0, "CSV must have decision_id, member_state (or current_seat_name), and amended_vote (or original_vote) columns."

        count = 0
        batch = []
        batch_size = 500
        for row in reader:
            if limit and count >= limit:
                break
            decision_id = (row.get(decision_col) or "").strip()
            member_state = (row.get(member_col) or "").strip()
            vote_raw = row.get(vote_col) or ""
            raw_date = row.get(date_col) if date_col else ""
            vote_date = _parse_un_date(raw_date) if raw_date else "0000-00-00"
            resolution_title = (row.get(title_col) or "").strip() or None if title_col and row.get(title_col) else None

            if not decision_id or not member_state:
                continue

            country_code = _un_name_to_iso3(member_state)
            if not country_code or len(country_code) != 3:
                continue  # Only store ISO3; skip unmappable names

            batch.append((str(decision_id), resolution_title, country_code, _normalize_vote(vote_raw), vote_date))
            if len(batch) >= batch_size:
                add_un_votes_bulk(batch)
                count += len(batch)
                batch = []

        if batch:
            add_un_votes_bulk(batch)
            count += len(batch)

        return count, None
    except Exception as e:
        return 0, str(e)
    finally:
        if close_f:
            f.close()


def _local_csv_path() -> Optional[str]:
    """Return path to All_Votes CSV. Checks: 1) project UNGA-DM RS-077, 2) UNIGE_UN_VOTES_PATH."""
    # 1. Project-local: GeopoliticalNews/UNGA-DM RS-077/All_Votes_RS-077.csv
    project_csv = BASE_DIR / "UNGA-DM RS-077" / "All_Votes_RS-077.csv"
    if project_csv.exists():
        return str(project_csv)
    # 2. Env/config path (e.g. ~/Downloads/UNGA-DM RS-077)
    if UNIGE_UN_VOTES_PATH and UNIGE_UN_VOTES_PATH.strip():
        base = Path(UNIGE_UN_VOTES_PATH.strip()).expanduser()
        if base.exists():
            csv_file = base / "All_Votes_RS-077.csv"
            if csv_file.exists():
                return str(csv_file)
            if base.suffix.lower() == ".csv":
                return str(base)
    return None


def sync_un_votes_from_unige(limit: Optional[int] = None) -> Tuple[int, Optional[str]]:
    """
    Fetch votes from UNIGE UNGA-DM. Prefers local All_Votes_RS-077.csv if available,
    else MariaDB, else Google Drive CSV fallback.
    Returns (count_inserted, error_message).
    """
    init_db(DATABASE_PATH)

    # 1. Prefer local UNGA-DM RS-077 folder
    local_csv = _local_csv_path()
    if local_csv:
        return sync_un_votes_from_csv(local_csv, limit=limit)

    # 2. Try MariaDB
    try:
        import pymysql
    except ImportError:
        return 0, "PyMySQL not installed. Run: pip install PyMySQL"

    conn_args = {
        "host": UNIGE_DB_HOST,
        "port": UNIGE_DB_PORT,
        "database": UNIGE_DB_NAME,
        "charset": "utf8mb4",
        "cursorclass": pymysql.cursors.DictCursor,
    }
    # UNIGE may allow unauthenticated read. Try with credentials if set, else anonymous.
    if UNIGE_DB_USER:
        conn_args["user"] = UNIGE_DB_USER
        conn_args["password"] = UNIGE_DB_PASSWORD or ""
    else:
        conn_args["user"] = "anonymous"
        conn_args["password"] = ""

    try:
        conn = pymysql.connect(**conn_args)
    except Exception as e:
        err_msg = str(e)
        # Connection refused: MariaDB may be campus/VPN only. Try CSV fallback.
        if "Connection refused" in err_msg or "2003" in err_msg:
            return _sync_from_csv_fallback(limit)
        return 0, err_msg

    query = """
        SELECT decisions.decision_id, meeting_records.meeting_date,
               votes.member_state, votes.original_vote
        FROM votes
        JOIN decisions ON decisions.decision_id = votes.decision_id
        JOIN meeting_records ON decisions.meeting_record_id = meeting_records.meeting_record_id
        WHERE votes.member_state IS NOT NULL AND votes.member_state != ''
        ORDER BY meeting_records.meeting_date DESC, decisions.decision_id
    """
    if limit:
        query += f" LIMIT {int(limit)}"

    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
    except Exception as e:
        conn.close()
        return 0, str(e)

    count = 0
    batch = []
    for row in rows:
        decision_id = row.get("decision_id")
        meeting_date = row.get("meeting_date")
        member_state = row.get("member_state")
        original_vote = row.get("original_vote")

        if not decision_id or not member_state:
            continue

        country_code = _un_name_to_iso3(member_state.strip())
        if not country_code or len(country_code) != 3:
            continue

        resolution_id = str(decision_id)
        vote_date = str(meeting_date)[:10] if meeting_date else ""
        vote = _normalize_vote(original_vote)

        batch.append((resolution_id, None, country_code, vote, vote_date or "0000-00-00"))
        if len(batch) >= 500:
            add_un_votes_bulk(batch)
            count += len(batch)
            batch = []

    if batch:
        add_un_votes_bulk(batch)
        count += len(batch)

    conn.close()
    return count, None


def _sync_from_csv_fallback(limit: Optional[int] = None) -> Tuple[int, Optional[str]]:
    """Try to fetch UN votes from Google Drive CSV zip when MariaDB is unavailable."""
    try:
        r = requests.get(UNIGE_CSV_ZIP_URL, timeout=60, stream=True)
        r.raise_for_status()
        data = r.content
    except Exception as e:
        return 0, (
            "MariaDB connection refused (server may be campus/VPN only). "
            "CSV fallback also failed: %s. "
            "You can manually download from https://unvotes.unige.ch/ (CSV extract) "
            "or https://sites.google.com/view/christopher-kilby/datasets and use 'Import from CSV'."
        ) % str(e)

    # Google Drive may return HTML (virus scan page) for large files
    if data[:4] != b"PK\x03\x04" and (b"<!DOCTYPE" in data[:1000] or b"<html" in data[:1000].lower()):
        return 0, (
            "MariaDB connection refused (campus/VPN only). "
            "Automatic CSV download failed (Google Drive may require manual download). "
            "Download All_Votes from https://sites.google.com/view/christopher-kilby/datasets "
            "(UNGA-DM RS-077 zip) or https://unvotes.unige.ch/, then use 'Import from CSV' below."
        )
    try:
        with zipfile.ZipFile(io.BytesIO(data), "r") as zf:
            # Look for All_Votes*.csv
            csv_name = None
            for n in zf.namelist():
                if "all_votes" in n.lower() and n.lower().endswith(".csv"):
                    csv_name = n
                    break
            if not csv_name:
                return 0, "CSV fallback: zip does not contain All_Votes CSV."
            with zf.open(csv_name) as cf:
                # Handle encoding
                raw = cf.read().decode("utf-8", errors="replace")
                return sync_un_votes_from_csv(io.StringIO(raw), limit=limit)
    except Exception as e:
        return 0, (
            "MariaDB unavailable. CSV fallback failed: %s. "
            "Download CSV from https://unvotes.unige.ch/ and use 'Import from CSV'."
        ) % str(e)
