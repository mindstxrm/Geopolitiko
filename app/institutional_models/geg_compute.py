"""GEG (Geopolitiko Event Graph) - events, links, and link inference."""
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

RELATIONSHIP_TYPES = ["TRIGGERS", "ESCALATES", "RETALIATES", "SANCTIONS_FOR", "BREAKS_TALKS_WITH", "DE_ESCALATES"]


def _get_connection():
    from app.models import _connection
    return _connection


def ingest_events_from_sources(as_of: Optional[str] = None) -> int:
    """Create gpi_events from border_incidents, sanctions_registry, protest_tracking. Returns count."""
    as_of = as_of or datetime.utcnow().strftime("%Y-%m-%d")
    now = datetime.utcnow().isoformat() + "Z"
    _conn = _get_connection()
    count = 0
    with _conn() as conn:
        cur = conn.execute(
            """SELECT id, country_a_code, country_a_name, country_b_code, country_b_name, incident_date, summary, severity
               FROM border_incidents WHERE incident_date >= date(?, '-90 days')""",
            (as_of[:10],),
        )
        for row in cur:
            eid, ca, can, cb, cbn, dt, summary, sev = row
            actors = json.dumps([can or ca, cbn or cb])
            country_code = ca or cb
            cred = 0.8 if sev in ("high", "critical") else 0.6
            conn.execute(
                """INSERT INTO gpi_events (event_type, actors, targets, country_code, start_date, end_date, source_list, credibility_score, confidence_score, impact_channels, created_at)
                   VALUES ('border_incident', ?, ?, ?, ?, ?, 'border_incidents', ?, 0.7, ?, ?)""",
                (actors, actors, country_code, (dt or "")[:10], (dt or "")[:10], cred, json.dumps(["military"]), now),
            )
            count += 1
        cur = conn.execute(
            """SELECT id, imposing_country, target_country, measure_type, start_date, description
               FROM sanctions_registry WHERE start_date >= date(?, '-90 days')""",
            (as_of[:10],),
        )
        for row in cur:
            imp, tgt, mtype, dt, desc = row[1], row[2], row[3], (row[4] or "")[:10], row[5]
            conn.execute(
                """INSERT INTO gpi_events (event_type, actors, targets, country_code, start_date, end_date, source_list, credibility_score, confidence_score, impact_channels, created_at)
                   VALUES ('sanctions', ?, ?, ?, ?, ?, 'sanctions_registry', 0.85, 0.75, ?, ?)""",
                (json.dumps([imp]), json.dumps([tgt]), tgt, dt, dt, json.dumps(["sanctions"]), now),
            )
            count += 1
    return count


def infer_links(as_of: Optional[str] = None) -> int:
    """Infer event links: sanctions within 7d of military test -> SANCTIONS_FOR; rhetoric after incident -> ESCALATES."""
    as_of = as_of or datetime.utcnow().strftime("%Y-%m-%d")
    cutoff = (datetime.strptime(as_of[:10], "%Y-%m-%d") - timedelta(days=60)).strftime("%Y-%m-%d")
    now = datetime.utcnow().isoformat() + "Z"
    _conn = _get_connection()
    count = 0
    with _conn() as conn:
        cur = conn.execute(
            """SELECT event_id, event_type, country_code, start_date FROM gpi_events
               WHERE start_date >= ? ORDER BY start_date""",
            (cutoff,),
        )
        events = [dict(zip(("event_id", "event_type", "country_code", "start_date"), row)) for row in cur]
        for i, src in enumerate(events):
            for tgt in events[i + 1:]:
                sd = datetime.strptime((src["start_date"] or "2099-01-01")[:10], "%Y-%m-%d")
                td = datetime.strptime((tgt["start_date"] or "2099-01-01")[:10], "%Y-%m-%d")
                delta = (td - sd).days
                if delta < 0 or delta > 14:
                    continue
                rel = None
                conf = 0.5
                if src["event_type"] in ("border_incident", "military") and tgt["event_type"] == "sanctions" and delta <= 7:
                    rel = "SANCTIONS_FOR"
                    conf = 0.7
                elif src["event_type"] == "border_incident" and tgt["event_type"] == "border_incident" and delta <= 3:
                    rel = "ESCALATES"
                    conf = 0.6
                if rel:
                    conn.execute(
                        """INSERT OR IGNORE INTO gpi_event_links (source_event_id, target_event_id, relationship_type, confidence_score, inferred_at)
                           VALUES (?, ?, ?, ?, ?)""",
                        (src["event_id"], tgt["event_id"], rel, conf, now),
                    )
                    count += 1
    return count


def run_geg(as_of: Optional[str] = None) -> Tuple[int, int]:
    """Run full GEG pipeline. Returns (events_ingested, links_inferred)."""
    n1 = ingest_events_from_sources(as_of)
    n2 = infer_links(as_of)
    return n1, n2
