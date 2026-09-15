"""Unit tests for Oceania Desk scope, event filter, and heuristic daily product."""
from __future__ import annotations

import unittest
from datetime import datetime, timezone

from app.analyst_desk.agents import AGENTS, get_agent
from app.analyst_desk.oceania_desk import (
    COFA_WATCH,
    DAILY_CUTOFF_HOUR_UTC,
    OCEANIA_AGENT_ID,
    OCEANIA_CORE,
    OCEANIA_DESK_IDS,
    OCEANIA_PLUS,
    TERRITORY_WATCH_TAGS,
    default_daily_window,
    empty_sidecar,
    filter_oceania_events,
    heuristic_daily_brief,
    oceania_scope_countries,
    oceania_watch_tags,
    sidecar_from_events,
)


class OceaniaDeskTests(unittest.TestCase):
    def test_workspace_agent_unions_oceania_desks(self):
        agent = get_agent(OCEANIA_AGENT_ID)
        self.assertIsNotNone(agent)
        self.assertEqual(agent["label"], "Oceania Desk")
        countries = set(agent["countries"])
        for desk_id in OCEANIA_DESK_IDS:
            for cc in AGENTS[desk_id]["countries"]:
                self.assertIn(cc, countries)
        self.assertTrue(set(OCEANIA_CORE).issubset(countries))
        for cc in COFA_WATCH:
            self.assertIn(cc, countries)
        for tag in TERRITORY_WATCH_TAGS:
            self.assertNotIn(tag, countries)

    def test_scope_helper_matches_agent(self):
        self.assertEqual(set(oceania_scope_countries()), set(AGENTS[OCEANIA_AGENT_ID]["countries"]))
        self.assertEqual(set(OCEANIA_PLUS), set(oceania_scope_countries()))
        self.assertEqual(set(oceania_watch_tags()), set(TERRITORY_WATCH_TAGS))

    def test_filter_keeps_oceania_institutional_and_drops_noise(self):
        events = [
            {"event_title": "Ferry sinks off Santo", "primary_country_code": "VUT", "importance_score": 80},
            {"event_title": "NATO statement", "primary_country_code": "USA", "importance_score": 90},
            {
                "event_title": "Pacific Islands Forum communiqué residual",
                "primary_country_code": "",
                "region": "Oceania",
                "importance_score": 55,
            },
            {"event_title": "AUKUS five-year explainer", "primary_country_code": "GBR", "importance_score": 40},
            {"event_title": "Unrelated", "primary_country_code": "DEU", "importance_score": 40},
        ]
        kept = filter_oceania_events(events)
        titles = [e["event_title"] for e in kept]
        self.assertEqual(
            titles,
            [
                "Ferry sinks off Santo",
                "Pacific Islands Forum communiqué residual",
                "AUKUS five-year explainer",
            ],
        )

    def test_daily_window_ends_at_0700_utc(self):
        now = datetime(2026, 9, 15, 9, 52, tzinfo=timezone.utc)
        start, end = default_daily_window(now)
        self.assertEqual(end.hour, DAILY_CUTOFF_HOUR_UTC)
        self.assertEqual(end.minute, 0)
        self.assertEqual((end - start).total_seconds(), 36 * 3600)
        self.assertEqual(end.date().isoformat(), "2026-09-15")
        self.assertEqual(start.isoformat(), "2026-09-13T19:00:00+00:00")

    def test_heuristic_brief_has_mandatory_sections(self):
        start, end = default_daily_window(datetime(2026, 9, 15, 9, 52, tzinfo=timezone.utc))
        body = heuristic_daily_brief(
            events=[
                {
                    "event_title": "Matui SAR",
                    "event_type": "disaster",
                    "primary_country_code": "VUT",
                    "region": "Oceania",
                    "importance_score": 80,
                    "confidence_score": 0.7,
                }
            ],
            articles=[{"title": "Search continues", "source_name": "RNZ", "url": "https://example.test"}],
            window_start=start,
            window_end=end,
        )
        for heading in (
            "# Oceania Daily Brief — 2026-09-15 (UTC)",
            "## 1. Priority developments (ranked)",
            "## 2. Regional institutions (PIF / MSG / partners)",
            "## 3. Country & territory notes (movers only)",
            "## 4. Agreements & negotiations tracker (deltas only)",
            "## 5. Climate, ocean & disaster watch",
            "## 6. Macro & development finance (if material)",
            "## 7. Risk board",
            "## 8. Open questions / collection needs",
            "## 9. Sources appendix",
            "pending_review",
        ):
            self.assertIn(heading, body)

    def test_sidecar_contract(self):
        start, end = default_daily_window(datetime(2026, 9, 15, 9, 52, tzinfo=timezone.utc))
        payload = sidecar_from_events(
            [{"event_title": "Panguna arson", "primary_country_code": "PNG", "importance_score": 75}],
            [],
            start,
            end,
        )
        self.assertEqual(payload["product"], "daily")
        self.assertEqual(payload["desk"], "oceania")
        self.assertTrue(payload["pending_review"])
        self.assertEqual(payload["status"], "pending_review")
        self.assertTrue(payload["pif_institutional"])
        self.assertEqual(payload["countries_touched"], ["PNG"])
        self.assertEqual(len(payload["priority_items"]), 1)
        empty = empty_sidecar(window_start=start, window_end=end)
        self.assertEqual(empty["priority_items"], [])


if __name__ == "__main__":
    unittest.main()
