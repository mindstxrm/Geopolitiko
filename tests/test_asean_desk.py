"""Unit tests for ASEAN Desk scope, event filter, and heuristic daily product."""
from __future__ import annotations

import unittest
from datetime import datetime, timezone

from app.analyst_desk.agents import AGENTS, get_agent
from app.analyst_desk.asean_desk import (
    ASEAN_10,
    ASEAN_AGENT_ID,
    ASEAN_PLUS,
    SEA_DESK_IDS,
    asean_scope_countries,
    default_daily_window,
    empty_sidecar,
    filter_asean_events,
    heuristic_daily_brief,
    sidecar_from_events,
)


class AseanDeskTests(unittest.TestCase):
    def test_workspace_agent_unions_sea_desks(self):
        agent = get_agent(ASEAN_AGENT_ID)
        self.assertIsNotNone(agent)
        self.assertEqual(agent["label"], "ASEAN Desk")
        countries = set(agent["countries"])
        for desk_id in SEA_DESK_IDS:
            for cc in AGENTS[desk_id]["countries"]:
                self.assertIn(cc, countries)
        self.assertTrue(set(ASEAN_10).issubset(countries))
        self.assertIn("TLS", countries)

    def test_scope_helper_matches_agent(self):
        self.assertEqual(set(asean_scope_countries()), set(AGENTS[ASEAN_AGENT_ID]["countries"]))
        self.assertEqual(set(ASEAN_PLUS), set(asean_scope_countries()))

    def test_filter_keeps_asean_and_institutional(self):
        events = [
            {"event_title": "PSI spike", "primary_country_code": "SGP", "importance_score": 70},
            {"event_title": "NATO statement", "primary_country_code": "USA", "importance_score": 90},
            {
                "event_title": "ASEAN ministers meet in Manila",
                "primary_country_code": "",
                "region": "Southeast Asia",
                "importance_score": 55,
            },
            {"event_title": "Unrelated", "primary_country_code": "DEU", "importance_score": 40},
        ]
        kept = filter_asean_events(events)
        titles = [e["event_title"] for e in kept]
        self.assertEqual(titles, ["PSI spike", "ASEAN ministers meet in Manila"])

    def test_daily_window_ends_at_0800_utc(self):
        now = datetime(2026, 9, 15, 8, 53, tzinfo=timezone.utc)
        start, end = default_daily_window(now)
        self.assertEqual(end.hour, 8)
        self.assertEqual(end.minute, 0)
        self.assertEqual((end - start).total_seconds(), 36 * 3600)
        self.assertEqual(end.date().isoformat(), "2026-09-15")

    def test_heuristic_brief_has_mandatory_sections(self):
        start, end = default_daily_window(datetime(2026, 9, 15, 8, 53, tzinfo=timezone.utc))
        body = heuristic_daily_brief(
            events=[
                {
                    "event_title": "Haze",
                    "event_type": "disaster",
                    "primary_country_code": "MYS",
                    "region": "Southeast Asia",
                    "importance_score": 80,
                    "confidence_score": 0.7,
                }
            ],
            articles=[{"title": "Haze worsens", "source_name": "CNA", "url": "https://example.test"}],
            window_start=start,
            window_end=end,
        )
        for heading in (
            "# ASEAN Daily Brief — 2026-09-15 (UTC)",
            "## 1. Priority developments (ranked)",
            "## 2. ASEAN institutional / Chair track",
            "## 3. Country notes (only movers)",
            "## 4. Agreements & negotiations tracker (deltas only)",
            "## 5. Macro & markets (if material)",
            "## 6. Risk board",
            "## 7. Open questions / collection needs",
            "## 8. Sources appendix",
            "pending_review",
        ):
            self.assertIn(heading, body)

    def test_sidecar_contract(self):
        start, end = default_daily_window(datetime(2026, 9, 15, 8, 53, tzinfo=timezone.utc))
        payload = sidecar_from_events(
            [{"event_title": "FM swearing-in", "primary_country_code": "IDN", "importance_score": 75}],
            [],
            start,
            end,
        )
        self.assertEqual(payload["product"], "daily")
        self.assertEqual(payload["desk"], "asean")
        self.assertTrue(payload["pending_review"])
        self.assertEqual(payload["status"], "pending_review")
        self.assertEqual(payload["countries_touched"], ["IDN"])
        self.assertEqual(len(payload["priority_items"]), 1)
        empty = empty_sidecar(window_start=start, window_end=end)
        self.assertEqual(empty["priority_items"], [])


if __name__ == "__main__":
    unittest.main()
