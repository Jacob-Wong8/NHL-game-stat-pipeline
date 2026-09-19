"""Contract tests for the cross-package usage the repo relies on.

Two modules import from a sibling package via a relative import with a
``try/except ImportError`` fallback (fetch_game, extract_batch). These tests
pin down both forms actually working so a future refactor can't silently
break direct-script execution.
"""
import unittest

from pipelines.actual.ingestion.fetch_game import extract_game_roster
from pipelines.expected.batch.hockey_reference.fetch_expected_stats import (
    TEAM_ID_TO_ABBREV,
    build_expected_baseline_for_game,
    normalize_name,
)


class TestContracts(unittest.TestCase):
    def test_normalize_name_takes_algebraic_form(self):
        self.assertEqual(normalize_name("Jean-Guy Béliveau Jr."), "jeanguybeliveaujr")
        self.assertEqual(normalize_name(""), "")
        self.assertEqual(normalize_name("  O'Reilly  "), "oreilly")

    def test_team_abbreviations_carry_legacy_franchise_ids(self):
        # 34 is the Utah-size placeholder edge: what the map claims vs reality.
        dupes = {v for v in TEAM_ID_TO_ABBREV.values() if list(TEAM_ID_TO_ABBREV.values()).count(v) > 1}
        self.assertEqual(dupes, {"NJD"}, "anything besides the legacy NJD duplicate is unharmonized")

    def test_baseline_falls_back_for_unmatched_skater(self):
        roster = {
            "rosterSpots": [
                {"playerId": 1, "firstName": {"default": "Unknown"}, "lastName": {"default": "Skater"}, "teamId": 22, "positionCode": "C"},
            ]
        }

        baseline = build_expected_baseline_for_game(roster, season=2026, fetch_team_stats=lambda *a, **k: [])

        self.assertEqual(baseline["Unknown Skater"]["goals_per_game"], 0.0)
        self.assertEqual(baseline["Unknown Skater"]["games_played"], 0)

    def test_baseline_requires_a_fetcher(self):
        import pipelines.expected.batch.hockey_reference.fetch_expected_stats as mod

        with self.assertRaises(ValueError):
            build_expected_baseline_for_game({"rosterSpots": []}, season=2026, fetch_team_stats=None)
        self.assertIs(mod.build_expected_baseline_for_game, build_expected_baseline_for_game)

    def test_extract_roster_rejects_non_dict_and_skips_nameless(self):
        with self.assertRaises(ValueError):
            extract_game_roster("nope")
        empty = extract_game_roster({"rosterSpots": [{"playerId": 1}]})
        self.assertEqual(empty, [])


if __name__ == "__main__":
    unittest.main()
