import json
import tempfile
import unittest
from pathlib import Path

from pipelines.actual.ingestion.extract_plays import (
	extract_plays,
	load_plays,
	save_extracted_plays,
)

MOCK_DATA_DIR = Path(__file__).parent / "mock_data"


class TestExtractPlays(unittest.TestCase):
	def load_fixture(self, filename):
		with (MOCK_DATA_DIR / filename).open(encoding="utf-8") as input_file:
			return json.load(input_file)

	def test_extract_plays_flattens_play_fields(self):
		data = self.load_fixture("play_by_play_2025030212.json")

		self.assertEqual(
			extract_plays(data),
			[
				{
					"game_id": 2025030212,
					"event_id": 42,
					"period": 2,
					"period_type": "REG",
					"time_in_period": "05:12",
					"time_remaining": "14:48",
					"event_type": "goal",
					"details": {"scoringPlayerId": 8478402},
				},
				{
					"game_id": 2025030212,
					"event_id": 43,
					"period": 2,
					"period_type": "REG",
					"time_in_period": "07:30",
					"time_remaining": "12:30",
					"event_type": "shot-on-goal",
					"details": {"shootingPlayerId": 8478402},
				}
			],
		)

	def test_extract_plays_uses_explicit_game_id(self):
		data = self.load_fixture("play_by_play_2025030212.json")

		rows = extract_plays(data, game_id=2025030213)

		self.assertEqual(len(rows), 2)
		self.assertTrue(all(row["game_id"] == 2025030213 for row in rows))

	def test_extract_plays_rejects_invalid_data(self):
		not_an_object = self.load_fixture("not_an_object.json")
		missing_plays = self.load_fixture("missing_plays.json")

		with self.assertRaisesRegex(ValueError, "JSON object"):
			extract_plays(not_an_object)

		with self.assertRaisesRegex(ValueError, "plays list"):
			extract_plays(missing_plays)

	def test_load_plays_reads_game_id_from_filename(self):
		input_path = MOCK_DATA_DIR / "play_by_play_2025030212.json"
		rows = load_plays(input_path)

		self.assertEqual(rows[0]["game_id"], 2025030212)
		self.assertEqual(rows[0]["event_id"], 42)

	def test_save_extracted_plays_writes_json_lines(self):
		input_path = MOCK_DATA_DIR / "play_by_play_2025030212.json"

		with tempfile.TemporaryDirectory() as temp_dir:
			output_path = Path(temp_dir) / "nested" / "plays.jsonl"

			result_path = save_extracted_plays(input_path, output_path)

			self.assertEqual(result_path, output_path)
			rows = [json.loads(line) for line in output_path.read_text().splitlines()]
			self.assertEqual([row["event_id"] for row in rows], [42, 43])


if __name__ == "__main__":
	unittest.main()
