import argparse
import json
import time
from pathlib import Path
from typing import Any, Callable

from confluent_kafka import Producer


GAME_PERIOD_SECONDS = 20 * 60
STREAM_SECONDS = 3 * 60
SPEEDUP = (3 * GAME_PERIOD_SECONDS) / STREAM_SECONDS


def game_time_seconds(event: dict[str, Any]) -> int:
	"""Return the event's position in the three-period game clock."""
	minutes, seconds = event["time_in_period"].split(":")
	return (int(event["period"]) - 1) * GAME_PERIOD_SECONDS + int(minutes) * 60 + int(seconds)


def load_events(input_path: str | Path, game_id: int) -> list[dict[str, Any]]:
	"""Load JSONL events and ensure they belong to the requested game."""
	events = []
	with Path(input_path).open(encoding="utf-8") as input_file:
		for line_number, line in enumerate(input_file, 1):
			if not line.strip():
				continue
			event = json.loads(line)
			if event.get("game_id") != game_id:
				raise ValueError(f"Line {line_number} does not belong to game {game_id}.")
			events.append(event)
	return events


def stream_events(
	events: list[dict[str, Any]],
	topic: str,
	bootstrap_servers: str = "localhost:9092",
	producer: Producer | None = None,
	sleep: Callable[[float], None] = time.sleep,
) -> None:
	"""Publish events using game-time gaps compressed into three minutes."""
	kafka_producer = producer or Producer({"bootstrap.servers": bootstrap_servers})
	previous_time = None
	for event in events:
		current_time = game_time_seconds(event)
		if previous_time is not None:
			sleep(max(0, current_time - previous_time) / SPEEDUP)
		kafka_producer.produce(
			topic,
			key=str(event["game_id"]),
			value=json.dumps(event),
		)
		kafka_producer.poll(0)
		previous_time = current_time
	kafka_producer.flush()


def main() -> None:
	parser = argparse.ArgumentParser(description="Replay extracted NHL events to Kafka.")
	parser.add_argument("input_path", type=Path)
	parser.add_argument("game_id", type=int)
	parser.add_argument("topic")
	parser.add_argument("--bootstrap-servers", default="localhost:9092")
	args = parser.parse_args()

	events = load_events(args.input_path, args.game_id)
	stream_events(events, args.topic, args.bootstrap_servers)


if __name__ == "__main__":
	main()