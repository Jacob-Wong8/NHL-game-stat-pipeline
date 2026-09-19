import argparse
import json

from confluent_kafka import Consumer


def consume_events(
	topic: str,
	bootstrap_servers: str = "localhost:9092",
	group_id: str = "nhl-game-stat-consumer",
) -> None:
	"""Consume and print events from a user-created Kafka topic."""
	consumer = Consumer(
		{
			"bootstrap.servers": bootstrap_servers,
			"group.id": group_id,
			"auto.offset.reset": "earliest",
		}
	)
	consumer.subscribe([topic])
	try:
		while True:
			message = consumer.poll(1.0)
			if message is None:
				continue
			if message.error():
				raise RuntimeError(message.error())
			value = message.value()
			if value is not None:
				print(json.loads(value))
	finally:
		consumer.close()


def main() -> None:
	parser = argparse.ArgumentParser(description="Consume replayed NHL events from Kafka.")
	parser.add_argument("topic")
	parser.add_argument("--bootstrap-servers", default="localhost:9092")
	parser.add_argument("--group-id", default="nhl-game-stat-consumer")
	args = parser.parse_args()
	consume_events(args.topic, args.bootstrap_servers, args.group_id)


if __name__ == "__main__":
	main()