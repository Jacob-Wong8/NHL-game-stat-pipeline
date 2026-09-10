import unittest
from unittest.mock import Mock, patch

from pipelines.actual.producer.consumer import consume_events


class TestConsumer(unittest.TestCase):
	@patch("pipelines.actual.producer.consumer.Consumer")
	@patch("builtins.print")
	def test_consumes_and_prints_json_events(self, print, consumer_class):
		consumer = consumer_class.return_value
		message = Mock()
		message.error.return_value = None
		message.value.return_value = '{"event_id": 7, "game_id": 99}'
		consumer.poll.side_effect = [message, KeyboardInterrupt]

		with self.assertRaises(KeyboardInterrupt):
			consume_events("plays", "kafka:9092", "test-consumer")

		consumer_class.assert_called_once_with(
			{
				"bootstrap.servers": "kafka:9092",
				"group.id": "test-consumer",
				"auto.offset.reset": "earliest",
			}
		)
		consumer.subscribe.assert_called_once_with(["plays"])
		print.assert_called_once_with({"event_id": 7, "game_id": 99})
		consumer.close.assert_called_once_with()

	@patch("pipelines.actual.producer.consumer.Consumer")
	@patch("builtins.print")
	def test_skips_empty_polls_and_messages_without_values(self, print, consumer_class):
		consumer = consumer_class.return_value
		message = Mock()
		message.error.return_value = None
		message.value.return_value = None
		consumer.poll.side_effect = [None, message, KeyboardInterrupt]

		with self.assertRaises(KeyboardInterrupt):
			consume_events("plays")

		self.assertEqual(consumer.poll.call_count, 3)
		print.assert_not_called()
		consumer.close.assert_called_once_with()

	@patch("pipelines.actual.producer.consumer.Consumer")
	def test_raises_for_kafka_message_errors_and_closes_consumer(self, consumer_class):
		consumer = consumer_class.return_value
		message = Mock()
		message.error.return_value = "broker unavailable"
		consumer.poll.return_value = message

		with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
			consume_events("plays")

		consumer.close.assert_called_once_with()


if __name__ == "__main__":
	unittest.main()
