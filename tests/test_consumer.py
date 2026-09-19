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
	def test_fatal_kafka_error_still_raises_and_closes_consumer(self, consumer_class):
		consumer = consumer_class.return_value
		message = Mock()
		error = Mock()
		error.fatal.return_value = True
		message.error.return_value = error
		consumer.poll.return_value = message

		with self.assertRaises(RuntimeError):
			consume_events("plays")

		consumer.close.assert_called_once_with()

	@patch("pipelines.actual.producer.consumer.Consumer")
	@patch("builtins.print")
	def test_non_fatal_kafka_error_logged_and_recoverable(self, print, consumer_class):
		consumer = consumer_class.return_value
		warning = Mock()
		warning.fatal.return_value = False
		good = Mock()
		good.error.return_value = None
		good.value.return_value = '{"event_id": 8, "game_id": 99}'
		warning.error.return_value = warning
		warning.value.return_value = None
		consumer.poll.side_effect = [warning, good, KeyboardInterrupt]

		with self.assertRaises(KeyboardInterrupt):
			consume_events("plays")

		print.assert_any_call("kafka warning: " + str(warning))
		print.assert_called_with({"event_id": 8, "game_id": 99})
		consumer.close.assert_called_once_with()


if __name__ == "__main__":
	unittest.main()
