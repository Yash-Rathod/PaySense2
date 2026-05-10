import json
from unittest.mock import MagicMock, patch

from producer.kafka_producer import KafkaProducer


def test_kafka_producer_sends_message():
    with patch("producer.kafka_producer.ConfluentProducer") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance

        producer = KafkaProducer(bootstrap_servers="localhost:9092", topic="transactions")
        producer.send({"transaction_id": "abc", "is_fraud": False})

        mock_instance.produce.assert_called_once()
        args, kwargs = mock_instance.produce.call_args
        assert kwargs["topic"] == "transactions" or args[0] == "transactions"
        value = json.loads(kwargs.get("value", args[1] if len(args) > 1 else b"{}"))
        assert value["transaction_id"] == "abc"


def test_kafka_producer_uses_transaction_id_as_key():
    with patch("producer.kafka_producer.ConfluentProducer") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance

        producer = KafkaProducer(bootstrap_servers="localhost:9092", topic="transactions")
        producer.send({"transaction_id": "xyz-123", "is_fraud": True})

        _, kwargs = mock_instance.produce.call_args
        assert kwargs.get("key") == "xyz-123"


def test_kafka_producer_flush_on_close():
    with patch("producer.kafka_producer.ConfluentProducer") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance

        producer = KafkaProducer(bootstrap_servers="localhost:9092", topic="transactions")
        producer.close()

        mock_instance.flush.assert_called_once()
