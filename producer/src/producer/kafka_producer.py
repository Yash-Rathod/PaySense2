"""Kafka producer wrapper for transaction events."""

import json
from typing import Any

from confluent_kafka import Producer as ConfluentProducer


class KafkaProducer:
    """Small adapter around confluent-kafka's producer."""

    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self._topic = topic
        self._producer = ConfluentProducer(
            {
                "bootstrap.servers": bootstrap_servers,
                "linger.ms": 5,
                "compression.type": "snappy",
            }
        )

    def send(self, record: dict[str, Any]) -> None:
        self._producer.produce(
            topic=self._topic,
            key=record.get("transaction_id", ""),
            value=json.dumps(record).encode(),
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush()

    def close(self) -> None:
        self.flush()

    @staticmethod
    def _on_delivery(err, msg) -> None:
        if err:
            print(f"[kafka] delivery error: {err}")
