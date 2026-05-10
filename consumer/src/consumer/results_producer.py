import json
import os
from confluent_kafka import Producer as ConfluentProducer


class ResultsProducer:
    def __init__(self) -> None:
        bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        topic = os.environ.get("KAFKA_TOPIC_RESULTS", "results")
        self._topic = topic
        self._producer = ConfluentProducer({"bootstrap.servers": bootstrap})

    def publish(self, result: dict) -> None:
        self._producer.produce(
            topic=self._topic,
            key=result.get("transaction_id", ""),
            value=json.dumps(result).encode(),
        )
        self._producer.poll(0)

    def close(self) -> None:
        self._producer.flush()
