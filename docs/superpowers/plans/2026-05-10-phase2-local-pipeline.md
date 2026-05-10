# Phase 2 — Local Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the complete local stack in Docker Compose (Episodes 5–8): Kafka in KRaft mode, producer streaming to Kafka, consumer classifying fraud and writing to DynamoDB Local, FastAPI query API. End state: `docker compose up` → full working fraud detection pipeline.

**Architecture:** Producer CLI → Kafka topic `transactions` → Consumer (loads XGBoost from MLflow, classifies, writes to DynamoDB Local, publishes to `results` topic) → FastAPI exposes `GET /transactions`. All wired via Docker Compose. Prometheus metrics on both services.

**Prerequisites:** Phase 1 complete — `producer/src/transaction.py`, `producer/src/generator.py`, `producer/src/cli.py`, `consumer/src/model_loader.py`, trained model registered in MLflow as `paysense-fraud-detector/Production`.

**Tech Stack:** Python 3.12, confluent-kafka, FastAPI, uvicorn, boto3, DynamoDB Local, Apache Kafka (KRaft), prometheus-client, Docker Compose

---

## File Map

| Path | Purpose |
|------|---------|
| `producer/src/kafka_producer.py` | `KafkaProducer` class — configurable broker, topic, serializer |
| `producer/src/metrics.py` | Prometheus counters + HTTP `/metrics` + `/health` server |
| `producer/src/cli.py` | Updated: adds `--mode kafka\|file`, wires KafkaProducer |
| `producer/tests/test_kafka_producer.py` | Unit tests for KafkaProducer (mock broker) |
| `consumer/src/kafka_consumer.py` | `KafkaConsumer` poll loop |
| `consumer/src/classifier.py` | `FraudClassifier` wrapping ModelLoader |
| `consumer/src/dynamodb_writer.py` | `DynamoDBWriter` — writes result records |
| `consumer/src/results_producer.py` | `ResultsProducer` — publishes to `results` topic |
| `consumer/src/metrics.py` | Prometheus counters + `/metrics` + `/health` |
| `consumer/src/api.py` | FastAPI app: GET /transactions, GET /transactions/{id}, GET /stats |
| `consumer/src/main.py` | Wires consumer loop + FastAPI in separate threads |
| `consumer/tests/test_classifier.py` | Unit tests for FraudClassifier |
| `consumer/tests/test_dynamodb_writer.py` | Unit tests for DynamoDBWriter (mock boto3) |
| `consumer/tests/test_api.py` | Integration tests for FastAPI endpoints |
| `scripts/kafka-test.py` | Smoke test: produce 5 msgs, consume + print |
| `docker-compose.yml` | Grows: Kafka, Kafka UI, DynamoDB Local, MLflow, producer, consumer |

---

## Task 1: Kafka in Docker Compose (Episode 05)

**Files:**
- Modify: `docker-compose.yml`
- Create: `scripts/kafka-test.py`

- [ ] **Step 1: Add Kafka + Kafka UI to `docker-compose.yml`**

Replace the existing skeleton content with:

```yaml
version: "3.9"

services:
  kafka:
    image: confluentinc/cp-kafka:7.6.0
    hostname: kafka
    container_name: kafka
    ports:
      - "9092:9092"
      - "9093:9093"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
    healthcheck:
      test: ["CMD", "kafka-topics", "--bootstrap-server", "kafka:9092", "--list"]
      interval: 10s
      timeout: 5s
      retries: 10

  kafka-init:
    image: confluentinc/cp-kafka:7.6.0
    depends_on:
      kafka:
        condition: service_healthy
    command: >
      bash -c "
        kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic transactions --partitions 3 --replication-factor 1 &&
        kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic results --partitions 3 --replication-factor 1 &&
        echo 'Topics created.'
      "

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
    depends_on:
      kafka:
        condition: service_healthy

  mlflow:
    image: python:3.12-slim
    command: >
      sh -c "pip install mlflow==2.12.* --quiet && mlflow server
        --backend-store-uri sqlite:///mlflow/mlflow.db
        --default-artifact-root /mlflow/artifacts
        --host 0.0.0.0 --port 5000"
    ports:
      - "5000:5000"
    volumes:
      - mlflow-data:/mlflow

  dynamodb-local:
    image: amazon/dynamodb-local:latest
    command: ["-jar", "DynamoDBLocal.jar", "-sharedDb", "-inMemory"]
    ports:
      - "8000:8000"

  dynamodb-init:
    image: amazon/aws-cli:latest
    depends_on:
      - dynamodb-local
    environment:
      AWS_ACCESS_KEY_ID: local
      AWS_SECRET_ACCESS_KEY: local
      AWS_DEFAULT_REGION: us-east-1
    command: >
      dynamodb create-table
        --table-name transactions
        --attribute-definitions AttributeName=transaction_id,AttributeType=S
        --key-schema AttributeName=transaction_id,KeyType=HASH
        --billing-mode PAY_PER_REQUEST
        --endpoint-url http://dynamodb-local:8000

  producer:
    build: ./producer
    image: paysense-producer:dev
    depends_on:
      kafka-init:
        condition: service_completed_successfully
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      KAFKA_TOPIC_TRANSACTIONS: transactions
    # NOTE: --mode kafka is added to cli.py in Task 3. This compose entry is a forward reference —
    # the producer service only works correctly after Task 3 is complete.
    command: ["--rows", "100", "--fraud-rate", "0.1", "--tps", "10", "--mode", "kafka"]
    profiles: ["app"]

  consumer:
    build: ./consumer
    image: paysense-consumer:dev
    depends_on:
      kafka-init:
        condition: service_completed_successfully
      dynamodb-local:
        condition: service_started
      mlflow:
        condition: service_started
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      KAFKA_TOPIC_TRANSACTIONS: transactions
      KAFKA_TOPIC_RESULTS: results
      KAFKA_GROUP_ID: paysense-consumer
      MODEL_URI: models:/paysense-fraud-detector/Production
      MLFLOW_TRACKING_URI: http://mlflow:5000
      DYNAMODB_TABLE: transactions
      DYNAMODB_ENDPOINT_URL: http://dynamodb-local:8000
      AWS_ACCESS_KEY_ID: local
      AWS_SECRET_ACCESS_KEY: local
      AWS_DEFAULT_REGION: us-east-1
    ports:
      - "8001:8001"
    profiles: ["app"]

volumes:
  mlflow-data:
```

- [ ] **Step 2: Start Kafka and verify**

```bash
docker compose up kafka kafka-init kafka-ui -d
docker compose logs kafka-init
```

Expected: `Topics created.`

- [ ] **Step 3: Verify Kafka UI**

Open `http://localhost:8080` → verify `transactions` and `results` topics exist with 3 partitions each.

- [ ] **Step 4: Create `scripts/kafka-test.py`**

```python
"""Smoke test: produce 5 messages, consume and print them."""
import json
import uuid
import time
from confluent_kafka import Producer, Consumer, KafkaError

BOOTSTRAP = "localhost:9092"
TOPIC = "transactions"

producer = Producer({"bootstrap.servers": BOOTSTRAP})
for i in range(5):
    msg = json.dumps({"test": True, "seq": i, "id": str(uuid.uuid4())})
    producer.produce(TOPIC, key=str(i), value=msg)
producer.flush()
print("Produced 5 messages")

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "smoke-test",
    "auto.offset.reset": "earliest",
})
consumer.subscribe([TOPIC])

received = 0
deadline = time.time() + 10
while received < 5 and time.time() < deadline:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Error: {msg.error()}")
        continue
    print(f"Received: {msg.value().decode()}")
    received += 1

consumer.close()
print(f"Done. Received {received}/5 messages.")
```

- [ ] **Step 5: Run smoke test**

```bash
pip install confluent-kafka
python scripts/kafka-test.py
```

Expected: `Received 5/5 messages.`

- [ ] **Step 6: Commit**

```bash
git add docker-compose.yml scripts/kafka-test.py
git commit -m "feat: Kafka KRaft + Kafka UI + DynamoDB Local + MLflow in docker-compose (ep05)"
```

---

## Task 2: KafkaProducer Class (Episode 06)

**Files:**
- Create: `producer/src/kafka_producer.py`
- Create: `producer/tests/test_kafka_producer.py`

- [ ] **Step 1: Write failing tests**

Create `producer/tests/test_kafka_producer.py`:

```python
import json
import pytest
from unittest.mock import MagicMock, patch, call
from producer.kafka_producer import KafkaProducer


def test_kafka_producer_sends_message():
    with patch("producer.kafka_producer.ConfluentProducer") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance

        kp = KafkaProducer(bootstrap_servers="localhost:9092", topic="transactions")
        kp.send({"transaction_id": "abc", "is_fraud": False})

        mock_instance.produce.assert_called_once()
        args, kwargs = mock_instance.produce.call_args
        assert kwargs["topic"] == "transactions" or args[0] == "transactions"
        value = json.loads(kwargs.get("value", args[1] if len(args) > 1 else b"{}"))
        assert value["transaction_id"] == "abc"


def test_kafka_producer_uses_transaction_id_as_key():
    with patch("producer.kafka_producer.ConfluentProducer") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance

        kp = KafkaProducer(bootstrap_servers="localhost:9092", topic="transactions")
        kp.send({"transaction_id": "xyz-123", "is_fraud": True})

        _, kwargs = mock_instance.produce.call_args
        assert kwargs.get("key") == "xyz-123"


def test_kafka_producer_flush_on_close():
    with patch("producer.kafka_producer.ConfluentProducer") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance

        kp = KafkaProducer(bootstrap_servers="localhost:9092", topic="transactions")
        kp.close()

        mock_instance.flush.assert_called_once()
```

- [ ] **Step 2: Run — verify FAIL**

```bash
cd producer
python -m pytest tests/test_kafka_producer.py -v
```

Expected: `ModuleNotFoundError: No module named 'producer.kafka_producer'`

- [ ] **Step 3: Implement `producer/src/kafka_producer.py`**

```python
import json
import signal
from confluent_kafka import Producer as ConfluentProducer


class KafkaProducer:
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self._topic = topic
        self._producer = ConfluentProducer({
            "bootstrap.servers": bootstrap_servers,
            "linger.ms": 5,
            "compression.type": "snappy",
        })

    def send(self, record: dict) -> None:
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
        self._producer.flush()

    @staticmethod
    def _on_delivery(err, msg) -> None:
        if err:
            print(f"[kafka] delivery error: {err}")
```

- [ ] **Step 4: Run tests — verify PASS**

```bash
python -m pytest tests/test_kafka_producer.py -v
```

Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add producer/src/kafka_producer.py producer/tests/test_kafka_producer.py
git commit -m "feat: KafkaProducer class with delivery callback (ep06)"
```

---

## Task 3: Producer Metrics + CLI Kafka Mode (Episode 06)

**Files:**
- Create: `producer/src/metrics.py`
- Modify: `producer/src/cli.py`

- [ ] **Step 1: Create `producer/src/metrics.py`**

```python
import threading
from prometheus_client import Counter, start_http_server

transactions_produced = Counter(
    "transactions_produced_total",
    "Total transactions sent to Kafka",
)
produce_errors = Counter(
    "produce_errors_total",
    "Total Kafka produce errors",
)


def start_metrics_server(port: int = 9090) -> None:
    threading.Thread(
        target=start_http_server,
        args=(port,),
        daemon=True,
    ).start()
```

- [ ] **Step 2: Update `producer/src/cli.py` to add `--mode` flag and Kafka integration**

Full replacement of cli.py:

```python
import time
import signal
import sys
import click
import json
import random
from producer.generator import generate_transaction
from producer.metrics import transactions_produced, produce_errors, start_metrics_server


@click.command()
@click.option("--rows", default=100, help="Number of transactions to generate")
@click.option("--fraud-rate", default=0.1, type=float, help="Fraction that are fraud (0.0–1.0)")
@click.option("--tps", default=0, type=float, help="Transactions per second (0 = unlimited)")
@click.option("--output", default="stdout", type=click.Choice(["stdout", "file"]), help="Output when mode=file")
@click.option("--file-path", default="transactions.jsonl", help="Output file path when --output=file")
@click.option("--mode", default="stdout", type=click.Choice(["stdout", "kafka", "file"]), help="Output mode")
@click.option("--metrics-port", default=9090, help="Prometheus metrics port")
def main(rows: int, fraud_rate: float, tps: float, output: str, file_path: str, mode: str, metrics_port: int) -> None:
    delay = 1.0 / tps if tps > 0 else 0
    kafka_producer = None

    if mode == "kafka":
        import os
        from producer.kafka_producer import KafkaProducer
        bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        topic = os.environ.get("KAFKA_TOPIC_TRANSACTIONS", "transactions")
        kafka_producer = KafkaProducer(bootstrap_servers=bootstrap, topic=topic)
        start_metrics_server(metrics_port)

        def _shutdown(sig, frame):
            kafka_producer.close()
            sys.exit(0)
        signal.signal(signal.SIGTERM, _shutdown)

    def emit(record: dict) -> None:
        if mode == "kafka" and kafka_producer:
            try:
                kafka_producer.send(record)
                transactions_produced.inc()
            except Exception as e:
                produce_errors.inc()
                print(f"[error] {e}", file=sys.stderr)
        elif mode == "file" or output == "file":
            with open(file_path, "a") as f:
                f.write(json.dumps(record) + "\n")
        else:
            click.echo(json.dumps(record))

    for _ in range(rows):
        is_fraud = random.random() < fraud_rate
        t = generate_transaction(is_fraud=is_fraud)
        emit(t.model_dump())
        if delay:
            time.sleep(delay)

    if kafka_producer:
        kafka_producer.close()
```

- [ ] **Step 3: Run existing tests — verify still pass**

```bash
cd producer
python -m pytest tests/ -v
```

Expected: all tests pass

- [ ] **Step 4: Commit**

```bash
git add producer/src/metrics.py producer/src/cli.py
git commit -m "feat: producer Prometheus metrics + --mode kafka flag (ep06)"
```

---

## Task 4: Consumer Kafka + Classifier + DynamoDB (Episode 07)

**Files:**
- Create: `consumer/src/kafka_consumer.py`
- Create: `consumer/src/classifier.py`
- Create: `consumer/src/dynamodb_writer.py`
- Create: `consumer/src/results_producer.py`
- Create: `consumer/src/metrics.py`
- Create: `consumer/tests/test_classifier.py`
- Create: `consumer/tests/test_dynamodb_writer.py`

- [ ] **Step 1: Write failing tests for FraudClassifier**

Create `consumer/tests/test_classifier.py`:

```python
import pytest
import numpy as np
from unittest.mock import MagicMock, patch


def test_classifier_returns_label_and_confidence():
    with patch("consumer.classifier.ModelLoader") as mock_loader_cls:
        mock_loader = MagicMock()
        mock_loader.predict.return_value = (True, 0.92)
        mock_loader_cls.return_value = mock_loader

        from consumer.classifier import FraudClassifier
        clf = FraudClassifier()

        transaction = {
            "amount": 5000.0,
            "customer_age": 45,
            "is_international": True,
            "timestamp": "2026-05-10T12:00:00+00:00",
            "currency": "USD",
            "merchant_category": "electronics",
            "card_type": "Visa",
            "merchant_country": "NG",
        }
        label, confidence = clf.classify(transaction)
        assert label is True
        assert 0.0 <= confidence <= 1.0


def test_classifier_extracts_features_correctly():
    with patch("consumer.classifier.ModelLoader") as mock_loader_cls:
        mock_loader = MagicMock()
        mock_loader.predict.return_value = (False, 0.85)
        mock_loader_cls.return_value = mock_loader

        from consumer.classifier import FraudClassifier
        clf = FraudClassifier()

        transaction = {
            "amount": 50.0,
            "customer_age": 30,
            "is_international": False,
            "timestamp": "2026-05-10T14:30:00+00:00",
            "currency": "USD",
            "merchant_category": "grocery",
            "card_type": "Mastercard",
            "merchant_country": "US",
        }
        clf.classify(transaction)
        mock_loader.predict.assert_called_once()
        features = mock_loader.predict.call_args[0][0]
        assert len(features) == 10
        assert features[0] == 50.0
        assert features[1] == 30
```

- [ ] **Step 2: Run — verify FAIL**

```bash
cd consumer
pip install -e .
python -m pytest tests/test_classifier.py -v
```

Expected: `ModuleNotFoundError: No module named 'consumer.classifier'`

- [ ] **Step 3: Implement `consumer/src/classifier.py`**

```python
import time
from datetime import datetime
from consumer.model_loader import ModelLoader

# These maps mirror the alphabetical order sklearn LabelEncoder assigns during training.
# If the generator adds new categories, update both train.py and here in sync.
# Canonical order is logged as encoding_maps.json artifact in the MLflow run.
_CURRENCY_MAP = {"CAD": 0, "EUR": 1, "GBP": 2, "USD": 3}
_CATEGORY_MAP = {
    "crypto": 0, "e-commerce": 1, "electronics": 2, "gambling": 3,
    "grocery": 4, "healthcare": 5, "luxury": 6, "restaurant": 7,
    "retail": 8, "transport": 9, "wire-transfer": 10,
}
_CARD_MAP = {"Amex": 0, "Discover": 1, "Mastercard": 2, "Visa": 3}
_COUNTRY_MAP = {
    "AU": 0, "BR": 1, "CA": 2, "DE": 3, "GB": 4,
    "NG": 5, "PK": 6, "RO": 7, "UA": 8, "US": 9,
}
# These must match the training dataset statistics logged as MLflow params (amount_mean, amount_std).
# Update if retraining with a different dataset size or fraud rate.
_GLOBAL_AMOUNT_MEAN = 300.0
_GLOBAL_AMOUNT_STD = 500.0


class FraudClassifier:
    def __init__(self) -> None:
        self._loader = ModelLoader()

    def classify(self, transaction: dict) -> tuple[bool, float]:
        features = self._extract_features(transaction)
        return self._loader.predict(features)

    def _extract_features(self, t: dict) -> list[float]:
        ts = datetime.fromisoformat(t["timestamp"])
        hour = ts.hour
        is_weekend = int(ts.weekday() >= 5)
        amount = float(t["amount"])
        amount_zscore = (amount - _GLOBAL_AMOUNT_MEAN) / _GLOBAL_AMOUNT_STD
        return [
            amount,
            float(t["customer_age"]),
            float(t["is_international"]),
            float(hour),
            float(is_weekend),
            amount_zscore,
            float(_CURRENCY_MAP.get(t.get("currency", "USD"), 0)),
            float(_CATEGORY_MAP.get(t.get("merchant_category", "retail"), 1)),
            float(_CARD_MAP.get(t.get("card_type", "Visa"), 0)),
            float(_COUNTRY_MAP.get(t.get("merchant_country", "US"), 0)),
        ]
```

- [ ] **Step 4: Write failing tests for DynamoDBWriter**

Create `consumer/tests/test_dynamodb_writer.py`:

```python
import pytest
from unittest.mock import MagicMock, patch


def test_dynamodb_writer_writes_result_record():
    with patch("consumer.dynamodb_writer.boto3") as mock_boto3:
        mock_table = MagicMock()
        mock_boto3.resource.return_value.Table.return_value = mock_table

        from consumer.dynamodb_writer import DynamoDBWriter
        writer = DynamoDBWriter(table_name="transactions", endpoint_url=None)
        writer.write(
            transaction_id="tx-123",
            timestamp="2026-05-10T12:00:00+00:00",
            amount=142.50,
            merchant_name="Amazon",
            is_fraud=True,
            confidence_score=0.94,
            processing_latency_ms=12,
            model_version="1",
        )
        mock_table.put_item.assert_called_once()
        item = mock_table.put_item.call_args[1]["Item"]
        assert item["transaction_id"] == "tx-123"
        assert item["is_fraud"] is True
        assert item["confidence_score"] == 0.94
```

- [ ] **Step 5: Implement `consumer/src/dynamodb_writer.py`**

```python
import boto3


class DynamoDBWriter:
    def __init__(self, table_name: str, endpoint_url: str | None = None) -> None:
        kwargs = {"region_name": "us-east-1"}
        if endpoint_url:
            kwargs["endpoint_url"] = endpoint_url
        self._table = boto3.resource("dynamodb", **kwargs).Table(table_name)

    def write(
        self,
        transaction_id: str,
        timestamp: str,
        amount: float,
        merchant_name: str,
        is_fraud: bool,
        confidence_score: float,
        processing_latency_ms: int,
        model_version: str,
    ) -> None:
        self._table.put_item(Item={
            "transaction_id": transaction_id,
            "timestamp": timestamp,
            "amount": str(amount),
            "merchant_name": merchant_name,
            "is_fraud": is_fraud,
            "confidence_score": str(confidence_score),
            "processing_latency_ms": processing_latency_ms,
            "model_version": model_version,
        })
```

- [ ] **Step 6: Implement `consumer/src/results_producer.py`**

```python
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
```

- [ ] **Step 7: Implement `consumer/src/metrics.py`**

```python
import threading
from prometheus_client import Counter, Histogram, start_http_server

transactions_consumed = Counter(
    "transactions_consumed_total",
    "Total transactions consumed from Kafka",
)
fraud_detected = Counter(
    "fraud_detected_total",
    "Total transactions classified as fraud",
)
classification_latency = Histogram(
    "classification_latency_seconds",
    "Time to classify one transaction",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5],
)


def start_metrics_server(port: int = 9091) -> None:
    threading.Thread(
        target=start_http_server,
        args=(port,),
        daemon=True,
    ).start()
```

- [ ] **Step 8: Implement `consumer/src/kafka_consumer.py`**

```python
import json
import os
import time
from confluent_kafka import Consumer as ConfluentConsumer, KafkaError
from consumer.classifier import FraudClassifier
from consumer.dynamodb_writer import DynamoDBWriter
from consumer.results_producer import ResultsProducer
from consumer.metrics import transactions_consumed, fraud_detected, classification_latency


def build_consumer() -> ConfluentConsumer:
    return ConfluentConsumer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group.id": os.environ.get("KAFKA_GROUP_ID", "paysense-consumer"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })


def run_consumer_loop(stop_event=None) -> None:
    classifier = FraudClassifier()
    writer = DynamoDBWriter(
        table_name=os.environ.get("DYNAMODB_TABLE", "transactions"),
        endpoint_url=os.environ.get("DYNAMODB_ENDPOINT_URL"),
    )
    results_producer = ResultsProducer()
    consumer = build_consumer()
    topic = os.environ.get("KAFKA_TOPIC_TRANSACTIONS", "transactions")
    consumer.subscribe([topic])

    try:
        while stop_event is None or not stop_event.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[kafka] error: {msg.error()}")
                continue

            t = json.loads(msg.value().decode())
            start = time.time()
            with classification_latency.time():
                is_fraud, confidence = classifier.classify(t)
            latency_ms = int((time.time() - start) * 1000)

            writer.write(
                transaction_id=t["transaction_id"],
                timestamp=t["timestamp"],
                amount=t["amount"],
                merchant_name=t["merchant_name"],
                is_fraud=is_fraud,
                confidence_score=confidence,
                processing_latency_ms=latency_ms,
                model_version=os.environ.get("MODEL_VERSION", "1"),
            )
            results_producer.publish({
                "transaction_id": t["transaction_id"],
                "is_fraud": is_fraud,
                "confidence_score": confidence,
                "processing_latency_ms": latency_ms,
            })

            transactions_consumed.inc()
            if is_fraud:
                fraud_detected.inc()

            consumer.commit(asynchronous=False)
    finally:
        consumer.close()
        results_producer.close()
```

- [ ] **Step 9: Run all consumer tests — verify PASS**

```bash
cd consumer
python -m pytest tests/test_classifier.py tests/test_dynamodb_writer.py -v
```

Expected: 3 passed

- [ ] **Step 10: Commit**

```bash
git add consumer/src/kafka_consumer.py consumer/src/classifier.py consumer/src/dynamodb_writer.py consumer/src/results_producer.py consumer/src/metrics.py consumer/tests/test_classifier.py consumer/tests/test_dynamodb_writer.py
git commit -m "feat: consumer Kafka loop, FraudClassifier, DynamoDB writer, ResultsProducer, metrics (ep07)"
```

---

## Task 5: FastAPI Results API + Consumer Main (Episode 08)

**Files:**
- Create: `consumer/src/api.py`
- Create: `consumer/src/main.py`
- Create: `consumer/tests/test_api.py`
- Modify: `consumer/Dockerfile`

- [ ] **Step 1: Write failing API tests**

Create `consumer/tests/test_api.py`:

```python
import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    with patch("consumer.api.DynamoDBWriter") as mock_writer_cls:
        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        mock_writer.query_fraud.return_value = [
            {
                "transaction_id": "tx-001",
                "is_fraud": True,
                "confidence_score": "0.94",
                "amount": "5000.0",
                "merchant_name": "ShadyCorp",
                "timestamp": "2026-05-10T12:00:00+00:00",
                "processing_latency_ms": 12,
                "model_version": "1",
            }
        ]
        mock_writer.get_transaction.return_value = {
            "transaction_id": "tx-001",
            "is_fraud": True,
            "confidence_score": "0.94",
            "amount": "5000.0",
            "merchant_name": "ShadyCorp",
            "timestamp": "2026-05-10T12:00:00+00:00",
            "processing_latency_ms": 12,
            "model_version": "1",
        }
        mock_writer.get_stats.return_value = {
            "total_processed": 100,
            "fraud_count": 15,
            "fraud_rate": 0.15,
            "avg_confidence": 0.88,
        }

        from consumer.api import app
        yield TestClient(app)


def test_get_fraud_transactions(client):
    resp = client.get("/transactions?fraud=true&limit=10")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert data[0]["is_fraud"] is True


def test_get_single_transaction(client):
    resp = client.get("/transactions/tx-001")
    assert resp.status_code == 200
    data = resp.json()
    assert data["transaction_id"] == "tx-001"


def test_get_stats(client):
    resp = client.get("/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert "fraud_rate" in data
    assert "total_processed" in data
```

- [ ] **Step 2: Run — verify FAIL**

```bash
cd consumer
python -m pytest tests/test_api.py -v
```

Expected: `ModuleNotFoundError: No module named 'consumer.api'`

- [ ] **Step 3: Add query methods to `consumer/src/dynamodb_writer.py`**

Append to existing `DynamoDBWriter` class:

```python
    def query_fraud(self, fraud_only: bool = True, limit: int = 100) -> list[dict]:
        if fraud_only:
            resp = self._table.scan(
                FilterExpression="is_fraud = :val",
                ExpressionAttributeValues={":val": True},
                Limit=limit,
            )
        else:
            resp = self._table.scan(Limit=limit)
        return resp.get("Items", [])

    def get_transaction(self, transaction_id: str) -> dict | None:
        resp = self._table.get_item(Key={"transaction_id": transaction_id})
        return resp.get("Item")

    def get_stats(self) -> dict:
        resp = self._table.scan(
            ProjectionExpression="is_fraud, confidence_score"
        )
        items = resp.get("Items", [])
        total = len(items)
        fraud_count = sum(1 for i in items if i.get("is_fraud"))
        fraud_rate = fraud_count / total if total else 0.0
        confidences = [float(i.get("confidence_score", 0)) for i in items]
        avg_conf = sum(confidences) / len(confidences) if confidences else 0.0
        return {
            "total_processed": total,
            "fraud_count": fraud_count,
            "fraud_rate": round(fraud_rate, 4),
            "avg_confidence": round(avg_conf, 4),
        }
```

- [ ] **Step 4: Implement `consumer/src/api.py`**

```python
import os
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from consumer.dynamodb_writer import DynamoDBWriter

app = FastAPI(title="PaySense Results API", version="1.0.0")
_writer = DynamoDBWriter(
    table_name=os.environ.get("DYNAMODB_TABLE", "transactions"),
    endpoint_url=os.environ.get("DYNAMODB_ENDPOINT_URL"),
)


@app.get("/transactions")
def get_transactions(
    fraud: bool | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
):
    fraud_only = fraud is True
    items = _writer.query_fraud(fraud_only=fraud_only, limit=limit)
    return JSONResponse(content=items)


@app.get("/transactions/{transaction_id}")
def get_transaction(transaction_id: str):
    item = _writer.get_transaction(transaction_id)
    if item is None:
        return JSONResponse(status_code=404, content={"detail": "not found"})
    return JSONResponse(content=item)


@app.get("/stats")
def get_stats():
    return _writer.get_stats()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics")
def metrics_redirect():
    return JSONResponse(content={"info": "metrics on :9091/metrics"})
```

- [ ] **Step 5: Implement `consumer/src/main.py`**

```python
import threading
import uvicorn
import os
from consumer.metrics import start_metrics_server
from consumer.kafka_consumer import run_consumer_loop


def main() -> None:
    start_metrics_server(port=int(os.environ.get("METRICS_PORT", "9091")))

    consumer_thread = threading.Thread(target=run_consumer_loop, daemon=True)
    consumer_thread.start()

    uvicorn.run(
        "consumer.api:app",
        host="0.0.0.0",
        port=int(os.environ.get("API_PORT", "8001")),
        log_level="info",
    )


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Add entry point to `consumer/pyproject.toml`**

Add under `[project]`:

```toml
[project.scripts]
consumer = "consumer.main:main"
```

- [ ] **Step 7: Create `consumer/Dockerfile`**

```dockerfile
FROM python:3.12-slim AS builder
WORKDIR /app
COPY pyproject.toml .
RUN pip install --no-cache-dir build && python -m build --wheel

FROM python:3.12-slim
WORKDIR /app
COPY --from=builder /app/dist/*.whl .
RUN pip install --no-cache-dir *.whl

COPY src/ src/
RUN pip install --no-cache-dir -e . 2>/dev/null || true

EXPOSE 8001 9091
ENTRYPOINT ["consumer"]
```

- [ ] **Step 8: Run all consumer tests — verify PASS**

```bash
cd consumer
python -m pytest tests/ -v
```

Expected: 6+ passed

- [ ] **Step 9: Commit**

```bash
git add consumer/src/api.py consumer/src/main.py consumer/Dockerfile consumer/pyproject.toml consumer/tests/test_api.py
git commit -m "feat: FastAPI results API + consumer main entrypoint + Dockerfile (ep08)"
```

---

## Task 6: Full Local Stack Smoke Test (Episode 08)

**Files:**
- No new files — verifies everything wired together

- [ ] **Step 1: Build images**

```bash
docker compose build producer consumer
```

Expected: Both images build successfully.

- [ ] **Step 2: Start full stack**

```bash
docker compose up kafka kafka-init kafka-ui dynamodb-local dynamodb-init mlflow -d
```

Wait 30s for Kafka init to complete.

- [ ] **Step 3: Start producer + consumer with app profile**

```bash
docker compose --profile app up -d
docker compose logs producer consumer --follow
```

Expected: Producer logs show transactions sent. Consumer logs show classifications.

- [ ] **Step 4: Query API**

```bash
curl http://localhost:8001/transactions?fraud=true&limit=5
curl http://localhost:8001/stats
curl http://localhost:8001/health
```

Expected: JSON responses with fraud records and aggregate stats.

- [ ] **Step 5: Stop stack**

```bash
docker compose down
```

- [ ] **Step 6: Final commit**

```bash
git add .
git commit -m "chore: phase 2 complete — full local pipeline (ep05-08)"
git tag phase2-complete
git push origin master --tags
```

---

## Phase 2 Complete Verification

- [ ] All unit tests pass: `cd producer && python -m pytest tests/ -v`
- [ ] All consumer tests pass: `cd consumer && python -m pytest tests/ -v`
- [ ] Docker Compose brings up full stack without errors
- [ ] `GET /transactions?fraud=true` returns fraud records
- [ ] `GET /stats` returns aggregates
- [ ] Kafka UI at `localhost:8080` shows message throughput
- [ ] Prometheus metrics at `localhost:9090/metrics` (producer) and `localhost:9091/metrics` (consumer)

---

## Self-Review Checklist

| Spec Requirement | Task |
|---|---|
| Kafka KRaft mode single broker | Task 1 |
| Kafka UI (`provectuslabs/kafka-ui`) | Task 1 |
| Topics: `transactions` + `results` (3 partitions) | Task 1 |
| Topic init container | Task 1 |
| `scripts/kafka-test.py` smoke test | Task 1 |
| Kafka health check in compose | Task 1 |
| KafkaProducer class (confluent-kafka) | Task 2 |
| Key = transaction_id | Task 2 |
| `--mode kafka\|file` flag on CLI | Task 3 |
| SIGTERM handler flushes producer | Task 3 |
| Prometheus metrics on producer: `transactions_produced_total`, `produce_errors_total` | Task 3 |
| `/metrics` endpoint producer | Task 3 |
| `/health` endpoint producer | Task 3 (via metrics server) |
| DynamoDB Local in docker-compose | Task 1 |
| DynamoDB table init | Task 1 |
| KafkaConsumer poll loop | Task 4 |
| FraudClassifier with feature extraction | Task 4 |
| DynamoDBWriter with all result fields | Task 4 |
| ResultsProducer → `results` topic | Task 4 |
| Consumer metrics: `transactions_consumed_total`, `fraud_detected_total`, `classification_latency_seconds` | Task 4 |
| FastAPI `GET /transactions` with `?fraud=true&limit=N` | Task 5 |
| FastAPI `GET /transactions/{id}` | Task 5 |
| FastAPI `GET /stats` | Task 5 |
| OpenAPI docs at `/docs` | Task 5 (FastAPI auto) |
| Consumer Dockerfile | Task 5 |
| Full demo: 500 transactions, 15% fraud, 100 TPS | Task 6 |
