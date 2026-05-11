import json
import os
import time
from confluent_kafka import Consumer as ConfluentConsumer, KafkaError, KafkaException
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

            try:
                t = json.loads(msg.value().decode())
                required = {"transaction_id", "timestamp", "amount", "merchant_name",
                            "customer_age", "is_international"}
                missing = required - t.keys()
                if missing:
                    print(f"[kafka] skip msg missing fields: {missing}")
                else:
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
            except Exception as exc:
                print(f"[kafka] error processing msg: {exc}")

            try:
                consumer.commit(asynchronous=False)
            except KafkaException as ke:
                if ke.args[0].code() != KafkaError._NO_OFFSET:
                    raise
    finally:
        consumer.close()
        results_producer.close()
