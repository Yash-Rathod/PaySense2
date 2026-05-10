import json
import os
import random
import signal
import sys
import time

import click

from producer.generator import generate_transaction
from producer.metrics import produce_errors, start_metrics_server, transactions_produced


@click.command()
@click.option("--rows", default=100, help="Number of transactions to generate")
@click.option("--fraud-rate", default=0.1, type=float, help="Fraction that are fraud (0.0-1.0)")
@click.option("--tps", default=0, type=float, help="Transactions per second (0 = unlimited)")
@click.option("--output", default="stdout", type=click.Choice(["stdout", "file"]), help="Output when mode=stdout/file")
@click.option("--file-path", default="transactions.jsonl", help="Output file path when --output=file")
@click.option("--mode", default="stdout", type=click.Choice(["stdout", "kafka", "file"]), help="Output mode")
@click.option("--metrics-port", default=9090, help="Prometheus metrics port")
def main(
    rows: int,
    fraud_rate: float,
    tps: float,
    output: str,
    file_path: str,
    mode: str,
    metrics_port: int,
) -> None:
    delay = 1.0 / tps if tps > 0 else 0
    kafka_producer = None

    if mode == "kafka":
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
            except Exception as exc:
                produce_errors.inc()
                print(f"[error] {exc}", file=sys.stderr)
        elif mode == "file" or output == "file":
            with open(file_path, "a") as f:
                f.write(json.dumps(record) + "\n")
        else:
            click.echo(json.dumps(record))

    for _ in range(rows):
        is_fraud = random.random() < fraud_rate
        transaction = generate_transaction(is_fraud=is_fraud)
        emit(transaction.model_dump())
        if delay:
            time.sleep(delay)

    if kafka_producer:
        kafka_producer.close()


if __name__ == "__main__":
    main()
