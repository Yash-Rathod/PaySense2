"""Prometheus metrics for the producer."""

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
