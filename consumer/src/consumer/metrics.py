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
