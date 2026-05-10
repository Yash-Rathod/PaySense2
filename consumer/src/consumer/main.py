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
