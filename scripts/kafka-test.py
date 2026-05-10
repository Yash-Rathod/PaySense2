"""Smoke test: produce 5 messages, consume them, and print them."""

import json
import time
import uuid

from confluent_kafka import Consumer, Producer


BOOTSTRAP = "localhost:9092"
TOPIC = "transactions"
COUNT = 5


def main() -> None:
    batch_id = str(uuid.uuid4())
    producer = Producer({"bootstrap.servers": BOOTSTRAP})

    for seq in range(COUNT):
        msg = json.dumps({"test": True, "seq": seq, "id": str(uuid.uuid4()), "batch_id": batch_id})
        producer.produce(TOPIC, key=f"{batch_id}-{seq}", value=msg)
    producer.flush()
    print(f"Produced {COUNT} messages")

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"smoke-test-{batch_id}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])

    received = 0
    deadline = time.time() + 10
    try:
        while received < COUNT and time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
                continue

            value = json.loads(msg.value().decode())
            if value.get("batch_id") != batch_id:
                continue

            print(f"Received: {json.dumps(value)}")
            received += 1
    finally:
        consumer.close()

    print(f"Done. Received {received}/{COUNT} messages.")
    if received != COUNT:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
