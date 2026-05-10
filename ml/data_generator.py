"""Generate a labeled dataset for model training."""
import json
import random
import sys
from pathlib import Path

# allow running as script from ml/
sys.path.insert(0, str(Path(__file__).parent.parent / "producer" / "src"))

from producer.generator import generate_transaction


def generate_dataset(n_rows: int, fraud_rate: float) -> list[dict]:
    records = []
    for _ in range(n_rows):
        is_fraud = random.random() < fraud_rate
        t = generate_transaction(is_fraud=is_fraud)
        records.append(json.loads(t.model_dump_json()))
    return records


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=50_000)
    parser.add_argument("--fraud-rate", type=float, default=0.1)
    parser.add_argument("--output", default="data/transactions.jsonl")
    args = parser.parse_args()

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    records = generate_dataset(args.rows, args.fraud_rate)
    with open(args.output, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    print(f"Generated {len(records)} rows -> {args.output}")
