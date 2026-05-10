import time
import click
import json
import random
from producer.generator import generate_transaction


@click.command()
@click.option("--rows", default=100, help="Number of transactions to generate")
@click.option("--fraud-rate", default=0.1, type=float, help="Fraction of transactions that are fraud (0.0–1.0)")
@click.option("--tps", default=0, type=float, help="Transactions per second (0 = unlimited)")
@click.option("--output", default="stdout", type=click.Choice(["stdout", "file"]), help="Output destination")
@click.option("--file-path", default="transactions.jsonl", help="Output file path when --output=file")
def main(rows: int, fraud_rate: float, tps: float, output: str, file_path: str) -> None:
    delay = 1.0 / tps if tps > 0 else 0

    def emit(line: str) -> None:
        if output == "stdout":
            click.echo(line)
        else:
            with open(file_path, "a") as f:
                f.write(line + "\n")

    for _ in range(rows):
        is_fraud = random.random() < fraud_rate
        t = generate_transaction(is_fraud=is_fraud)
        emit(t.model_dump_json())
        if delay:
            time.sleep(delay)

if __name__ == "__main__":
    main()
