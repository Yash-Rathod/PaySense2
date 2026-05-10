import pytest
from producer.transaction import Transaction
from datetime import datetime, timezone
import uuid


def test_transaction_required_fields():
    t = Transaction(
        transaction_id=str(uuid.uuid4()),
        timestamp=datetime.now(timezone.utc).isoformat(),
        amount=142.50,
        currency="USD",
        merchant_name="Amazon",
        merchant_category="e-commerce",
        merchant_country="US",
        card_last4="4242",
        card_type="Visa",
        customer_id=str(uuid.uuid4()),
        customer_age=34,
        customer_location="New York, US",
        is_international=False,
        is_fraud=False,
    )
    assert t.amount == 142.50
    assert t.currency == "USD"
    assert t.is_fraud is False


def test_transaction_amount_positive():
    with pytest.raises(Exception):
        Transaction(
            transaction_id=str(uuid.uuid4()),
            timestamp=datetime.now(timezone.utc).isoformat(),
            amount=-10.0,
            currency="USD",
            merchant_name="Amazon",
            merchant_category="e-commerce",
            merchant_country="US",
            card_last4="4242",
            card_type="Visa",
            customer_id=str(uuid.uuid4()),
            customer_age=34,
            customer_location="New York, US",
            is_international=False,
            is_fraud=False,
        )


def test_transaction_serializes_to_json():
    t = Transaction(
        transaction_id=str(uuid.uuid4()),
        timestamp=datetime.now(timezone.utc).isoformat(),
        amount=99.99,
        currency="USD",
        merchant_name="Walmart",
        merchant_category="retail",
        merchant_country="US",
        card_last4="1234",
        card_type="Mastercard",
        customer_id=str(uuid.uuid4()),
        customer_age=28,
        customer_location="Los Angeles, US",
        is_international=False,
        is_fraud=True,
    )
    data = t.model_dump_json()
    assert "transaction_id" in data
    assert "is_fraud" in data


from producer.generator import generate_transaction


def test_generate_legit_transaction():
    t = generate_transaction(is_fraud=False)
    assert t.is_fraud is False
    assert t.amount > 0
    assert t.currency in ("USD", "EUR", "GBP", "CAD")
    assert len(t.card_last4) == 4


def test_generate_fraud_transaction():
    t = generate_transaction(is_fraud=True)
    assert t.is_fraud is True
    # Fraud transactions lean high-amount or international
    assert t.amount > 0


def test_generate_transaction_has_valid_uuid_fields():
    import uuid
    t = generate_transaction(is_fraud=False)
    uuid.UUID(t.transaction_id)
    uuid.UUID(t.customer_id)


def test_fraud_rate_distribution():
    transactions = [generate_transaction(is_fraud=(i % 10 < 1)) for i in range(100)]
    fraud_count = sum(1 for t in transactions if t.is_fraud)
    assert fraud_count == 10


from click.testing import CliRunner
from producer.cli import main
import json


def test_cli_generates_rows():
    runner = CliRunner()
    result = runner.invoke(main, ["--rows", "5", "--fraud-rate", "0.0", "--output", "stdout"])
    assert result.exit_code == 0
    lines = [l for l in result.output.strip().split("\n") if l]
    assert len(lines) == 5
    for line in lines:
        t = json.loads(line)
        assert t["is_fraud"] is False


def test_cli_fraud_rate():
    runner = CliRunner()
    result = runner.invoke(main, ["--rows", "100", "--fraud-rate", "1.0", "--output", "stdout"])
    assert result.exit_code == 0
    lines = [l for l in result.output.strip().split("\n") if l]
    transactions = [json.loads(l) for l in lines]
    assert all(t["is_fraud"] for t in transactions)
