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
