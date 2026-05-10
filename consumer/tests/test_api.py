import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    with patch("consumer.dynamodb_writer.boto3") as mock_boto3:
        mock_table = MagicMock()
        mock_boto3.resource.return_value.Table.return_value = mock_table

        mock_table.scan.return_value = {
            "Items": [
                {
                    "transaction_id": "tx-001",
                    "is_fraud": True,
                    "confidence_score": "0.94",
                    "amount": "5000.0",
                    "merchant_name": "ShadyCorp",
                    "timestamp": "2026-05-10T12:00:00+00:00",
                    "processing_latency_ms": 12,
                    "model_version": "1",
                }
            ]
        }
        mock_table.get_item.return_value = {
            "Item": {
                "transaction_id": "tx-001",
                "is_fraud": True,
                "confidence_score": "0.94",
                "amount": "5000.0",
                "merchant_name": "ShadyCorp",
                "timestamp": "2026-05-10T12:00:00+00:00",
                "processing_latency_ms": 12,
                "model_version": "1",
            }
        }

        # Reimport to apply mocks
        import sys
        if "consumer.api" in sys.modules:
            del sys.modules["consumer.api"]

        from consumer.api import app
        yield TestClient(app)


def test_get_fraud_transactions(client):
    resp = client.get("/transactions?fraud=true&limit=10")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    if len(data) > 0:
        assert data[0]["is_fraud"] is True


def test_get_single_transaction(client):
    resp = client.get("/transactions/tx-001")
    assert resp.status_code == 200
    data = resp.json()
    assert data["transaction_id"] == "tx-001"


def test_get_stats(client):
    with patch("consumer.dynamodb_writer.boto3") as mock_boto3:
        mock_table = MagicMock()
        mock_boto3.resource.return_value.Table.return_value = mock_table
        mock_table.scan.return_value = {
            "Items": [
                {"is_fraud": True, "confidence_score": "0.94"},
                {"is_fraud": False, "confidence_score": "0.15"},
            ]
        }

        resp = client.get("/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "fraud_rate" in data
        assert "total_processed" in data
