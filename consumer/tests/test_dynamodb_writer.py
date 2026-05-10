import pytest
from unittest.mock import MagicMock, patch


def test_dynamodb_writer_writes_result_record():
    with patch("consumer.dynamodb_writer.boto3") as mock_boto3:
        mock_table = MagicMock()
        mock_boto3.resource.return_value.Table.return_value = mock_table

        from consumer.dynamodb_writer import DynamoDBWriter
        writer = DynamoDBWriter(table_name="transactions", endpoint_url=None)
        writer.write(
            transaction_id="tx-123",
            timestamp="2026-05-10T12:00:00+00:00",
            amount=142.50,
            merchant_name="Amazon",
            is_fraud=True,
            confidence_score=0.94,
            processing_latency_ms=12,
            model_version="1",
        )
        mock_table.put_item.assert_called_once()
        item = mock_table.put_item.call_args[1]["Item"]
        assert item["transaction_id"] == "tx-123"
        assert item["is_fraud"] is True
        assert item["confidence_score"] == "0.94"
