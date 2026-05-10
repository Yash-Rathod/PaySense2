import boto3


class DynamoDBWriter:
    def __init__(self, table_name: str, endpoint_url: str | None = None) -> None:
        kwargs = {"region_name": "us-east-1"}
        if endpoint_url:
            kwargs["endpoint_url"] = endpoint_url
        self._table = boto3.resource("dynamodb", **kwargs).Table(table_name)

    def write(
        self,
        transaction_id: str,
        timestamp: str,
        amount: float,
        merchant_name: str,
        is_fraud: bool,
        confidence_score: float,
        processing_latency_ms: int,
        model_version: str,
    ) -> None:
        self._table.put_item(Item={
            "transaction_id": transaction_id,
            "timestamp": timestamp,
            "amount": str(amount),
            "merchant_name": merchant_name,
            "is_fraud": is_fraud,
            "confidence_score": str(confidence_score),
            "processing_latency_ms": processing_latency_ms,
            "model_version": model_version,
        })

    def query_fraud(self, fraud_only: bool = True, limit: int = 100) -> list[dict]:
        if fraud_only:
            resp = self._table.scan(
                FilterExpression="is_fraud = :val",
                ExpressionAttributeValues={":val": True},
                Limit=limit,
            )
        else:
            resp = self._table.scan(Limit=limit)
        return resp.get("Items", [])

    def get_transaction(self, transaction_id: str) -> dict | None:
        resp = self._table.get_item(Key={"transaction_id": transaction_id})
        return resp.get("Item")

    def get_stats(self) -> dict:
        resp = self._table.scan(
            ProjectionExpression="is_fraud, confidence_score"
        )
        items = resp.get("Items", [])
        total = len(items)
        fraud_count = sum(1 for i in items if i.get("is_fraud"))
        fraud_rate = fraud_count / total if total else 0.0
        confidences = [float(i.get("confidence_score", 0)) for i in items]
        avg_conf = sum(confidences) / len(confidences) if confidences else 0.0
        return {
            "total_processed": total,
            "fraud_count": fraud_count,
            "fraud_rate": round(fraud_rate, 4),
            "avg_confidence": round(avg_conf, 4),
        }
