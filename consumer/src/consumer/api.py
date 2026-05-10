import os
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from consumer.dynamodb_writer import DynamoDBWriter

app = FastAPI(title="PaySense Results API", version="1.0.0")
_writer = DynamoDBWriter(
    table_name=os.environ.get("DYNAMODB_TABLE", "transactions"),
    endpoint_url=os.environ.get("DYNAMODB_ENDPOINT_URL"),
)


@app.get("/transactions")
def get_transactions(
    fraud: bool | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
):
    fraud_only = fraud is True
    items = _writer.query_fraud(fraud_only=fraud_only, limit=limit)
    return JSONResponse(content=items)


@app.get("/transactions/{transaction_id}")
def get_transaction(transaction_id: str):
    item = _writer.get_transaction(transaction_id)
    if item is None:
        return JSONResponse(status_code=404, content={"detail": "not found"})
    return JSONResponse(content=item)


@app.get("/stats")
def get_stats():
    return _writer.get_stats()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics")
def metrics_redirect():
    return JSONResponse(content={"info": "metrics on :9091/metrics"})
