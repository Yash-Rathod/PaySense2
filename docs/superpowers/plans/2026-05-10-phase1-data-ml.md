# Phase 1 — Data & ML Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the full local data + ML foundation (Episodes 1–4): monorepo skeleton, synthetic transaction generator CLI, XGBoost fraud classifier trained with MLflow tracking, and model registry with loadable artifact URI.

**Architecture:** Producer generates synthetic transactions via CLI; ML module trains XGBoost on that data and registers the best model in MLflow Model Registry; Consumer model loader reads from a `MODEL_URI` env var (local filesystem for Phase 1, S3 in Phase 3). No Kafka, no cloud — 100% local, Docker only.

**Tech Stack:** Python 3.12, Faker, click, pydantic, scikit-learn, XGBoost, imbalanced-learn, MLflow, pandas, Docker, Docker Compose

---

## File Map

| Path | Purpose |
|------|---------|
| `producer/src/transaction.py` | Pydantic `Transaction` model |
| `producer/src/generator.py` | `generate_transaction()` logic |
| `producer/src/cli.py` | click CLI entry point |
| `producer/pyproject.toml` | producer dependencies |
| `producer/Dockerfile` | multi-stage Docker image |
| `producer/tests/test_transaction.py` | unit tests for model + generator |
| `ml/data_generator.py` | dataset generation for training (imports from producer) |
| `ml/train.py` | feature engineering + XGBoost training + MLflow logging |
| `ml/requirements.txt` | ML dependencies |
| `consumer/src/model_loader.py` | loads model from `MODEL_URI` env var |
| `consumer/pyproject.toml` | consumer dependencies |
| `consumer/tests/test_model_loader.py` | unit tests for model loader |
| `docker-compose.yml` | skeleton → grows each task |
| `.gitignore` | Python, Docker, Terraform, `.env`, MLflow |
| `README.md` | architecture + episode index |

---

## Task 1: Monorepo Skeleton (Episode 01)

**Files:**
- Create: `README.md`
- Create: `.gitignore`
- Create: `docker-compose.yml`
- Create: `producer/pyproject.toml`
- Create: `consumer/pyproject.toml`
- Create: `ml/requirements.txt`
- Create: `producer/src/__init__.py`
- Create: `consumer/src/__init__.py`
- Create: `k8s/apps/.gitkeep`
- Create: `infra/terraform/.gitkeep`
- Create: `infra/helm/.gitkeep`
- Create: `.github/workflows/.gitkeep`

- [ ] **Step 1: Create folder structure**

```bash
mkdir -p producer/src producer/tests
mkdir -p consumer/src consumer/tests
mkdir -p ml
mkdir -p infra/terraform infra/helm
mkdir -p k8s/apps
mkdir -p .github/workflows
```

- [ ] **Step 2: Create `.gitignore`**

```
# Python
__pycache__/
*.py[cod]
*.egg-info/
.venv/
venv/
dist/
build/
*.egg

# Env
.env
.env.*
!.env.example

# Docker
.docker/

# Terraform
.terraform/
*.tfstate
*.tfstate.backup
*.tfvars
!*.tfvars.example

# MLflow
mlruns/
mlartifacts/

# Superpowers
.superpowers/

# IDE
.idea/
.vscode/
*.iml
```

- [ ] **Step 3: Create `README.md`**

```markdown
# PaySense — Real-time Transaction Fraud Detection

A production-grade DevOps project showcasing ML pipelines, Kafka streaming, Kubernetes, GitOps, and observability.

## Architecture

```
Producer CLI ──▶ Kafka (transactions) ──▶ Consumer ──▶ Kafka (results)
                                              │
                                              ├──▶ DynamoDB (persist)
                                              └──▶ FastAPI (query)

MLflow ──▶ S3 artifact store ◀── Consumer (loads model)
Prometheus ──▶ Grafana (dashboards)
GitHub push ──▶ GitHub Actions ──▶ ECR ──▶ ArgoCD ──▶ EKS
```

## Episode Index

| Episode | Topic |
|---------|-------|
| 01 | Project Intro & Repo Setup |
| 02 | Transaction Generator |
| 03 | ML Model Training |
| 04 | MLflow Model Registry |
| 05 | Kafka with Docker Compose |
| 06 | Producer Kafka Integration |
| 07 | Consumer Fraud Classifier |
| 08 | FastAPI Results API |
| 09 | Terraform AWS Infrastructure |
| 10 | Helm Charts + EKS Deploy |
| 11 | Strimzi Kafka on K8s |
| 12 | ArgoCD GitOps |
| 13 | GitHub Actions CI/CD |
| 14 | Prometheus & Grafana |
| 15 | Alerting & Full Demo |

## Quick Start

```bash
# Generate 1000 transactions, 10% fraud
docker run paysense-producer --rows 1000 --fraud-rate 0.1 --tps 50

# Full local stack
docker compose up
```
```

- [ ] **Step 4: Create `docker-compose.yml` skeleton**

```yaml
version: "3.9"

services:
  producer:
    build: ./producer
    profiles: ["app"]

  consumer:
    build: ./consumer
    profiles: ["app"]
```

- [ ] **Step 5: Create `producer/pyproject.toml`**

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.backends.legacy:build"

[project]
name = "paysense-producer"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "faker>=24.0",
    "click>=8.1",
    "pydantic>=2.6",
    "prometheus-client>=0.20",
]

[project.scripts]
producer = "producer.cli:main"

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
testpaths = ["tests"]
```

- [ ] **Step 6: Create `consumer/pyproject.toml`**

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.backends.legacy:build"

[project]
name = "paysense-consumer"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "mlflow>=2.12",
    "xgboost>=2.0",
    "boto3>=1.34",
    "confluent-kafka>=2.3",
    "fastapi>=0.110",
    "uvicorn>=0.29",
    "pydantic>=2.6",
    "prometheus-client>=0.20",
]

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
testpaths = ["tests"]
```

- [ ] **Step 7: Create `ml/requirements.txt`**

```
scikit-learn>=1.4
xgboost>=2.0
mlflow>=2.12
pandas>=2.2
imbalanced-learn>=0.12
faker>=24.0
pydantic>=2.6
```

- [ ] **Step 8: Create `__init__.py` files**

```bash
touch producer/src/__init__.py
touch consumer/src/__init__.py
touch producer/tests/__init__.py
touch consumer/tests/__init__.py
touch k8s/apps/.gitkeep
touch infra/terraform/.gitkeep
touch infra/helm/.gitkeep
touch .github/workflows/.gitkeep
```

- [ ] **Step 9: Verify Python 3.12 + Docker available**

```bash
python --version   # must be 3.12.x
docker --version   # must be 20+
docker compose version
```

- [ ] **Step 10: Commit**

```bash
git add .
git commit -m "feat: initialize monorepo skeleton (ep01)"
```

---

## Task 2: Transaction Pydantic Model (Episode 02)

**Files:**
- Create: `producer/src/transaction.py`
- Create: `producer/tests/test_transaction.py`

- [ ] **Step 1: Write failing tests**

Create `producer/tests/test_transaction.py`:

```python
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
```

- [ ] **Step 2: Run test — verify FAIL**

```bash
cd producer
pip install -e ".[dev]" 2>/dev/null || pip install pydantic
python -m pytest tests/test_transaction.py -v
```

Expected: `ModuleNotFoundError: No module named 'producer.transaction'`

- [ ] **Step 3: Implement `producer/src/transaction.py`**

```python
from pydantic import BaseModel, field_validator
from typing import Literal


class Transaction(BaseModel):
    transaction_id: str
    timestamp: str
    amount: float
    currency: str
    merchant_name: str
    merchant_category: str
    merchant_country: str
    card_last4: str
    card_type: str
    customer_id: str
    customer_age: int
    customer_location: str
    is_international: bool
    is_fraud: bool

    @field_validator("amount")
    @classmethod
    def amount_must_be_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("amount must be positive")
        return v
```

- [ ] **Step 4: Install producer in editable mode**

```bash
cd producer
pip install -e .
```

- [ ] **Step 5: Run tests — verify PASS**

```bash
python -m pytest tests/test_transaction.py -v
```

Expected: 3 passed

- [ ] **Step 6: Commit**

```bash
git add producer/src/transaction.py producer/tests/test_transaction.py
git commit -m "feat: add Transaction pydantic model with validation (ep02)"
```

---

## Task 3: Transaction Generator (Episode 02)

**Files:**
- Create: `producer/src/generator.py`
- Modify: `producer/tests/test_transaction.py` (add generator tests)

- [ ] **Step 1: Add failing generator tests to `producer/tests/test_transaction.py`**

Append to the existing file:

```python
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
```

- [ ] **Step 2: Run — verify FAIL**

```bash
python -m pytest tests/test_transaction.py::test_generate_legit_transaction -v
```

Expected: `ImportError: cannot import name 'generate_transaction'`

- [ ] **Step 3: Implement `producer/src/generator.py`**

```python
import uuid
from datetime import datetime, timezone
from faker import Faker
from producer.transaction import Transaction

fake = Faker()

_CURRENCIES = ["USD", "EUR", "GBP", "CAD"]
_CARD_TYPES = ["Visa", "Mastercard", "Amex", "Discover"]
_LEGIT_CATEGORIES = ["grocery", "retail", "restaurant", "transport", "healthcare"]
_FRAUD_CATEGORIES = ["electronics", "luxury", "crypto", "wire-transfer", "gambling"]
_COUNTRIES = ["US", "CA", "GB", "AU", "DE"]
_FRAUD_COUNTRIES = ["NG", "RO", "UA", "BR", "PK"]


def generate_transaction(is_fraud: bool) -> Transaction:
    if is_fraud:
        amount = round(fake.pyfloat(min_value=500, max_value=9999, right_digits=2), 2)
        merchant_category = fake.random_element(_FRAUD_CATEGORIES)
        merchant_country = fake.random_element(_FRAUD_COUNTRIES)
        is_international = merchant_country != "US"
    else:
        amount = round(fake.pyfloat(min_value=1, max_value=499, right_digits=2), 2)
        merchant_category = fake.random_element(_LEGIT_CATEGORIES)
        merchant_country = fake.random_element(_COUNTRIES)
        is_international = merchant_country != "US"

    return Transaction(
        transaction_id=str(uuid.uuid4()),
        timestamp=datetime.now(timezone.utc).isoformat(),
        amount=amount,
        currency=fake.random_element(_CURRENCIES),
        merchant_name=fake.company(),
        merchant_category=merchant_category,
        merchant_country=merchant_country,
        card_last4=str(fake.random_number(digits=4, fix_len=True)),
        card_type=fake.random_element(_CARD_TYPES),
        customer_id=str(uuid.uuid4()),
        customer_age=fake.random_int(min=18, max=80),
        customer_location=f"{fake.city()}, {fake.country_code()}",
        is_international=is_international,
        is_fraud=is_fraud,
    )
```

- [ ] **Step 4: Run all tests — verify PASS**

```bash
python -m pytest tests/ -v
```

Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add producer/src/generator.py producer/tests/test_transaction.py
git commit -m "feat: add transaction generator with fraud/legit differentiation (ep02)"
```

---

## Task 4: Producer CLI (Episode 02)

**Files:**
- Create: `producer/src/cli.py`
- Modify: `producer/tests/test_transaction.py` (add CLI tests)

- [ ] **Step 1: Add failing CLI tests**

Append to `producer/tests/test_transaction.py`:

```python
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
```

- [ ] **Step 2: Run — verify FAIL**

```bash
python -m pytest tests/test_transaction.py::test_cli_generates_rows -v
```

Expected: `ImportError: cannot import name 'main'`

- [ ] **Step 3: Implement `producer/src/cli.py`**

```python
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
```

- [ ] **Step 4: Run all tests — verify PASS**

```bash
python -m pytest tests/ -v
```

Expected: 9 passed

- [ ] **Step 5: Smoke test CLI manually**

```bash
python -m producer.cli --rows 5 --fraud-rate 0.5 | python -m json.tool
```

Expected: 5 pretty-printed JSON transaction objects.

- [ ] **Step 6: Commit**

```bash
git add producer/src/cli.py producer/tests/test_transaction.py
git commit -m "feat: add click CLI with rows/fraud-rate/tps/output flags (ep02)"
```

---

## Task 5: Producer Dockerfile (Episode 02)

**Files:**
- Create: `producer/Dockerfile`
- Modify: `docker-compose.yml`

- [ ] **Step 1: Create `producer/Dockerfile`**

```dockerfile
FROM python:3.12-slim AS builder
WORKDIR /app
COPY pyproject.toml .
RUN pip install --no-cache-dir build && python -m build --wheel

FROM python:3.12-slim
WORKDIR /app
COPY --from=builder /app/dist/*.whl .
RUN pip install --no-cache-dir *.whl

COPY src/ src/
RUN pip install --no-cache-dir -e . 2>/dev/null || true

ENTRYPOINT ["producer"]
CMD ["--rows", "100", "--fraud-rate", "0.1"]
```

- [ ] **Step 2: Build image**

```bash
cd producer
docker build -t paysense-producer:dev .
```

Expected: Successfully built (no errors)

- [ ] **Step 3: Smoke test Docker image**

```bash
docker run --rm paysense-producer:dev --rows 3 --fraud-rate 0.5
```

Expected: 3 JSON lines printed to stdout.

- [ ] **Step 4: Update `docker-compose.yml`**

```yaml
version: "3.9"

services:
  producer:
    build: ./producer
    image: paysense-producer:dev
    command: ["--rows", "100", "--fraud-rate", "0.1", "--tps", "10"]

  consumer:
    build: ./consumer
    profiles: ["app"]
```

- [ ] **Step 5: Test via compose**

```bash
docker compose run --rm producer --rows 5 --fraud-rate 0.2
```

Expected: 5 JSON transactions.

- [ ] **Step 6: Commit**

```bash
git add producer/Dockerfile docker-compose.yml
git commit -m "feat: add multi-stage producer Dockerfile and compose integration (ep02)"
```

---

## Task 6: ML Data Generator (Episode 03)

**Files:**
- Create: `ml/data_generator.py`

> Note: `ml/` re-uses `producer`'s generator. Install producer as a local dependency so we don't duplicate code.

- [ ] **Step 1: Install ml dependencies**

```bash
cd ml
pip install -r requirements.txt
pip install -e ../producer  # reuse Transaction model + generator
```

- [ ] **Step 2: Create `ml/data_generator.py`**

```python
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
    print(f"Generated {len(records)} rows → {args.output}")
```

- [ ] **Step 3: Generate training data**

```bash
cd ml
python data_generator.py --rows 50000 --fraud-rate 0.1 --output data/transactions.jsonl
```

Expected: `Generated 50000 rows → data/transactions.jsonl`

- [ ] **Step 4: Add data/ to .gitignore**

Append to `.gitignore`:

```
ml/data/
```

- [ ] **Step 5: Commit**

```bash
git add ml/data_generator.py .gitignore
git commit -m "feat: add ml data generator (reuses producer Transaction model) (ep03)"
```

---

## Task 7: Feature Engineering + XGBoost Training (Episode 03)

**Files:**
- Create: `ml/train.py`

- [ ] **Step 1: Create `ml/train.py`**

```python
"""Train XGBoost fraud classifier and log to MLflow."""
import json
import argparse
from pathlib import Path

import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import precision_score, recall_score, f1_score, roc_auc_score
from xgboost import XGBClassifier
from imblearn.over_sampling import SMOTE


CATEGORICAL_COLS = ["currency", "merchant_category", "card_type", "merchant_country"]
FEATURE_COLS = [
    "amount", "customer_age", "is_international",
    "hour_of_day", "is_weekend", "amount_zscore",
    "currency_enc", "merchant_category_enc", "card_type_enc", "merchant_country_enc",
]


def load_data(path: str) -> pd.DataFrame:
    records = []
    with open(path) as f:
        for line in f:
            records.append(json.loads(line))
    return pd.DataFrame(records)


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["hour_of_day"] = df["timestamp"].dt.hour
    df["is_weekend"] = df["timestamp"].dt.dayofweek.isin([5, 6]).astype(int)
    df["amount_zscore"] = (df["amount"] - df["amount"].mean()) / df["amount"].std()
    df["is_international"] = df["is_international"].astype(int)

    for col in CATEGORICAL_COLS:
        le = LabelEncoder()
        df[f"{col}_enc"] = le.fit_transform(df[col].astype(str))

    # Log encoding maps so consumer can reproduce identical encodings at inference time
    _amount_mean = float(df["amount"].mean())
    _amount_std = float(df["amount"].std())

    return df, _amount_mean, _amount_std


def train(data_path: str, params: dict) -> None:
    mlflow.set_experiment("paysense-fraud-detector")

    df = load_data(data_path)
    df, amount_mean, amount_std = engineer_features(df)

    X = df[FEATURE_COLS].values
    y = df["is_fraud"].astype(int).values

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    smote = SMOTE(random_state=42)
    X_train_res, y_train_res = smote.fit_resample(X_train, y_train)

    with mlflow.start_run():
        mlflow.log_params(params)
        mlflow.log_param("n_train", len(X_train_res))
        mlflow.log_param("n_test", len(X_test))
        mlflow.log_param("features", FEATURE_COLS)

        model = XGBClassifier(
            n_estimators=params.get("n_estimators", 100),
            max_depth=params.get("max_depth", 5),
            learning_rate=params.get("learning_rate", 0.1),
            scale_pos_weight=1,
            eval_metric="logloss",
            random_state=42,
        )
        model.fit(X_train_res, y_train_res)

        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1]

        metrics = {
            "precision": precision_score(y_test, y_pred),
            "recall": recall_score(y_test, y_pred),
            "f1": f1_score(y_test, y_pred),
            "auc_roc": roc_auc_score(y_test, y_prob),
        }
        mlflow.log_metrics(metrics)
        # Log normalization stats so consumer can reproduce identical amount_zscore at inference
        mlflow.log_params({"amount_mean": amount_mean, "amount_std": amount_std})

        feature_importance = dict(zip(FEATURE_COLS, model.feature_importances_.tolist()))
        mlflow.log_dict(feature_importance, "feature_importance.json")

        # Log LabelEncoder category order so consumer uses identical integer mapping
        # LabelEncoder sorts alphabetically — document canonical orders here
        # currency: CAD=0, EUR=1, GBP=2, USD=3
        # merchant_category: crypto=0, e-commerce=1, electronics=2, gambling=3, grocery=4, healthcare=5, luxury=6, restaurant=7, retail=8, transport=9, wire-transfer=10
        # card_type: Amex=0, Discover=1, Mastercard=2, Visa=3
        # merchant_country: AU=0, BR=1, CA=2, DE=3, GB=4, NG=5, PK=6, RO=7, UA=8, US=9
        mlflow.log_dict({
            "currency": {"CAD": 0, "EUR": 1, "GBP": 2, "USD": 3},
            "merchant_category": {"crypto": 0, "e-commerce": 1, "electronics": 2, "gambling": 3, "grocery": 4, "healthcare": 5, "luxury": 6, "restaurant": 7, "retail": 8, "transport": 9, "wire-transfer": 10},
            "card_type": {"Amex": 0, "Discover": 1, "Mastercard": 2, "Visa": 3},
            "merchant_country": {"AU": 0, "BR": 1, "CA": 2, "DE": 3, "GB": 4, "NG": 5, "PK": 6, "RO": 7, "UA": 8, "US": 9},
        }, "encoding_maps.json")

        mlflow.sklearn.log_model(model, "model")

        print(f"Metrics: {metrics}")
        print(f"Run ID: {mlflow.active_run().info.run_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="data/transactions.jsonl")
    parser.add_argument("--n-estimators", type=int, default=100)
    parser.add_argument("--max-depth", type=int, default=5)
    parser.add_argument("--learning-rate", type=float, default=0.1)
    args = parser.parse_args()

    params = {
        "n_estimators": args.n_estimators,
        "max_depth": args.max_depth,
        "learning_rate": args.learning_rate,
    }
    train(args.data, params)
```

- [ ] **Step 2: Start MLflow UI in Docker Compose**

Add to `docker-compose.yml`:

```yaml
  mlflow:
    image: python:3.12-slim
    command: >
      sh -c "pip install mlflow==2.12.* && mlflow server
        --backend-store-uri sqlite:///mlflow/mlflow.db
        --default-artifact-root /mlflow/artifacts
        --host 0.0.0.0 --port 5000"
    ports:
      - "5000:5000"
    volumes:
      - mlflow-data:/mlflow

volumes:
  mlflow-data:
```

- [ ] **Step 3: Start MLflow**

```bash
docker compose up mlflow -d
```

Expected: MLflow UI accessible at `http://localhost:5000`

- [ ] **Step 4: Run first training experiment**

```bash
cd ml
MLFLOW_TRACKING_URI=http://localhost:5000 python train.py \
  --data data/transactions.jsonl \
  --n-estimators 100 --max-depth 5 --learning-rate 0.1
```

Expected output:
```
Metrics: {'precision': ~0.9, 'recall': ~0.85, 'f1': ~0.87, 'auc_roc': ~0.97}
Run ID: <some-uuid>
```

- [ ] **Step 5: Run second experiment (different hyperparams)**

```bash
cd ml
MLFLOW_TRACKING_URI=http://localhost:5000 python train.py \
  --data data/transactions.jsonl \
  --n-estimators 200 --max-depth 7 --learning-rate 0.05
```

- [ ] **Step 6: Verify in MLflow UI**

Open `http://localhost:5000` → Experiments → `paysense-fraud-detector` → verify 2 runs with metrics logged.

- [ ] **Step 7: Commit**

```bash
git add ml/train.py docker-compose.yml
git commit -m "feat: XGBoost training script with MLflow tracking + SMOTE (ep03)"
```

---

## Task 8: MLflow Model Registry (Episode 04)

**Files:**
- Create: `ml/register_model.py`

- [x] **Step 1: Create `ml/register_model.py`**

```python
"""Register the best MLflow run to the Model Registry and promote to Production."""
import argparse
import mlflow
from mlflow.tracking import MlflowClient


MODEL_NAME = "paysense-fraud-detector"


def register_best_model(experiment_name: str, metric: str) -> str:
    client = MlflowClient()
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment is None:
        raise ValueError(f"Experiment '{experiment_name}' not found")

    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=[f"metrics.{metric} DESC"],
        max_results=1,
    )
    if not runs:
        raise ValueError("No runs found in experiment")

    best_run = runs[0]
    run_id = best_run.info.run_id
    artifact_uri = f"runs:/{run_id}/model"

    print(f"Best run: {run_id} | {metric}={best_run.data.metrics[metric]:.4f}")
    print(f"Registering as '{MODEL_NAME}'...")

    model_version = mlflow.register_model(artifact_uri, MODEL_NAME)
    client.transition_model_version_stage(
        name=MODEL_NAME,
        version=model_version.version,
        stage="Production",
    )

    print(f"Registered version {model_version.version} → Production")
    print(f"Load URI: models:/{MODEL_NAME}/Production")
    return f"models:/{MODEL_NAME}/Production"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment", default="paysense-fraud-detector")
    parser.add_argument("--metric", default="f1")
    args = parser.parse_args()

    register_best_model(args.experiment, args.metric)
```

- [x] **Step 2: Register best model**

```bash
cd ml
MLFLOW_TRACKING_URI=http://localhost:5000 python register_model.py \
  --experiment paysense-fraud-detector --metric f1
```

Expected:
```
Best run: <uuid> | f1=0.XXXX
Registering as 'paysense-fraud-detector'...
Registered version 1 → Production
Load URI: models:/paysense-fraud-detector/Production
```

- [x] **Step 3: Verify in MLflow UI**

Open `http://localhost:5000` → Models → `paysense-fraud-detector` → Version 1 should show `Production` stage.

- [x] **Step 4: Commit**

```bash
git add ml/register_model.py
git commit -m "feat: MLflow model registration + Production stage promotion (ep04)"
```

---

## Task 9: Consumer Model Loader (Episode 04)

**Files:**
- Create: `consumer/src/model_loader.py`
- Create: `consumer/tests/test_model_loader.py`

- [ ] **Step 1: Write failing tests**

Create `consumer/tests/test_model_loader.py`:

```python
import os
import pytest
import numpy as np


def test_model_loader_requires_model_uri_env(monkeypatch):
    monkeypatch.delenv("MODEL_URI", raising=False)
    from consumer.model_loader import ModelLoader
    with pytest.raises(ValueError, match="MODEL_URI"):
        ModelLoader()


def test_model_loader_loads_and_predicts(tmp_path, monkeypatch):
    """Train a tiny local model and verify loader can predict on it."""
    import mlflow
    import mlflow.sklearn
    from sklearn.ensemble import RandomForestClassifier
    import json

    mlflow.set_tracking_uri(f"sqlite:///{tmp_path}/mlflow.db")
    mlflow.set_experiment("test-loader")

    X = np.array([[100.0, 25, 0, 14, 0, 0.5, 0, 1, 0, 2],
                  [5000.0, 45, 1, 2, 1, 3.1, 1, 3, 2, 5]])
    y = np.array([0, 1])

    with mlflow.start_run() as run:
        model = RandomForestClassifier(n_estimators=2, random_state=0)
        model.fit(X, y)
        mlflow.sklearn.log_model(model, "model")
        run_id = run.info.run_id

    model_uri = f"runs:/{run_id}/model"
    monkeypatch.setenv("MODEL_URI", model_uri)
    monkeypatch.setenv("MLFLOW_TRACKING_URI", f"sqlite:///{tmp_path}/mlflow.db")

    from importlib import reload
    import consumer.model_loader as ml_mod
    reload(ml_mod)

    loader = ml_mod.ModelLoader()
    transaction_features = [100.0, 25, 0, 14, 0, 0.5, 0, 1, 0, 2]
    label, confidence = loader.predict(transaction_features)

    assert label in (True, False)
    assert 0.0 <= confidence <= 1.0
```

- [ ] **Step 2: Run — verify FAIL**

```bash
cd consumer
pip install -e .
python -m pytest tests/test_model_loader.py -v
```

Expected: `ModuleNotFoundError: No module named 'consumer.model_loader'`

- [ ] **Step 3: Implement `consumer/src/model_loader.py`**

```python
import os
import numpy as np
import mlflow.pyfunc


class ModelLoader:
    def __init__(self) -> None:
        model_uri = os.environ.get("MODEL_URI")
        if not model_uri:
            raise ValueError("MODEL_URI environment variable is required")

        tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
        if tracking_uri:
            mlflow.set_tracking_uri(tracking_uri)

        # Load as sklearn model so predict_proba is available (mlflow.sklearn.log_model was used in training)
        self._model = mlflow.sklearn.load_model(model_uri)

    def predict(self, features: list[float]) -> tuple[bool, float]:
        x = np.array([features])
        prob_fraud = float(self._model.predict_proba(x)[0, 1])
        label = prob_fraud >= 0.5
        confidence = prob_fraud if label else 1.0 - prob_fraud
        return label, round(confidence, 4)
```

- [ ] **Step 4: Run tests — verify PASS**

```bash
python -m pytest tests/test_model_loader.py -v
```

Expected: 2 passed

- [ ] **Step 5: Smoke test with real MLflow registry**

```bash
MODEL_URI="models:/paysense-fraud-detector/Production" \
MLFLOW_TRACKING_URI=http://localhost:5000 \
python -c "
from consumer.model_loader import ModelLoader
loader = ModelLoader()
features = [142.5, 34, 0, 14, 0, 0.2, 0, 1, 3, 0]
label, confidence = loader.predict(features)
print(f'is_fraud={label}, confidence={confidence}')
"
```

Expected: prints `is_fraud=True/False, confidence=0.XXXX`

- [ ] **Step 6: Document `MODEL_URI` pattern**

Append to `README.md`:

```markdown
## MODEL_URI Pattern

| Phase | MODEL_URI value |
|-------|----------------|
| Ep 04–08 (local) | `models:/paysense-fraud-detector/Production` (local MLflow) |
| Ep 09+ (AWS) | `s3://paysense-mlflow-artifacts/<run-id>/artifacts/model` |

Set `MLFLOW_TRACKING_URI=http://localhost:5000` for local.
Set `MLFLOW_TRACKING_URI=sqlite:///mlflow.db` for embedded.
```

- [ ] **Step 7: Commit**

```bash
git add consumer/src/model_loader.py consumer/tests/test_model_loader.py README.md
git commit -m "feat: consumer model loader reads MODEL_URI env var, supports local + S3 URIs (ep04)"
```

---

## Phase 1 Complete Verification

- [ ] **Run all tests**

```bash
cd producer && python -m pytest tests/ -v
cd ../consumer && python -m pytest tests/ -v
```

Expected: All green.

- [ ] **Full CLI smoke test**

```bash
cd producer
python -m producer.cli --rows 10 --fraud-rate 0.3 --output stdout | python -c "
import sys, json
lines = sys.stdin.read().strip().split('\n')
for l in lines:
    t = json.loads(l)
    print(t['is_fraud'], t['amount'], t['merchant_category'])
"
```

- [ ] **Docker image smoke test**

```bash
docker build -t paysense-producer:dev producer/
docker run --rm paysense-producer:dev --rows 5 --fraud-rate 0.5
```

- [ ] **MLflow UI verification**

Open `http://localhost:5000`:
- Experiment `paysense-fraud-detector` shows ≥2 runs
- Model Registry shows `paysense-fraud-detector` version 1 in `Production`

- [ ] **Final commit and tag**

```bash
git add .
git commit -m "chore: phase 1 complete — data + ML foundation (ep01-04)"
git tag phase1-complete
git push origin master --tags
```

---

## Self-Review

| Spec Requirement | Task |
|---|---|
| Monorepo folder structure | Task 1 |
| Transaction Pydantic model with all fields | Task 2 |
| `generate_transaction(is_fraud)` realistic fraud differentiation | Task 3 |
| CLI: `--rows`, `--fraud-rate`, `--tps`, `--output` | Task 4 |
| Rate limiter for `--tps` | Task 4 (Step 3, `delay` logic) |
| Unit tests for transaction generation | Tasks 2, 3, 4 |
| Producer Dockerfile (multi-stage) | Task 5 |
| ML: 50k row dataset, 10% fraud | Task 6 |
| Feature engineering: categoricals, hour_of_day, is_weekend, amount_zscore | Task 7 |
| Class imbalance handling (SMOTE) | Task 7 |
| XGBoost classifier | Task 7 |
| MLflow: params, metrics, feature importance | Task 7 |
| Second experiment for comparison | Task 7 Step 5 |
| MLflow server in docker-compose | Task 7 Step 2 |
| Register best model in MLflow Registry | Task 8 |
| Promote to Production stage | Task 8 |
| `consumer/model_loader.py` with `MODEL_URI` env var | Task 9 |
| Model loader works with local path | Task 9 |
| `MODEL_URI` env var documentation | Task 9 Step 6 |
| docker-compose.yml grows across tasks | Tasks 1, 5, 7 |
| `.gitignore` (Python, Docker, Terraform, `.env`, MLflow) | Task 1 |
| `README.md` with architecture + episode index | Task 1 |
