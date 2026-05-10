# PaySense — Design Spec

**Date:** 2026-05-08  
**Project:** PaySense — Real-time Transaction Anomaly Detection  
**Series:** YouTube DevOps series (15 episodes, intermediate audience)

---

## 1. Project Overview

PaySense is a high-volume, real-time transaction fraud detection pipeline built to showcase a production-grade DevOps project on YouTube. The codebase runs locally in Docker Compose and deploys identically to AWS EKS via GitOps (ArgoCD + Helm + Terraform). Infrastructure is ephemeral — spun up for recording sessions and destroyed after, keeping AWS costs near zero.

**Goal:** A resume-ready, interview-demonstrable DevOps project that covers ML pipelines, streaming data, Kubernetes, CI/CD, IaC, and observability in one cohesive system.

---

## 2. Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.12 |
| Data generation | Faker, click (CLI) |
| ML framework | scikit-learn, XGBoost |
| ML tracking | MLflow (local + S3 artifact store) |
| Message broker | Apache Kafka (KRaft mode) |
| Storage | Amazon DynamoDB (free tier) / DynamoDB Local (Docker) |
| Results API | FastAPI |
| Containerization | Docker, Docker Compose |
| Container registry | Amazon ECR |
| Orchestration | Kubernetes (Amazon EKS — 3× t3.small) |
| Kafka on K8s | Strimzi Operator |
| GitOps | ArgoCD |
| IaC | Terraform |
| Package manager (K8s) | Helm |
| CI/CD | GitHub Actions (OIDC — no hardcoded AWS keys) |
| Monitoring | Prometheus (kube-prometheus-stack), Grafana |
| Secrets | Kubernetes Secrets + IRSA (IAM Roles for Service Accounts) |
| Artifact store | Amazon S3 |

---

## 3. System Architecture

```
Producer CLI ──▶ Kafka (transactions topic) ──▶ Consumer ──▶ Kafka (results topic)
                                                    │
                                                    ├──▶ DynamoDB (persist result record)
                                                    └──▶ FastAPI (query fraud results)

MLflow (local training) ──▶ S3 artifact store ◀── Consumer (loads model at startup)

Prometheus ──▶ Grafana (dashboards + alerts)
               scrapes: producer, consumer, Kafka JMX exporter

GitHub push ──▶ GitHub Actions ──▶ ECR push ──▶ ArgoCD sync ──▶ EKS
                                                 Helm charts in Git
```

### Data Flow Detail

1. **Producer CLI** generates synthetic bank transaction records (Faker) and publishes to Kafka topic `transactions` as JSON.
2. **Kafka** (KRaft mode, no Zookeeper) brokers messages. Local: Docker. AWS: Strimzi operator on EKS.
3. **Consumer** reads from `transactions` topic, loads XGBoost model from MLflow S3 artifact URI, classifies each transaction as `fraud` or `legit` with a confidence score.
4. **Consumer** writes the result record to:
   - Kafka topic `results` (downstream consumers can subscribe)
   - DynamoDB table `transactions` (persistent store)
5. **FastAPI** (part of consumer service) exposes `GET /transactions?fraud=true&limit=100` querying DynamoDB.
6. **Prometheus** scrapes `/metrics` endpoints on producer and consumer, plus Kafka JMX exporter.
7. **Grafana** visualizes: TPS, fraud detection rate, classification latency, DynamoDB write count, Kafka consumer lag.

### Local ↔ Cloud Parity

| Component | Local (Docker Compose) | AWS (EKS) |
|---|---|---|
| Kafka | `confluentinc/cp-kafka` (KRaft) | Strimzi KafkaCluster CRD |
| DynamoDB | `amazon/dynamodb-local` | AWS DynamoDB (managed) |
| MLflow artifacts | Local filesystem | S3 bucket |
| App images | Built locally | ECR-hosted |
| Secrets | `.env` file | K8s Secrets + IRSA |

---

## 4. Monorepo Structure

```
paysense/
├── producer/                  # Transaction generator + Kafka producer
│   ├── src/
│   ├── Dockerfile
│   └── pyproject.toml
├── consumer/                  # Fraud classifier + FastAPI + DynamoDB writer
│   ├── src/
│   ├── Dockerfile
│   └── pyproject.toml
├── ml/                        # Model training scripts + MLflow
│   ├── train.py
│   ├── data_generator.py      # shared with producer, generates training data
│   └── requirements.txt
├── infra/
│   ├── terraform/             # VPC, EKS, ECR, S3, DynamoDB, IAM/IRSA
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   ├── outputs.tf
│   │   └── modules/
│   └── helm/                  # Helm charts for all services
│       ├── producer/
│       ├── consumer/
│       └── strimzi/
├── k8s/                       # ArgoCD Application manifests
│   └── apps/
├── .github/
│   └── workflows/
│       ├── ci-producer.yml
│       └── ci-consumer.yml
├── docker-compose.yml         # Full local stack
├── docker-compose.dev.yml     # Dev overrides (hot reload)
└── docs/
    └── superpowers/
        └── specs/
```

---

## 5. Transaction Data Schema

```json
{
  "transaction_id": "uuid4",
  "timestamp": "ISO8601",
  "amount": 142.50,
  "currency": "USD",
  "merchant_name": "Amazon",
  "merchant_category": "e-commerce",
  "merchant_country": "US",
  "card_last4": "4242",
  "card_type": "Visa",
  "customer_id": "uuid4",
  "customer_age": 34,
  "customer_location": "New York, US",
  "is_international": false,
  "is_fraud": true
}
```

**Result record stored in DynamoDB:**

```json
{
  "transaction_id": "uuid4",         // PK
  "timestamp": "ISO8601",
  "amount": 142.50,
  "merchant_name": "Amazon",
  "is_fraud": true,
  "confidence_score": 0.94,
  "processing_latency_ms": 12,
  "model_version": "1.2"
}
```

---

## 6. Infrastructure Constraints

- **AWS instances:** 3× t3.small (2 vCPU, 2GB RAM each)
- **Cost model:** Ephemeral. `terraform apply` before recording, `terraform destroy` after. EKS control plane ($0.10/hr) + 3× t3.small ($0.063/hr total) = ~$0.49 for a 3-hour session.
- **Always-on AWS services:** DynamoDB (free tier: 25GB, 25 RCU/WCU), S3 (free tier: 5GB), ECR (free tier: 500MB)
- **Terraform remote state:** S3 bucket + DynamoDB lock table (both free tier)
- **GitHub Actions auth:** OIDC (no long-lived AWS keys stored in GitHub Secrets)

---

## 7. YouTube Episode Task Breakdown

---

### PHASE 1 — Data & ML (Episodes 1–4)
> 100% local. Zero AWS cost. Docker required, no Kubernetes yet.

---

#### Episode 01 — Project Intro & Repo Setup

**Goal:** Viewers understand what PaySense is, why it exists, and can clone a working skeleton.

- [ ] Record series intro — what we're building, full architecture walkthrough (diagram)
- [ ] Create GitHub repository (public, monorepo)
- [ ] Initialize monorepo folder structure (`producer/`, `consumer/`, `ml/`, `infra/`, `k8s/`, `.github/`)
- [ ] Add root `README.md` with architecture diagram and episode index
- [ ] Add `.gitignore` (Python, Docker, Terraform, `.env`, `.superpowers/`)
- [ ] Add `pyproject.toml` per service (or `requirements.txt` for simplicity)
- [ ] Add `docker-compose.yml` skeleton (services stubbed, not yet functional)
- [ ] Verify Python 3.12 + Docker installed in dev environment
- [ ] First commit and push to GitHub
- [ ] Show viewers how to clone and verify setup

**Deliverable:** Cloneable repo skeleton. Every viewer starts from the same baseline.

---

#### Episode 02 — Building the Transaction Generator

**Goal:** A CLI tool that generates realistic bank transaction data with configurable fraud rate and throughput.

- [ ] Install Faker, click, pydantic in `producer/`
- [ ] Define `Transaction` pydantic model matching the schema (all fields)
- [ ] Implement `generate_transaction(is_fraud: bool)` — realistic field values per fraud type (high amount, international, unusual merchant)
- [ ] Implement CLI with click: `--rows`, `--fraud-rate` (0.0–1.0), `--tps` (transactions per second), `--output` (stdout/file)
- [ ] Add rate limiter (sleep between batches to honour `--tps`)
- [ ] Write unit tests for transaction generation (valid fields, fraud flag distribution)
- [ ] Add `Dockerfile` for producer (multi-stage, slim image)
- [ ] Add producer service to `docker-compose.yml` (standalone, no Kafka yet)
- [ ] Demo: generate 1000 rows, 10% fraud, pipe to `jq` to inspect
- [ ] Commit and push

**Deliverable:** `docker run paysense-producer --rows 1000 --fraud-rate 0.1 --tps 50` produces valid JSON transaction stream.

---

#### Episode 03 — Training the Fraud Detection Model

**Goal:** Train an XGBoost classifier on synthetic data, track experiments with MLflow.

- [ ] Add `ml/` dependencies: scikit-learn, xgboost, mlflow, pandas, imbalanced-learn
- [ ] Generate large synthetic training dataset using the generator (50k rows, 10% fraud)
- [ ] Implement feature engineering:
  - [ ] Encode categorical fields (merchant_category, card_type, currency)
  - [ ] Add derived features: hour_of_day, is_weekend, amount_zscore
- [ ] Handle class imbalance (SMOTE or class_weight)
- [ ] Train baseline model: XGBoost classifier
- [ ] Log to MLflow: params, metrics (precision, recall, F1, AUC-ROC), feature importance
- [ ] Run second experiment (different hyperparameters or Isolation Forest) — compare in MLflow UI
- [ ] Add MLflow server to `docker-compose.yml` (backend: sqlite, artifact root: local)
- [ ] Demo: open MLflow UI, compare runs, explain metrics to viewers
- [ ] Commit and push

**Deliverable:** Two or more tracked MLflow experiments. Best model visible in MLflow UI.

---

#### Episode 04 — MLflow Model Registry & S3 Artifacts

**Goal:** Register the best model, promote it to Production stage, configure S3 as artifact store, verify consumer can load model from S3 URI.

- [ ] MLflow artifact root stays on **local filesystem** for this episode (S3 migration happens in Ep 09 when Terraform creates the bucket)
- [ ] Register best model in MLflow Model Registry: name `paysense-fraud-detector`
- [ ] Promote model version to `Production` stage in registry
- [ ] Write `consumer/src/model_loader.py`: loads model via `mlflow.pyfunc.load_model(model_uri)` — URI is an env var (`MODEL_URI`), supports both local path and S3 URI transparently
- [ ] Test model loader standalone: load model from local MLflow path, run inference on 10 sample transactions
- [ ] Document `MODEL_URI` env var pattern: local path for Ep 04–08, S3 URI from Ep 09 onwards
- [ ] Commit and push

**Deliverable:** Model registered in MLflow, loadable via `MODEL_URI` env var. Consumer model loader works with local artifact path.

---

### PHASE 2 — Local Pipeline (Episodes 5–8)
> Full stack in Docker Compose. Zero AWS cost. End of phase: complete working local system.

---

#### Episode 05 — Kafka with Docker Compose

**Goal:** Kafka running locally in KRaft mode (no Zookeeper), with Kafka UI for visualization.

- [ ] Add Kafka service to `docker-compose.yml` (KRaft mode — single broker for local dev)
- [ ] Add Kafka UI service (`provectuslabs/kafka-ui`) for topic inspection
- [ ] Configure topics: `transactions` (3 partitions), `results` (3 partitions)
- [ ] Add topic init container (creates topics on startup via kafka-topics.sh)
- [ ] Write `scripts/kafka-test.py` — produce 5 test messages, consume and print them
- [ ] Verify Kafka UI shows topics and messages
- [ ] Add health check to Kafka service in Docker Compose
- [ ] Document Kafka connection strings for local vs cloud (env var pattern)
- [ ] Demo: produce test messages, show in Kafka UI, consume in terminal
- [ ] Commit and push

**Deliverable:** `docker compose up kafka kafka-ui` → Kafka running, topics visible in UI at `localhost:8080`.

---

#### Episode 06 — Producer App: Kafka Integration

**Goal:** Producer CLI sends generated transactions to the Kafka `transactions` topic.

- [ ] Add `confluent-kafka` (or `kafka-python`) dependency to producer
- [ ] Implement `KafkaProducer` class: configurable bootstrap servers, topic, serializer
- [ ] Wire generator output into Kafka producer (each transaction → Kafka message, key = `transaction_id`)
- [ ] Add `--mode` flag: `kafka` (stream to Kafka) vs `file` (write to JSON file)
- [ ] Implement graceful shutdown (SIGTERM handler flushes producer queue)
- [ ] Add Prometheus metrics to producer: `transactions_produced_total`, `produce_errors_total`
- [ ] Expose `/metrics` endpoint (Prometheus format) on producer HTTP port
- [ ] Add health/readiness endpoint (`/health`) for K8s probes later
- [ ] Update `docker-compose.yml`: producer depends_on Kafka healthy
- [ ] Demo: run producer in kafka mode, watch messages appear in Kafka UI
- [ ] Commit and push

**Deliverable:** Producer streams transactions to Kafka at configurable TPS. Metrics endpoint live.

---

#### Episode 07 — Consumer App: Fraud Classifier

**Goal:** Consumer reads from Kafka, classifies each transaction using the MLflow model, writes result to DynamoDB, publishes to results topic.

- [ ] Add dependencies to consumer: `confluent-kafka`, `mlflow`, `boto3`, `xgboost`
- [ ] Add DynamoDB Local to `docker-compose.yml` (`amazon/dynamodb-local`)
- [ ] Add init script to create DynamoDB table `transactions` (PK: `transaction_id`)
- [ ] Implement `KafkaConsumer` class: poll loop, deserialize JSON, commit offset after processing
- [ ] Implement `FraudClassifier`: loads model from `MODEL_URI` env var at startup, exposes `predict(transaction) -> (label, confidence)`
- [ ] Implement `DynamoDBWriter`: writes result record (transaction_id, is_fraud, confidence_score, processing_latency_ms, model_version, timestamp)
- [ ] Implement `ResultsProducer`: publishes result JSON to `results` topic
- [ ] Wire all together in consumer main loop
- [ ] Add Prometheus metrics: `transactions_consumed_total`, `fraud_detected_total`, `classification_latency_seconds`
- [ ] Add `/health` and `/metrics` endpoints
- [ ] Update `docker-compose.yml`: consumer depends_on Kafka + DynamoDB Local
- [ ] Demo: run full stack, produce 200 transactions, watch fraud labels in logs
- [ ] Commit and push

**Deliverable:** End-to-end classification running. Fraud labels written to DynamoDB.

---

#### Episode 08 — FastAPI Results API + Full Local Demo

**Goal:** Queryable REST API over fraud results in DynamoDB. Full Docker Compose demo — everything working together.

- [ ] Add `fastapi`, `uvicorn`, `boto3` to consumer dependencies
- [ ] Implement `GET /transactions` — query DynamoDB, supports `?fraud=true`, `?limit=N`, `?from_timestamp=`
- [ ] Implement `GET /transactions/{transaction_id}` — single record lookup
- [ ] Implement `GET /stats` — aggregate: total processed, fraud count, fraud rate, avg confidence
- [ ] Add OpenAPI docs auto-generated by FastAPI (visible at `/docs`)
- [ ] Run FastAPI alongside consumer in same Docker container (separate thread or process)
- [ ] Write integration test: produce N transactions, assert results appear in API
- [ ] Update `docker-compose.yml`: expose FastAPI port (8000)
- [ ] Full demo recording:
  - [ ] `docker compose up` — all services start
  - [ ] Run producer: 500 transactions, 15% fraud, 100 TPS
  - [ ] Watch Kafka UI — messages flowing
  - [ ] Query `GET /transactions?fraud=true` — show flagged transactions
  - [ ] `GET /stats` — live aggregate numbers
- [ ] Commit and push

**Deliverable:** Complete working local system. Impressive demo moment for YouTube.

---

### PHASE 3 — DevOps & Cloud (Episodes 9–13)
> AWS spins up here. Record → destroy. ~$0.49/session.

---

#### Episode 09 — Terraform: Provision AWS Infrastructure

**Goal:** All AWS infrastructure defined as code, provisionable and destroyable in one command.

- [ ] Install Terraform, configure AWS CLI with IAM user/role
- [ ] Write Terraform module: **VPC** (2 AZs, public + private subnets, NAT gateway)
- [ ] Write Terraform module: **EKS cluster** (3× t3.small nodes, managed node group)
- [ ] Write Terraform resource: **ECR** repositories (producer, consumer)
- [ ] Write Terraform resource: **S3 bucket** (MLflow artifacts — new bucket, created by Terraform here in Ep 09)
- [ ] After `terraform apply`: copy local MLflow model artifact to S3 bucket, update `MODEL_URI` env var in consumer Helm values to S3 URI
- [ ] Write Terraform resource: **DynamoDB table** `transactions` (PAY_PER_REQUEST billing)
- [ ] Write Terraform module: **IAM + IRSA** (K8s service accounts → IAM roles → S3 + DynamoDB permissions)
- [ ] Write Terraform resource: **Remote state** (S3 bucket + DynamoDB lock table for state)
- [ ] Add `variables.tf` for region, cluster name, node count
- [ ] Add `outputs.tf` for cluster endpoint, ECR URLs, DynamoDB table name
- [ ] Run `terraform plan` on camera — walk through every resource
- [ ] Run `terraform apply` — cluster live in ~12 min
- [ ] Verify: `kubectl get nodes` shows 3× t3.small
- [ ] Run `terraform destroy` at end of recording
- [ ] Commit and push

**Deliverable:** EKS cluster + all AWS resources provisioned and destroyable via Terraform.

---

#### Episode 10 — Helm Charts + First EKS Deploy

**Goal:** Apps running on EKS via Helm. Manual deploy to verify before GitOps automation.

- [ ] Build producer and consumer Docker images
- [ ] Push images to ECR (`docker push`)
- [ ] Write Helm chart for **producer**: Deployment, ConfigMap, ServiceAccount, Service, HPA skeleton
- [ ] Write Helm chart for **consumer**: Deployment, ConfigMap, ServiceAccount, Service, HPA skeleton
- [ ] Configure `values.yaml` per environment (local vs prod): image tag, replicas, resource limits
- [ ] Set resource limits for t3.small (producer: 256m CPU / 256Mi RAM, consumer: 512m CPU / 512Mi RAM)
- [ ] Add liveness + readiness probes (use `/health` endpoints)
- [ ] Add K8s Secrets for `MODEL_URI`, Kafka bootstrap, DynamoDB table name
- [ ] Add IRSA annotation to ServiceAccounts (links to IAM roles from Terraform)
- [ ] `helm install` producer and consumer to EKS
- [ ] Verify pods running: `kubectl get pods`, check logs
- [ ] Demo: port-forward consumer, hit `GET /transactions` from local machine
- [ ] Commit and push

**Deliverable:** Apps running on EKS via Helm. API accessible via port-forward.

---

#### Episode 11 — Strimzi: Kafka on Kubernetes

**Goal:** Replace external Kafka with Strimzi-managed Kafka cluster inside EKS.

- [ ] Install Strimzi operator via Helm (`helm install strimzi strimzi/strimzi-kafka-operator`)
- [ ] Write `KafkaCluster` CRD YAML (1 broker + 1 ZooKeeper node — ZooKeeper mode for Strimzi stability on a demo cluster; KRaft requires Strimzi 0.39+ in preview)
- [ ] Write `KafkaTopic` CRD YAMLs for `transactions` and `results` topics
- [ ] Apply CRDs: `kubectl apply -f kafka-cluster.yaml`
- [ ] Wait for Kafka pods to be ready (`kubectl wait`)
- [ ] Update producer and consumer Helm `values.yaml`: `KAFKA_BOOTSTRAP_SERVERS` → internal Strimzi service DNS
- [ ] Redeploy producer + consumer pointing to Strimzi Kafka
- [ ] Verify end-to-end: run producer job, check consumer logs, query API
- [ ] Add Kafka cluster resource limits appropriate for t3.small
- [ ] Commit and push (Strimzi manifests in `infra/helm/strimzi/`)

**Deliverable:** Kafka running natively in Kubernetes via Strimzi. No external broker needed.

---

#### Episode 12 — ArgoCD: GitOps Deployments

**Goal:** ArgoCD manages all deployments. Git is the source of truth. No manual `helm install` after this episode.

- [ ] Install ArgoCD on EKS (`kubectl create namespace argocd`, apply ArgoCD install manifest)
- [ ] Expose ArgoCD UI via port-forward or LoadBalancer (port-forward for cost)
- [ ] Log into ArgoCD UI — show viewers the dashboard
- [ ] Write `Application` CRD for producer (points to `infra/helm/producer/` in Git, target namespace `paysense`)
- [ ] Write `Application` CRD for consumer (same pattern)
- [ ] Write `Application` CRD for Strimzi Kafka cluster
- [ ] Apply Application CRDs: ArgoCD syncs automatically
- [ ] Demo self-healing: manually delete a pod, ArgoCD reconciles it back
- [ ] Demo rollout: change replica count in Git → push → ArgoCD auto-syncs
- [ ] Store all ArgoCD Application manifests in `k8s/apps/`
- [ ] Commit and push

**Deliverable:** GitOps live. All deployments controlled via Git commits. Manual kubectl no longer needed.

---

#### Episode 13 — GitHub Actions CI/CD Pipeline

**Goal:** Push code → tests run → image built → pushed to ECR → ArgoCD auto-deploys to EKS.

- [ ] Configure GitHub Actions OIDC for AWS (no hardcoded keys — IAM role with trust policy for GitHub Actions)
- [ ] Write `ci-producer.yml` workflow:
  - [ ] Trigger: push to `main` affecting `producer/**`
  - [ ] Steps: checkout → setup Python → run unit tests → docker build → ECR login → docker push (tag = git SHA)
  - [ ] On success: update `infra/helm/producer/values.yaml` image tag → commit → push
- [ ] Write `ci-consumer.yml` workflow (same pattern for consumer)
- [ ] ArgoCD detects Helm values changed → auto-syncs → rolling deploy
- [ ] Demo full cycle: change producer code → push → watch GitHub Actions → watch ArgoCD sync → verify new pod
- [ ] Add workflow status badges to `README.md`
- [ ] Commit and push

**Deliverable:** Push code → auto deploy to EKS. Full CI/CD pipeline live.

---

### PHASE 4 — Observability (Episodes 14–15)
> The visual payoff. Dashboards make the YouTube thumbnail.

---

#### Episode 14 — Prometheus & Grafana Dashboards

**Goal:** Live metrics from producer, consumer, and Kafka visible in Grafana dashboards.

- [ ] Install `kube-prometheus-stack` Helm chart (includes Prometheus + Grafana + AlertManager)
- [ ] Configure Prometheus `ServiceMonitor` CRDs for producer and consumer (scrape `/metrics` every 15s)
- [ ] Install Kafka JMX Prometheus exporter (Strimzi has built-in support — enable in KafkaCluster CRD)
- [ ] Verify metrics flowing: query PromQL in Prometheus UI
- [ ] Build Grafana dashboard **"PaySense Overview"**:
  - [ ] Panel: Transactions per second (TPS)
  - [ ] Panel: Fraud detection rate (%)
  - [ ] Panel: Classification latency (p50, p95, p99)
  - [ ] Panel: Kafka consumer lag
  - [ ] Panel: DynamoDB write count
  - [ ] Panel: Pod CPU + memory (from kube-metrics-server)
- [ ] Export dashboard JSON to `infra/helm/grafana/dashboards/paysense.json` (as ConfigMap)
- [ ] Add Grafana to local Docker Compose (for local monitoring too)
- [ ] Demo: run producer at high TPS, watch dashboard update live
- [ ] Commit and push

**Deliverable:** Live Grafana dashboard. The YouTube thumbnail shot.

---

#### Episode 15 — Alerting, Full Demo & Teardown

**Goal:** Alerting configured, full end-to-end cloud demo recorded, infrastructure destroyed cleanly.

- [ ] Configure Grafana alert: fraud rate > 20% for 2 minutes → alert fires
- [ ] Configure Grafana alert: Kafka consumer lag > 1000 messages → alert fires
- [ ] Show alert routing (AlertManager → email or Slack webhook)
- [ ] **Full end-to-end cloud demo:**
  - [ ] `terraform apply` — provision everything
  - [ ] ArgoCD syncs all apps automatically
  - [ ] Run producer at 200 TPS, 15% fraud rate
  - [ ] Show Kafka UI (Strimzi metrics)
  - [ ] Show Grafana dashboard updating live
  - [ ] Query `GET /transactions?fraud=true` — show flagged results
  - [ ] `GET /stats` — live aggregate
  - [ ] Trigger fraud rate alert — show it firing in Grafana
- [ ] Review AWS costs incurred (should be under $2 for recording session)
- [ ] `terraform destroy` — tear everything down on camera (shows reproducibility)
- [ ] Series wrap-up: what to add next (Schema Registry, model retraining trigger, multi-region)
- [ ] Commit final state, tag repo `v1.0.0`

**Deliverable:** Complete series done. Resume-ready, interview-demonstrable project.

---

## 8. Key Design Decisions

| Decision | Choice | Reason |
|---|---|---|
| Kafka mode | KRaft (no Zookeeper) | Simpler, modern, fewer containers locally |
| Kafka on K8s | Strimzi operator | Industry standard, great K8s operator demo |
| MLflow in prod | Local training only, S3 artifacts | Keeps K8s clean, no extra stateful service on EKS |
| Database | DynamoDB | Free tier covers all demo load, no infra to manage |
| Auth (GitHub → AWS) | OIDC | No long-lived keys, best practice, great DevOps content |
| Repo structure | Monorepo | Simpler GitHub Actions, easier for viewers to follow |
| Infra lifecycle | Ephemeral (terraform destroy after recording) | Cost control, proves reproducibility |
| K8s node size | 3× t3.small | ~$0.49 per 3-hour session including EKS control plane |

---

## 9. Episode Recording Checklist (reuse each session)

- [ ] `terraform apply` — wait ~12 min for EKS
- [ ] `kubectl get nodes` — verify 3 nodes ready
- [ ] Record episode
- [ ] `git push` any code changes
- [ ] `terraform destroy` — destroy cluster
- [ ] Verify DynamoDB + S3 still intact (always-on free tier services)
