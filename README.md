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

## MODEL_URI Pattern

| Phase | MODEL_URI value |
|-------|-----------------|
| Ep 04-08 (local) | `models:/paysense-fraud-detector/Production` (local MLflow) |
| Ep 09+ (AWS) | `s3://paysense-mlflow-artifacts/<run-id>/artifacts/model` |

Set `MLFLOW_TRACKING_URI=http://localhost:5000` for local.
Set `MLFLOW_TRACKING_URI=sqlite:///mlflow.db` for embedded.
