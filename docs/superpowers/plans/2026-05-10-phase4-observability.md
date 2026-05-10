# Phase 4 — Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Prometheus + Grafana monitoring to the local Docker Compose stack and EKS cluster, build a "PaySense Overview" Grafana dashboard, configure alerting for fraud rate and Kafka lag, and complete the full end-to-end recorded demo (Episodes 14–15).

**Architecture:** `kube-prometheus-stack` Helm chart installs Prometheus + Grafana + AlertManager on EKS. `ServiceMonitor` CRDs scrape producer and consumer `/metrics` endpoints. Strimzi's built-in JMX exporter provides Kafka metrics. Grafana dashboard JSON is committed to Git and loaded via ConfigMap so it survives pod restarts. Grafana added to local Docker Compose for local dev monitoring.

**Prerequisites:** Phases 1–3 complete. EKS cluster running. Producer and consumer deployed with `/metrics` endpoints exposed. Strimzi Kafka running.

**Tech Stack:** Prometheus (kube-prometheus-stack), Grafana, AlertManager, Strimzi JMX exporter, prometheus-client (Python), Docker Compose

---

## File Map

| Path | Purpose |
|------|---------|
| `infra/helm/monitoring/servicemonitor-producer.yaml` | ServiceMonitor CRD — scrape producer `/metrics` |
| `infra/helm/monitoring/servicemonitor-consumer.yaml` | ServiceMonitor CRD — scrape consumer `/metrics` |
| `infra/helm/monitoring/paysense-dashboard.json` | Grafana dashboard JSON |
| `infra/helm/monitoring/dashboard-configmap.yaml` | ConfigMap wrapping dashboard JSON for auto-load |
| `infra/helm/monitoring/alerts.yaml` | PrometheusRule — fraud rate + Kafka lag alerts |
| `docker-compose.yml` | Add Prometheus + Grafana services for local monitoring |

---

## Task 1: Grafana in Docker Compose (Episode 14)

**Files:**
- Modify: `docker-compose.yml`
- Create: `infra/helm/monitoring/prometheus-local.yml`

- [ ] **Step 1: Create local Prometheus scrape config**

Create `infra/helm/monitoring/prometheus-local.yml`:

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: "producer"
    static_configs:
      - targets: ["producer:9090"]

  - job_name: "consumer"
    static_configs:
      - targets: ["consumer:9091"]

  # kafka_consumergroup_lag metric is only available on EKS via Strimzi JMX exporter.
  # For local Kafka lag monitoring, add danielqsj/kafka-exporter to docker-compose
  # and uncomment this block:
  # - job_name: "kafka-exporter"
  #   static_configs:
  #     - targets: ["kafka-exporter:9308"]
```

- [ ] **Step 2: Add Prometheus + Grafana to `docker-compose.yml`**

Append under `services:` (before `volumes:`):

```yaml
  prometheus:
    image: prom/prometheus:v2.51.0
    ports:
      - "9090:9090"
    volumes:
      - ./infra/helm/monitoring/prometheus-local.yml:/etc/prometheus/prometheus.yml:ro
    command:
      - "--config.file=/etc/prometheus/prometheus.yml"
      - "--storage.tsdb.path=/prometheus"

  grafana:
    image: grafana/grafana:10.4.0
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: "paysense"
      GF_USERS_ALLOW_SIGN_UP: "false"
    volumes:
      - grafana-data:/var/lib/grafana
    depends_on:
      - prometheus
```

Add `grafana-data:` under `volumes:`.

- [ ] **Step 3: Start monitoring stack**

```bash
docker compose up prometheus grafana -d
```

- [ ] **Step 4: Verify**

Open `http://localhost:3000` (admin / paysense) → Add Prometheus datasource → URL: `http://prometheus:9090`

Run producer briefly and verify metrics visible in Prometheus UI at `http://localhost:9090`:

```
transactions_produced_total
```

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml infra/helm/monitoring/prometheus-local.yml
git commit -m "feat: add Prometheus + Grafana to local Docker Compose (ep14)"
```

---

## Task 2: ServiceMonitor CRDs for EKS (Episode 14)

**Files:**
- Create: `infra/helm/monitoring/servicemonitor-producer.yaml`
- Create: `infra/helm/monitoring/servicemonitor-consumer.yaml`

- [ ] **Step 1: Create `infra/helm/monitoring/servicemonitor-producer.yaml`**

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: producer
  namespace: paysense
  labels:
    release: kube-prometheus-stack
spec:
  selector:
    matchLabels:
      app: producer
  endpoints:
    - port: metrics
      interval: 15s
      path: /metrics
```

- [ ] **Step 2: Create `infra/helm/monitoring/servicemonitor-consumer.yaml`**

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: consumer
  namespace: paysense
  labels:
    release: kube-prometheus-stack
spec:
  selector:
    matchLabels:
      app: consumer
  endpoints:
    - port: metrics
      interval: 15s
      path: /metrics
```

- [ ] **Step 3: Validate YAML**

```bash
python -c "
import yaml
for f in ['infra/helm/monitoring/servicemonitor-producer.yaml', 'infra/helm/monitoring/servicemonitor-consumer.yaml']:
    yaml.safe_load(open(f))
    print(f'{f}: OK')
"
```

- [ ] **Step 4: Commit**

```bash
git add infra/helm/monitoring/servicemonitor-producer.yaml infra/helm/monitoring/servicemonitor-consumer.yaml
git commit -m "feat: Prometheus ServiceMonitor CRDs for producer + consumer (ep14)"
```

---

## Task 3: Grafana Dashboard JSON (Episode 14)

**Files:**
- Create: `infra/helm/monitoring/paysense-dashboard.json`
- Create: `infra/helm/monitoring/dashboard-configmap.yaml`

- [ ] **Step 1: Create `infra/helm/monitoring/paysense-dashboard.json`**

```json
{
  "__inputs": [],
  "__requires": [],
  "annotations": { "list": [] },
  "description": "PaySense fraud detection pipeline overview",
  "editable": true,
  "fiscalYearStartMonth": 0,
  "graphTooltip": 1,
  "id": null,
  "links": [],
  "panels": [
    {
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "fieldConfig": {
        "defaults": { "color": { "mode": "palette-classic" }, "unit": "reqps" },
        "overrides": []
      },
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 0 },
      "id": 1,
      "options": { "legend": { "calcs": ["mean", "max"], "displayMode": "list" }, "tooltip": { "mode": "single" } },
      "targets": [
        {
          "expr": "rate(transactions_produced_total[1m])",
          "legendFormat": "TPS",
          "refId": "A"
        }
      ],
      "title": "Transactions Per Second",
      "type": "timeseries"
    },
    {
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "fieldConfig": {
        "defaults": { "color": { "mode": "thresholds" }, "unit": "percentunit",
          "thresholds": { "mode": "absolute", "steps": [
            { "color": "green", "value": null },
            { "color": "orange", "value": 0.1 },
            { "color": "red", "value": 0.2 }
          ]}
        },
        "overrides": []
      },
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 0 },
      "id": 2,
      "options": { "reduceOptions": { "calcs": ["lastNotNull"] }, "orientation": "auto", "textMode": "auto", "colorMode": "background" },
      "targets": [
        {
          "expr": "rate(fraud_detected_total[5m]) / rate(transactions_consumed_total[5m])",
          "legendFormat": "Fraud Rate",
          "refId": "A"
        }
      ],
      "title": "Fraud Detection Rate",
      "type": "stat"
    },
    {
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "fieldConfig": {
        "defaults": { "unit": "s" },
        "overrides": []
      },
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 8 },
      "id": 3,
      "options": { "legend": { "calcs": ["mean"], "displayMode": "list" } },
      "targets": [
        {
          "expr": "histogram_quantile(0.50, rate(classification_latency_seconds_bucket[5m]))",
          "legendFormat": "p50",
          "refId": "A"
        },
        {
          "expr": "histogram_quantile(0.95, rate(classification_latency_seconds_bucket[5m]))",
          "legendFormat": "p95",
          "refId": "B"
        },
        {
          "expr": "histogram_quantile(0.99, rate(classification_latency_seconds_bucket[5m]))",
          "legendFormat": "p99",
          "refId": "C"
        }
      ],
      "title": "Classification Latency (p50 / p95 / p99)",
      "type": "timeseries"
    },
    {
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "fieldConfig": { "defaults": { "unit": "short" }, "overrides": [] },
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 8 },
      "id": 4,
      "options": { "legend": { "calcs": ["max"], "displayMode": "list" } },
      "targets": [
        {
          "expr": "kafka_consumergroup_lag",
          "legendFormat": "Consumer Lag — {{topic}}",
          "refId": "A"
        }
      ],
      "title": "Kafka Consumer Lag",
      "type": "timeseries"
    },
    {
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "fieldConfig": { "defaults": { "unit": "short" }, "overrides": [] },
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 16 },
      "id": 5,
      "options": { "legend": { "calcs": ["sum"], "displayMode": "list" } },
      "targets": [
        {
          "expr": "increase(transactions_consumed_total[1m])",
          "legendFormat": "DynamoDB Writes/min",
          "refId": "A"
        }
      ],
      "title": "DynamoDB Write Count",
      "type": "timeseries"
    },
    {
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "fieldConfig": { "defaults": { "unit": "percentunit" }, "overrides": [] },
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 16 },
      "id": 6,
      "options": { "legend": { "calcs": ["mean", "max"], "displayMode": "list" } },
      "targets": [
        {
          "expr": "rate(container_cpu_usage_seconds_total{namespace=\"paysense\"}[1m])",
          "legendFormat": "{{pod}} CPU",
          "refId": "A"
        }
      ],
      "title": "Pod CPU Usage",
      "type": "timeseries"
    },
    {
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "fieldConfig": { "defaults": { "unit": "bytes" }, "overrides": [] },
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 24 },
      "id": 7,
      "options": { "legend": { "calcs": ["mean", "max"], "displayMode": "list" } },
      "targets": [
        {
          "expr": "container_memory_working_set_bytes{namespace=\"paysense\", container!=\"\"}",
          "legendFormat": "{{pod}} memory",
          "refId": "A"
        }
      ],
      "title": "Pod Memory Usage",
      "type": "timeseries"
    }
  ],
  "refresh": "10s",
  "schemaVersion": 39,
  "tags": ["paysense", "fraud-detection"],
  "templating": { "list": [] },
  "time": { "from": "now-30m", "to": "now" },
  "timepicker": {},
  "timezone": "browser",
  "title": "PaySense Overview",
  "uid": "paysense-overview",
  "version": 1
}
```

- [ ] **Step 2: Generate `infra/helm/monitoring/dashboard-configmap.yaml`**

Run this command to produce a valid, complete ConfigMap with the JSON inlined:

```bash
kubectl create configmap paysense-grafana-dashboard \
  --from-file=paysense-overview.json=infra/helm/monitoring/paysense-dashboard.json \
  --namespace paysense \
  --dry-run=client -o yaml > infra/helm/monitoring/dashboard-configmap.yaml
```

Then add the Grafana sidecar label so Grafana auto-discovers it — open the generated file and add under `metadata:`:

```yaml
  labels:
    grafana_dashboard: "1"
```

- [ ] **Step 3: Import dashboard in local Grafana**

```
http://localhost:3000 → Dashboards → Import → Upload JSON → select infra/helm/monitoring/paysense-dashboard.json
```

Verify all 6 panels render (TPS, Fraud Rate, Latency p50/95/99, Kafka Lag, DynamoDB Writes, CPU).

- [ ] **Step 4: Commit**

```bash
git add infra/helm/monitoring/paysense-dashboard.json infra/helm/monitoring/dashboard-configmap.yaml
git commit -m "feat: Grafana PaySense Overview dashboard — 6 panels (ep14)"
```

---

## Task 4: Prometheus Alerts (Episode 15)

**Files:**
- Create: `infra/helm/monitoring/alerts.yaml`

- [ ] **Step 1: Create `infra/helm/monitoring/alerts.yaml`**

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: paysense-alerts
  namespace: paysense
  labels:
    release: kube-prometheus-stack
spec:
  groups:
    - name: paysense.fraud
      interval: 30s
      rules:
        - alert: HighFraudRate
          expr: >
            rate(fraud_detected_total[2m]) / rate(transactions_consumed_total[2m]) > 0.20
          for: 2m
          labels:
            severity: warning
          annotations:
            summary: "Fraud rate above 20%"
            description: "Fraud rate is {{ $value | humanizePercentage }} over the last 2 minutes."

    - name: paysense.kafka
      interval: 30s
      rules:
        - alert: KafkaConsumerLagHigh
          expr: kafka_consumergroup_lag > 1000
          for: 1m
          labels:
            severity: warning
          annotations:
            summary: "Kafka consumer lag above 1000"
            description: "Consumer group lag is {{ $value }} messages on topic {{ $labels.topic }}."
```

- [ ] **Step 2: Validate YAML**

```bash
python -c "import yaml; yaml.safe_load(open('infra/helm/monitoring/alerts.yaml'))"
```

- [ ] **Step 3: Commit**

```bash
git add infra/helm/monitoring/alerts.yaml
git commit -m "feat: PrometheusRule alerts — high fraud rate (>20%) + Kafka lag (>1000) (ep15)"
```

---

## Task 5: Strimzi JMX Exporter Config (Episode 14)

**Files:**
- Modify: `infra/helm/strimzi/kafka-cluster.yaml`

- [ ] **Step 1: Add JMX metrics config to `infra/helm/strimzi/kafka-cluster.yaml`**

In the `spec.kafka` section, add:

```yaml
    metricsConfig:
      type: jmxPrometheusExporter
      valueFrom:
        configMapKeyRef:
          name: kafka-metrics
          key: kafka-metrics-config.yml
```

In the `spec.zookeeper` section, add:

```yaml
    metricsConfig:
      type: jmxPrometheusExporter
      valueFrom:
        configMapKeyRef:
          name: kafka-metrics
          key: zookeeper-metrics-config.yml
```

- [ ] **Step 2: Create `infra/helm/strimzi/kafka-metrics-configmap.yaml`**

```yaml
kind: ConfigMap
apiVersion: v1
metadata:
  name: kafka-metrics
  namespace: paysense
data:
  kafka-metrics-config.yml: |
    lowercaseOutputName: true
    rules:
      - pattern: kafka.controller<type=(.+), name=(.+)><>Value
        name: kafka_controller_$1_$2
        type: GAUGE
      - pattern: kafka.server<type=BrokerTopicMetrics, name=(BytesInPerSec|BytesOutPerSec)><>OneMinuteRate
        name: kafka_server_brokertopicmetrics_$1_rate
        type: GAUGE
      - pattern: kafka.server<type=ReplicaManager, name=(.+)><>Value
        name: kafka_server_replicamanager_$1
        type: GAUGE
  zookeeper-metrics-config.yml: |
    lowercaseOutputName: true
    rules:
      - pattern: "org.apache.ZooKeeperService<name0=ReplicatedServer_id(\\d+)><>(\\w+)"
        name: "zookeeper_$2"
        type: GAUGE
```

- [ ] **Step 3: Commit**

```bash
git add infra/helm/strimzi/kafka-cluster.yaml infra/helm/strimzi/kafka-metrics-configmap.yaml
git commit -m "feat: Strimzi JMX Prometheus exporter config for Kafka + ZooKeeper metrics (ep14)"
```

---

## Task 6: Final Tag + Series Wrap-Up (Episode 15)

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update `README.md` with final status**

Append to `README.md`:

```markdown
## Series Complete

All 15 episodes implemented. Full stack:
- **Local:** `docker compose up` → Kafka + producer + consumer + FastAPI + Prometheus + Grafana
- **Cloud:** `terraform apply` → EKS + Strimzi + ArgoCD GitOps + GitHub Actions CI/CD + Grafana dashboards

### What to Add Next
- Schema Registry (Confluent or AWS Glue) for Avro schema enforcement
- Model retraining trigger on fraud rate drift (GitHub Actions + MLflow)
- Multi-region DynamoDB global tables
- Consumer horizontal pod autoscaling on `kafka_consumergroup_lag`
```

- [ ] **Step 2: Final commit + tag**

```bash
git add README.md
git commit -m "docs: series complete — Phase 4 observability done (ep14-15)"
git tag v1.0.0
git push origin master --tags
```

---

## Phase 4 Complete Verification

- [ ] Local: `docker compose up prometheus grafana -d` → `http://localhost:3000` shows Grafana
- [ ] Local: Producer metrics visible at `http://localhost:9090/metrics`
- [ ] Local: Consumer metrics visible at `http://localhost:9091/metrics`
- [ ] Local: Grafana dashboard "PaySense Overview" imports and shows all 6 panels
- [ ] EKS (requires running cluster): `kubectl apply -f infra/helm/monitoring/` → ServiceMonitors active
- [ ] EKS: `kubectl apply -f infra/helm/monitoring/alerts.yaml` → PrometheusRule created
- [ ] EKS: Fraud rate alert fires when fraud rate > 20%
- [ ] `git tag v1.0.0` exists

---

## EKS Manual Steps (during recording — not automated)

These require a live EKS cluster:

```bash
# Install kube-prometheus-stack
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  --namespace monitoring --create-namespace \
  --set grafana.adminPassword=paysense

# Apply ServiceMonitors + alerts
kubectl apply -f infra/helm/monitoring/servicemonitor-producer.yaml
kubectl apply -f infra/helm/monitoring/servicemonitor-consumer.yaml
kubectl apply -f infra/helm/monitoring/alerts.yaml

# Apply Strimzi metrics ConfigMap
kubectl apply -f infra/helm/strimzi/kafka-metrics-configmap.yaml

# Port-forward Grafana
kubectl port-forward -n monitoring svc/kube-prometheus-stack-grafana 3000:80

# Import dashboard via Grafana UI or via ConfigMap auto-load (if grafana.sidecar.dashboards.enabled=true)
kubectl apply -f infra/helm/monitoring/dashboard-configmap.yaml
```

---

## Self-Review Checklist

| Spec Requirement | Task |
|---|---|
| `kube-prometheus-stack` Helm chart (EKS) | Manual EKS step |
| Prometheus ServiceMonitor for producer (scrape `/metrics` every 15s) | Task 2 |
| Prometheus ServiceMonitor for consumer (scrape `/metrics` every 15s) | Task 2 |
| Strimzi built-in JMX exporter enabled | Task 5 |
| Grafana dashboard "PaySense Overview" | Task 3 |
| Panel: TPS | Task 3 |
| Panel: Fraud detection rate (%) | Task 3 |
| Panel: Classification latency (p50, p95, p99) | Task 3 |
| Panel: Kafka consumer lag | Task 3 |
| Panel: DynamoDB write count | Task 3 |
| Panel: Pod CPU usage | Task 3 |
| Panel: Pod memory usage | Task 3 |
| Dashboard JSON exported to `infra/helm/monitoring/` as ConfigMap | Task 3 |
| Grafana added to local Docker Compose | Task 1 |
| Alert: fraud rate > 20% for 2 minutes | Task 4 |
| Alert: Kafka consumer lag > 1000 | Task 4 |
| Alert routing (AlertManager) | Task 4 (PrometheusRule fires → AlertManager routes) |
| Series wrap-up in README | Task 6 |
| Repo tagged `v1.0.0` | Task 6 |
