# Phase 3 — DevOps & Cloud Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Provision AWS infrastructure with Terraform, deploy apps on EKS via Helm, run Kafka inside Kubernetes with Strimzi, enable GitOps with ArgoCD, and automate build+deploy with GitHub Actions CI/CD (Episodes 9–13).

**Architecture:** Terraform manages VPC + EKS + ECR + S3 + DynamoDB + IAM/IRSA. Helm charts deploy producer and consumer to EKS. Strimzi operator manages Kafka cluster inside EKS. ArgoCD syncs all Helm charts from Git. GitHub Actions (OIDC auth, no long-lived keys) builds Docker images → pushes to ECR → updates Helm values → ArgoCD auto-deploys.

**Prerequisites:** Phase 1 + Phase 2 complete. Docker images buildable. MLflow model registered. AWS CLI configured with admin-level IAM credentials for bootstrapping. Terraform installed. `kubectl`, `helm`, `gh` CLI installed.

**Cost note:** Ephemeral infra. `terraform apply` before recording, `terraform destroy` after. ~$0.49/3-hour session.

**Tech Stack:** Terraform >= 1.7, AWS EKS (3× t3.small), ECR, DynamoDB, S3, IAM/IRSA, Strimzi Kafka operator, Helm 3, ArgoCD, GitHub Actions (OIDC)

---

## File Map

| Path | Purpose |
|------|---------|
| `infra/terraform/main.tf` | Root module: VPC, EKS, ECR, S3, DynamoDB, IAM |
| `infra/terraform/variables.tf` | Input variables: region, cluster name, node count |
| `infra/terraform/outputs.tf` | Outputs: cluster endpoint, ECR URLs, DynamoDB name |
| `infra/terraform/modules/vpc/main.tf` | VPC: 2 AZs, public + private subnets, NAT gateway |
| `infra/terraform/modules/eks/main.tf` | EKS cluster + managed node group (3× t3.small) |
| `infra/terraform/modules/irsa/main.tf` | IAM roles for K8s service accounts |
| `infra/helm/producer/Chart.yaml` | Helm chart metadata |
| `infra/helm/producer/values.yaml` | Default values (image, replicas, resources, env) |
| `infra/helm/producer/templates/deployment.yaml` | Producer Deployment |
| `infra/helm/producer/templates/serviceaccount.yaml` | ServiceAccount with IRSA annotation |
| `infra/helm/producer/templates/service.yaml` | ClusterIP service |
| `infra/helm/consumer/Chart.yaml` | Helm chart metadata |
| `infra/helm/consumer/values.yaml` | Default values |
| `infra/helm/consumer/templates/deployment.yaml` | Consumer Deployment |
| `infra/helm/consumer/templates/serviceaccount.yaml` | ServiceAccount with IRSA annotation |
| `infra/helm/consumer/templates/service.yaml` | ClusterIP service |
| `infra/helm/strimzi/kafka-cluster.yaml` | KafkaCluster CRD |
| `infra/helm/strimzi/kafka-topics.yaml` | KafkaTopic CRDs |
| `k8s/apps/producer.yaml` | ArgoCD Application for producer |
| `k8s/apps/consumer.yaml` | ArgoCD Application for consumer |
| `k8s/apps/kafka.yaml` | ArgoCD Application for Strimzi Kafka |
| `.github/workflows/ci-producer.yml` | GitHub Actions: test → build → push ECR → update values |
| `.github/workflows/ci-consumer.yml` | GitHub Actions: test → build → push ECR → update values |

---

## Task 0: Bootstrap Terraform Remote State (One-Time Setup)

**Files:** none committed — these AWS resources are prerequisites for `terraform init`

> Run once before any recording session. These resources must exist before `terraform init` can succeed.

- [ ] **Step 1: Create remote state S3 bucket**

```bash
aws s3 mb s3://paysense-terraform-state --region us-east-1
aws s3api put-bucket-versioning \
  --bucket paysense-terraform-state \
  --versioning-configuration Status=Enabled
aws s3api put-bucket-encryption \
  --bucket paysense-terraform-state \
  --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
```

- [ ] **Step 2: Create DynamoDB lock table**

```bash
aws dynamodb create-table \
  --table-name paysense-terraform-locks \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-1
```

- [ ] **Step 3: Verify both exist**

```bash
aws s3 ls s3://paysense-terraform-state
aws dynamodb describe-table --table-name paysense-terraform-locks --query "Table.TableStatus"
```

Expected: bucket accessible, table status `"ACTIVE"`

> These resources are free-tier and always-on — do NOT destroy them with `terraform destroy`.

---

## Task 1: Terraform — VPC Module (Episode 09)

**Files:**
- Create: `infra/terraform/modules/vpc/main.tf`
- Create: `infra/terraform/modules/vpc/variables.tf`
- Create: `infra/terraform/modules/vpc/outputs.tf`

- [ ] **Step 1: Create `infra/terraform/modules/vpc/variables.tf`**

```hcl
variable "cluster_name" {
  type = string
}

variable "region" {
  type    = string
  default = "us-east-1"
}
```

- [ ] **Step 2: Create `infra/terraform/modules/vpc/main.tf`**

```hcl
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = "${var.cluster_name}-vpc"
  cidr = "10.0.0.0/16"

  azs             = ["${var.region}a", "${var.region}b"]
  private_subnets = ["10.0.1.0/24", "10.0.2.0/24"]
  public_subnets  = ["10.0.101.0/24", "10.0.102.0/24"]

  enable_nat_gateway   = true
  single_nat_gateway   = true
  enable_dns_hostnames = true

  tags = {
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
    Project = "paysense"
  }

  private_subnet_tags = {
    "kubernetes.io/role/internal-elb"            = "1"
    "kubernetes.io/cluster/${var.cluster_name}"  = "owned"
  }

  public_subnet_tags = {
    "kubernetes.io/role/elb"                     = "1"
    "kubernetes.io/cluster/${var.cluster_name}"  = "owned"
  }
}
```

- [ ] **Step 3: Create `infra/terraform/modules/vpc/outputs.tf`**

```hcl
output "vpc_id" {
  value = module.vpc.vpc_id
}

output "private_subnet_ids" {
  value = module.vpc.private_subnets
}

output "public_subnet_ids" {
  value = module.vpc.public_subnets
}
```

- [ ] **Step 4: Commit**

```bash
git add infra/terraform/modules/vpc/
git commit -m "feat: Terraform VPC module (2 AZs, NAT gateway) (ep09)"
```

---

## Task 2: Terraform — EKS Module (Episode 09)

**Files:**
- Create: `infra/terraform/modules/eks/main.tf`
- Create: `infra/terraform/modules/eks/variables.tf`
- Create: `infra/terraform/modules/eks/outputs.tf`

- [ ] **Step 1: Create `infra/terraform/modules/eks/variables.tf`**

```hcl
variable "cluster_name" { type = string }
variable "cluster_version" { type = string; default = "1.29" }
variable "vpc_id" { type = string }
variable "subnet_ids" { type = list(string) }
variable "node_count" { type = number; default = 3 }
```

- [ ] **Step 2: Create `infra/terraform/modules/eks/main.tf`**

```hcl
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = var.cluster_name
  cluster_version = var.cluster_version

  vpc_id     = var.vpc_id
  subnet_ids = var.subnet_ids

  cluster_endpoint_public_access = true

  eks_managed_node_groups = {
    default = {
      instance_types = ["t3.small"]
      min_size       = var.node_count
      max_size       = var.node_count
      desired_size   = var.node_count

      labels = { Project = "paysense" }
    }
  }

  tags = { Project = "paysense" }
}
```

- [ ] **Step 3: Create `infra/terraform/modules/eks/outputs.tf`**

```hcl
output "cluster_name" {
  value = module.eks.cluster_name
}

output "cluster_endpoint" {
  value = module.eks.cluster_endpoint
}

output "oidc_provider_arn" {
  value = module.eks.oidc_provider_arn
}

output "oidc_provider" {
  value = module.eks.oidc_provider
}
```

- [ ] **Step 4: Commit**

```bash
git add infra/terraform/modules/eks/
git commit -m "feat: Terraform EKS module (3x t3.small) (ep09)"
```

---

## Task 3: Terraform — IRSA Module (Episode 09)

**Files:**
- Create: `infra/terraform/modules/irsa/main.tf`
- Create: `infra/terraform/modules/irsa/variables.tf`
- Create: `infra/terraform/modules/irsa/outputs.tf`

- [ ] **Step 1: Create `infra/terraform/modules/irsa/variables.tf`**

```hcl
variable "cluster_name" { type = string }
variable "oidc_provider_arn" { type = string }
variable "oidc_provider" { type = string }
variable "dynamodb_table_arn" { type = string }
variable "s3_bucket_arn" { type = string }
variable "namespace" { type = string; default = "paysense" }
```

- [ ] **Step 2: Create `infra/terraform/modules/irsa/main.tf`**

```hcl
data "aws_iam_policy_document" "paysense_assume_role" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [var.oidc_provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "${var.oidc_provider}:sub"
      values   = [
        "system:serviceaccount:${var.namespace}:producer",
        "system:serviceaccount:${var.namespace}:consumer",
      ]
    }
  }
}

resource "aws_iam_role" "paysense" {
  name               = "${var.cluster_name}-paysense-role"
  assume_role_policy = data.aws_iam_policy_document.paysense_assume_role.json
}

resource "aws_iam_role_policy" "paysense" {
  role = aws_iam_role.paysense.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:Scan", "dynamodb:Query"]
        Resource = var.dynamodb_table_arn
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket"]
        Resource = [var.s3_bucket_arn, "${var.s3_bucket_arn}/*"]
      }
    ]
  })
}
```

- [ ] **Step 3: Create `infra/terraform/modules/irsa/outputs.tf`**

```hcl
output "role_arn" {
  value = aws_iam_role.paysense.arn
}
```

- [ ] **Step 4: Commit**

```bash
git add infra/terraform/modules/irsa/
git commit -m "feat: Terraform IRSA module (DynamoDB + S3 access for K8s service accounts) (ep09)"
```

---

## Task 4: Terraform — Root Module + Remote State (Episode 09)

**Files:**
- Create: `infra/terraform/main.tf`
- Create: `infra/terraform/variables.tf`
- Create: `infra/terraform/outputs.tf`
- Create: `infra/terraform/backend.tf`

- [ ] **Step 1: Create `infra/terraform/variables.tf`**

```hcl
variable "region" {
  type    = string
  default = "us-east-1"
}

variable "cluster_name" {
  type    = string
  default = "paysense-eks"
}

variable "node_count" {
  type    = number
  default = 3
}

variable "mlflow_bucket_name" {
  type    = string
  default = "paysense-mlflow-artifacts"
}
```

- [ ] **Step 2: Create `infra/terraform/backend.tf`**

```hcl
terraform {
  backend "s3" {
    bucket         = "paysense-terraform-state"
    key            = "eks/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "paysense-terraform-locks"
    encrypt        = true
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  required_version = ">= 1.7"
}

provider "aws" {
  region = var.region
}
```

- [ ] **Step 3: Create `infra/terraform/main.tf`**

```hcl
module "vpc" {
  source       = "./modules/vpc"
  cluster_name = var.cluster_name
  region       = var.region
}

module "eks" {
  source          = "./modules/eks"
  cluster_name    = var.cluster_name
  vpc_id          = module.vpc.vpc_id
  subnet_ids      = module.vpc.private_subnet_ids
  node_count      = var.node_count
}

resource "aws_ecr_repository" "producer" {
  name                 = "paysense-producer"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}

resource "aws_ecr_repository" "consumer" {
  name                 = "paysense-consumer"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}

resource "aws_s3_bucket" "mlflow" {
  bucket        = var.mlflow_bucket_name
  force_destroy = true
  tags          = { Project = "paysense" }
}

resource "aws_dynamodb_table" "transactions" {
  name         = "transactions"
  billing_mode = "PAY_PER_REQUEST"

  attribute {
    name = "transaction_id"
    type = "S"
  }

  hash_key = "transaction_id"
  tags     = { Project = "paysense" }
}

module "irsa" {
  source             = "./modules/irsa"
  cluster_name       = var.cluster_name
  oidc_provider_arn  = module.eks.oidc_provider_arn
  oidc_provider      = module.eks.oidc_provider
  dynamodb_table_arn = aws_dynamodb_table.transactions.arn
  s3_bucket_arn      = aws_s3_bucket.mlflow.arn
}
```

- [ ] **Step 4: Create `infra/terraform/outputs.tf`**

```hcl
output "cluster_name" {
  value = module.eks.cluster_name
}

output "cluster_endpoint" {
  value = module.eks.cluster_endpoint
}

output "ecr_producer_url" {
  value = aws_ecr_repository.producer.repository_url
}

output "ecr_consumer_url" {
  value = aws_ecr_repository.consumer.repository_url
}

output "dynamodb_table_name" {
  value = aws_dynamodb_table.transactions.name
}

output "mlflow_bucket_name" {
  value = aws_s3_bucket.mlflow.bucket
}

output "irsa_role_arn" {
  value = module.irsa.role_arn
}
```

- [ ] **Step 5: Validate Terraform config**

```bash
cd infra/terraform
terraform init -backend=false
terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 6: Commit**

```bash
git add infra/terraform/
git commit -m "feat: Terraform root module — VPC, EKS, ECR, S3, DynamoDB, IRSA (ep09)"
```

---

## Task 5: Helm Chart — Producer (Episode 10)

**Files:**
- Create: `infra/helm/producer/Chart.yaml`
- Create: `infra/helm/producer/values.yaml`
- Create: `infra/helm/producer/templates/deployment.yaml`
- Create: `infra/helm/producer/templates/serviceaccount.yaml`
- Create: `infra/helm/producer/templates/service.yaml`

- [ ] **Step 1: Create `infra/helm/producer/Chart.yaml`**

```yaml
apiVersion: v2
name: paysense-producer
description: PaySense transaction producer
type: application
version: 0.1.0
appVersion: "0.1.0"
```

- [ ] **Step 2: Create `infra/helm/producer/values.yaml`**

```yaml
image:
  repository: "PLACEHOLDER_ECR_URL/paysense-producer"
  tag: "latest"
  pullPolicy: IfNotPresent

replicaCount: 1

env:
  KAFKA_BOOTSTRAP_SERVERS: "kafka-cluster-kafka-bootstrap.paysense.svc.cluster.local:9092"
  KAFKA_TOPIC_TRANSACTIONS: "transactions"

args:
  - "--rows"
  - "0"
  - "--fraud-rate"
  - "0.1"
  - "--tps"
  - "50"
  - "--mode"
  - "kafka"

resources:
  requests:
    cpu: 128m
    memory: 128Mi
  limits:
    cpu: 256m
    memory: 256Mi

serviceAccount:
  irsaRoleArn: ""

metricsPort: 9090
```

- [ ] **Step 3: Create `infra/helm/producer/templates/serviceaccount.yaml`**

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: producer
  namespace: {{ .Release.Namespace }}
  annotations:
    {{- if .Values.serviceAccount.irsaRoleArn }}
    eks.amazonaws.com/role-arn: {{ .Values.serviceAccount.irsaRoleArn }}
    {{- end }}
```

- [ ] **Step 4: Create `infra/helm/producer/templates/deployment.yaml`**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: producer
  namespace: {{ .Release.Namespace }}
spec:
  replicas: {{ .Values.replicaCount }}
  selector:
    matchLabels:
      app: producer
  template:
    metadata:
      labels:
        app: producer
    spec:
      serviceAccountName: producer
      containers:
        - name: producer
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          args: {{ toJson .Values.args }}
          env:
            {{- range $key, $val := .Values.env }}
            - name: {{ $key }}
              value: {{ $val | quote }}
            {{- end }}
          resources:
            requests:
              cpu: {{ .Values.resources.requests.cpu }}
              memory: {{ .Values.resources.requests.memory }}
            limits:
              cpu: {{ .Values.resources.limits.cpu }}
              memory: {{ .Values.resources.limits.memory }}
          ports:
            - containerPort: {{ .Values.metricsPort }}
              name: metrics
          livenessProbe:
            httpGet:
              path: /metrics
              port: {{ .Values.metricsPort }}
            initialDelaySeconds: 15
            periodSeconds: 20
```

- [ ] **Step 5: Create `infra/helm/producer/templates/service.yaml`**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: producer
  namespace: {{ .Release.Namespace }}
spec:
  selector:
    app: producer
  ports:
    - name: metrics
      port: {{ .Values.metricsPort }}
      targetPort: {{ .Values.metricsPort }}
```

- [ ] **Step 6: Lint Helm chart**

```bash
helm lint infra/helm/producer/
```

Expected: `1 chart(s) linted, 0 chart(s) failed`

- [ ] **Step 7: Commit**

```bash
git add infra/helm/producer/
git commit -m "feat: Helm chart for producer (Deployment, SA with IRSA, Service) (ep10)"
```

---

## Task 6: Helm Chart — Consumer (Episode 10)

**Files:**
- Create: `infra/helm/consumer/Chart.yaml`
- Create: `infra/helm/consumer/values.yaml`
- Create: `infra/helm/consumer/templates/deployment.yaml`
- Create: `infra/helm/consumer/templates/serviceaccount.yaml`
- Create: `infra/helm/consumer/templates/service.yaml`

- [ ] **Step 1: Create `infra/helm/consumer/Chart.yaml`**

```yaml
apiVersion: v2
name: paysense-consumer
description: PaySense fraud classifier + FastAPI
type: application
version: 0.1.0
appVersion: "0.1.0"
```

- [ ] **Step 2: Create `infra/helm/consumer/values.yaml`**

```yaml
image:
  repository: "PLACEHOLDER_ECR_URL/paysense-consumer"
  tag: "latest"
  pullPolicy: IfNotPresent

replicaCount: 1

env:
  KAFKA_BOOTSTRAP_SERVERS: "kafka-cluster-kafka-bootstrap.paysense.svc.cluster.local:9092"
  KAFKA_TOPIC_TRANSACTIONS: "transactions"
  KAFKA_TOPIC_RESULTS: "results"
  KAFKA_GROUP_ID: "paysense-consumer"
  DYNAMODB_TABLE: "transactions"
  AWS_DEFAULT_REGION: "us-east-1"
  # Set to direct S3 path after terraform apply + model upload step (ep09 manual step 3)
  # Format: s3://paysense-mlflow-artifacts/<run-id>/artifacts/model
  # Do NOT use models:/ registry alias here — no MLflow tracking server runs on EKS
  MODEL_URI: "PLACEHOLDER_S3_MODEL_URI"
  API_PORT: "8001"
  METRICS_PORT: "9091"

resources:
  requests:
    cpu: 256m
    memory: 256Mi
  limits:
    cpu: 512m
    memory: 512Mi

serviceAccount:
  irsaRoleArn: ""

apiPort: 8001
metricsPort: 9091
```

- [ ] **Step 3: Create `infra/helm/consumer/templates/serviceaccount.yaml`**

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: consumer
  namespace: {{ .Release.Namespace }}
  annotations:
    {{- if .Values.serviceAccount.irsaRoleArn }}
    eks.amazonaws.com/role-arn: {{ .Values.serviceAccount.irsaRoleArn }}
    {{- end }}
```

- [ ] **Step 4: Create `infra/helm/consumer/templates/deployment.yaml`**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: consumer
  namespace: {{ .Release.Namespace }}
spec:
  replicas: {{ .Values.replicaCount }}
  selector:
    matchLabels:
      app: consumer
  template:
    metadata:
      labels:
        app: consumer
    spec:
      serviceAccountName: consumer
      containers:
        - name: consumer
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          env:
            {{- range $key, $val := .Values.env }}
            - name: {{ $key }}
              value: {{ $val | quote }}
            {{- end }}
          resources:
            requests:
              cpu: {{ .Values.resources.requests.cpu }}
              memory: {{ .Values.resources.requests.memory }}
            limits:
              cpu: {{ .Values.resources.limits.cpu }}
              memory: {{ .Values.resources.limits.memory }}
          ports:
            - containerPort: {{ .Values.apiPort }}
              name: api
            - containerPort: {{ .Values.metricsPort }}
              name: metrics
          livenessProbe:
            httpGet:
              path: /health
              port: {{ .Values.apiPort }}
            initialDelaySeconds: 30
            periodSeconds: 20
          readinessProbe:
            httpGet:
              path: /health
              port: {{ .Values.apiPort }}
            initialDelaySeconds: 10
            periodSeconds: 5
```

- [ ] **Step 5: Create `infra/helm/consumer/templates/service.yaml`**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: consumer
  namespace: {{ .Release.Namespace }}
spec:
  selector:
    app: consumer
  ports:
    - name: api
      port: {{ .Values.apiPort }}
      targetPort: {{ .Values.apiPort }}
    - name: metrics
      port: {{ .Values.metricsPort }}
      targetPort: {{ .Values.metricsPort }}
```

- [ ] **Step 6: Lint Helm chart**

```bash
helm lint infra/helm/consumer/
```

Expected: `1 chart(s) linted, 0 chart(s) failed`

- [ ] **Step 7: Commit**

```bash
git add infra/helm/consumer/
git commit -m "feat: Helm chart for consumer (Deployment, SA with IRSA, Service, probes) (ep10)"
```

---

## Task 7: Strimzi Kafka Manifests (Episode 11)

**Files:**
- Create: `infra/helm/strimzi/kafka-cluster.yaml`
- Create: `infra/helm/strimzi/kafka-topics.yaml`

- [ ] **Step 1: Create `infra/helm/strimzi/kafka-cluster.yaml`**

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: kafka-cluster
  namespace: paysense
spec:
  kafka:
    version: 3.7.0
    replicas: 1
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
    config:
      offsets.topic.replication.factor: 1
      transaction.state.log.replication.factor: 1
      transaction.state.log.min.isr: 1
      default.replication.factor: 1
      min.insync.replicas: 1
    resources:
      requests:
        memory: 512Mi
        cpu: 250m
      limits:
        memory: 1Gi
        cpu: 500m
    storage:
      type: ephemeral
  zookeeper:
    replicas: 1
    resources:
      requests:
        memory: 256Mi
        cpu: 100m
      limits:
        memory: 512Mi
        cpu: 250m
    storage:
      type: ephemeral
  entityOperator:
    topicOperator: {}
```

- [ ] **Step 2: Create `infra/helm/strimzi/kafka-topics.yaml`**

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: transactions
  namespace: paysense
  labels:
    strimzi.io/cluster: kafka-cluster
spec:
  partitions: 3
  replicas: 1
  config:
    retention.ms: 86400000
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: results
  namespace: paysense
  labels:
    strimzi.io/cluster: kafka-cluster
spec:
  partitions: 3
  replicas: 1
  config:
    retention.ms: 86400000
```

- [ ] **Step 3: Validate YAML syntax**

```bash
python -c "import yaml; yaml.safe_load_all(open('infra/helm/strimzi/kafka-cluster.yaml'))"
python -c "import yaml; yaml.safe_load_all(open('infra/helm/strimzi/kafka-topics.yaml'))"
```

Expected: no errors

- [ ] **Step 4: Commit**

```bash
git add infra/helm/strimzi/
git commit -m "feat: Strimzi KafkaCluster + KafkaTopic CRDs (ep11)"
```

---

## Task 8: ArgoCD Application Manifests (Episode 12)

**Files:**
- Create: `k8s/apps/producer.yaml`
- Create: `k8s/apps/consumer.yaml`
- Create: `k8s/apps/kafka.yaml`

- [ ] **Step 1: Create `k8s/apps/kafka.yaml`**

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: paysense-kafka
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/PLACEHOLDER_GITHUB_USER/PaySense2.git
    targetRevision: HEAD
    path: infra/helm/strimzi
  destination:
    server: https://kubernetes.default.svc
    namespace: paysense
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

- [ ] **Step 2: Create `k8s/apps/producer.yaml`**

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: paysense-producer
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/PLACEHOLDER_GITHUB_USER/PaySense2.git
    targetRevision: HEAD
    path: infra/helm/producer
  destination:
    server: https://kubernetes.default.svc
    namespace: paysense
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

- [ ] **Step 3: Create `k8s/apps/consumer.yaml`**

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: paysense-consumer
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/PLACEHOLDER_GITHUB_USER/PaySense2.git
    targetRevision: HEAD
    path: infra/helm/consumer
  destination:
    server: https://kubernetes.default.svc
    namespace: paysense
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

- [ ] **Step 4: Validate YAML**

```bash
python -c "import yaml; [yaml.safe_load(open(f)) for f in ['k8s/apps/producer.yaml','k8s/apps/consumer.yaml','k8s/apps/kafka.yaml']]"
```

Expected: no errors

- [ ] **Step 5: Commit**

```bash
git add k8s/apps/
git commit -m "feat: ArgoCD Application manifests for producer, consumer, Kafka (ep12)"
```

> **Note:** Replace `PLACEHOLDER_GITHUB_USER` with your actual GitHub username before applying these manifests.

---

## Task 9: GitHub Actions — Producer CI/CD (Episode 13)

**Files:**
- Create: `.github/workflows/ci-producer.yml`

- [ ] **Step 1: Create `.github/workflows/ci-producer.yml`**

```yaml
name: CI — Producer

on:
  push:
    branches: [main, master]
    paths:
      - "producer/**"
      - ".github/workflows/ci-producer.yml"

permissions:
  id-token: write
  contents: write

jobs:
  test-build-push:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4
        with:
          token: ${{ secrets.GITHUB_TOKEN }}

      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install and test
        working-directory: producer
        run: |
          pip install -e ".[dev]" 2>/dev/null || pip install -e . pytest
          pip install faker click pydantic prometheus-client pytest
          python -m pytest tests/ -v

      - name: Configure AWS credentials (OIDC)
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.AWS_ROLE_ARN }}
          aws-region: us-east-1

      - name: Login to ECR
        id: ecr-login
        uses: aws-actions/amazon-ecr-login@v2

      - name: Build and push
        env:
          ECR_REGISTRY: ${{ steps.ecr-login.outputs.registry }}
          IMAGE_TAG: ${{ github.sha }}
        run: |
          docker build -t $ECR_REGISTRY/paysense-producer:$IMAGE_TAG producer/
          docker push $ECR_REGISTRY/paysense-producer:$IMAGE_TAG

      - name: Update Helm values image tag
        env:
          IMAGE_TAG: ${{ github.sha }}
          ECR_REGISTRY: ${{ steps.ecr-login.outputs.registry }}
        run: |
          sed -i "s|tag: .*|tag: \"$IMAGE_TAG\"|" infra/helm/producer/values.yaml
          sed -i "s|repository: .*|repository: \"$ECR_REGISTRY/paysense-producer\"|" infra/helm/producer/values.yaml
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add infra/helm/producer/values.yaml
          git commit -m "ci: update producer image tag to $IMAGE_TAG [skip ci]" || echo "No changes"
          git push
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/ci-producer.yml
git commit -m "feat: GitHub Actions CI/CD for producer (OIDC → ECR → Helm values update) (ep13)"
```

---

## Task 10: GitHub Actions — Consumer CI/CD (Episode 13)

**Files:**
- Create: `.github/workflows/ci-consumer.yml`

- [ ] **Step 1: Create `.github/workflows/ci-consumer.yml`**

```yaml
name: CI — Consumer

on:
  push:
    branches: [main, master]
    paths:
      - "consumer/**"
      - ".github/workflows/ci-consumer.yml"

permissions:
  id-token: write
  contents: write

jobs:
  test-build-push:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4
        with:
          token: ${{ secrets.GITHUB_TOKEN }}

      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install and test
        working-directory: consumer
        run: |
          pip install -e . 2>/dev/null || pip install -e .
          pip install mlflow xgboost boto3 confluent-kafka fastapi uvicorn pydantic prometheus-client pytest httpx
          python -m pytest tests/ -v

      - name: Configure AWS credentials (OIDC)
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.AWS_ROLE_ARN }}
          aws-region: us-east-1

      - name: Login to ECR
        id: ecr-login
        uses: aws-actions/amazon-ecr-login@v2

      - name: Build and push
        env:
          ECR_REGISTRY: ${{ steps.ecr-login.outputs.registry }}
          IMAGE_TAG: ${{ github.sha }}
        run: |
          docker build -t $ECR_REGISTRY/paysense-consumer:$IMAGE_TAG consumer/
          docker push $ECR_REGISTRY/paysense-consumer:$IMAGE_TAG

      - name: Update Helm values image tag
        env:
          IMAGE_TAG: ${{ github.sha }}
          ECR_REGISTRY: ${{ steps.ecr-login.outputs.registry }}
        run: |
          sed -i "s|tag: .*|tag: \"$IMAGE_TAG\"|" infra/helm/consumer/values.yaml
          sed -i "s|repository: .*|repository: \"$ECR_REGISTRY/paysense-consumer\"|" infra/helm/consumer/values.yaml
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add infra/helm/consumer/values.yaml
          git commit -m "ci: update consumer image tag to $IMAGE_TAG [skip ci]" || echo "No changes"
          git push
```

- [ ] **Step 2: Add workflow status badges to README.md**

Append to `README.md`:

```markdown
## CI Status

![Producer CI](https://github.com/PLACEHOLDER_GITHUB_USER/PaySense2/actions/workflows/ci-producer.yml/badge.svg)
![Consumer CI](https://github.com/PLACEHOLDER_GITHUB_USER/PaySense2/actions/workflows/ci-consumer.yml/badge.svg)
```

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci-consumer.yml README.md
git commit -m "feat: GitHub Actions CI/CD for consumer + README badges (ep13)"
git tag phase3-complete
git push origin master --tags
```

---

## Phase 3 Manual EKS Steps (not in code — recorded live)

These steps run during recording sessions and cannot be committed as code:

1. Bootstrap remote state: create S3 bucket + DynamoDB lock table manually (one-time)
2. `terraform init && terraform apply` — ~12 min, verify `kubectl get nodes`
3. Copy MLflow model artifact to S3: `aws s3 cp mlruns/... s3://paysense-mlflow-artifacts/...`
4. `kubectl create namespace paysense`
5. `helm install strimzi strimzi/strimzi-kafka-operator -n paysense`
6. Wait for Kafka cluster ready
7. `helm install producer infra/helm/producer/ -n paysense --set serviceAccount.irsaRoleArn=<ARN>`
8. `helm install consumer infra/helm/consumer/ -n paysense --set serviceAccount.irsaRoleArn=<ARN>`
9. Install ArgoCD: `kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml`
10. Apply ArgoCD Applications: `kubectl apply -f k8s/apps/`
11. Configure GitHub Secrets: `AWS_ROLE_ARN` (OIDC role ARN from Terraform output)
12. `terraform destroy` at end of recording

---

## Self-Review Checklist

| Spec Requirement | Task |
|---|---|
| VPC (2 AZs, public + private, NAT gateway) | Task 1 |
| EKS cluster (3× t3.small) | Task 2 |
| ECR repositories (producer, consumer) | Task 4 |
| S3 bucket (MLflow artifacts) | Task 4 |
| DynamoDB table (PAY_PER_REQUEST) | Task 4 |
| IAM + IRSA (S3 + DynamoDB permissions) | Task 3 |
| Terraform remote state (S3 + DynamoDB lock) | Task 4 |
| `variables.tf` for region, cluster name, node count | Task 4 |
| `outputs.tf` for cluster endpoint, ECR URLs, DynamoDB table name | Task 4 |
| Helm chart producer: Deployment, SA, Service | Task 5 |
| Helm chart consumer: Deployment, SA, Service, probes | Task 6 |
| Resource limits for t3.small | Tasks 5, 6 |
| Liveness + readiness probes | Tasks 5, 6 |
| IRSA annotation on ServiceAccounts | Tasks 5, 6 |
| `values.yaml` per service (image, replicas, resources, env) | Tasks 5, 6 |
| Strimzi KafkaCluster CRD | Task 7 |
| KafkaTopic CRDs (transactions + results) | Task 7 |
| ArgoCD Application for producer | Task 8 |
| ArgoCD Application for consumer | Task 8 |
| ArgoCD Application for Kafka | Task 8 |
| GitHub Actions OIDC (no hardcoded keys) | Tasks 9, 10 |
| CI producer: test → build → ECR push → values update | Task 9 |
| CI consumer: test → build → ECR push → values update | Task 10 |
| README CI badges | Task 10 |
