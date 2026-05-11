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
