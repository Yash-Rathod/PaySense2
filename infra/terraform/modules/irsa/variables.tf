variable "cluster_name" { type = string }
variable "oidc_provider_arn" { type = string }
variable "oidc_provider" { type = string }
variable "dynamodb_table_arn" { type = string }
variable "s3_bucket_arn" { type = string }
variable "namespace" {
  type    = string
  default = "paysense"
}

variable "github_repo" {
  type    = string
  default = "Yash-Rathod/PaySense2"
}

variable "github_oidc_provider_arn" {
  type    = string
  default = ""
}
