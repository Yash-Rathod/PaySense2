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
