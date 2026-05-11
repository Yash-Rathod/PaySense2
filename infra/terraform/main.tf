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

  github_oidc_provider_arn = "arn:aws:iam::347486023960:oidc-provider/token.actions.githubusercontent.com"
}
