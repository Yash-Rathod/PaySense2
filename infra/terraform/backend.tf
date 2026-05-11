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
