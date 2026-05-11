module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = var.cluster_name
  cluster_version = var.cluster_version

  vpc_id     = var.vpc_id
  subnet_ids = var.subnet_ids

  cluster_endpoint_public_access = true

  enable_cluster_creator_admin_permissions = true

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
