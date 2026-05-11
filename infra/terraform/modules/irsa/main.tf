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
