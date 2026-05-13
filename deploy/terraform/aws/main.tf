terraform {
  required_version = ">= 1.5"
  required_providers {
    aws    = { source = "hashicorp/aws", version = "~> 5.0" }
    random = { source = "hashicorp/random", version = "~> 3.6" }
  }
}

# -----------------------------------------------------------------------------
# Inputs
# -----------------------------------------------------------------------------

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "name_prefix" {
  type    = string
  default = "zenithdb"
}

variable "vpc_id" {
  description = "Existing VPC to attach the catalog DB into. Bring-your-own-VPC keeps blast radius bounded."
  type        = string
}

variable "private_subnet_ids" {
  description = "Two or more private subnet IDs across distinct AZs for the catalog DB."
  type        = list(string)
}

variable "tags" {
  type    = map(string)
  default = {}
}

provider "aws" {
  region = var.region
}

# -----------------------------------------------------------------------------
# KMS key for SSE-KMS on segments + catalog snapshots
# -----------------------------------------------------------------------------

resource "aws_kms_key" "zen" {
  description             = "${var.name_prefix} envelope-encryption key"
  deletion_window_in_days = 30
  enable_key_rotation     = true
  tags                    = var.tags
}

resource "aws_kms_alias" "zen" {
  name          = "alias/${var.name_prefix}"
  target_key_id = aws_kms_key.zen.key_id
}

# -----------------------------------------------------------------------------
# Object storage (segments + WAL)
# -----------------------------------------------------------------------------

resource "aws_s3_bucket" "zen" {
  bucket = "${var.name_prefix}-prod"
  tags   = var.tags
}

resource "aws_s3_bucket_versioning" "zen" {
  bucket = aws_s3_bucket.zen.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "zen" {
  bucket = aws_s3_bucket.zen.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.zen.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "zen" {
  bucket                  = aws_s3_bucket.zen.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# -----------------------------------------------------------------------------
# Catalog DB credentials in Secrets Manager (no plaintext passwords)
# -----------------------------------------------------------------------------

resource "random_password" "catalog" {
  length  = 32
  special = true
}

resource "aws_secretsmanager_secret" "catalog" {
  name = "${var.name_prefix}/catalog"
  tags = var.tags
  kms_key_id = aws_kms_key.zen.arn
}

resource "aws_secretsmanager_secret_version" "catalog" {
  secret_id = aws_secretsmanager_secret.catalog.id
  secret_string = jsonencode({
    username = "zen"
    password = random_password.catalog.result
  })
}

# -----------------------------------------------------------------------------
# RDS Postgres for catalog
# -----------------------------------------------------------------------------

resource "aws_db_subnet_group" "catalog" {
  name       = "${var.name_prefix}-catalog"
  subnet_ids = var.private_subnet_ids
  tags       = var.tags
}

resource "aws_security_group" "catalog" {
  name        = "${var.name_prefix}-catalog"
  description = "${var.name_prefix} catalog (Postgres) — only Zenith pods may connect"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_instance" "catalog" {
  identifier              = "${var.name_prefix}-catalog"
  engine                  = "postgres"
  engine_version          = "14.19"
  instance_class          = "db.m6g.large"
  allocated_storage       = 100
  storage_type            = "gp3"
  storage_encrypted       = true
  kms_key_id              = aws_kms_key.zen.arn
  username                = "zen"
  password                = random_password.catalog.result
  db_name                 = "zenith"
  publicly_accessible     = false
  multi_az                = true
  backup_retention_period = 14
  copy_tags_to_snapshot   = true
  skip_final_snapshot     = false
  final_snapshot_identifier = "${var.name_prefix}-catalog-final"
  deletion_protection     = true
  performance_insights_enabled = true
  performance_insights_kms_key_id = aws_kms_key.zen.arn
  db_subnet_group_name    = aws_db_subnet_group.catalog.name
  vpc_security_group_ids  = [aws_security_group.catalog.id]
  apply_immediately       = false
  tags                    = var.tags
}

# -----------------------------------------------------------------------------
# IAM role for the Zenith pod (IRSA-friendly): least-privilege S3 + KMS
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "zen_pod" {
  statement {
    sid     = "S3ReadWrite"
    effect  = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:AbortMultipartUpload",
    ]
    resources = [
      aws_s3_bucket.zen.arn,
      "${aws_s3_bucket.zen.arn}/*",
    ]
  }
  statement {
    sid     = "KmsEnvelope"
    effect  = "Allow"
    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:GenerateDataKey",
      "kms:DescribeKey",
    ]
    resources = [aws_kms_key.zen.arn]
  }
  statement {
    sid     = "SecretsRead"
    effect  = "Allow"
    actions = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.catalog.arn]
  }
}

resource "aws_iam_policy" "zen_pod" {
  name   = "${var.name_prefix}-pod"
  policy = data.aws_iam_policy_document.zen_pod.json
}

# -----------------------------------------------------------------------------
# Outputs
# -----------------------------------------------------------------------------

output "bucket" {
  value = aws_s3_bucket.zen.id
}

output "postgres_endpoint" {
  value = aws_db_instance.catalog.endpoint
}

output "kms_key_arn" {
  value = aws_kms_key.zen.arn
}

output "secret_arn" {
  value = aws_secretsmanager_secret.catalog.arn
}

output "iam_policy_arn" {
  value = aws_iam_policy.zen_pod.arn
}
