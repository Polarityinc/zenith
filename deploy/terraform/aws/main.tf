terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "name_prefix" {
  type    = string
  default = "zenithdb"
}

provider "aws" {
  region = var.region
}

resource "aws_s3_bucket" "zen" {
  bucket = "${var.name_prefix}-prod"
}

resource "aws_s3_bucket_versioning" "zen" {
  bucket = aws_s3_bucket.zen.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_db_instance" "catalog" {
  identifier             = "${var.name_prefix}-catalog"
  engine                 = "postgres"
  engine_version         = "14.19"
  instance_class         = "db.m6g.large"
  allocated_storage      = 100
  storage_type           = "gp3"
  username               = "zen"
  password               = "REPLACE_ME_WITH_SECRETS_MANAGER"
  db_name                = "zenith"
  publicly_accessible    = false
  skip_final_snapshot    = true
  apply_immediately      = true
}

output "bucket" {
  value = aws_s3_bucket.zen.id
}

output "postgres_endpoint" {
  value = aws_db_instance.catalog.endpoint
}
