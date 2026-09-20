terraform {
  required_version = ">= 1.9"
  required_providers { aws = { source = "hashicorp/aws", version = "~> 5.0" } }
}
variable "bucket_name" { type = string default = "banking-lakehouse" }
variable "env" { type = string default = "dev" }

resource "aws_s3_bucket" "warehouse" {
  bucket = "${var.bucket_name}-${var.env}"
}
resource "aws_s3_bucket_versioning" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id
  versioning_configuration { status = "Enabled" }
}
