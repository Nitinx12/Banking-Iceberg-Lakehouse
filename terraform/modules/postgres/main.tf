terraform {
  required_version = ">= 1.9"
  required_providers { postgresql = { source = "cyrilgdn/postgresql", version = "~> 1.22" } }
}
variable "warehouse_db_name" { type=string default="banking_dw" }
variable "etl_writer_password" { type=string sensitive=true }
resource "postgresql_database" "warehouse" { name=var.warehouse_db_name }
