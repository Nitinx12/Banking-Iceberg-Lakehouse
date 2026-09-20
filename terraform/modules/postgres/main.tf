terraform {
  required_version = ">= 1.9"
  required_providers { postgresql = { source = "cyrilgdn/postgresql", version = "~> 1.22" } }
}
variable "warehouse_db_name" { type = string default = "banking_dw" }
variable "etl_writer_password" { type = string sensitive = true }
variable "streamlit_reader_password" { type = string sensitive = true }
variable "grafana_reader_password" { type = string sensitive = true }

resource "postgresql_role" "etl_writer" {
  name     = "etl_writer"
  login    = true
  password = var.etl_writer_password
}
resource "postgresql_role" "streamlit_reader" {
  name     = "streamlit_reader"
  login    = true
  password = var.streamlit_reader_password
}
resource "postgresql_role" "grafana_reader" {
  name     = "grafana_reader"
  login    = true
  password = var.grafana_reader_password
}
resource "postgresql_database" "warehouse" {
  name  = var.warehouse_db_name
  owner = postgresql_role.etl_writer.name
}
resource "postgresql_schema" "serving" {
  name     = "serving"
  database = postgresql_database.warehouse.name
  owner    = postgresql_role.etl_writer.name
}
resource "postgresql_grant" "reader_serving" {
  database    = postgresql_database.warehouse.name
  role        = postgresql_role.streamlit_reader.name
  schema      = postgresql_schema.serving.name
  object_type = "table"
  privileges  = ["SELECT"]
}
