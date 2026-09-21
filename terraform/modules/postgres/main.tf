terraform {
  required_version = ">= 1.9"

  required_providers {
    postgresql = {
      source  = "cyrilgdn/postgresql"
      version = "~> 1.22"
    }
  }
}

variable "warehouse_db_name" {
  type    = string
  default = "banking_dw"
}

variable "etl_writer_password" {
  type      = string
  sensitive = true
}

variable "dq_writer_password" {
  type      = string
  sensitive = true
  default   = "local_dq_writer"
}

variable "streamlit_reader_password" {
  type      = string
  sensitive = true
}

variable "grafana_reader_password" {
  type      = string
  sensitive = true
}

variable "airflow_app_password" {
  type      = string
  sensitive = true
  default   = "local_airflow_app"
}

variable "catalog_app_password" {
  type      = string
  sensitive = true
  default   = "local_catalog_app"
}

resource "postgresql_role" "etl_writer" {
  name     = "etl_writer"
  login    = true
  password = var.etl_writer_password
}

resource "postgresql_role" "dq_writer" {
  name     = "dq_writer"
  login    = true
  password = var.dq_writer_password
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

resource "postgresql_role" "airflow_app" {
  name     = "airflow_app"
  login    = true
  password = var.airflow_app_password
}

resource "postgresql_role" "catalog_app" {
  name     = "catalog_app"
  login    = true
  password = var.catalog_app_password
}
