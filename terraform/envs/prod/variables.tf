variable "etl_writer_password" { type = string; sensitive = true }
variable "dq_writer_password" { type = string; sensitive = true }
variable "streamlit_reader_password" { type = string; sensitive = true }
variable "grafana_reader_password" { type = string; sensitive = true }
variable "airflow_app_password" { type = string; sensitive = true }
variable "catalog_app_password" { type = string; sensitive = true }
