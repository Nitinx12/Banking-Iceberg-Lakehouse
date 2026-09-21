variable "etl_writer_password" {
  type      = string
  sensitive = true
}

variable "streamlit_reader_password" {
  type      = string
  sensitive = true
}

variable "grafana_reader_password" {
  type      = string
  sensitive = true
}

variable "databricks_host" {
  type        = string
  default     = ""
  sensitive   = false
  description = "Databricks workspace host (empty = stub, Phase 7)"
}
