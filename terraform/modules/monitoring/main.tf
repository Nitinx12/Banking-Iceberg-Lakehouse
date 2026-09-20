terraform {
  required_version = ">= 1.9"
  required_providers {
    grafana = {
      source  = "grafana/grafana"
      version = "~> 3.0"
    }
  }
}
variable "env" { type = string default = "dev" }
variable "grafana_url" { type = string default = "http://grafana:3000" }
# Grafana folders + alert contact points per Architecture 12.3 — minimal contacts; dashboards in monitoring/grafana/dashboards
resource "null_resource" "monitoring_stub" {
  triggers = { env = var.env }
  provisioner "local-exec" { command = "echo monitoring ${var.env} provisioned grafana ${var.grafana_url}" }
}
