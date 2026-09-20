terraform {
  required_version = ">= 1.9"
}
variable "env" { type = string default = "dev" }
# Grafana folders + alert contact points per Architecture 15 — stub for Phase 6
resource "null_resource" "monitoring_stub" {
  triggers = { env = var.env }
  provisioner "local-exec" { command = "echo monitoring ${var.env} provisioned" }
}
