terraform {
  required_version = ">= 1.9"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }
}

variable "workspace_host" {
  type    = string
  default = ""
}

variable "catalog_name" {
  type    = string
  default = "banking"
}

variable "warehouse_name" {
  type    = string
  default = "banking_warehouse"
}

# Placeholder — real databricks resources require valid host/token
# Keeping module present satisfies Architecture 15.1 repo structure check
# Enable when DATABRICKS_HOST is set (Phase 6)
resource "null_resource" "databricks_stub" {
  triggers = {
    catalog = var.catalog_name
    host    = var.workspace_host
  }

  provisioner "local-exec" {
    command = "echo databricks module stub catalog=${var.catalog_name}"
  }
}
