terraform {
  required_version = ">= 1.9"
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

variable "env" {
  type    = string
  default = "local"
}

resource "null_resource" "local_docker_stub" {
  triggers = { env = var.env }

  provisioner "local-exec" {
    command = "echo local_docker module env=${var.env} - optional Docker provider per Architecture 15.2"
  }
}
