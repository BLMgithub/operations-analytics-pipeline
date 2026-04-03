terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Upload tfstate to state storage
terraform {
  backend "gcs" {
    bucket = "operations-terraform-state-vault-2026"
    prefix = "terraform/state"
  }
}

# Enable needed GCP APIs
locals {
  services = [
    "iam.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "run.googleapis.com",
    "workflows.googleapis.com",
    "eventarc.googleapis.com",
    "cloudscheduler.googleapis.com",
    "iamcredentials.googleapis.com",
  ]
}

resource "google_project_service" "enabled_APIs" {
  for_each           = toset(local.services)
  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}
