variable "project_id" {
  description = "The GCP Project ID"
  type        = string
}

variable "region" {
  description = "The Default GCP region"
  type        = string
  default     = "us-east1"
}

variable "environment" {
  description = "The environment (e.g. Dev, Staging, Production)"
  type        = string
}

variable "github_repo" {
  description = "GitHub Repository (Format: owner/repository)"
  type        = string
}
