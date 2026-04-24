variable "project_id" {
  description = "The GCP Project ID"
  type        = string
}

variable "region" {
  description = "The Project GCP region"
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

variable "alert_email_map" {
  type        = map(string)
  description = "List of emails to receive pipeline alerts"
  sensitive   = true
}

variable "bq_dataset_id" {
  description = "BigQuery dataset containing externalized GCS tables"
  type        = string
}
