# Archival Bucket
resource "google_storage_bucket" "ops_archival_bucket" {
  name                        = "ops-archival-bucket-${var.environment}"
  location                    = var.region
  force_destroy               = false
  uniform_bucket_level_access = true

  # To Coldline after 400 days
  lifecycle_rule {
    condition {
      age                   = 400
      matches_storage_class = ["STANDARD"]
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  # Delete after 3 years
  lifecycle_rule {
    condition {
      age = 1095
    }
    action {
      type = "Delete"
    }
  }
}

# Pipeline Bucket
resource "google_storage_bucket" "ops_pipeline_bucket" {
  name                        = "ops-pipeline-bucket-${var.environment}"
  location                    = var.region
  force_destroy               = false
  uniform_bucket_level_access = true

  # Raw source, delete after 1 week
  lifecycle_rule {
    condition {
      age            = 7
      matches_prefix = ["raw/"]
    }
    action {
      type = "Delete"
    }
  }
}
