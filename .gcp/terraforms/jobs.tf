resource "google_cloud_run_v2_job" "pipeline" {
  name       = "operations-pipeline-${var.environment}"
  location   = var.region
  depends_on = [google_project_service.enabled_APIs]

  template {
    template {
      service_account = google_service_account.platform_accounts["ops-pipeline-sa"].email

      # 30-minute timeout and 0 retries
      timeout     = "1800s"
      max_retries = 0

      containers {
        image = "us-docker.pkg.dev/cloudrun/container/hello"

        resources {
          limits = {
            cpu    = "4"
            memory = "8Gi"
          }
        }
        env {
          name  = "POLARS_MAX_THREADS"
          value = "4"
        }
        env {
          name  = "GCP_REGION"
          value = var.region
        }
        env {
          name  = "BQ_DATASET_ID"
          value = var.bq_dataset_id
        }
        env {
          name  = "GCP_PROJECT"
          value = var.project_id
        }

        volume_mounts {
          name       = "ephemeral-disk-1"
          mount_path = "/tmp"
        }
      }

      volumes {
        name = "ephemeral-disk-1"
        empty_dir {
          size_limit = "10Gi"
        }
      }
    }
  }
  lifecycle {
    ignore_changes = [
      # Github ci-infra updates image every update
      template[0].template[0].containers[0].image,
      client,
      client_version,
      # Block terraform from defaulting medium to MEMORY, DISK isn't supported by provider yet
      template[0].template[0].volumes[0].empty_dir[0].medium
    ]
  }
}

resource "google_cloud_run_v2_job" "extractor" {
  name       = "drive-extractor-${var.environment}"
  location   = var.region
  depends_on = [google_project_service.enabled_APIs]

  template {
    template {
      service_account = google_service_account.platform_accounts["drive-extractor-sa"].email

      # 15-minute timeout and 2 retry
      timeout     = "900s"
      max_retries = 2

      containers {
        image = "us-docker.pkg.dev/cloudrun/container/hello"

        resources {
          limits = {
            cpu    = "1"
            memory = "1Gi"
          }
        }
      }
    }
  }
  lifecycle {
    ignore_changes = [
      template[0].template[0].containers[0].image,
      client,
      client_version
    ]
  }
}




resource "google_artifact_registry_repository" "ops_repo" {
  location      = var.region
  repository_id = "operations-artifacts-${var.environment}"
  description   = "Operations Artifacts Repository"
  format        = "DOCKER"
}

