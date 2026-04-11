# ------------------------------------------------------------
# PROJECT-LEVEL BINDINGS
# ------------------------------------------------------------

# SA to enable repository deployment
resource "google_service_account_iam_member" "github_deployer_sa" {
  service_account_id = google_service_account.platform_accounts["github-actions-deployer"].name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github_pool.name}/attribute.repository/${var.github_repo}"
}

# Roles for github-actions-deployer
locals {
  deployer_roles = [
    "roles/run.developer",                   # Manage Cloud Run jobs
    "roles/workflows.editor",                # Manage Workflows
    "roles/cloudscheduler.admin",            # Manage Scheduler
    "roles/iam.serviceAccountUser",          # Act as SAs for jobs
    "roles/artifactregistry.admin",          # Manage Artifact Registry
    "roles/eventarc.admin",                  # Manage Eventarc triggers
    "roles/storage.admin",                   # Manage buckets and state locking
    "roles/resourcemanager.projectIamAdmin", # Manage the IAM bindings in this code
    "roles/iam.workloadIdentityPoolAdmin",   # Manage WIF in wif.tf
    "roles/monitoring.admin",                # Manage Monitoring in monitoring.tf
    "roles/logging.configWriter",            # Required for log-based alert policies
    "roles/iam.serviceAccountAdmin",         # Manage Alert policies in monitoring.tf
    "roles/iam.admin"                        # Manage Iam roles
  ]
}

resource "google_project_iam_member" "github_deployer_permissions" {
  for_each = toset(local.deployer_roles)
  project  = var.project_id
  role     = each.key
  member   = "serviceAccount:${google_service_account.platform_accounts["github-actions-deployer"].email}"
}

# Enable to receive events
resource "google_project_iam_member" "eventarc_event_receiver" {
  project = var.project_id
  role    = "roles/eventarc.eventReceiver"
  member  = "serviceAccount:${google_service_account.platform_accounts["eventarc-invoker-sa"].email}"
}

# Enable Eventarc to trigger Workflows
resource "google_project_iam_member" "eventarc_workflows_invoker" {
  project = var.project_id
  role    = "roles/workflows.invoker"
  member  = "serviceAccount:${google_service_account.platform_accounts["eventarc-invoker-sa"].email}"
}


# ------------------------------------------------------------
# RESOURCE-LEVEL BINDINGS (Storage & Compute)
# ------------------------------------------------------------

# Extractor Bucket Access
resource "google_storage_bucket_iam_member" "drive_extractor_archival_access" {
  bucket = google_storage_bucket.ops_archival_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.platform_accounts["drive-extractor-sa"].email}"
}

resource "google_storage_bucket_iam_member" "drive_extractor_pipeline_access" {
  bucket = google_storage_bucket.ops_pipeline_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.platform_accounts["drive-extractor-sa"].email}"
}

# Pipeline Runner Bucket Access
resource "google_storage_bucket_iam_member" "pipeline_runner_pipeline_access" {
  bucket = google_storage_bucket.ops_pipeline_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.platform_accounts["ops-pipeline-sa"].email}"
}


# ------------------------------------------------------------
# GOOGLE SERVICE AGENTS (Pub/Sub)
# ------------------------------------------------------------

# GCS Service Agent email
data "google_storage_project_service_account" "gcs_account" {
  project = var.project_id
}

# Enable permssion to publishing to agent
resource "google_project_iam_member" "gcs_pubsub_publishing" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${data.google_storage_project_service_account.gcs_account.email_address}"
}
