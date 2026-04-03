# Google Workflows
resource "google_workflows_workflow" "pipeline_dispatcher" {
  name            = "pipeline-dispatcher-${var.environment}"
  region          = var.region
  description     = "Evaluates .success files and triggers pipeline"
  service_account = google_service_account.platform_accounts["eventarc-invoker-sa"].email

  # Workflow yml
  source_contents = file("${path.module}/../workflows/pipeline-dispatcher.yml")

  depends_on = [google_project_service.enabled_APIs]
}

# Pipeline Trigger: Eventarc
resource "google_eventarc_trigger" "pipeline_dispatcher" {
  name     = "pipeline-trigger-${var.environment}"
  location = var.region

  # Monitor Archival Bucket
  matching_criteria {
    attribute = "type"
    value     = "google.cloud.storage.object.v1.finalized"
  }
  matching_criteria {
    attribute = "bucket"
    value     = google_storage_bucket.ops_archival_bucket.name
  }

  # Route event to Workflow
  destination {
    workflow = google_workflows_workflow.pipeline_dispatcher.id
  }

  service_account = google_service_account.platform_accounts["eventarc-invoker-sa"].email

  depends_on = [
    google_project_iam_member.eventarc_event_receiver,
    google_project_iam_member.eventarc_workflows_invoker
  ]
}

# Drive Extractor Trigger: Cloud Scheduler
resource "google_cloud_scheduler_job" "extractor_trigger" {
  name        = "midnight-trigger-${var.environment}"
  description = "Execute drive-extractor daily 12AM (PHT)"
  schedule    = "0 0 * * *"
  time_zone   = "Asia/Manila"
  region      = var.region

  http_target {
    http_method = "POST"
    # Points to the Deployed Cloud Run job (data extractor)
    uri = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/drive-extractor-${var.environment}:run"

    oauth_token {
      service_account_email = google_service_account.platform_accounts["job-invoker-sa"].email
    }
  }
}
