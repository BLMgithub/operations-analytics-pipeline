resource "google_monitoring_notification_channel" "email" {

  for_each     = nonsensitive(var.alert_email_map)
  display_name = "Pipeline Alert - ${each.value}"
  type         = "email"
  labels       = { email_address = each.value }
}

# All severity CRITICAL are open for 6 hours with 30mins interval repeat notification
resource "google_monitoring_alert_policy" "pipeline_failure" {
  display_name = "Pipeline Failure Alert"
  combiner     = "OR"
  severity     = "CRITICAL"

  notification_channels = [for channel in google_monitoring_notification_channel.email : channel.name]

  conditions {
    display_name = "pipeline_crashed"

    condition_matched_log {
      filter = <<-EOT
      resource.type="cloud_run_job"
      resource.labels.job_name="operations-pipeline${var.environment}"
      textPayload:"[ERROR]"
      EOT
    }
  }
  alert_strategy {

    auto_close           = "21600s"
    notification_prompts = ["OPENED"]

    notification_rate_limit {
      period = "1800s"
    }
  }

  documentation {
    mime_type = "text/markdown"
    content   = <<-EOT
    ## ALERT: Operations Pipeline Processing Failed!

    **What Happened:** The `operations-pipeline-${var.environment}` Cloud Run Job crashed during data processing.

    **Impact:** The raw data was extracted, but the final Parquet files were NOT updated. Dashboards will show yesterday's data.

    **Next Steps for On-Call:**

    1. Check job logs: Did it run out of memory (OOM)?
    2. Check runtime artifact logs: Was there any captured error logs?
    4. Fix the underlying issue and manually execute the pipeline job.
    EOT
  }
}

resource "google_monitoring_alert_policy" "extractor_failure" {

  display_name = "Drive Extractor Failure Alert"
  combiner     = "OR"
  severity     = "CRITICAL"

  notification_channels = [for channel in google_monitoring_notification_channel.email : channel.name]

  conditions {
    display_name = "extractor_crashed"

    condition_matched_log {
      filter = <<-EOT
        resource.type="cloud_run_job"
        resource.labels.job_name="drive-extractor-${var.environment}"
        severity="ERROR"
      EOT
    }
  }

  alert_strategy {
    auto_close           = "21600s"
    notification_prompts = ["OPENED"]

    notification_rate_limit {
      period = "1800s"
    }
  }

  documentation {
    mime_type = "text/markdown"
    content   = <<-EOT
    ## ALERT: Drive Extractor Job Crashed!

    **What Happened:** The `drive-extractor-${var.environment}` Cloud Run Job threw a fatal error. The pipeline is halted.

    **Impact:**
    - Raw CSVs were not successfully pulled from Drive.
    - The `metadata.json` was not written.
    - Or the `.success` flag was not planted.

    **Next Steps for On-Call Responder:**
    1. Check Cloud Run Job logs for Python tracebacks.
    2. Verify that the Google Drive folder is shared with the Drive Extractor SA email.
    3. Once fixed, manually execute the job.
    EOT
  }
}


resource "google_monitoring_alert_policy" "workflow_failure" {
  display_name = "Pipeline Dispatcher Failure Alert"
  combiner     = "OR"
  severity     = "CRITICAL"

  notification_channels = [for channel in google_monitoring_notification_channel.email : channel.name]

  conditions {
    display_name = "pipeline_dispatch_failed"

    condition_matched_log {
      filter = <<-EOT
        resource.type="workflows.googleapis.com/Workflow"
        resource.labels.workflow_id="pipeline-dispatcher-${var.environment}"
        severity>=ERROR
        EOT 
    }
  }
  alert_strategy {
    auto_close           = "21600s"
    notification_prompts = ["OPENED"]

    notification_rate_limit {
      period = "1800s"
    }
  }
  documentation {
    mime_type = "text/markdown"
    content   = <<EOT
    ## ALERT: Running Operations Pipeline has failed!

    **What Happened**: The Eventarc Workflow `pipeline-dispatcher-${var.environment}` encountered a fatal error.

    **Impact**: Dashboard consumers will see stale data.

    **Next Steps for On-Call Responder:**

    1. Click the "View Logs" button below to see the exact error.
    2. Check if the `drive-extractor` successfully dropped the `.success` file.
    3. Check if the `operations-pipeline` Cloud Run Job ran out of memory.
    EOT
  }
}


resource "google_monitoring_alert_policy" "scheduler_failure" {
  display_name = "Midnight Scheduler Failure Alert"
  combiner     = "OR"
  severity     = "CRITICAL"

  notification_channels = [for channel in google_monitoring_notification_channel.email : channel.name]

  conditions {
    display_name = "midnight_scheduler_failed"

    condition_matched_log {
      filter = <<EOT
      resource.type="cloud_scheduler_job"
      jsonPayload.debugInfo="URL_ERROR-ERROR_NOT_FOUND. Original HTTP response code number = 404" OR jsonPayload.debugInfo="URL_ERROR-ERROR_AUTHENTICATION. Original HTTP response code number = 401"
      resource.labels.job_id="midnight-trigger-${var.environment}"
      EOT
    }
  }
  alert_strategy {
    auto_close           = "21600s"
    notification_prompts = ["OPENED"]

    notification_rate_limit {
      period = "1800s"
    }
  }
  documentation {
    mime_type = "text/markdown"
    content   = <<EOT
    ## ALERT: Cloud Scheduler Failed to Run!
      
    **What Happened:** The `midnight-trigger-${var.environment}` job failed to execute.
    
    **Impact:** The entire data pipeline has not started today.

    **Next Steps for On-Call Responder:**

    1. Check the Cloud Scheduler logs. Look for a 401 (Auth token expired) or 403 (IAM permission revoked).
    2. Manually click "Force Run" in the Cloud Scheduler console to start today's extraction.
    EOT
  }
}
