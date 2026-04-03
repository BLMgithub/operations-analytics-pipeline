# WIF Pool
resource "google_iam_workload_identity_pool" "github_pool" {
  project                   = var.project_id
  workload_identity_pool_id = "github-actions-pool-${var.environment}"
  display_name              = "GitHub Pool (${var.environment})"

  depends_on = [google_project_service.enabled_APIs]
}

# Access/auth provider for Github
resource "google_iam_workload_identity_pool_provider" "github_provider" {
  project                            = var.project_id
  workload_identity_pool_id          = google_iam_workload_identity_pool.github_pool.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-provider"
  display_name                       = "GitHub Provider"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.actor"      = "assertion.actor"
    "attribute.repository" = "assertion.repository"
  }

  # Set allowed repo
  attribute_condition = "attribute.repository == \"${var.github_repo}\""

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

output "GITHUB_WIF_PROVIDER_NAME" {
  value       = google_iam_workload_identity_pool_provider.github_provider.name
  description = "GitHub Repository Secret: WIF_PROVIDER"
  sensitive   = true
}
