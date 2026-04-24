# ------------------------------------------------------------
# OPS EXTERNALIZED TABLES (For metadata caching)
# ------------------------------------------------------------

resource "google_bigquery_connection" "biglake_connection" {
  connection_id = "ops_biglake_connection"
  location      = var.region
  friendly_name = "BigLake Connection for GCS Parquet Scanning"
  cloud_resource {}
}

# Enable connection service to access pipeline bucket
resource "google_storage_bucket_iam_member" "biglake_storage_viewer" {
  bucket = google_storage_bucket.ops_pipeline_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_bigquery_connection.biglake_connection.cloud_resource[0].service_account_id}"
}

resource "google_bigquery_dataset" "silver_dataset" {
  dataset_id = var.bq_dataset_id
  location   = var.region

  delete_contents_on_destroy = false
}

locals {
  external_tables = [
    "df_orders",
    "df_customers",
    "df_order_items",
    "df_products",
    "df_payments"
  ]
}

resource "google_bigquery_table" "external_tables" {
  for_each   = toset(local.external_tables)
  dataset_id = google_bigquery_dataset.silver_dataset.dataset_id
  table_id   = each.key

  # Might throw error if contracted/ is empty
  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    connection_id = google_bigquery_connection.biglake_connection.name
    source_uris   = ["gs://${google_storage_bucket.ops_pipeline_bucket.name}/contracted/${each.key}_*.parquet"]

    # Triggered manually by pipeline
    metadata_cache_mode = "MANUAL"
  }
  lifecycle {
    prevent_destroy = true
  }
}


# ------------------------------------------------------------
# BIGQUERY SEMANTTIC DATASETS (For table versionining)
# ------------------------------------------------------------

locals {
  # Expiration for versioned tables
  one_month_ms = 2678400000

  semantic_datasets = [
    "seller_semantic",
    "customer_semantic",
    "product_semantic"
  ]
}

resource "google_bigquery_dataset" "semantic_datasets" {
  for_each   = toset(local.semantic_datasets)
  dataset_id = each.key
  location   = var.region

  delete_contents_on_destroy  = false
  default_table_expiration_ms = local.one_month_ms

  description = "Semantic layer for ${each.key}. Tables expire after 1 month."

  labels = {
    env   = var.environment
    layer = "semantic"
  }

  lifecycle {
    prevent_destroy = true
  }
}
