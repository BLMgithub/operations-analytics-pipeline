# GCP Infrastructure: Operations Analytics Pipeline

This repository contains the Terraform configuration for the Operations Analytics data pipeline. The infrastructure is designed to be serverless, event-driven, and highly secure, utilizing Google Cloud Run, Workflows, and Eventarc.

## Architecture Overview
The pipeline follows a **Trigger-Action-Archive** flow:
1.  **Extraction:** A Cloud Scheduler job triggers the `drive-extractor` Cloud Run job at midnight (PHT).
2.  **Archival:** The extractor saves raw data into the **Archival Bucket** (Coldline storage for 3 years).
3.  **Dispatch:** An Eventarc trigger detects the new file and invokes a Google Workflow (`pipeline-dispatcher`).
4.  **Processing:** The Workflow triggers the main `operations-pipeline` Cloud Run job (2 vCPU, 8Gi RAM) for heavy-duty data processing.
5.  **Transient Storage:** Intermediate files are stored in the **Pipeline Bucket** with a 7-day TTL on raw data to minimize costs and exposure.
6.  **Serving Layer:** The final semantic models are published as **BigQuery External Tables** and presented via stable **Authorized Views** for Power BI and dashboard consumers.

## Prerequisites
*   **Terraform:** Version `~> 1.5.0`
*   **Provider:** `hashicorp/google` version `~> 7.0`
*   **Backend:** GCS bucket `operations-terraform-state-vault-2026` must exist for state management.

## Post-Provisioning (CI/CD Handshake)
The integration between GCP and GitHub Actions requires a one-time "Bootstrap" extraction to populate Repository Secrets. This process completes the cryptographic trust relationship established by Workload Identity Federation (WIF).

### Secret Injection Matrix
| GitHub Secret | Source / Origin | Purpose |
| :--- | :--- | :--- |
| `WIF_PROVIDER` | `terraform output -raw GITHUB_WIF_PROVIDER_NAME` | Logical path for the WIF identity provider handshake. |
| `DEPLOYER_SA_EMAIL` | `github-actions-deployer@...` | Target identity for GitHub OIDC impersonation. |
| `GCP_PROJECT_ID` | `var.project_id` | Project scoping for GCP API and resource discovery. |

### Bootstrapping Constraint
The initial infrastructure provisioning must be executed by a maintainer with `Project IAM Admin` or `Owner` privileges. This "privileged apply" is required to establish the WIF provider and assign the administrative roles to the `github-actions-deployer` service account. Subsequent updates are autonomously managed by the CI/CD identity.

## Infrastructure Components

### Compute & Jobs (`jobs.tf`)
| Resource Name | Type | Memory | Timeout | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| `operations-pipeline` | Cloud Run Job | 8Gi | 30m | Main Polars-based processing engine. Includes 10Gi Local SSD mount at `/tmp`. |
| `drive-extractor` | Cloud Run Job | 1Gi | 15m | Pulls source data from external APIs. |
| `ops-repo` | Artifact Registry | n/a | n/a | Docker repository for pipeline images. |

### Storage & Lifecycle (`storage.tf`)
| Resource Name | Type | Policy / Details |
| :--- | :--- | :--- |
| `ops-archival-storage` | GCS Bucket | Move to Coldline after 400 days; Delete after 3 years. |
| `ops-pipeline-storage` | GCS Bucket | Delete files with prefix `raw/` after 7 days. |
| `seller_semantic` | BQ Dataset | **Protected:** `prevent_destroy = true`; Logical container for Seller fact/dim views. |
| `customer_semantic` | BQ Dataset | **Protected:** `prevent_destroy = true`; Logical container for Customer fact/dim views. |
| `product_semantic` | BQ Dataset | **Protected:** `prevent_destroy = true`; Logical container for Product fact/dim views. |

## Infrastructure-as-Code Workarounds

### Cloud Run Local SSD Strategy (Preview)
The `operations-pipeline` utilizes a **Local SSD** mount at `/tmp` (10Gi) **by provisioning manually** to offload memory pressure from Polars streaming joins. 
*   **The Problem:** As of April 2026, the Google Terraform provider does not natively support the `DISK` medium for `empty_dir` volumes (it defaults to `MEMORY`). 
*   **The Resolution:** Provision manually and utilize lifecycle `ignore_changes` on the `medium` attribute. This allows the job to be created with the SSD partition enabled via the CLI or UI, while preventing Terraform from "correcting" it back to RAM-based storage during subsequent runs.

### BigQuery Accidental Deletion Protection
To safeguard analytical history, all semantic datasets are configured with:
*   `delete_contents_on_destroy = false`: Ensures data/views remain even if the resource is deleted.
*   `prevent_destroy = true`: Forces a manual override to destroy the dataset, protecting it from `terraform destroy` or accidental refactoring.

### Orchestration (`orchestration.tf`)
*   **Cloud Scheduler:** `0 0 * * *` (Daily 12AM PHT) triggers the Extractor.
*   **Eventarc:** Monitors `object.v1.finalized` on the Archival bucket.
*   **Workflows:** `pipeline-dispatcher` evaluates logic to trigger the main pipeline.

## IAM & Security Matrix (`iam_bindings.tf`, `wif.tf`)

This project implements **Zero Trust** via Workload Identity Federation and granular Service Account (SA) permissions.

### Identity Registry
| Identity Name | Role/Purpose |
| :--- | :--- |
| `github-actions-deployer` | CI/CD automation for infra and code updates. |
| `drive-extractor-sa` | I/O identity for data extraction and archival. |
| `ops-pipeline-sa` | Compute identity for the main processing pipeline. |
| `eventarc-invoker-sa` | Orchestration identity to receive events and trigger workflows. |
| `job-invoker-sa` | Scheduler identity to trigger Cloud Run jobs. |

### Permission Bindings
| Identity | Target | Roles | Rationale |
| :--- | :--- | :--- | :--- |
| **Github Deployer** | Project | `run.developer`, `workflows.editor`, `cloudscheduler.admin`, `artifactregistry.admin`, `eventarc.admin`, `storage.admin`, `resourcemanager.projectIamAdmin`, `iam.workloadIdentityPoolAdmin`, `monitoring.admin`, `iam.serviceAccountAdmin`, `iam.serviceAccountUser`, `iam.admin`, `logging.configWriter`, `bigquery.admin`| **Least Privilege:** Granular roles for managing the entire pipeline lifecycle, IAM bindings, state management, and BigQuery schemas. |
| **Drive Extractor** | Archival/Pipeline Buckets | `roles/storage.objectAdmin` | Full CRUD for data landing and archival. |
| **Ops Pipeline** | Pipeline Bucket | `roles/storage.objectAdmin` | Read raw data and write processed artifacts. |
| | Project | `roles/bigquery.dataEditor`, `roles/bigquery.jobUser` | Permission to create External Tables, swap Authorized Views, and execute queries. |
| **Event Invoker** | Project | `roles/eventarc.eventReceiver` | Receive GCS notifications. |
| | Project | `roles/workflows.invoker` | Permission to start workflow execution. |

### Workload Identity Federation
*   **Pool:** `github-pool`
*   **Trust Policy:** Restricted to `${var.github_repo}` to prevent unauthorized repository access.

## Inputs & Variables (`variables.tf`)
| Name | Type | Sensitive | Description |
| :--- | :--- | :--- | :--- |
| `project_id` | `string` | No | Target Google Cloud Project ID. |
| `region` | `string` | No | The Project GCP region. |
| `environment` | `string` | No | Deployment environment (dev, prod). |
| `github_repo` | `string` | No | Format: `owner/repository`. |
| `bq_dataset_id` | `string` | No | BigQuery dataset containing externalized GCS tables. |
| `alert_email_map` | `map` | **Yes** | Monitoring notification recipients. |


## State Management
State is managed remotely in GCS to ensure consistency and locking.
```hcl
terraform {
  backend "gcs" {
    bucket = "operations-terraform-state-vault-2026"
    prefix = "terraform/state"
  }
}
```
