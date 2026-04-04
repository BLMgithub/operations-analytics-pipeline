# Operations Analytics Pipeline: Scalable Integrity Engine

[![CI * Code Quality & Tests](https://github.com/BLMgithub/operations-analytics-pipeline/actions/workflows/ci-code.yml/badge.svg)](https://github.com/BLMgithub/operations-analytics-pipeline/actions/workflows/ci-code.yml)
[![CI * Infra Enforcement](https://github.com/BLMgithub/operations-analytics-pipeline/actions/workflows/ci-infra.yml/badge.svg)](https://github.com/BLMgithub/operations-analytics-pipeline/actions/workflows/ci-infra.yml)
[![CD * Data Pipeline](https://github.com/BLMgithub/operations-analytics-pipeline/actions/workflows/cd-pipeline.yml/badge.svg)](https://github.com/BLMgithub/operations-analytics-pipeline/actions/workflows/cd-pipeline.yml)
[![CD * Data Extractor](https://github.com/BLMgithub/operations-analytics-pipeline/actions/workflows/cd-extract.yml/badge.svg)](https://github.com/BLMgithub/operations-analytics-pipeline/actions/workflows/cd-extract.yml)

## Overview
Small to mid-sized organizations are trapped in a cycle where they outgrow the capabilities of spreadsheets but lack the technical infrastructure to migrate to databases. This creates a "structural debt" where the very tools that allow a business to be agile (spreadsheets) are the same tools that make their data fundamentally untrustworthy for scaling or reporting.

### The Solution
This project solves that challenge by delivering a highly resilient, event-driven data pipeline on Google Cloud Platform for reliable operational analytics. It guarantees data integrity through a strict Medallion architecture (Bronze, Silver, Gold) that relies on rigid data contracts and validation gates to catch and isolate bad data early in the lifecycle.

### Defensive Pipeline Architecture
![WIP_pipeline_diagram_picture](https://still-working-on-it.need-to-finish-readme.first)

To eliminate the risk of cross-run data contamination and memory bloat, the pipeline employs a defensive state-management strategy where local compute environments are strictly temporary:
* **Stateless Orchestration:** Every execution operates within an isolated, deterministic `run_id` workspace that is aggressively purged post-run.
* **Cloud Sync & Purge:** After processing data into the Silver layer, the system syncs the output to Cloud Storage, purging the local environment.
* **Historical Context Pull:** It then safely re-downloads the complete historical state for Gold layer aggregation, ensuring every run builds analytical models in a clean, untainted environment.
* **Linear Gating:** Stages are strictly gated; failure at any tier (Ingestion, Contract, or Assembly) prevents downstream processing and ensures partial data is never promoted.
* **Resource-Optimized Compute:** Leverages a highly efficient lazy-evaluation engine to process large-scale datasets seamlessly within the strict memory constraints of serverless environments.

### Event-Driven Cloud Infrastructure
![gcp-orchestration-diagram](assets/screenshots/gcp-orchestration-diagram.png)

The underlying infrastructure is entirely serverless, decoupled, and codified via Terraform to ensure reproducibility and security:
* **Orchestrated Compute:** Cloud Scheduler initiates daily extraction via Cloud Run, separating the extraction layer from the main processing logic.
* **Event-Driven Triggers:** Eventarc monitors Cloud Storage for `.success` flags, triggering the main processing job via Cloud Workflows only when extraction succeeds.
* **Zero-Trust CI/CD:** GitHub Actions leverage Workload Identity Federation (WIF) for keyless, secure deployments of all infrastructure and Cloud Run jobs.
* **Integrated Observability:** Native Cloud Logging and Cloud Monitoring provide comprehensive telemetry and automated responder alerts for pipeline health.

## Architecture & System Design

### The Medallion Data Model & Contracts

The pipeline does not just move data; it actively defends the analytical layer from upstream anomalies. It enforces a strict Medallion architecture governed by a registry-driven rule engine.

**Bronze (Raw Snapshots)**
* **Purpose:** Immutable, un-typed snapshots of source systems.
* **State:** Temporarily downloaded into the isolated workspace. Data here is assumed to be structurally untrustworthy, containing nulls, duplicates, and orphaned records.

**Silver (The Contract Layer)**
* **Philosophy (Subtractive-Only Logic):** The pipeline never guesses, imputes, or "repairs" bad data. If a record violates the contract, it is explicitly dropped, and the loss is logged in the telemetry report.
* **Role-Based Rules:** Tables are classified by role (`event_fact`, `transaction_detail`, `entity_reference`) and subjected to specific registry rules (e.g., deduplication, non-null assertions).
* **Referential Integrity (Cascade Cleanup):** The pipeline tracks invalidated parent IDs (e.g., malformed `order_id`s) and propagates them downstream. If an order is dropped, all associated child records (like line items) are cascade-dropped to prevent orphan data from polluting joins.
* **Schema Freeze:** Output files are strictly cast to predefined data types and projected to contain only approved columns before being written to Cloud Storage.

**Gold (The Semantic Layer)**
* **Purpose:** Business-ready Fact and Dimension tables modeled for entity-centric and cohort analysis (Customers, Sellers, Products).
* **Strict Grain Enforcement:**
    * **Temporal:** All fact tables are deterministically aligned to an ISO-Week grain (`W-MON`).
    * **Entity:** The engine validates that Dimension tables contain exactly one row per `Entity_ID`, and Fact tables contain exactly one row per `(Entity_ID, order_year_week)`.
* **Lineage Integrity:** The Semantic builder aggressively checks that the assembled data belongs to a single `run_id`. Cross-run data contamination triggers a terminal failure, preventing poisoned data from ever reaching production.

### Validation Gates & Deployment Integrity

* **Dual-Pass Validation Strategy:**
    * **Initial Validation (Raw Gate):** The orchestrator evaluates raw snapshots. At this stage, `warnings` (like duplicate IDs or nulls) are tolerated and passed down to the Contract Stage for subtractive cleanup. Only fatal structural errors abort the run.
    * **Post-Contract Revalidation (Silver Gate):** After contract rules are applied, the system re-runs validation. In this phase, `warnings` are escalated to fatal. Because the contract stage guarantees a clean schema, any remaining warnings trigger a terminal `RuntimeError`, halting the pipeline immediately to prevent downstream corruption.
* **Atomic Publishing Lifecycle:** The pipeline protects the Gold layer by writing intermediate analytical models to isolated temporary directories during computation. Only when *all* semantic modules successfully finish processing does the system execute an atomic publish via `latest_version.json` pointer updates, guaranteeing that partial or incomplete data is never served to dashboards.
* **Comprehensive Telemetry:**
    * **End-to-End Traceability:** A single `run_id` is propagated through all raw snapshots, metadata logs, and published artifacts to provide absolute lineage tracking.
    * **Resilient Logging:** Even in the event of a fatal crash, the orchestrator's `finally` block guarantees that partial logs and stage reports are synced back to cloud storage before the local workspace is purged, ensuring debuggability.

## Performance & Scale

The pipeline is explicitly engineered to process large-scale historical data without breaching the strict memory constraints of serverless compute (Cloud Run). To achieve this, the core execution engine was migrated from Pandas to Polars, utilizing `LazyFrames` and streaming evaluation.

**The Benchmark Constraint: 4GB RAM / 2 vCPU**

![polars-vs-pandas](assets/screenshots/pandas-vs-polars.png)
>The dataset for this chart is available at [`benchmark`](/assets/benchmarks/) and the instruction to download 15m rows dataset found in [`data/`](/data/README)

* **Measurement Methodology:** Performance profiles were captured by executing the pipeline locally via [`docker-compose.benchmark`](docker-compose.benchmark.yml) configured to precisely mirror the Cloud Run constraints (`memory="4G" cpus="2" POLARS_MAX_THREADS=2`). Resource footprints were tracked sequentially via a PowerShell polling script:
  ```powershell
  while ($true) { docker stats --no-stream --format "{{.Name}}, {{.CPUPerc}}, {{.MemUsage}}, {{.MemPerc}}" >> stats_log.csv; Start-Sleep -Seconds 1 }
  ```
* **The Pandas Ceiling (4M Rows in 88s):** Under the legacy Pandas engine, memory usage became fully saturated (100% / 4GiB) when processing a 4-million-row dataset. Because Pandas executes eagerly and loads entire datasets into memory, any dataset larger than 4M rows resulted in an inevitable Out-Of-Memory (OOM) crash.
* **The Polars Migration (15M Rows in 67s):** By switching to the Polars Lazy API, the pipeline now processes a dataset nearly 4x larger (15 million rows) while actually reducing execution time from 88 seconds to 67 seconds within the exact same 4GB/2vCPU constraint.
    * **Streaming Evaluation:** Instead of eagerly loading the whole dataset, Polars processes data in batches, drastically reducing the memory footprint.
    * **Multi-Core Utilization:** Unlike single-threaded Pandas (peaking at ~100% CPU), the Polars engine effectively parallelizes the workload, consistently utilizing ~200% CPU across both provisioned cores.
    * **Zero-Copy Export:** The Semantic stage leverages `sink_parquet` to write analytical models directly to disk via streaming, ensuring memory is freed instantaneously during the final Gold-layer assembly.


## Observability & Alerting

![WIP_google_cloud_dashboard_monitoring_pictures](https://still-working-on-it.need-to-finish-readme.first)

Operational maturity requires assuming things will eventually break. The pipeline features a comprehensive observability suite managed natively via Google Cloud Monitoring and Cloud Logging, codified entirely in Terraform.

### Monitored Telemetry & Dashboards
The custom Cloud Monitoring dashboard tracks granular operational metrics to proactively identify resource bottlenecks and execution failures:

**Pipeline Job Metrics:**
1. **Workflow Execution Traffic:** Measures the volume of finished pipeline runs.
2. **Execution Status Ratio:** Tracks the count of `SUCCESS` vs. `FAILED` runs to monitor overall reliability.
3. **Memory Allocation Bottlenecks:** Plots the actual Cloud Run memory usage against a hardcoded 4GB horizontal threshold to visualize proximity to OOM exhaustion.

**Extractor Job Metrics:**
1. **Drive Extractor Latency:** Tracks the billable instance time of the extractor job (the most accurate proxy for API usage cost, as the extractor utilizes the Drive API continuously during runtime).
2. **Drive API Latencies (Median):** Monitors the median response times for core Google Workspace API calls (e.g., `google.apis.drive.v3.DriveFiles.Get` for extraction and `DriveFiles.List` for directory parsing).
3. **Memory Allocation Bottlenecks:** Plots the extractor's memory usage against its specific 1GB hardcoded Cloud Run threshold.

### Automated Responders & Alerts
The system monitors specific log payloads across the infrastructure and dispatches `CRITICAL` severity email alerts to on-call responders with actionable, markdown-formatted runbooks. Alerts are configured for:
* **Ingestion Failures (`midnight_scheduler_failed`):** Detects IAM permission revokes (403) or token expiries (401) preventing the daily trigger.
* **Extraction Crashes (`extractor_crashed`):** Captures Python tracebacks if the Drive Extractor fails to pull raw data or plant the `.success` flag.
* **Orchestration Breakdowns (`pipeline_dispatch_failed`):** Catches Eventarc workflow failures if downstream routing breaks.
* **Pipeline Fatals (`pipeline_crashed`):** Detects out-of-memory (OOM) events or unhandled exceptions within the main processing logic, ensuring dashboard consumers are never silently served stale data.

## Repository Structure

```text
operations-analytics-pipeline/
├── .gcp/
│   └── terraforms/         # IaC for all GCP resources (Cloud Run, Eventarc, Storage, IAM)
├── .github/
│   └── workflows/          # CI/CD pipelines (Terraform apply, Docker build/push, Code quality & test)
├── assets/
│   └── benchmarks/         # Performance profiling logs (Pandas vs Polars memory usage)
├── data/                   # Git-ignored local directories used when simulating cloud storage
│   ├── raw/                # Extracted snapshot dumps
│   ├── contracted/         # Intermediate Silver-layer files
│   ├── published/          # Final Gold-layer analytical models
│   └── run_artifact/       # Lineage metadata and stage execution logs
├── data_extract/
│   ├── shared/             # Extractor logic and core I/O utilities
│   └── run_extract.py      # The Drive extractor orchestrator
├── data_pipeline/
│   ├── assembly/           # Delta merging and event mapping logic (Gold Pre-processing)
│   ├── contract/           # Subtractive filtering logic (Silver Layer)
│   ├── publish/            # Manages the atomic publish lifecycle of semantic datasets
│   ├── semantic/           # Fact/Dimension table builders (Gold Layer)
│   ├── shared/             # Storage adapters, IO wrappers, and registry configurations
│   ├── validation/         # Dual-pass structural data validation gates
│   └── run_pipeline.py     # The pipeline orchestrator and state manager
├── docs/                   # Detailed architectural and stage-level system contracts
├── runtime/                # Git-ignored ephemeral workspace used by the local pipeline executor
└── tests/                  # Pytest suite for pipeline logic and validation rules
```

## CI/CD & Security

The project adheres to a strict "Zero-Trust" deployment model. No permanent service account keys are generated, downloaded, or stored as GitHub Secrets.

* **Workload Identity Federation (WIF):** GitHub Actions is authenticated to Google Cloud via short-lived, dynamically requested OIDC tokens.
* **Infrastructure as Code:** The deployment of the infrastructure and the configuration of IAM bindings are strictly managed via automated `terraform plan` and `terraform apply` workflows.
* **Containerized Artifacts:** Upon passing CI checks, the pipeline and extractor codebases are packaged into Docker images and pushed to the GCP Artifact Registry.