# **Pipeline Orchestrator**

**File:** [`run_pipeline.py`](../../data_pipeline/run_pipeline.py)

**Role:** End-to-End Lifecycle, Resource, and Persistence Manager.

![pipeline-orchestration-diagram](/assets/diagrams/01-pipeline-orchestration-diagram.png)

## **System Contract**

**Purpose**

Serves as the central nervous system of the pipeline. It synchronizes data between cloud storage and local compute, manages the strict chronological execution of processing stages, and ensures the absolute cleanup of system resources regardless of run outcome.

**Invariants**
* **Linearity Guarantee:** Stages are strictly gated. No stage can execute unless the preceding required stage returns a "SUCCESS" signal.
* **Resource Isolation:** Every execution is isolated within a unique `run_id` workspace.
* **Silver Persistence Invariant:** The local `contracted/` runtime directory is treated as transient. The **Cloud Silver Store** is the only authoritative source of truth for the Silver layer. Data must be synchronized to the cloud before the local environment is purged.
* **Mandatory Cleanup:** The local `workspace_root` is deterministically purged at the end of every lifecycle (via `finally` blocks) to prevent disk saturation and cross-run data contamination.
* **Lineage Consistency:** A single `run_id` is propagated through every stage, metadata file, and published artifact to ensure 100% traceability.

**Inputs**
* `RunContext`: (The global configuration object containing IDs and Path mappings).
* **Cloud State**: Raw snapshots and historical Silver (contracted) deltas stored in Google Cloud Storage.

**Outputs**
* **Operational Telemetry**: `run_metadata.json` and a collection of `.json` stage reports.
* **Persistent Silver Store**: Updated contract-compliant datasets in the cloud `contract/` directory.
* **Semantic Artifacts**: Validated Fact and Dimension tables in the production zone.
* **System State**: An updated `latest_version.json` pointer in the production zone.

## **Execution Workflow**

The orchestrator manages the lifecycle through a strictly gated 13-step sequence, emphasizing memory efficiency and cloud-local synchronization:

### **Phase I: Environment Initialization**
1.  **Resolve**: Instantiates the `RunContext` and initializes background memory telemetry for real-time benchmarking.
2.  **Hydrate (Raw)**: Synchronizes the required raw data snapshot from Cloud Storage to the local workspace.
3.  **Initialize**: Registers the run commencement by generating `run_metadata.json` with initial "RUNNING" status.

### **Phase II: Processing & Memory Reclamation**
4.  **Validate (Raw)**: Asserts the health of the raw data snapshot; fail-fast on structural errors.
5.  **Contract Processing**: Executes subtractive filtering and freezes schemas into the local `contracted/` path (Silver layer).
6.  **Gate II (Revalidation)**: Defensive check to ensure contracted data meets downstream semantic requirements.
7.  **Promote (Silver)**: Persists the newly contracted datasets to **Cloud Silver Storage**.
8.  **Synchronize (BQ)**: Forces a metadata cache refresh for BigQuery External Tables via system procedures (`BQ.REFRESH_EXTERNAL_METADATA_CACHE`) for immediate visibility.
9.  **Purge (Local)**: Deterministically deletes local `raw/` and `contracted/` directories and invokes `force_gc()` to reclaim RAM before the high-compute Assembly stage.
10. **Assemble**: Flattens relational data into a unified Gold-layer event grain using the **BigQuery Storage Read API** (bypassing the need for local Silver restoration).
11. **Modeling (Semantic)**: Builds entity-centric analytical modules (Fact/Dim tables).

### **Phase III: Activation & Finalization**
12. **Publish**: Executes final integrity gates, performs the **BigQuery View Swap** for the BI layer, and triggers the atomic pointer swap (`_latest.json`) to activate the new version.
13. **Finalize**: Updates terminal metadata (status, duration), uploads all telemetry/stage reports to Cloud Storage, and purges the entire local workspace.

## **Boundaries**

| This component **DOES** | This component **DOES NOT** |
| :--- | :--- |
| Coordinate the sequence of high-level executors. | Modify rows, columns, or data values. |
| Manage local/cloud data synchronization and BQ caching. | Implement business logic or aggregation rules. |
| Enforce the "Purge-before-Assembly" memory optimization. | Define the technical schema for Fact/Dim tables. |
| Manage the `finally` block for resource safety. | Direct file-level I/O within a stage (Delegated). |
| Aggregate stage-level reports into a run summary. | Perform granular row-level validation. |
| Monitor and log real-time memory telemetry. | Execute SQL transformations directly (Delegated). |

## **Failure & Severity Model**

### **System Failures (Fatal)**
* **Sync Failure**: If the pipeline cannot upload to or download from the Cloud Silver Store, it raises an exception and halts. This prevents the Assembly stage from running on stale or missing data.
* **Stage Crash**: Any unhandled exception within an executor is caught. The pipeline terminates immediately to prevent "partial processing."

### **Resource Recovery**
* **The "Finally" Contract**: Even if a stage crashes mid-execution, the orchestrator guarantees that the local workspace is purged. This prevents a failed run from leaving "ghost" data that could be picked up by the next run.

### **Telemetry on Failure**
* **Partial Logs**: Even on failure, the orchestrator attempts to upload available stage reports to the cloud. This ensures you have the diagnostic data to see which rule in the "Contract" stage triggered the failure before the "Purge" occurred.