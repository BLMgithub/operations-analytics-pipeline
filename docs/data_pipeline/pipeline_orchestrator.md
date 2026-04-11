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

The orchestrator manages the lifecycle in three high-level phases, featuring a defensive synchronization loop:

### **Phase I: Environment Initialization**
1.  **Context Resolution**: Instantiates the `RunContext`.
2.  **Metadata Start**: Persists the initial "RUNNING" state to `run_metadata.json`.
3.  **Ingestion**: Downloads the required raw data snapshot from the cloud to the local workspace.

### **Phase II: The Defensive Cloud-Sync Loop**
1.  **Gate I (Raw Validation)**: Asserts the health of the downloaded raw data.
2.  **Contract Processing**: Filters rows and freezes schemas into the local `contracted/` path.
3.  **Gate II (Revalidation)**: Defensive check to ensure the local Silver data is structurally sound.
4.  **Silver Synchronization (Upload)**: Promotes the newly contracted data to the **Cloud Silver Storage** to ensure delta accumulation and persistence.
5.  **Environment Purge**: Deletes the local `raw_snapshot/` and `contracted/` directories and triggers `gc.collect()` to free system memory.
6.  **Silver Restoration (Download)**: Recreates the local `contracted/` directory and downloads the **accumulated Silver deltas** from the Cloud storage.
7.  **Integration (Assembly)**: Merges the restored data into the Gold-layer event grain.
8.  **Modeling (Semantic)**: Builds the final analytical modules.
9.  **Gate III (Pre-Publish)**: Verifies the completeness of semantic artifacts.

### **Phase III: Finalization & Cleanup**
1.  **Promotion**: Atomically updates the production pointer (`latest_version.json`).
2.  **Persistence**: Uploads all logs and metadata back to cloud storage.
3.  **Final Purge**: Deletes the entire local `workspace_root`.

## **Boundaries**

| This component **DOES** | This component **DOES NOT** |
| :--- | :--- |
| Coordinate the sequence of high-level executors. | Modify rows, columns, or data values. |
| Manage local/cloud data synchronization. | Implement business logic or aggregation rules. |
| Manage the Silver "Upload-Purge-Download" cycle. | Define the technical schema for Fact/Dim tables. |
| Manage the `finally` block for resource safety. | Direct file-level I/O within a stage (Delegated). |
| Aggregate stage-level reports into a run summary. | Perform granular row-level validation. |

## **Failure & Severity Model**

### **System Failures (Fatal)**
* **Sync Failure**: If the pipeline cannot upload to or download from the Cloud Silver Store, it raises an exception and halts. This prevents the Assembly stage from running on stale or missing data.
* **Stage Crash**: Any unhandled exception within an executor is caught. The pipeline terminates immediately to prevent "partial processing."

### **Resource Recovery**
* **The "Finally" Contract**: Even if a stage crashes mid-execution, the orchestrator guarantees that the local workspace is purged. This prevents a failed run from leaving "ghost" data that could be picked up by the next run.

### **Telemetry on Failure**
* **Partial Logs**: Even on failure, the orchestrator attempts to upload available stage reports to the cloud. This ensures you have the diagnostic data to see which rule in the "Contract" stage triggered the failure before the "Purge" occurred.