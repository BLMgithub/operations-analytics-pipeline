# **Publish Stage**

**Files:**
* **Executor:** [`publish_executor.py`](../../data_pipeline/publish/publish_executor.py)
* **Logic:** [`publish_logic.py`](../../data_pipeline/publish/publish_logic.py)

**Role:** Production Promotion and Versioning.

## **System Contract**

**Purpose**

Serves as the final gate and deployment mechanism for the pipeline. It transitions validated semantic artifacts into a permanent, versioned storage layer and updates the system's "latest" pointer to ensure Business Intelligence (BI) tools consume the most recent high-quality data.

**Invariants**
* **Integrity-Gated Promotion:** Promotion to the production zone is strictly prohibited if any table defined in the `SEMANTIC_MODULES` registry is missing or inaccessible.
* **Atomic Activation:** The update to the `latest_version.json` pointer must be atomic (e.g., using `os.replace` locally) to prevent downstream tools from reading a partially written or corrupted manifest.
* **Version Immutability:** Once a run is archived in a `v{run_id}` directory, the files are treated as read-only snapshots; they are never updated or overwritten by subsequent runs.
* **Decoupled Storage:** Supports transparent publishing across both Local Filesystems and Google Cloud Storage (GCS) via a storage adapter.

**Inputs**
* `run_context`: `RunContext` (Contains the unique `run_id` and the semantic/published path configurations).
* `SEMANTIC_MODULES`: `Registry` (The source of truth for which artifacts must exist to pass the integrity gate).

**Outputs**
* **Publish Report:** `dict` (Telemetry for the integrity check, file promotion status, and pointer update).
* **Versioned Artifacts:** A new directory `/published/v{run_id}/` containing the full suite of semantic Fact and Dimension tables.
* **Latest Pointer:** An updated `latest_version.json` file in the root of the published zone.

## **Execution Workflow**

The **Executor** ensures the production release follows a fail-fast, three-phase sequence:

1.  **Integrity Gate:** `run_integrity_gate` scans the semantic zone to verify that 100% of the expected tables (defined in the registry) were successfully produced.
2.  **Promotion:** `promote_semantic_version` transfers all verified artifacts from the transient run-scoped directory to a permanent versioned path (`/published/v{run_id}`).
3.  **Metadata Generation:** Constructs a publication manifest containing the `run_id`, a timestamped `published_at` field, and temporal metadata (Year/Month/Week).
4.  **Activation:** `activate_published_version` performs the terminal swap of the `latest_version.json` file, effectively "going live" for downstream consumers.

## **Boundaries**

| This component **DOES** | This component **DOES NOT** |
| :--- | :--- |
| Verify the physical existence of semantic artifacts. | Re-validate data quality (handled in Validation/Contract). |
| Copy or upload files to a versioned production path. | Perform any data transformation or aggregation. |
| Update the atomic production pointer (`latest`). | Manage historical version cleanup (Garbage collection). |
| Provide abstraction for Local vs. Cloud storage. | Handle automated rollbacks (pointer must be reverted manually). |
| Capture lifecycle metadata (Publication timestamps). | Modify the contents of the `.parquet` files. |

## **Failure & Severity Model**

### **Operational Failures (System Level)**
* **Storage Access Denied:** If the service account lacks write permissions to the published zone (Local or GCS), the lifecycle halts before activation.
* **Network/IO Exception:** Interrupted file transfers during the promotion phase result in an immediate `failed` status, ensuring the `latest` pointer remains on the previous stable version.

### **Functional Findings (Data Level)**
* **Integrity Breach:** If a builder in the Semantic stage failed to produce even one required table, the `run_integrity_gate` will fail. This prevents "partial data" from being promoted to production.
* **Activation Collision:** If the `latest_version.json` cannot be replaced atomically, the executor traps the error and logs a fatal failure, preserving the existing production state.