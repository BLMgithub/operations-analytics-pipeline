# **Publish Stage**

**Files:**
* **Executor:** [`publish_executor.py`](../../data_pipeline/publish/publish_executor.py)
* **Logic:** [`publish_logic.py`](../../data_pipeline/publish/publish_logic.py)

**Role:** Production Promotion and Versioning.

## **System Contract**

**Purpose**

Serves as the final gate and deployment mechanism for the pipeline. It transitions validated semantic artifacts into a permanent, versioned storage layer and updates a dual-pointer system: a `latest_version.json` manifest for automated systems and BigQuery Authorized Views for Power BI/Business Intelligence tools.

**Invariants**
* **Integrity-Gated Promotion:** Promotion to the production zone is strictly prohibited if any table defined in the `SEMANTIC_MODULES` registry is missing or inaccessible.
* **Atomic Multi-System Swap:** The "switch" to the new version must happen across both GCS and BigQuery. The BigQuery View swap ensures Power BI never experiences "partial data" reads during the file promotion phase.
* **Version Immutability:** Once a run is archived in a `v{run_id}` directory, the files are treated as read-only snapshots; they are never updated or overwritten by subsequent runs.
* **SQL Decoupling:** Dashboards connect to "Stable" Views (e.g., `published_seller_weekly_fact`) which are dynamically redirected to version-specific External Tables (e.g., `seller_weekly_fact_v20260413`).

**Inputs**
* `run_context`: `RunContext` (Contains the unique `run_id` and the semantic/published path configurations).
* `SEMANTIC_MODULES`: `Registry` (The source of truth for which artifacts must exist to pass the integrity gate).

**Outputs**
* **Publish Report:** `dict` (Telemetry for the integrity check, file promotion status, and SQL/JSON pointer updates).
* **Versioned Artifacts:** A new directory `/published/v{run_id}/` containing the full suite of semantic Fact and Dimension tables.
* **BigQuery Pointers:** Updated External Tables and Authorized Views reflecting the new version.
* **Latest Pointer:** An updated `latest_version.json` file in the root of the published zone.

## **Execution Workflow**

The **Executor** ensures the production release follows a fail-fast, four-phase sequence:

1.  **Integrity Gate:** `run_integrity_gate` scans the semantic zone to verify that 100% of the expected tables (defined in the registry) were successfully produced.
2.  **Promotion:** `promote_semantic_version` transfers all verified artifacts from the transient run-scoped directory to a permanent versioned path (`/published/v{run_id}`).
3.  **SQL Sync:** `swap_bigquery_view` executes DDL commands to create versioned External Tables and atomically redirect the "Published" Views used by dashboards.
4.  **Activation:** `activate_published_version` performs the terminal swap of the `latest_version.json` file, effectively "going live" for downstream file-system consumers.

## **Boundaries**

| This component **DOES** | This component **DOES NOT** |
| :--- | :--- |
| Verify the physical existence of semantic artifacts. | Re-validate data quality (handled in Validation/Contract). |
| Copy or upload files to a versioned production path. | Perform any data transformation or aggregation. |
| Manage BigQuery DDL for External Tables and Views. | Manage historical version cleanup (Garbage collection). |
| Update the atomic production pointers (SQL and JSON). | Handle automated rollbacks (pointers must be reverted manually). |
| Capture lifecycle metadata (Publication timestamps). | Modify the contents of the `.parquet` files. |

## **Failure & Severity Model**

### **Operational Failures (System Level)**
* **Storage Access Denied:** If the service account lacks write permissions to the published zone (Local or GCS), the lifecycle halts before activation.
* **BigQuery DDL Error:** If the SQL swap fails (e.g., dataset permissions or syntax), the `latest_version.json` is never updated, ensuring systems stay in sync.
* **Network/IO Exception:** Interrupted file transfers during the promotion phase result in an immediate `failed` status, ensuring the pointers remain on the previous stable version.