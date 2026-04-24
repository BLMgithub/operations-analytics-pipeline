# **Data Contract Stage**

**Files:**
* **Executor:** [`contract_executor.py`](../../data_pipeline/contract/contract_executor.py)
* **Logic:** [`contract_logic.py`](../../data_pipeline/contract/contract_logic.py)
* **Registrar:** [`id_registrar.py`](../../data_pipeline/contract/id_registrar.py)
* **Registry:** [`registry.py`](../../data_pipeline/contract/registry.py)

**Role:** Structural Enforcement, Subtractive Filtering, and Discovery-First ID Mapping.

![contract-stage-diagram](/assets/diagrams/03-contract-stage-diagram.png)

## **System Contract**

**Purpose**
Enforces role-based structural rules and referential integrity on raw snapshots to ensure that only "contract-compliant" records reach the Silver layer. It acts as a gate that prunes malformed data, enforces referential integrity via ID propagation, and freezes the technical schema using a discovery-first integer mapping approach.

**Invariants**
* **Subtractive-Only Row Logic:** With the exception of type casting, this stage never modifies business values or "repairs" data. If a row is non-compliant, it is dropped.
* **Structural Parity:** Every file within a logical table's contracted zone MUST share an identical schema width and data types to support high-speed vertical concatenation in the Assembly stage.
* **ID Propagation:** If an `order_id` is invalidated (e.g., due to nulls or unparsable dates), that ID is propagated to child tables to ensure a clean cascade drop.
* **Discovery-First Mapping:** Guarantees that all UUIDs are resolved and mapped to deterministic `UInt32` integers BEFORE table enforcement begins, preventing join collisions and schema drift.
* **Final Schema Freeze:** The terminal step for every role always executes `enforce_schema` to project only required columns and cast to strictly defined types.

**Inputs**
* `run_context`: `RunContext` (Path resolution for source raw snapshots and destination contracted zone).
* `table_name`: `str` (Logical identifier used to look up role-based rules).
* `master_mappings`: `dict[str, pl.LazyFrame]` (The pre-resolved dictionary of UUID-to-Integer mappings).
* `invalid_order_ids`: `set` (Blacklist of IDs from preceding tables to be dropped).
* `valid_order_ids`: `set` (Whitelist of IDs used to ensure child-parent referential integrity).

**Outputs**
* **Contract Report:** `dict` (Telemetry including `initial_rows`, `final_rows`, and counts for each rule applied).
* **Invalidated IDs:** `set` (New IDs discovered to be non-compliant during this run).
* **Valid IDs:** `set` (Emitted specifically by the `orders` table to act as a parent whitelist).
* **Side Effect:** Writes a schema-enforced and integer-mapped Parquet file to the `contracted/` directory.

## **Execution Workflow**

The Contract stage is split into a global Discovery phase and a table-specific Enforcement phase:

### **Phase A: Global Discovery**
1. **Discover:** Scans all raw sources (CSV/Parquet) for the unique set of UUIDs in the current run.
2. **Lookup:** Surgically retrieves existing mappings from Cloud Storage.
3. **Generate:** Maps truly new UUIDs to a continuous integer sequence.
4. **Promote:** Persists new mapping deltas to local disk and synchronizes them to central storage.

### **Phase B: Table Enforcement**
1. **Hydrate:** Fetches the raw snapshot from the lake's snapshot zone.
2. **Logic Sequencing:** Fetches rules (dedupe, null-checks, cascade drops) from `ROLE_STEPS`.
3. **Atomic Filtering:** Iteratively applies rules. For `event_fact` roles, it captures IDs triggering violations.
4. **Structural Freeze:** Executes `enforce_schema` as the final step in the registry sequence to project the required columns.
5. **ID Mapping:** Joins the filtered and projected DataFrame against the `master_mappings` to attach integer IDs.
6. **Persistence:** Saves the resulting compliant and integer-mapped dataset to the Silver layer.

## **Boundaries**

| This component **DOES** | This component **DOES NOT** |
| :--- | :--- |
| Discover UUIDs across all raw sources (CSV/Parquet) before processing. | Calculate business metrics, KPIs, or aggregates. |
| Subtractively filter rows violating structural or temporal rules. | Impute missing values or repair malformed records. |
| Propagate `order_id` invalidations to child tables (Cascade Drop). | Perform cross-table business joins (delegated to Assembly). |
| Guarantee fixed-width schemas via terminal `enforce_schema`. | Alter business definitions or rename columns. |
| Map UUIDs to UInt32 primitives for optimized joins. | Handle cross-run global state (delegated to Storage Adapter). |

## **Failure & Severity Model**

### **Operational Failures (Fatal)**
* **Discovery Failure:** If mappings cannot be resolved, the pipeline halts to prevent schema corruption.
* **Schema Breach:** If `enforce_schema` is called but a required column is missing from the source data.
* **Persistence Failure:** If disk I/O or GCS promotion fails during the write phase.

### **Functional Findings (Warnings)**
* **Contract Violations:** Data issues (duplicates, nulls) result in row removal and are logged in the telemetry report.
* **Referential Cleanup:** 
    * **Cascade:** Dropped child records are logged under `removed_cascade_rows`.
    * **Orphans:** Records without parent references are dropped and logged under `removed_ghost_orphan_rows`.
