# **Data Contract Stage**

**Files:**
* **Executor:** [`contract_executor.py`](../../data_pipeline/contract/contract_executor.py)
* **Logic:** [`contract_logic.py`](../../data_pipeline/contract/contract_logic.py)
* **Registry:** [`registry.py`](../../data_pipeline/contract/registry.py)

**Role:** Structural Enforcement and Subtractive Filtering.

## **System Contract**

**Purpose**

Enforces role-based structural rules on logical tables to ensure that only "contract-compliant" records reach the Silver layer. It acts as a gate that prunes malformed data, enforces referential integrity via ID propagation, and freezes the technical schema.

**Invariants**
* **Subtractive-Only Row Logic:** With the exception of type casting, this stage never modifies data values or "repairs" them. If a row is non-compliant, it is dropped.
* **Grain Enforcement:** Guarantees the removal of duplicates and the enforcement of the primary key grain defined in the registry.
* **ID Propagation:** If an `order_id` is invalidated (e.g., due to nulls or unparsable dates), that ID is propagated to child tables to ensure a clean cascade drop.
* **Final Schema Freeze:** The terminal step always ensures the output contains only approved columns with strictly defined data types.

**Inputs**
* `run_context`: `RunContext` (Path resolution for source raw snapshots and destination contracted zone).
* `table_name`: `str` (Logical identifier used to look up role-based rules).
* `invalid_order_ids`: `set` (Blacklist of IDs from preceding tables to be dropped).
* `valid_order_ids`: `set` (Whitelist of IDs used to ensure child-parent referential integrity).

**Outputs**
* **Contract Report:** `dict` (Telemetry including `initial_rows`, `final_rows`, and counts for each rule applied).
* **Invalidated IDs:** `set` (New IDs discovered to be non-compliant during this run).
* **Valid IDs:** `set` (Emitted specifically by the `orders` table to act as a parent whitelist).
* **Side Effect:** Writes a schema-enforced Parquet file to the `contracted/` directory.

## **Execution Workflow**

The **Executor** applies the contract through a registry-driven sequence:

1.  **Role Resolution:** Identifies if the table is an `event_fact`, `transaction_detail`, or `entity_reference`.
2.  **Logic Sequencing:** Fetches the specific list of rules (e.g., `deduplicate`, `remove_nulls`) from the `ROLE_STEPS` registry.
3.  **Atomic Filtering:** Iteratively applies each logic function. For `event_fact` roles, it captures any `order_id` that triggers a violation.
4.  **Cascade Cleanup:** If `invalid_order_ids` are provided, it drops child records whose parents were previously invalidated.
5.  **Referential Gate:** If `valid_order_ids` are provided (post-orders processing), it prunes orphan records.
6.  **Schema Freeze:** As the final operation, it executes `enforce_schema` to project only required columns and cast types.
7.  **Persistence:** Saves the resulting compliant DataFrame to the Silver layer.

## **Boundaries**

| This component **DOES** | This component **DOES NOT** |
| :--- | :--- |
| Remove rows violating structural rules (Nulls, Duplicates). | Calculate business metrics or durations. |
| Drop child records based on parent invalidation (Cascade). | Impute missing values or "fix" bad data. |
| Enforce chronological logic (Purchase < Delivery). | Join multiple tables (delegated to Assembly stage). |
| Project a final schema and enforce strictly defined types. | Rename columns or change business definitions. |
| Track exactly how many rows were lost at each rule. | Handle global orchestration of all tables. |

## **Failure & Severity Model**

### **Operational Failures (System Level)**
* **Configuration Mismatch:** If a `table_name` is not in `TABLE_CONFIG` or `ROLE_STEPS`, the executor returns a `failed` status immediately.
* **Schema Breach:** If `enforce_schema` is called but a required column is missing from the data, it raises a `KeyError` and halts the export.

### **Functional Findings (Data Level)**
* **Contract Violations:** Data issues (duplicates, nulls) are not treated as "pipeline crashes." They are treated as expected noise; the rows are removed, the count is logged, and the pipeline continues with the remaining "clean" data.
* **Referential Cleanup:** 
    * **Cascade:** Compromised IDs from parents (e.g., orders) trigger removal of children (e.g., items) logged under `removed_cascade_rows`.
    * **Orphans:** Ghost records without any parent reference are logged under `removed_ghost_orphan_rows`, ensuring downstream joins in the Assembly stage are 100% clean.