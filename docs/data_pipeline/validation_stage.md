# **Validation Stage**

**Files:** 
* **Executor:** [`validation_executor.py`](../../data_pipeline/validation/validation_executor.py)
* **Logic:** [`validation_logic.py`](../../data_pipeline/validation/validation_logic.py)

**Role:** Structural Data Quality Gatekeeper.

![validation-stage-diagram](/assets/diagrams/02-validation-stage-diagram.png)

## **System Contract**

**Purpose** 

Evaluates raw datasets against declared structural contracts before any mutation or transformation occurs. It prevents "garbage-in" scenarios by detecting schema violations, structural inconsistencies, and referential integrity issues. In the modern Polars-native architecture, it also serves as a verification gate for the 'Normalize-at-Source' I/O strategy.

**Invariants** 
* **Non-Mutation Guarantee:** This stage is strictly read-only. It never modifies values, removes rows, or casts types in the source data.
* **Resolution Verification:** Asserts that all timestamps are pre-normalized to microsecond (us) resolution by the I/O layer.
* **Severity Hierarchy:** 
    * `errors`: Fatal structural violations (e.g., missing columns, duplicate PKs).
    * `warnings`: Admissible integrity issues (e.g., orphan records, chronological anomalies).
* **Execution Sequence:** Base structural validations are mandatory and must pass for a table before role-specific or cross-table validations are attempted.

**Inputs:** 
* `run_context`: `RunContext` (Object containing path resolution for the raw snapshot).
* `TABLE_CONFIG`: `Registry` (Defines Primary Keys, Required Columns, and Entity Roles).
* `base_path`: `Path` (Optional override; defaults to the run-scoped snapshot directory).

**Outputs** 
* **Validation Report:** `dict` (Standardized telemetry object containing `status`, `errors`, `warnings`, and `info`).

## **Execution Workflow**

The **Executor** coordinates the validation lifecycle through the following deterministic steps:

1.  **Table Discovery:** Iterates through all logical tables registered in `TABLE_CONFIG`.
2.  **Data Loading:** Attempts to load each table as a DataFrame. If a table is missing, an `error` is logged to the report.
3.  **Base Validation:** Dispatches the DataFrame to `run_base_validations` to check for:
    * Presence of required columns.
    * Uniqueness of Primary Keys and column names using Polars-native expressions.
    * Compliance with non-nullable constraints.
4.  **Role-Specific Dispatch:** If base validations pass, the executor applies specialized rules:
    * `event_fact`: Triggers `run_event_fact_validations` (temporal chronology and microsecond resolution verification).
    * `transaction_detail`: Triggers `run_transaction_detail_validations` (numeric range checks).
5.  **Cross-Table Integrity:** Once all tables are processed individually, `run_cross_table_validations` evaluates Foreign Key relationships (e.g., ensuring all Items belong to an existing Order).

## **Boundaries**

| This component **DOES** | This component **DOES NOT** |
| :--- | :--- |
| Load logical tables from the snapshot zone. | Remove rows or filter data. |
| Detect schema and primary key violations. | Correct or impute missing values. |
| Verify microsecond (us) timestamp resolution. | Deduplicate records (delegated to Contract stage). |
| Evaluate temporal chronology using clean Polars syntax. | Perform data type casting. |
| Detect numeric anomalies (negative prices/lags). | Mutate the physical state of the data lake. |
| Produce structured, machine-readable reports. | Halt the pipeline (Decision owned by global orchestrator). |

## **Failure & Severity Model**

### **Operational Failures (System Level)**
* **Missing Logical Table:** Logged as a fatal `error` in the report; the table is marked as unprocessable.
* **Load Failure:** If a Parquet file is corrupted, the executor traps the exception and logs it as an `error`.

* **Functional Findings (Data Level)**
* **Structural Errors & Integrity Warnings:** 
    * Fatal structural violations (e.g., PK duplicates) OR integrity issues (e.g., orphan records) set the stage status to `failed`. 
    * This informs the orchestrator that the data contains quality issues, though the diagnostic pass will still complete for all tables.
* **The Halting Caveat:** 
    * **Initial Validation:** The orchestrator allows the pipeline to proceed if only `warnings` are present, delegating the cleanup to the `Contract Stage`.
    * **Post-Contract (Revalidation):** In this phase, `warnings` are treated as fatal. Since the contract stage should have already pruned orphans and anomalies, any remaining warning triggers a terminal `RuntimeError` to prevent downstream corruption.

* **Incomplete Context:** 
    * If a parent table (e.g., `df_orders`) is missing, cross-table validation is skipped and logged as `info` rather than a failure.