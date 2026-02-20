# Raw Data Validation Gate

**Related Script/Stage:** [`validate_raw_data.py`](../data_pipeline/stages/validate_raw_data.py)

## Component Role

Deterministic structural and semantic validation engine.

Executed by [`run_pipeline.py`](../data_pipeline/run_pipeline.py) at multiple pipeline checkpoints.

This module evaluates admissibility of logical tables against defined schema and integrity rules.  
It never mutates data and never decides stage progression.

## Purpose

Ensure logical tables conform to:

- Declared schema contract [(`TABLE_CONFIG`)](../data_pipeline/shared/table_configs.py)
- Primary key integrity
- Timestamp semantics
- Referential integrity across tables

Protect downstream joins, aggregations, and timeline logic from corruption.

## Inputs

- [`RunContext`](../data_pipeline/shared/run_context.py)
- Snapshot base path (raw or contracted)
- [`TABLE_CONFIG`](../data_pipeline/shared/table_configs.py)
  - primary keys
  - allowed columns
  - role definition
- [`REQUIRED_TIMESTAMPS`](../data_pipeline/shared/table_configs.py)
- [`TIMESTAMP_FORMATS`](../data_pipeline/shared/table_configs.py)


## Outputs

Structured validation report:

```
"errors": List[str],
"warnings": List[str],
"info": List[str]
```

The module does not raise exceptions for data issues.  
It returns severity-scoped signals for the orchestrator.


## Validation Coverage

### 1. Base Structural Validation (All Tables)

- Dataset not empty
- Exact schema enforcement (no missing or extra columns)
- Primary key presence
- Conflicting duplicate primary keys (error)
- Repairable duplicate rows (warning)
- Null values in primary key (warning)
- Duplicate column labels (warning)

Stops deeper validation for that logical table when structural integrity fails.


### 2. Role-Specific Validation

#### Event Fact Tables

- Required timestamp columns present (error)
- Unparsable timestamp values (warning)
- Approval before purchase (warning)
- Delivery before purchase (warning)

Protects event timeline consistency.

#### Transaction Detail Tables

- Negative numeric values (error)

Flags potential aggregation distortion.


### 3. Cross-Table Validation

Executed only when required tables are successfully loaded

- Orphan `order_id` in `df_order_items` (warning)
- Orphan `order_id` in `df_payments` (warning)

Protects parent-child attachment semantics.


## Invariants

- No data mutation
- No schema correction
- No deduplication
- No record deletion
- No repair logic
- Deterministic output for identical inputs

## Stage Execution Context

This engine is reused at multiple pipeline stages.

Severity interpretation is controlled by the orchestrator:

Pre-contract validation:
- Halt on ERROR
- Allow WARNING

Post-contract validation:
- Halt on ERROR
- Halt on WARNING

The validator produces truth.
The orchestrator defines tolerance.

## Failure Semantics

The presence of:

- `errors` → structural or integrity break
- `warnings` → data quality anomaly
- `info` → non-blocking execution note

The module itself does not decide pipeline continuation.


## Design Principles

- Contract-driven schema enforcement
- Fail-fast on structural break
- Explicit referential integrity checks
- CI/CD compatible
- Stage-agnostic severity signaling
- Deterministic validation behavior under fixed input and configuration

## Dependency Boundary

Relies on:

- `raw_loader_exporter.py` for table loading
- `TABLE_CONFIG` for schema contract
- Timestamp configuration definitions
- `run_pipeline.py` for gating logic

## Change Impact

Any modification to:

- TABLE_CONFIG
- Primary keys
- Timestamp requirements
- Severity behavior

Must be evaluated against both pre-contract and post-contract execution stages.

This component defines structural admissibility across the pipeline.
