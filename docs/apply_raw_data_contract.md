# Raw Data Contract Enforcement

**Related Script/Stage:** [`apply_raw_data_contract.py`](../data_pipeline/stages/apply_raw_data_contract.py)

## Component Role

Deterministic structural repair and enforcement layer.

Executed by [`run_pipeline.py`](../data_pipeline/run_pipeline.py) between validation stages.

This module mutates datasets to enforce non-negotiable structural and temporal contracts.  
It produces contract-compliant outputs for re-validation and downstream modeling.


## Purpose

Transform raw logical tables into structurally admissible datasets by:

- Removing exact duplicate records
- Removing records with invalid timestamps
- Removing records violating temporal invariants
- Cascading drops across parent-child tables

Reduce repairable violations to enable post-contract validation to pass without warnings.


## Inputs

- [`RunContext`](../data_pipeline/shared/run_context.py)
  - raw snapshot path
  - contracted output path
- [`TABLE_CONFIG`](../data_pipeline/shared/table_configs.py)
  - role definitions
- [`REQUIRED_TIMESTAMPS`](../data_pipeline/shared/table_configs.py)
- [`TIMESTAMP_FORMATS`](../data_pipeline/shared/table_configs.py)
- Optional `invalid_order_ids` (for cascade enforcement)


## Outputs

For each table:

- Contracted parquet file written to contracted path
- Structured enforcement report:
  - initial row count
  - final row count
  - rows removed by category
  - status
  - errors (if any)

Additionally:

- Returns set of invalid parent primary keys (`order_id`) for cascade propagation.


## Enforcement Coverage

### 1. Exact Deduplication (All Roles)

- Removes identical duplicate rows
- Does not attempt fuzzy or partial deduplication
- Deterministic row removal


### 2. Event Fact Enforcement

Applied when role = `event_fact`.

Removes:

- Rows with unparsable required timestamps
- Rows violating temporal invariants:
  - approval < purchase
  - delivery < purchase

Cascade enforcement is applied via orchestrator-coordinated propagation of invalid parent identifiers.


### 3. Transaction Detail Enforcement

Applied when role = `transaction_detail`.

- Removes exact duplicates
- Removes rows referencing invalid parent `order_id` (cascade drop)

Protects aggregation integrity.


### 4. Entity Reference Enforcement

Applied when role = `entity_reference`.

- Removes exact duplicates only

No temporal or cascade logic applied.


## Invariants

- Deterministic mutation
- No schema changes
- No column renaming
- No null imputation
- No value correction
- No soft-fix of invalid timestamps (invalid rows are removed)


## Failure Behavior

Failure conditions:

- Unknown table name
- Load failure
- Export failure

No partial output is committed if export fails. 
Status is marked `"failed"` in report.


## Stage Execution Context

This module runs between:

validation_1 → contract → validation_2

Expectations:

- validation_1 may surface warnings
- contract removes repairable violations
- validation_2 must pass with zero warnings

If validation_2 surfaces warnings, manual intervention is required.


## Responsibility Boundary

This component:

- Repairs structurally admissible but invalid data
- Enforces declared temporal invariants
- Propagates parent invalidation to children

This component does NOT:

- Perform structural validation (handled by [validation engine](../data_pipeline/stages/validate_raw_data.py))
- Decide pipeline continuation (handled by [orchestrator](../data_pipeline/run_pipeline.py))
- Perform business logic enrichment
- Modify schema contract


## Design Principles

- Deterministic repair only
- Explicit row removal over silent correction
- No tolerance for temporal contradictions
- Clear mutation reporting
- CI-compatible enforcement
- Idempotent for identical input snapshot and configuration


## Change Impact

Changes to:

- Timestamp definitions
- Temporal invariants
- Cascade logic
- Deduplication rules

Directly affect post-contract validation strictness and downstream fact reliability.

This component defines the repair boundary of the raw data layer.
