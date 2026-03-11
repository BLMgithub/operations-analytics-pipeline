# **Publish Lifecycle**

File: [`publish_lifecycle.py`](../../data_pipeline/stages/publish_lifecycle.py)

**Role:**
Semantic Publish Lifecycle Stage

**Purpose:**
Control the release of semantic artifacts to the published environment.
Verify semantic completeness, promote artifacts into an immutable version directory, and atomically activate the published version for BI consumption.

## **Inputs:**

RunContext

* Provides run identifier.
* Provides semantic artifact directory.
* Provides publish storage paths.

Semantic Artifact Directory

* Run-scoped semantic outputs produced by [`build_bi_semantic_layer.py`](../../data_pipeline/stages/build_bi_semantic_layer.py).

Semantic Module Registry

* [`build_bi_semantic_layer.py`](../../data_pipeline/stages/build_bi_semantic_layer.py) defines:

  * expected module names
  * expected tables
  * schema definitions

Parquet Semantic Tables

* Fact and dimension tables generated during the semantic stage.

Storage Adapter

* [`storage_adapter.py`](../../data_pipeline/shared/storage_adapter.py) provides semantic artifact upload capability.

## **Outputs:**

Published Semantic Version

* Immutable semantic artifact directory:

    * `published/v{run_id}/` &rarr; Contains semantic module directories and exported tables

* Activation Pointer:

    * `published/_latest.json` &rarr; Pointer file identifying the currently active semantic version.

Publish Execution Report

* Structured dictionary containing:

    * stage status
    * step-level reports
    * failure indicators
    * informational logs

## **Coverage:**

Pre-Publish Integrity Gate

* Validation checks include:

    * semantic directory existence
    * semantic module set matches registry
    * semantic file set matches registry definitions
    * Parquet files load successfully
    * tables are non-empty
    * required schema columns present

Semantic Version Promotion

* verify version directory does not exist
* upload semantic artifacts to the version path

Published Version Activation

* construct version payload
* write pointer file atomically
* update `_latest.json`

Pointer Payload Contains:

* run identifier
* version identifier
* run calendar attributes
* publish timestamp

Publish Lifecycle Orchestration

* Sequential execution:

    * integrity gate
    * semantic promotion
    * pointer activation

Execution halts if any step fails.

## **Invariants:**

Immutable Version Directories

* semantic artifacts are written only to a new version directory.

Exact Module Set

* semantic module directories must match the semantic module registry.

Exact File Set

* each module must contain exactly the expected semantic tables.

Schema Presence

* exported semantic tables must contain required schema columns.

Non-Empty Tables

* semantic tables must contain rows.

Atomic Activation

* activation pointer update must occur as a single operation.

BI Resolution Rule

* dashboards resolve semantic artifacts only through `_latest.json`.

Stage Isolation

* semantic artifacts are read exclusively from the run-scoped semantic directory.

## **Boundaries:**

This component **does:**

* validate semantic artifact completeness
* enforce semantic module registry expectations
* promote semantic artifacts to immutable version storage
* activate semantic versions through pointer updates
* produce publish lifecycle execution reports

This component **does NOT:**

* build semantic tables
* validate raw data
* repair datasets
* modify semantic artifacts
* interpret semantic metrics
* perform BI logic
* determine intervention decisions

Semantic artifact creation occurs in [`build_bi_semantic_layer.py`](../../data_pipeline/stages/build_bi_semantic_layer.py).

Pipeline lifecycle control remains the responsibility of [`run_pipeline.py`](../../data_pipeline/run_pipeline.py).

## **Failure Behavior:**

Integrity Gate Failure

* Occurs when:

    * semantic directory missing
    * semantic modules mismatch registry
    * expected semantic files missing
    * Parquet files fail to load
    * semantic tables are empty
    * required schema columns missing

Promotion Failure

* Occurs when:

    * version directory already exists
    * artifact upload fails

Activation Failure

* Occurs when:

    * pointer update fails
    * storage access fails

Failure Reporting

* errors recorded in step-level reports
* publish lifecycle status set to failed

Activation Safety

* pointer is not updated when promotion fails.

Pipeline Halt Responsibility

* [`run_pipeline.py`](../../data_pipeline/run_pipeline.py) interprets publish lifecycle failure and terminates execution.