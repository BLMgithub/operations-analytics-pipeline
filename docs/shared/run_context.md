# **Run Context**

File: [`run_context.py`](../../data_pipeline/shared/run_context.py)

**Role:**
Run Execution Context

**Purpose:**
Provide a run-scoped execution environment that defines all workspace paths, storage paths, and identifiers required by pipeline stages.
Establish deterministic run isolation and consistent path resolution across the pipeline.

## **Inputs:**

Run Identifier

* Unique identifier representing a single pipeline execution.

Base Workspace Path

* Root location used to create the run-scoped workspace.

Storage Root Path

* Root storage location used for raw input, published artifacts, and run artifact persistence.

Run Identifier Factory

* Optional generator function used to produce deterministic or externally supplied run identifiers.

## **Outputs:**

Run Identifier

* Unique execution identifier assigned to the run.

Workspace Paths

* Run-scoped filesystem paths used by pipeline stages:

  * raw snapshot directory
  * contracted data directory
  * assembled dataset directory
  * semantic artifact directory
  * logs directory
  * metadata record path

Storage Paths

* Fully resolved storage locations used during the run:

  * raw data source path
  * published semantic root path
  * versioned publish directory
  * latest pointer location
  * run artifact storage directory

Workspace Directory Structure

* Deterministic directory tree created during run initialization.

## **Coverage:**

Run Identity Management

* Generates or accepts externally supplied run identifiers.

Workspace Path Construction

* Builds the full run-scoped directory structure used by pipeline stages.

Storage Path Resolution

* Constructs deterministic storage locations for raw input, semantic publication, and audit artifacts.

Run Isolation Enforcement

* Ensures every pipeline execution operates inside an independent workspace directory.

Directory Initialization

* Creates run-scoped directories required by downstream stages.

Path Distribution

* Provides centralized path references consumed by all stage modules.

## **Invariants:**

Run Identifier Stability

* The run identifier remains constant throughout the pipeline lifecycle.

Run Workspace Isolation

* Each run writes only within its own run directory.

Stage Directory Determinism

* All stage directories follow a fixed structure relative to the run identifier.

Workspace Root Consistency

* All run directories reside under the workspace root.

Storage Location Determinism

* Published version path is derived directly from `run_id`.

Pointer Location Stability

* Latest activation pointer location remains constant across runs.

Directory Creation Scope

* Only run-scoped directories are created during initialization.

Published Storage Non-Mutation

* Published version directories are not created or modified by this component.

## **Boundaries:**

This component **does:**

* Generate run identifiers when not provided.
* Define run-scoped filesystem paths.
* Define storage paths used during the run.
* Create workspace directories required by pipeline stages.
* Provide consistent path references to all pipeline components.

This component **does NOT:**

* Execute pipeline stages.
* Load or transform data.
* Perform validation or contract enforcement.
* Publish semantic artifacts.
* Modify published version directories.
* Interpret pipeline failures.

Stage modules and orchestrator consume this component as a passive configuration object.

##  **Failure Behavior:**

Run Identifier Generation Failure

* Errors raised by the run identifier factory propagate to the caller.

Directory Creation Failure

* Filesystem errors during workspace directory creation propagate as runtime exceptions.

Path Construction Errors

* Invalid base path or storage root values propagate errors to the caller.

No Internal Recovery

* This component does not attempt retry or recovery for filesystem errors.

Failure Handling Responsibility

* Error handling and lifecycle management remain the responsibility of [`run_pipeline.py`](../../data_pipeline/run_pipeline.py).