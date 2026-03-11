# **Storage Adapter**

File: [`storage_adapter.py`](../../data_pipeline/shared/storage_adapter.py)

**Role:**
External Storage Adapter

**Purpose:**
Provide a unified interface for transferring pipeline artifacts between the run workspace and persistent storage.<br>
Abstract differences between local filesystem storage and Google Cloud Storage while preserving deterministic pipeline behavior.

## **Inputs:**

RunContext

* Supplies workspace paths and storage paths used during transfer operations.

Raw Storage Path

* Source location containing raw datasets to be copied into the run workspace.

Semantic Artifact Directory

* Run-scoped semantic output directory produced by the semantic stage.

Run Artifact Paths

* Metadata record and structured stage logs produced during pipeline execution.

Published Version Path

* Target location for immutable semantic artifact promotion.

Latest Pointer Path

* Storage location used for version activation pointer updates.

## **Outputs:**

Raw Snapshot Files

* Raw datasets copied from storage into the run-scoped `raw_snapshot` directory.

Published Semantic Artifacts

* Semantic module files uploaded to the versioned publish directory.

Run Audit Artifacts

* Metadata record and logs uploaded to persistent storage.

Filesystem or Cloud Storage Objects

* Files written to either local filesystem paths or Google Cloud Storage objects.

## **Coverage:**

Storage Path Interpretation

* Determine whether the source or destination path represents local storage or Google Cloud Storage.

Raw Snapshot Acquisition

* Copy raw source datasets from storage into the run workspace.

Semantic Artifact Promotion

* Transfer semantic module outputs from the run workspace into the immutable version directory.

Run Artifact Persistence

* Upload run metadata and logs to storage for audit retention.

Cloud Path Resolution

* Parse cloud storage paths into bucket and object prefix components.

Recursive File Transfer

* Traverse workspace directories and transfer all contained files.

Environment Abstraction

* Provide consistent behavior regardless of storage backend.

## **Invariants:**

Run Workspace Integrity

* Files are copied into run-scoped directories only.

Published Artifact Immutability

* Semantic artifacts are written only into versioned publish directories.

Path Transparency

* All transfer behavior is derived from the path prefix indicating storage type.

No Data Mutation

* Files are transferred without modification.

Deterministic File Naming

* File names and directory structures remain unchanged during transfer.

Selective Artifact Persistence

* Only metadata and logs are persisted as run audit artifacts.

## **Boundaries:**

This component **does:**

* Transfer files between workspace and storage.
* Interpret storage paths.
* Resolve Google Cloud Storage bucket and prefix values.
* Download raw datasets to run workspace.
* Upload semantic artifacts to publish storage.
* Persist run metadata and logs.

This component **does NOT:**

* Interpret dataset contents.
* Perform schema validation.
* Modify data values.
* Enforce pipeline stage rules.
* Control publish activation.
* Decide pipeline failure behavior.

Pipeline orchestration and stage execution remain the responsibility of [`run_pipeline.py`](../../data_pipeline/run_pipeline.py).

## **Failure Behavior:**

Storage Access Errors

* Cloud authentication or bucket access failures propagate as runtime exceptions.

Filesystem Errors

* Local copy failures propagate to the caller.

Transfer Failures

* Upload or download failures raise exceptions during storage operations.

No Internal Retry

* This component performs no retry or recovery logic.

Failure Handling Ownership

* Error interpretation and pipeline halt decisions are handled by [`run_pipeline.py`](../../data_pipeline/run_pipeline.py) or [`publish_lifecycle.py`](../../data_pipeline/stages/publish_lifecycle.py) depending on the operation context.