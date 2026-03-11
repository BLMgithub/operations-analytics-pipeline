# **Loader and Exporter**

File: [`loader_exporter.py`](../../data_pipeline/shared/loader_exporter.py)

**Role:**
Logical Table I/O Adapter

**Purpose:**
Provide standardized loading and export operations for pipeline datasets.
Resolve logical table names into physical files and produce deterministic dataset exports used by downstream stages.

## **Inputs:**

Logical Table Identifier

* Table name used to locate matching files within a directory.

Base Directory Path

* Filesystem location containing table files belonging to a pipeline stage.

Supported File Types

* `.csv`
* `.parquet`

Optional Logging Callbacks

* Informational logger
* Error logger

DataFrame Input

* In-memory dataset provided for export operations.

Output File Path

* Target path used when exporting datasets.

## **Outputs:**

Loaded Logical Table

* Concatenated DataFrame assembled from all files belonging to a logical table.

Exported Dataset File

* Physical dataset written to disk using a supported file format.

Load Event Logs

* Informational and error messages emitted through provided logging callbacks.

Export Status Flag

* Boolean result indicating success or failure of export operations.

## **Coverage:**

Logical Table Resolution

* Identify files belonging to a logical table based on filename prefix matching.

File Format Dispatch

* Route file loading through registered loader functions based on file extension.

Dataset Concatenation

* Combine multiple physical files representing a single logical table.

Format Consistency Enforcement

* Prevent mixed file formats within the same logical table.

Deterministic File Ordering

* Sort input files prior to loading to ensure stable concatenation order.

Dataset Export

* Write DataFrame outputs to disk using the file format inferred from the output path extension.

Filesystem Preparation

* Ensure parent directories exist prior to export operations.

## **Invariants:**

Logical Table Naming Rule

* Table files must begin with `<table_name>_`.

Single Format Requirement

* A logical table must contain files of only one supported format.

Supported Format Constraint

* Only `.csv` and `.parquet` formats are accepted.

Deterministic Load Order

* Input files are sorted before loading.

Export Format Determination

* Output format is determined solely by the output file extension.

Index Handling

* DataFrame index is excluded from exported files unless explicitly requested.

Dataset Integrity Preservation

* No schema modification or data transformation occurs during load or export.

## **Boundaries:**

This component **does:**

* Resolve logical table file sets from directory paths.
* Load CSV and Parquet files into DataFrames.
* Concatenate physical files belonging to a logical table.
* Enforce file format consistency.
* Export DataFrames to supported file formats.
* Emit logging messages through provided callbacks.

This component **does NOT:**

* Validate dataset schema or contents.
* Enforce primary keys or structural constraints.
* Perform deduplication or contract enforcement.
* Modify column values or types.
* Interpret logical table semantics.
* Perform business logic.

Data validation and transformation occur exclusively in pipeline stage modules.

## **Failure Behavior:**

Missing Logical Table Files

* Returns `None` when no files matching the logical table prefix are found.

Mixed File Formats

* Raises a runtime error when a logical table contains multiple file formats.

File Loading Failure

* Errors encountered while loading individual files are logged and skipped.

Total Load Failure

* Returns `None` if all matching files fail to load.

Unsupported Export Format

* Raises a value error when the output file extension is not supported.

Export Failure

* Returns `False` when export operations fail.

Exception Propagation

* Unexpected runtime errors propagate to the calling stage.

Stage modules determine failure severity and halt behavior.
