# Data & Synthetic Benchmarks

This directory serves as the local state provider for the pipeline when executing in a non-cloud environment. It mimics the structure of the Google Cloud Storage (GCS) buckets, allowing for high-fidelity local simulation and performance benchmarking.

## Synthetic Dataset
To replicate the high-volume environment described in the [Performance & Scale](/README#performance-&-scale) section, you can download the 15M-row synthetic dataset here: [**Kaggle Dataset Link**](https://15m-row-synthetic-dataset.com)

### Dataset Structure
The downloaded archive contains the following partitions:
*   **`raw/`**: Represents the **Bronze Layer**. Contains daily delta CSV snapshots. The `RunContext` class expects this directory to be populated when running locally. (~4.5GB total)
*   **`contracted/`**: Represents the **Silver Layer**. Contains accumulated, schema-enforced Parquet files. This acts as the authoritative state for Gold-layer assembly. (~1.55GB total)

## Local Execution Setup
1.  Extract the downloaded dataset archive.
2.  Copy the `raw/` and `contracted/` directories into this `data/` folder.
3.  The `RunContext` manager is configured to strictly recognize `.parquet` and `.csv` extensions; all other file types are ignored to prevent ingestion noise.

**Execute the local pipeline:**
```
python -m data_pipeline.run_pipeline
```