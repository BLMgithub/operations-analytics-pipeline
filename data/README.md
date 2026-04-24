# Data & Synthetic Benchmarks

This directory serves as the local state provider for the pipeline when executing in a non-cloud environment. It mimics the structure of the Google Cloud Storage (GCS) buckets.

## Synthetic Dataset
To replicate the high-volume environment described in the [GCP Stress-Test Metrics (Scaling Efficiency)](/README.md#gcp-stress-test-metrics-scaling-efficiency) section, you can download the 40M-row synthetic dataset here: [**Kaggle Dataset Link**](https://www.kaggle.com/datasets/melvidabryan/e-commerce-synthetic-dataset)

> *Note: This upload contains the **Contracted Version** of the dataset. The original "Raw" state, totaling approximately ~26GB of unrefined CSVs was omitted to prioritize transfer efficiency.*

## File Structure & Purpose
The dataset is divided into three primary directories to facilitate different stages of pipeline testing:

| Directory | Files | Description |
| :--- | :--- | :--- |
| `contracted/` | 125 files | **Production-Scale Test:** The full 36M row dataset (~5.34 GB) formatted to strict schema requirements. |
| `id_mapping/customer_id/` | 1 file | **Metadata Registry:** Central lookup mapping Customer UUIDs to Uint32 surrogate keys.  |
| `id_mapping/order_id/` | 40 files | **Metadata Registry (Sharded):** Fragmented lookup (40M+ keys) to test high-cardinality ID resolution. |
| `id_mapping/product_id/` | 1 file | **Metadata Registry:**  Central lookup mapping Product UUIDs to Uint32 surrogate keys. |
| `id_mapping/seller_id/` | 1 file |  **Metadata Registry:** Central lookup mapping Seller UUIDs to Uint32 surrogate keys. |
| `raw/` | 5 files | **Delta Sample (Validation):** Small-scale samples (~20k rows each) representing **daily incoming deltas**. These files are intentionally "noisy" to exhibit the full range of injected data quality errors. |

---

### ID Mapping & Surrogate Key Simulation
The id_mapping/ directory acts as a simulated metadata registrar for surrogate key generation. The pipeline utilizes these registries to resolve raw source UUIDs into memory-efficient Uint32 identifiers while enforcing global deduplication and referential integrity.

To benchmark ***[`mapping`](/data_pipeline/contract/id_registrar.py) throughput and memory footprint***, the order_id registry is partitioned into 40 sharded files (1M rows each). This fragmentation simulates the ingestion pressure of high-cardinality transactional data (40M+ unique keys) on serverless compute. Dimension-level registries (Customer, Product, Seller) remain unfragmented, as their lower cardinality is insufficient to trigger the resource-exhaustion thresholds required for these performance benchmarks.

## Included Tables

The dataset provides a complete relational snapshot of an e-commerce ecosystem:

  * **`df_orders`**: Fact table with lifecycle timestamps (Purchase, Approved, Delivered, Estimated).
  * **`df_order_items`**: Bridge table linking orders to products and sellers.
  * **`df_payments`**: Transactional data including sequential payment tracking.
  * **`df_products`**: Dimensions including weight, dimensions, and fragility indexes.
  * **`df_customers`**: Geographic data and business segments (D2C, SMB, Enterprise).

## Local Execution Setup
1.  Extract the downloaded dataset archive.
2.  Copy the `raw/` and `contracted/` directories into this `data/` folder.
3.  Use the commented out local path in [`RunContext.create()`](../data_pipeline/shared/run_context.py#L62).
4.  The `RunContext` manager is configured to strictly recognize `.parquet` and `.csv` extensions; all other file types are ignored to prevent ingestion noise.

**Execute the local pipeline:**
```
python -m data_pipeline.run_pipeline
```