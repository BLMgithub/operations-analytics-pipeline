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

## Data Dictionary: Contract-Compliant Schema (Silver Layer)
The following tables represent the technical contracts enforced during the **Contract Stage**. Source [`table_configs.py`](../data_pipeline/shared/table_configs.py).

### Table: `df_orders` (Role: `event_fact`)
| Attribute | Type | PK | Required | Non-nullable |
| :--- | :--- | :--- | :--- | :--- |
| `order_id` | string | True | True | True |
| `customer_id` | string | False | True | True |
| `order_status` | category | False | True | True |
| `order_purchase_timestamp` | datetime64[ns] | False | True | True |
| `order_approved_at` | datetime64[ns] | False | True | False |
| `order_delivered_timestamp` | datetime64[ns] | False | True | False |
| `order_estimated_delivery_date` | datetime64[ns] | False | True | False |

### Table: `df_order_items` (Role: `transaction_detail`)
| Attribute | Type | PK | Required | Non-nullable |
| :--- | :--- | :--- | :--- | :--- |
| `order_id` | string | True | True | True |
| `product_id` | string | False | True | True |
| `seller_id` | string | False | True | True |
| `price` | float32 | False | True | True |

### Table: `df_customers` (Role: `entity_reference`)
| Attribute | Type | PK | Required | Non-nullable |
| :--- | :--- | :--- | :--- | :--- |
| `customer_id` | string | True | True | True |
| `customer_state` | category | False | True | True |
| `customer_city` | category | False | True | True |
| `customer_segment` | category | False | True | True |
| `account_creation_date` | datetime64[ns] | False | True | True |

### Table: `df_payments` (Role: `transaction_detail`)
| Attribute | Type | PK | Required | Non-nullable |
| :--- | :--- | :--- | :--- | :--- |
| `order_id` | string | True | True | True |
| `payment_value` | float32 | False | True | True |

### Table: `df_products` (Role: `entity_reference`)
| Attribute | Type | PK | Required | Non-nullable |
| :--- | :--- | :--- | :--- | :--- |
| `product_id` | string | True | True | True |
| `product_category_name` | category | False | True | True |
| `product_length_cm` | float32 | False | True | True |
| `product_height_cm` | float32 | False | True | True |
| `product_width_cm` | float32 | False | True | True |
| `product_fragility_index` | category | False | True | True |
| `product_weight_g` | float32 | False | True | True |
| `supplier_tier` | category | False | True | True |
