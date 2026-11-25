# Retail Source Datasets

## Historical batch inputs

The Databricks notebook expects these CSVs under the configured Unity Catalog volume path:

| Input | Observed baseline rows | Repository status |
|---|---:|---|
| `raw/products/products.csv` | 27 | Not checked into this repository |
| `raw/customers/customers.csv` | 42 | Not checked into this repository |
| `raw/orders/orders.csv` | 161 | Not checked into this repository |
| `incremental/orders_updates/orders_updates.csv` | 17 | Not checked into this repository |
| `schema_evolution/orders_v2/orders_v2.csv` | 12 | Not checked into this repository |

These are trainer/workspace inputs and are not recreated here. Stage the authorized originals in the Unity Catalog volume for Databricks; the local batch Job also needs the three base CSVs supplied separately.

The [data dictionary](data_dictionary.csv) records the source field names and expected meaning based on the notebook's explicit schemas. Folder-level notes document the expected paths without adding placeholder CSV records.

## Streaming fixture

[`streaming/order-events`](streaming/order-events/) contains five JSONL files with 30 sample events used by the producer tests and demonstrations. The fixture includes valid lifecycle events and deliberately invalid/duplicate examples for validation. It is sample data, not a live Kafka export.
