# Project Nexus — Retail Medallion Lakehouse (Batch & Real-Time Streaming)

End-to-end **Bronze → Silver → Quarantine → Gold** data engineering pipeline built in **Scala**, combining batch Lakehouse governance on **Azure Databricks (Delta Lake)** with low-latency event streaming on **Apache Kafka, Spark Structured Streaming, and Apache Iceberg**.


## Architecture Overview

### Part I: Batch Processing (Azure Databricks & Delta Lake)
* **Catalog & Storage:** Unity Catalog (`nexus.retail_nexus`) backed by Volume `/Volumes/nexus/default/retail_nexus_volume/batch`.
* **Bronze Layer:** Ingests raw `products.csv` (27), `customers.csv` (42), and `orders.csv` (161) with `_ingested_at` audit metadata.
* **Silver & Quarantine Layers:** Enforces schema typing, regex/domain validation, window-based deduplication, and foreign-key referential integrity:
  * **Products:** `21` valid (`silver_products`), `6` rejected (`quarantine_products`)
  * **Customers:** `34` valid (`silver_customers`), `8` rejected (`quarantine_customers`)
  * **Orders:** `88` valid (`silver_orders`), `73` rejected (`quarantine_orders`)
* **Incremental Upserts (`MERGE INTO`):** Processes `orders_updates.csv` (17 raw → 7 quarantined, 10 valid → 6 updates + 4 inserts), bringing `silver_orders` to `94` rows.
* **Schema Evolution:** Ingests `orders_v2.csv` (`mergeSchema = true`), dynamically adding `sales_channel` and `discount_amount` (12 raw → 2 quarantined, 10 valid), bringing `silver_orders` to `104` rows.
* **Gold Analytics:** Builds `gold_category_sales` and `gold_customer_spend` (excluding `CANCELLED` orders).
* **Governance:** Verified via `DESCRIBE HISTORY`, Delta Time Travel (`VERSION AS OF 0` = 88 rows vs. current = 104 rows), `OPTIMIZE ... ZORDER BY`, and `VACUUM`.

### Part II: Real-Time Streaming (Kafka, Spark Structured Streaming & Apache Iceberg)
* **Ingestion:** Consumes 5 JSON micro-batches (`order_events_001.json`–`005.json`, 62 total events) from Kafka topic `order-events` into Iceberg table `local.retail_nexus.bronze_order_events`.
* **Stream Validation & Quarantine:** Filters malformed IDs, `ORDER_ERR` events, non-positive quantities/prices, future-skewed timestamps, cross-batch duplicates, and orphan FKs into `local.retail_nexus.quarantine_order_events` (`25` events).
* **Stateful Upserts, Late Arrivals & Schema Evolution:** Executes `MERGE INTO` on `local.retail_nexus.silver_order_events` (`37` orders), handling `ORDER_UPDATED`, `ORDER_CANCELLED`, flagging late arrivals (`ingest_time - event_time > 10m`), and dynamically evolving `sales_channel` and `discount_amount` on batch 5.
* **Gold Streaming Window & Snapshots:** Computes 10-minute event-time windowed category revenue in `local.retail_nexus.gold_streaming_sales_window` and verifies Iceberg snapshot history across all 5 micro-batches.

---

## Repository Structure

```text
├── part1-batch-databricks/
│   └── 01_bronze_ingestion.scala
├── part2-streaming-iceberg/
│   ├── build.sbt
│   ├── ProjectNexusStreamingApp.scala
│   └── VerifyProjectNexusIceberg.scala
├── screenshots/
│   ├── part1-databricks/
│   └── part2-iceberg/
└── README.md