# Retail Lakehouse Architecture

## Batch processing

```mermaid
flowchart LR
  CSV["Products, customers, orders CSVs"] --> VOL["Unity Catalog volume on ADLS Gen2"]
  VOL --> DB["Azure Databricks · Scala/Spark"]
  DB --> BR["Bronze · Delta"]
  BR --> SIL["Silver · validated Delta"]
  BR --> Q["Quarantine · rejected rows"]
  SIL --> GOLD["Gold · category sales and customer spend"]
  SIL --> MERGE["Incremental MERGE and schema evolution"]
  LOCAL["Local Spark batch app"] --> PVC["Separate local output PVC"]
```

The Databricks notebook is the primary batch workflow. The local Spark app demonstrates the base products/customers/orders processing and does not replace the notebook's incremental merge or schema-evolution stages.

## Streaming

```mermaid
flowchart LR
  FIXTURE["30-event JSONL fixture"] --> PROD["Scala Kafka producer"]
  PROD --> KAFKA["Kafka · order-events"]
  KAFKA --> SPARK["Spark Structured Streaming"]
  SPARK --> ICE["Iceberg · Bronze/Silver/Quarantine/Gold"]
  SPARK --> DELTA["Delta mirror"]
  SPARK --> STATE["Local PVC · checkpoint and watermark"]
```

Kafka, Spark, Iceberg, and the stream state in this demonstration run locally. The PVC is not an ADLS-backed production store. Databricks batch tables and their Unity Catalog volume were verified as ADLS Gen2-backed; explicit raw/curated/rejected/analytics `abfss` zones remain a separate verification item.

For table names and operating constraints, see the [design notes](../docs/solution-design.md) and [requirements/evidence matrix](../docs/requirements-compliance.md).
