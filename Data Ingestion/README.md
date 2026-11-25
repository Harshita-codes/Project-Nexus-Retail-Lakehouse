# Batch and Real-Time Ingestion

| Flow | Source and implementation | Output |
|---|---|---|
| Historical batch | [Raw CSV layout](../Datasets/README.md) → [Databricks notebook](Batch/01_bronze_ingestion.scala) | Delta Bronze, Silver, Quarantine, and Gold tables |
| Local batch demonstration | [Spark batch application](Batch/src/main/scala/com/nexus/batch/ProjectNexusBatchApp.scala) | Delta paths on the separate batch PVC |
| Event production | [JSONL fixture](../Datasets/streaming/order-events/) → [Kafka producer](Producer/src/main/scala/com/nexus/producer/OrderEventProducer.scala) | Kafka topic `order-events` |
| Event processing | Kafka → [Spark Structured Streaming](Streaming/ProjectNexusStreamingApp.scala) | Iceberg and Delta Bronze/Silver/Quarantine/Gold outputs |

Batch and streaming modules contain their own build files and Scala tests. Dockerfiles and Kubernetes manifests are centralized in [`Development`](../Development/README.md). See the [demo guide](../docs/demo-guide.md) for exact commands and safeguards.
