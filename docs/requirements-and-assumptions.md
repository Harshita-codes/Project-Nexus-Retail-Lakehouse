# Requirements and Assumptions

## Project goal

Demonstrate a retail lakehouse with historical batch processing and real-time order-event processing. The project uses Bronze, Silver, Quarantine, and Gold layers; validates and enriches data; and demonstrates Delta Lake and Apache Iceberg capabilities.

## Working assumptions

- Historical product, customer, and order CSV files are the batch source.
- Batch data is read from the Unity Catalog managed volume `/Volumes/nexus/default/retail_nexus_volume/batch`; its volume and Delta table metadata were verified to use ADLS Gen2 storage.
- Kafka `order-events` carries created, updated, and cancelled order events.
- Product and customer reference files are the streaming enrichment source.
- Delta tables store processed batch and streaming data. Iceberg stores streaming tables and the Gold streaming aggregate.
- The local Kubernetes demonstration uses Docker Desktop, one Spark pod in `local[*]`, and a `hostpath` PVC. This is a local demonstration, not production cloud storage.
- The Kafka example is plaintext and has no credentials. Any authenticated deployment must use Kubernetes Secrets or an approved cloud secret store.
- The stream uses a persistent checkpoint and Kafka partition/offset identifiers to recover and deduplicate reprocessed records.

## Requirement-to-evidence status

| Trainer deliverable | Current status |
|---|---|
| Requirements, assumptions, and participant-created design | This document and [solution-design.md](solution-design.md) provide the current draft. |
| Batch processing implementation and outputs | Verified in Azure Databricks. Bronze counts are 27 products, 42 customers, and 161 orders; Silver orders has 104 rows. Batch evidence is in `part1-batch-databricks/screenshots/01`–`16`. |
| Streaming implementation and event demonstration | The checked-in producer sample has 30 events. The updated streaming app was deployed without rerunning the producer; both formats show 30 Bronze, 12 Silver, 16 Quarantine, and 8 Gold rows. See `part2-streaming-iceberg/screenshots/07`–`11`. |
| Data quality and business aggregation | Batch and streaming validation and aggregation code are implemented and evidenced. The streaming Spark tests cover validation/quarantine, replay idempotency, and ten-minute revenue aggregation. |
| Delta Lake persistence evidence | Batch Delta tables and streaming Delta copies are verified. Streaming Delta counts match the Iceberg counts: 30 Bronze, 12 Silver, 16 Quarantine, and 8 Gold. |
| Iceberg tables, capabilities, and comparison note | Schema evolution, `MERGE`, snapshots, and historical snapshot reading are implemented and verified. The earliest silver snapshot contains 5 rows; see streaming screenshots `07`, `09`, and `10` and [delta-vs-iceberg.md](delta-vs-iceberg.md). |
| Azure Databricks and ADLS Gen2 evidence or blocker | Azure execution and ADLS Gen2 backing are confirmed: the Unity Catalog volume is managed, and its volume and Delta table locations use ADLS Gen2. This does not claim a separate customer-managed external location. |
| Docker configuration and usage notes | Producer and updated streaming Docker images build successfully; the updated streaming image is deployed locally. README and [runbook.md](runbook.md) contain usage and recovery steps. |
| Kubernetes manifests and runbook | Producer Job, Spark Deployment, Service/ConfigMap/PVC configuration, and [runbook.md](runbook.md) exist. |
| Kubernetes deployment, rollout/rollback, and recovery evidence | The updated stream is Ready; all streaming PVCs remain Bound. Kafka offsets were unchanged by the rollout. Rollout/rollback and pod recovery procedures are documented in [runbook.md](runbook.md). |
| Git repository and CI evidence | GitHub Actions run #5 passed on commit `fb155dd`; CI runs both test suites, packages both Scala apps, and builds both Docker images. |
| README and final presentation/live demo | README exists; [final-presentation-outline.md](final-presentation-outline.md) is a draft script, not a finished slide deck. |
| Known issues and future enhancements | Tracked in [known-issues.md](known-issues.md). |

## Remaining submission work

1. Produce the final presentation slides or complete the live demonstration using [final-presentation-outline.md](final-presentation-outline.md).
2. Check the original trainer rubric for any explicit requirement for a customer-managed external ADLS location or Iceberg maintenance beyond the verified schema evolution, `MERGE`, snapshots, and historical read.
3. If either is explicitly required, collect that additional evidence; otherwise, the current verified implementation and screenshots cover those sections.
