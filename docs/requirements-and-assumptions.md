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
| Batch processing implementation and outputs | The baseline notebook was verified in Azure Databricks: 27 products, 42 customers, and 161 orders in Bronze; 104 Silver orders. Evidence is in `part1-batch-databricks/screenshots/01`–`16`. The current helper/schema/widget edits still need a Databricks run. |
| Streaming implementation and event demonstration | The checked-in producer sample has 30 events. The updated streaming app was deployed without rerunning the producer; both formats still show 30 Bronze, 12 Silver, 16 Quarantine, and 8 Gold rows. Kafka offsets remained 0:8, 1:13, and 2:9. See `part2-streaming-iceberg/screenshots/07`–`11` for the sample run; the current rollout was verified from the live cluster. |
| Data quality and business aggregation | Batch and streaming validation and aggregation code are implemented. The updated stream has a configurable 10-minute watermark persisted on the PVC; events past the cutoff remain in Bronze and are quarantined. Local tests cover late-event rejection, watermark persistence/restart, replay idempotency, and ten-minute revenue aggregation. A new late event was not published to Kafka during this rollout. |
| Delta Lake persistence evidence | Batch Delta tables and streaming Delta copies are verified. Streaming Delta counts match the Iceberg counts: 30 Bronze, 12 Silver, 16 Quarantine, and 8 Gold. |
| Iceberg tables, capabilities, and comparison note | Schema evolution, `MERGE`, snapshots, and historical snapshot reading are implemented and verified. The earliest silver snapshot contains 5 rows; see streaming screenshots `07`, `09`, and `10` and [delta-vs-iceberg.md](delta-vs-iceberg.md). |
| Azure Databricks and ADLS Gen2 evidence or blocker | Azure execution and ADLS Gen2 backing are confirmed: the Unity Catalog volume is managed, and its volume and Delta table locations use ADLS Gen2. This does not claim a separate customer-managed external location. |
| Docker configuration and usage notes | The updated streaming image was rebuilt locally and deployed. README and [runbook.md](runbook.md) contain usage and recovery steps. |
| Kubernetes manifests and runbook | Producer Job, Spark Deployment, Service/ConfigMap/PVC configuration, and [runbook.md](runbook.md) exist. |
| Kubernetes deployment, rollout/rollback, and recovery evidence | The updated stream is Ready; all streaming PVCs remain Bound. The deployed pod uses the rebuilt image, retained the checkpoint and watermark file, and did not advance Kafka offsets. This successful rollout is verified; a deliberate rollback test is still outstanding. Procedures are in [runbook.md](runbook.md). |
| Git repository and CI evidence | GitHub Actions run #7 passed on commit `fb06c33`. The pushed changes also pass the targeted local Scala tests. |
| README and final presentation/live demo | README and the walkthrough outline/checklist are documented. The finished slide deck is assigned to another team member; the live demo itself has not yet been presented. |
| Known issues and future enhancements | Tracked in [known-issues.md](known-issues.md). |

## Remaining submission work

1. The assigned teammate produces the final presentation slides; the live demo still needs to be delivered.
2. Import and run the changed batch notebook in Azure Databricks. The new helper schemas, validation functions, widgets, and target namespace have local unit coverage where possible, but Databricks execution and job/run-history evidence have not been regenerated.
3. If required by the rubric, capture a Databricks Workflow/Job run and direct raw/curated/rejected/analytics ADLS zoning evidence. The checked-in batch pipeline currently uses the verified Unity Catalog managed volume and Delta tables.
4. The trainer confirmed that streaming storage should remain on the local PVC, so this is the intended design and not a rubric gap. Preserve the PVC, checkpoint, and watermark state.
5. Deliberate Kubernetes rollback/recovery evidence and producer scaling/consumer-lag evidence are not captured. Use the runbook procedures only when a planned demo permits the associated rollout.
6. Confirm whether the rubric explicitly requires a customer-managed external ADLS location or Iceberg maintenance beyond the verified schema evolution, `MERGE`, snapshots, and historical read.
