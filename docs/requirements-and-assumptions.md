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
| Batch processing implementation and outputs | The baseline notebook was verified in Azure Databricks: 27 products, 42 customers, and 161 orders in Bronze; 104 Silver orders. Evidence is in `part1-batch-databricks/screenshots/01`–`16`. The revised notebook still needs a Databricks Job/Workflow run with parameters, run history, counts, and duration. |
| Streaming implementation and event demonstration | The checked-in producer sample has 30 events. The updated streaming app was deployed without rerunning the producer; both formats still show 30 Bronze, 12 Silver, 16 Quarantine, and 8 Gold rows. Kafka offsets remained 0:8, 1:13, and 2:9. See `part2-streaming-iceberg/screenshots/07`–`11` for the sample run; the current rollout was verified from the live cluster. |
| Data quality and business aggregation | Batch and streaming validation and aggregation code are implemented. The updated stream has a configurable 10-minute watermark persisted on the PVC; events past the cutoff remain in Bronze and are quarantined. Local tests cover late-event rejection, watermark persistence/restart, replay idempotency, and ten-minute revenue aggregation. A new late event was not published to Kafka during this rollout. |
| Delta Lake persistence evidence | Batch Delta tables and streaming Delta copies are verified. Streaming Delta counts match the Iceberg counts: 30 Bronze, 12 Silver, 16 Quarantine, and 8 Gold. |
| Iceberg tables, capabilities, and comparison note | Schema evolution, `MERGE`, snapshots, and historical snapshot reading are implemented and verified. The earliest silver snapshot contains 5 rows; see streaming screenshots `07`, `09`, and `10` and [delta-vs-iceberg.md](delta-vs-iceberg.md). |
| Azure Databricks and ADLS Gen2 evidence | The Unity Catalog volume and Delta tables are confirmed ADLS-backed. The trainer additionally requires explicit raw, curated, rejected, and analytics zones with `abfss` paths and a sensible folder/partition layout; that mapping/layout still needs to be verified and evidenced or implemented. |
| Databricks job execution evidence | Widgets parameterize the notebook, but a Databricks Job/Workflow run with run history, row counts, and duration has not been captured for the revised notebook. |
| Docker configuration and usage notes | The producer now has a locally built and verified multi-stage, versioned, non-root image. The streaming image was rebuilt locally and deployed. The required batch Spark image is still absent. README and [runbook.md](runbook.md) document usage. |
| Kubernetes manifests and runbook | A streaming Deployment and one-time producer Job exist. A separate producer Deployment manifest is prepared with zero replicas and an isolated demo topic; scaling evidence and the batch Spark Kubernetes Job/CronJob are still outstanding. |
| Kubernetes demonstrations | The updated streaming Deployment rollout and state preservation were verified. Configuration-change/rollback, deliberately broken deployment diagnosis, pod-failure recovery, producer scaling, and Kafka consumer-lag evidence remain. Procedures are in [runbook.md](runbook.md). |
| Git repository and CI evidence | GitHub Actions runs #7, #8, and #9 passed; run #9 validates the complete checklist update at `e186e0b`. |
| README and final presentation/live demo | README and the walkthrough outline/checklist are documented. The finished slide deck is assigned to another team member; the live demo itself has not yet been presented. |
| Known issues and future enhancements | Tracked in [known-issues.md](known-issues.md). |

## Remaining submission work

1. The assigned teammate produces the final presentation slides; the live demo still needs to be delivered.
2. Run the revised batch notebook as a parameterized Azure Databricks Job/Workflow, using a separate validation schema to preserve existing outputs; capture run history, duration, counts, schemas, and table locations.
3. Verify or implement the required ADLS Gen2 raw, curated, rejected, and analytics zones with `abfss` paths and folder/partition layout; record how access is secured. ADLS-backed managed tables alone do not yet demonstrate this explicit zone layout.
4. Build the equivalent batch Spark image and deploy it as a Kubernetes Job/CronJob. Apply the prepared producer Deployment only for a controlled scaling demo, and capture the scaling evidence.
5. Capture the required Kubernetes configuration change, rollout and rollback, deliberately broken deployment diagnosis, pod-failure recovery, producer scaling, and consumer-lag evidence during the team demo. Avoid rerunning the existing producer sample against the production topic.
6. The trainer confirmed that streaming storage should remain on the local PVC, so this is the intended design and not a rubric gap. Preserve the PVC, checkpoint, and watermark state.
7. The assigned teammate produces the final presentation slides; the live demo still needs to be delivered.
8. The comparison note covers the required topics; ensure its rendered form stays within the trainer's one-page limit. Iceberg table maintenance is needed only if required beyond the three demonstrated capabilities.
9. A separate customer-managed ADLS external location is not in the confirmed streaming requirement; only add one if the trainer specifically asks for it.
