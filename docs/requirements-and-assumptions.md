# Requirements and Assumptions

## Project goal

Demonstrate a retail lakehouse with historical batch processing and real-time order-event processing. The project uses Bronze, Silver, Quarantine, and Gold layers; validates and enriches data; and demonstrates Delta Lake and Apache Iceberg capabilities.

## Working assumptions

- Historical product, customer, and order CSV files are the batch source.
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
| Batch processing implementation and outputs | Scala notebook and batch screenshots exist. Confirm Azure/ADLS execution evidence and the configured storage backing. |
| Streaming implementation and event demonstration | Scala app, producer, and 30-event JSONL sample exist. Capture fresh output after the dual-format update is deployed. |
| Data quality and business aggregation | Batch and streaming validation/aggregation code and screenshots exist; streaming rules now have local Spark tests. |
| Delta Lake persistence evidence | Batch Delta evidence exists. Streaming Delta copies are implemented and tested locally; Kubernetes runtime counts still need capture. |
| Iceberg tables, capabilities, and comparison note | Iceberg tables, schema evolution, `MERGE`, snapshots, and a tested historical read are implemented. Capture the updated verifier output; the comparison note is in [delta-vs-iceberg.md](delta-vs-iceberg.md). |
| Azure Databricks and ADLS Gen2 evidence or blocker | Not yet confirmed. Record a successful Azure run or document the specific access/environment limitation and local alternative. |
| Docker configuration and usage notes | Producer and streaming Dockerfiles and README instructions exist; the updated streaming image still needs a Docker build and rollout. |
| Kubernetes manifests and runbook | Producer Job, Spark Deployment, Service/ConfigMap/PVC configuration, and [runbook.md](runbook.md) exist. |
| Kubernetes deployment, rollout/rollback, and recovery evidence | These operations were verified in the prior local run. Preserve the existing PVC and collect the latest output after the updated image is deployed. |
| Git repository and CI evidence | GitHub Actions passed for an earlier commit. The latest dual-write changes need a new green CI run; the workflow builds/tests and builds images. |
| README and final presentation/live demo | README exists; [final-presentation-outline.md](final-presentation-outline.md) is a draft script, not a finished slide deck. |
| Known issues and future enhancements | Tracked in [known-issues.md](known-issues.md). |

## Acceptance evidence still to collect

1. New streaming Delta counts alongside Iceberg counts after deploying the updated image.
2. The Iceberg verifier's schema, snapshot history, and historical snapshot row count.
3. Latest GitHub Actions result for the code containing the Delta streaming implementation.
4. Azure Databricks/ADLS Gen2 run history and output, or an explicit blocker statement.
5. Final presentation slides or a completed live demonstration.
