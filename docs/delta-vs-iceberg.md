# Delta Lake vs. Apache Iceberg

## Scope

This is an implementation comparison, not a controlled benchmark. Delta processes batch CSVs in Databricks; Iceberg processes Kafka events with Spark Structured Streaming in a local Kubernetes demo. Their row counts and runtimes are not directly comparable.

| Capability | Delta Lake implementation | Iceberg implementation |
|---|---|---|
| Workload/storage | Batch tables in the `nexus.retail_nexus` Unity Catalog namespace. | Kafka stream in a local Hadoop catalog; Iceberg tables, Delta copies, and checkpoint use a Kubernetes PVC. |
| Updates | `MERGE` applies incoming orders only when `last_updated` is newer. | `MERGE` applies events when `event_time` is not older than the silver row. |
| Schema evolution | `mergeSchema` adds `sales_channel` and `discount_amount` to batch silver orders. | `ALTER TABLE` adds those columns when evolved events arrive. |
| History | `DESCRIBE HISTORY`; reads version 0 and compares it with the current table. | Snapshot history; verifier reads the earliest snapshot with `VERSION AS OF`. |
| Maintenance/layout | `OPTIMIZE` with Z-ordering and `VACUUM` with 168-hour retention. | Silver table is partitioned by event date; compaction and snapshot expiration are not yet demonstrated. |
| Analytics | Historical category sales and customer-spend aggregates. | Ten-minute event-time category revenue windows; stored in both formats. |

## Verified streaming results

The previously verified Iceberg run of the checked-in 30-event sample yielded 30 bronze rows, 12 silver orders, 16 quarantined events, and 8 gold windows. The updated app is expected to mirror those counts to Delta; this dual-write version still needs a Kubernetes rollout and runtime verification. These counts are not a like-for-like comparison with the batch pipeline's 104 silver rows.

## Trade-offs and open requirements

- Both formats support the updates and schema evolution used here. Delta demonstrates table history, time travel, file optimization, and retention cleanup; Iceberg demonstrates event-stream persistence, snapshots, partitioning, and checkpoint recovery.
- The Iceberg demo uses local PVC storage, not cloud object storage durability. Neither row counts nor this code establish which format is faster or cheaper; a same-input, comparable-compute benchmark is still needed for those claims.
- Streaming Delta bronze and quarantine appends use Delta transaction IDs for retry idempotency; silver and gold Delta snapshots are refreshed from Iceberg state. Those full-snapshot rewrites are acceptable for this small demo but need a more scalable merge strategy for production.
- The trainer checklist requires batch and streaming outputs in Delta, plus Iceberg aggregates. The streaming pipeline now writes all four layers to Delta and keeps the Iceberg tables.
- Delta is a natural fit here for the Databricks/Unity Catalog batch tables and their maintenance workflow; Iceberg demonstrates an open table format for streaming outputs and snapshot-based reads. The project does not establish a universal format winner.

## Evidence to capture

Capture the Delta row counts, history/time-travel/maintenance outputs, and the Iceberg verifier's schema, snapshot history, and historical row count. The dual-write changes have not yet been deployed to the Kubernetes cluster in this session.
