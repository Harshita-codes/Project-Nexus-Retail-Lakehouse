# Delta Lake vs. Apache Iceberg

## Scope

This is an implementation comparison, not a controlled benchmark. Delta processes batch CSVs in Databricks; Iceberg processes Kafka events with Spark Structured Streaming in a local Kubernetes demo. Their row counts and runtimes are not directly comparable.

| Capability | Delta Lake implementation | Iceberg implementation |
|---|---|---|
| Workload/storage | Batch tables in the `nexus.retail_nexus` Unity Catalog namespace. | Kafka stream in a local Hadoop catalog; data and checkpoint use a Kubernetes PVC. |
| Updates | `MERGE` applies incoming orders only when `last_updated` is newer. | `MERGE` applies events when `event_time` is not older than the silver row. |
| Schema evolution | `mergeSchema` adds `sales_channel` and `discount_amount` to batch silver orders. | `ALTER TABLE` adds those columns when evolved events arrive. |
| History | `DESCRIBE HISTORY`; reads version 0 and compares it with the current table. | Snapshot history; verifier reads the earliest snapshot with `VERSION AS OF`. |
| Maintenance/layout | `OPTIMIZE` with Z-ordering and `VACUUM` with 168-hour retention. | Silver table is partitioned by event date; compaction and snapshot expiration are not yet demonstrated. |
| Analytics | Historical category sales and customer-spend aggregates. | Ten-minute event-time category revenue windows. |

## Verified streaming results

The checked-in 30-event sample yielded 30 bronze rows, 12 silver orders, 16 quarantined events, and 8 gold windows. These are Iceberg demo results, not a like-for-like comparison with the batch pipeline's 104 silver rows.

## Trade-offs and open requirements

- Both formats support the updates and schema evolution used here. Delta demonstrates table history, time travel, file optimization, and retention cleanup; Iceberg demonstrates event-stream persistence, snapshots, partitioning, and checkpoint recovery.
- The Iceberg demo uses local PVC storage, not cloud object storage durability. Neither row counts nor this code establish which format is faster or cheaper; a same-input, comparable-compute benchmark is still needed for those claims.
- **Rubric check:** the trainer checklist wording says processed batch and streaming data should be persisted as Delta tables, with aggregate analytics also stored in Iceberg. The current streaming pipeline writes its bronze, silver, quarantine, and gold tables to Iceberg only. If that wording is literal, this is a gap and needs a design decision before implementation changes.

## Evidence to capture

Capture the Delta history/time-travel/maintenance outputs and the Iceberg verifier's schema, snapshot history, and historical row count. The historical Iceberg query is implemented but has not yet been run against the cluster in this session.
