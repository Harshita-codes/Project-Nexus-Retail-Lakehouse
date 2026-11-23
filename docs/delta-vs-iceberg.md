# Delta Lake vs. Apache Iceberg

## Scope

This is an implementation comparison, not a controlled benchmark. Delta processes batch CSVs in Databricks; Iceberg processes Kafka events with Spark Structured Streaming in a local Kubernetes demo. Their row counts and runtimes are not directly comparable.

| Capability | Delta Lake implementation | Iceberg implementation |
|---|---|---|
| Workload/storage | Batch tables in the `nexus.retail_nexus` Unity Catalog namespace. | Kafka stream in a local Hadoop catalog; Iceberg tables, Delta copies, and checkpoint use a Kubernetes PVC. |
| Updates | `MERGE` applies incoming orders only when `last_updated` is newer. | `MERGE` applies events when `event_time` is not older than the silver row. |
| Schema and partition evolution | `mergeSchema` adds `sales_channel` and `discount_amount` to batch Silver orders; changing a Delta partition layout is not demonstrated. | `ALTER TABLE` adds those fields and the Silver table uses `days(event_time)`; changing the Iceberg partition spec is not demonstrated. |
| History | `DESCRIBE HISTORY`; reads version 0 and compares it with the current table. | Snapshot history; verifier reads the earliest snapshot with `VERSION AS OF`. |
| Maintenance/layout | `OPTIMIZE` with Z-ordering and `VACUUM` with 168-hour retention. | Silver table is partitioned by event date; compaction and snapshot expiration are not yet demonstrated. |
| Transactions | Delta transaction log gives atomic table commits; streaming Bronze/Quarantine appends use transaction IDs for retry idempotency. | Iceberg commits table metadata snapshots atomically; Bronze/Quarantine merges deduplicate by Kafka partition and offset. The application does not provide a cross-format transaction. |
| Engine interoperability | Used through Databricks/Spark and Unity Catalog in this project. | Used through Spark with a Hadoop catalog; no second query engine has been tested. |
| Operational effort | Unity Catalog manages the batch namespace; the demo also shows Delta history, optimization, and retention cleanup. | Requires a catalog and storage configuration; the demo does not automate compaction or snapshot expiration. |
| Analytics | Historical category sales and customer-spend aggregates. | Ten-minute event-time category revenue windows; stored in both formats. |

## Verified streaming results

The updated app was deployed and verified against the existing Kubernetes warehouse without republishing the producer sample. The checked-in 30-event sample yielded matching counts in both formats: 30 Bronze rows, 12 Silver orders, 16 quarantined events, and 8 Gold windows. The Iceberg verifier confirmed the evolved schema, the earliest Silver snapshot contains 5 rows, and the earliest Gold snapshot contains 4 rows. The rollout retained the checkpoint and watermark file; Kafka end offsets stayed at partition 0:8, 1:13, and 2:9. These counts are not a like-for-like comparison with the batch pipeline's 104 Silver rows.

## Trade-offs and open requirements

- Both formats support the updates and schema evolution used here. Delta demonstrates table history, time travel, file optimization, and retention cleanup; Iceberg demonstrates event-stream persistence, snapshots, partitioning, and snapshot-based historical reads.
- Delta's per-table ACID commit and Iceberg's atomic metadata snapshot do not create a distributed transaction across the two formats. The app writes Iceberg and then mirrors Delta; retry keys make repeated operations idempotent, but a failed batch can temporarily leave one format ahead.
- Streaming Delta Bronze and Quarantine appends use Delta transaction IDs for retry idempotency; Silver and Gold Delta snapshots are refreshed from Iceberg state. Those full-snapshot rewrites are acceptable for this small demo but need a more scalable merge strategy for production.
- The Iceberg demo uses local PVC storage, not cloud object-storage durability. A separate engine interoperability test, controlled same-input performance benchmark, and cost comparison have not been performed; row counts are not performance evidence.
- For this project, Delta is the practical fit for Databricks/Unity Catalog batch tables and their governance/maintenance workflow. Iceberg demonstrates an open table format for the streaming output and snapshot reads. The project does not establish a universal format winner; the choice depends on the catalog, engines, governance, and operational model.

## Evidence captured and still to collect

The repository screenshots capture the batch Delta governance results, Azure/ADLS backing, streaming schema and row counts, Iceberg history/time travel, Delta copies, and Kubernetes PVC status. Commit `fb06c33` passed focused tests, a local Kubernetes rollout, and [GitHub Actions run #7](https://github.com/Harshita-codes/Project-Nexus-Retail-Lakehouse/actions/runs/45143048325). A controlled same-workload performance benchmark and cross-engine test have not been run; do not make relative speed or cost claims without them.
