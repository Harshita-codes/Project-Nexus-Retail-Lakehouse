# Known Issues and Future Enhancements

## Known issues and limitations

- The streaming deployment runs in one pod with Spark `local[*]`; it demonstrates a processing workload but is not a distributed Spark cluster.
- Docker Desktop `hostpath` storage is local demonstration storage, not a cloud durability or multi-node storage solution. The trainer confirmed this local PVC is the required streaming-storage design, so it is an operational limitation rather than a rubric gap.
- The streaming image downloads Kafka, Iceberg, and Delta runtime packages from Maven Central at startup and needs network access when the Ivy cache is empty.
- Delta Silver and Gold copies are refreshed by overwriting the full current state each batch. This is acceptable for the small sample, but causes unnecessary I/O at larger scale.
- Delta and Iceberg do not share an atomic commit. Idempotent Delta transaction IDs and Kafka-coordinate Iceberg merges support retries, but a failed batch can temporarily leave one format ahead until recovery finishes.
- Kafka is PLAINTEXT with no authentication in the local demo. Production deployments must configure TLS/SASL and manage credentials securely.
- The batch Unity Catalog managed volume and Delta table are confirmed to use ADLS Gen2 storage. A separate customer-managed external location or direct storage credential configuration is not part of this demo.
- The updated stream passed all nine local Spark tests and was rolled out to the existing Kubernetes deployment. Delta and Iceberg both report 30 Bronze, 12 Silver, 16 Quarantine, and 8 Gold rows; Kafka offsets and the bound streaming PVC were preserved. The 600-second watermark state file is on that PVC. No new Kafka event was published, so late-event quarantine was verified by unit test rather than a new live event.
- Iceberg schema evolution, `MERGE`, snapshots, and historical reads are verified. Compaction, snapshot expiration, and partition evolution are not part of the current demo unless explicitly required by the trainer rubric.
- No controlled throughput, latency, or cost benchmark compares Delta and Iceberg.
- GitHub Actions validates code, container builds, and Kubernetes manifests; it does not start Kafka or deploy workloads to Kubernetes. Run #14 passed before the current local layout update; a new hosted run is needed after pushing the updated paths.
- The revised batch notebook has not yet been run as a parameterized Azure Databricks Job/Workflow with run history, counts, and duration evidence.
- The trainer requires clear raw, curated, rejected, and analytics ADLS zones using `abfss` paths and an appropriate folder/partition layout. The current Unity Catalog volume and managed Delta tables are ADLS-backed, but the explicit zone mapping/layout has not been verified or evidenced.
- A separate local Spark batch app, multi-stage image, batch ConfigMap, output PVC, and one-shot Kubernetes Job manifest are now present. The app handles the base products/customers/orders CSVs only; the Databricks notebook remains the primary cloud implementation and still owns the incremental `MERGE` and schema-evolution steps. The live Kubernetes Job has not been run because the source CSVs are not checked into the repository; stage them through an input ConfigMap (maximum 1 MiB) or a separate input PVC first. An optional CronJob is deferred until the one-shot Job succeeds.
- Direct Spark/Delta integration tests on Windows require Hadoop `winutils.exe`; the Docker build runs the same tests in its Linux build stage, and CI runs them on Linux. Use the container build or CI rather than treating the Windows `winutils` error as an application failure.
- The isolated producer Deployment demonstration was run on 2026-11-23: an invalid image produced `ErrImageNeverPull`, the valid image recovered, and scaling from one to three replicas advanced only the `order-events-scaling-demo` topic. The Deployment was returned to zero replicas.
- Kubernetes operational checks were run on 2026-11-23: the streaming configuration changed from 2 and rolled back to 4; pod recovery preserved counts and offsets; and the broker end offsets matched the Bronze processed offsets (zero lag). The production `order-events` offsets remained 8, 13, and 9. Save terminal screenshots into the submission evidence set if required; do not rerun these tests just for screenshots.
- The presentation outline and live-demo checklist are documented. A teammate is responsible for the slide deck, and the live demonstration has not yet been presented.

## Recommended future enhancements

1. Replace full-snapshot Delta refreshes with incremental, replay-safe `MERGE` operations and benchmark against the current approach.
2. If the deployment scope changes from the trainer's local-PVC requirement, evaluate cloud-backed storage for production with least-privilege identity-based access and cost/performance evidence.
3. Add a same-input, comparable-compute benchmark for Delta and Iceberg.
4. Add scheduled Iceberg compaction and snapshot-retention maintenance after defining retention requirements.
5. Add end-to-end integration tests using an ephemeral Kafka broker and containerized Kubernetes environment.
6. Configure authenticated Kafka and secret rotation for non-local environments.
