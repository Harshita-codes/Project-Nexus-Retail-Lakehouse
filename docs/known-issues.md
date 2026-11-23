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
- GitHub Actions [run #9](https://github.com/Harshita-codes/Project-Nexus-Retail-Lakehouse/actions/runs/45154741482) passed on commit `e186e0b`; the batch-helper and streaming test suites also pass locally. CI does not start Kafka or execute a Kubernetes integration test.
- The revised batch notebook has not yet been run as a parameterized Azure Databricks Job/Workflow with run history, counts, and duration evidence.
- The trainer requires clear raw, curated, rejected, and analytics ADLS zones using `abfss` paths and an appropriate folder/partition layout. The current Unity Catalog volume and managed Delta tables are ADLS-backed, but the explicit zone mapping/layout has not been verified or evidenced.
- There is no container image/Kubernetes Job for the batch Spark workload. The producer image is now multi-stage, versioned, and non-root. A producer Deployment manifest is prepared with zero replicas and an isolated demo topic, but the scaling demonstration has not been run.
- Required Kubernetes demonstrations still missing: configuration change and rollback, diagnosis of a deliberately broken deployment, pod-failure recovery, producer replica scaling, and Kafka consumer-lag observation. A successful streaming rollout has been verified, but it does not cover these demonstrations.
- The presentation outline and live-demo checklist are documented. A teammate is responsible for the slide deck, and the live demonstration has not yet been presented.

## Recommended future enhancements

1. Replace full-snapshot Delta refreshes with incremental, replay-safe `MERGE` operations and benchmark against the current approach.
2. If the deployment scope changes from the trainer's local-PVC requirement, evaluate cloud-backed storage for production with least-privilege identity-based access and cost/performance evidence.
3. Add a same-input, comparable-compute benchmark for Delta and Iceberg.
4. Add scheduled Iceberg compaction and snapshot-retention maintenance after defining retention requirements.
5. Add end-to-end integration tests using an ephemeral Kafka broker and containerized Kubernetes environment.
6. Configure authenticated Kafka and secret rotation for non-local environments.
