# Known Issues and Future Enhancements

## Known issues and limitations

- The streaming deployment runs in one pod with Spark `local[*]`; it demonstrates a processing workload but is not a distributed Spark cluster.
- Docker Desktop `hostpath` storage is local demonstration storage, not a cloud durability or multi-node storage solution.
- The streaming image downloads Kafka, Iceberg, and Delta runtime packages from Maven Central at startup and needs network access when the Ivy cache is empty.
- Delta Silver and Gold copies are refreshed by overwriting the full current state each batch. This is acceptable for the small sample, but causes unnecessary I/O at larger scale.
- Delta and Iceberg do not share an atomic commit. Idempotent Delta transaction IDs and Kafka-coordinate Iceberg merges support retries, but a failed batch can temporarily leave one format ahead until recovery finishes.
- Kafka is PLAINTEXT with no authentication in the local demo. Production deployments must configure TLS/SASL and manage credentials securely.
- The batch Unity Catalog managed volume and Delta table are confirmed to use ADLS Gen2 storage. A separate customer-managed external location or direct storage credential configuration is not part of this demo.
- The Delta dual-write stream passed local Spark tests and was rolled out to the existing Kubernetes deployment. Delta and Iceberg both report 30 Bronze, 12 Silver, 16 Quarantine, and 8 Gold rows; Kafka offsets and the bound streaming PVC were preserved.
- Iceberg schema evolution, `MERGE`, snapshots, and historical reads are verified. Compaction, snapshot expiration, and partition evolution are not part of the current demo unless explicitly required by the trainer rubric.
- No controlled throughput, latency, or cost benchmark compares Delta and Iceberg.
- GitHub Actions run #5 passed on commit `fb155dd`. CI builds and tests the Scala modules and Docker images; it does not start Kafka or execute a Kubernetes integration test.
- The presentation outline is available, but a finished slide deck or completed live presentation remains to be prepared.

## Recommended future enhancements

1. Replace full-snapshot Delta refreshes with incremental, replay-safe `MERGE` operations and benchmark against the current approach.
2. Deploy batch and streaming storage on Azure-managed ADLS Gen2 with least-privilege identity-based access, then retain run history and cost/performance evidence.
3. Add a same-input, comparable-compute benchmark for Delta and Iceberg.
4. Add scheduled Iceberg compaction and snapshot-retention maintenance after defining retention requirements.
5. Add end-to-end integration tests using an ephemeral Kafka broker and containerized Kubernetes environment.
6. Configure authenticated Kafka and secret rotation for non-local environments.
