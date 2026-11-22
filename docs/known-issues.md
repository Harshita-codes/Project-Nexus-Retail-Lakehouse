# Known Issues and Future Enhancements

## Known issues and limitations

- The streaming deployment runs in one pod with Spark `local[*]`; it demonstrates a processing workload but is not a distributed Spark cluster.
- Docker Desktop `hostpath` storage is local demonstration storage, not a cloud durability or multi-node storage solution.
- The streaming image downloads Kafka, Iceberg, and Delta runtime packages from Maven Central at startup and needs network access when the Ivy cache is empty.
- Delta Silver and Gold copies are refreshed by overwriting the full current state each batch. This is acceptable for the small sample, but causes unnecessary I/O at larger scale.
- Delta and Iceberg do not share an atomic commit. Idempotent Delta transaction IDs and Kafka-coordinate Iceberg merges support retries, but a failed batch can temporarily leave one format ahead until recovery finishes.
- Kafka is PLAINTEXT with no authentication in the local demo. Production deployments must configure TLS/SASL and manage credentials securely.
- Direct ADLS Gen2 `abfss://` configuration, storage credentials, and Azure Databricks job history still need verification or a documented access blocker.
- The latest Delta dual-write code has passed local Spark tests but has not yet been rolled out to Kubernetes or verified against the existing persistent warehouse.
- Iceberg historical reads, schema evolution, and MERGE are covered in code/tests. Compaction, snapshot expiration, and partition evolution are not currently part of the verified demo.
- No controlled throughput, latency, or cost benchmark compares Delta and Iceberg.
- GitHub Actions builds and tests the Scala modules and Docker images; it does not start Kafka or execute a Kubernetes integration test.

## Recommended future enhancements

1. Replace full-snapshot Delta refreshes with incremental, replay-safe `MERGE` operations and benchmark against the current approach.
2. Deploy batch and streaming storage on Azure-managed ADLS Gen2 with least-privilege identity-based access, then retain run history and cost/performance evidence.
3. Add a same-input, comparable-compute benchmark for Delta and Iceberg.
4. Add scheduled Iceberg compaction and snapshot-retention maintenance after defining retention requirements.
5. Add end-to-end integration tests using an ephemeral Kafka broker and containerized Kubernetes environment.
6. Configure authenticated Kafka and secret rotation for non-local environments.
