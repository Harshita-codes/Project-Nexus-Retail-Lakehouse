# Final Presentation / Live Demonstration Outline

1. **Business problem and goals** — Retail orders arrive both as historical CSV files and live order events.
2. **Architecture and design** — Show the batch and streaming paths, Bronze/Silver/Quarantine/Gold layers, Delta and Iceberg, and the local-vs-Azure boundaries. Use [solution-design.md](solution-design.md).
3. **Batch results and data quality** — Show products/customers/orders validation, duplicate handling, quarantine, and the category/customer sales aggregates.
4. **Streaming demo** — Show the producer's 30 events, Kafka topic, Spark logs, and the Bronze/Silver/Quarantine/Gold counts.
5. **Storage capabilities** — Show Delta history/time travel/maintenance; show Iceberg schema evolution, MERGE, snapshots, and the historical snapshot read. Use [delta-vs-iceberg.md](delta-vs-iceberg.md).
6. **Kubernetes operations** — Show Deployment, Job, ConfigMap, PVC, probes/resource requests, configuration rollout/rollback, and pod recovery. Use [runbook.md](runbook.md).
7. **Automated validation** — Show producer and streaming tests, Docker image builds, and the latest successful GitHub Actions run.
8. **Limitations and next steps** — Explain local storage, single-pod Spark local mode, Azure access/evidence status, and the scaling opportunities in [known-issues.md](known-issues.md).

Before presenting, replace any pending evidence items in [requirements-and-assumptions.md](requirements-and-assumptions.md) with the latest run output or a precise blocker statement. Do not claim the Delta dual-write rollout or Azure/ADLS execution as complete until verified.
