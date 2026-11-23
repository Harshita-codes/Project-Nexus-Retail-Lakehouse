# Final Presentation / Live Demonstration Outline

1. **Business problem and goals** — Retail orders arrive both as historical CSV files and live order events.
2. **Architecture and design** — Show the batch and streaming paths, Bronze/Silver/Quarantine/Gold layers, Delta and Iceberg, and the local-vs-Azure boundaries. Use [solution-design.md](solution-design.md).
3. **Batch results and data quality** — Show products/customers/orders validation, duplicate handling, quarantine, and the category/customer sales aggregates.
4. **Streaming demo** — Show the producer's 30 events, Kafka topic, Spark logs, and the Bronze/Silver/Quarantine/Gold counts.
5. **Storage capabilities** — Show Delta history/time travel/maintenance; show Iceberg schema evolution, MERGE, snapshots, and the historical snapshot read. Use [delta-vs-iceberg.md](delta-vs-iceberg.md).
6. **Kubernetes operations** — Show Deployment, Job, ConfigMap, PVC, probes/resource requests, configuration rollout/rollback, and pod recovery. Use [runbook.md](runbook.md).
7. **Automated validation** — Show producer and streaming tests, Docker image builds, and the latest successful GitHub Actions run.
8. **Limitations and next steps** — Explain local storage, single-pod Spark local mode, the managed Unity Catalog/ADLS boundary, and the scaling opportunities in [known-issues.md](known-issues.md).

Before presenting, include the latest redacted Azure and streaming screenshots from the repository. State that the Unity Catalog managed volume is ADLS-backed; do not imply a separate customer-managed external storage configuration. The streaming rollout and live table counts are verified. The latest local tests have passed, but the changes have not yet been pushed for a new CI run. The slide deck or completed live demonstration is still outstanding.

The slide deck is assigned to another team member. For this project's live walkthrough, follow the [live-demo checklist](live-demo-checklist.md); it includes a safe fallback and avoids rerunning the producer or modifying the existing PVC.
