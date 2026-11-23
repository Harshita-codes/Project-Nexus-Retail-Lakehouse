# Live Demonstration Checklist

This guide is for the project walkthrough; it does not replace the teammate-owned slide deck. Allow about 5–7 minutes. Use the saved screenshots as the fallback if the local cluster is unavailable.

## Before the walkthrough

- Start Docker Desktop and make sure its Kubernetes cluster is enabled.
- In PowerShell, check the selected context and the existing streaming resources:

  ```powershell
  kubectl config current-context
  kubectl get deployments,pods,pvc -n streaming
  ```

- The expected context is `docker-desktop`; the streaming Deployment should be Ready and `nexus-stream-state` should remain Bound.
- Do not rerun the Kafka producer, delete PVCs, or reset the Kubernetes cluster for this walkthrough. The producer's 30 events have already been processed and verified.
- Keep the [redacted evidence screenshots](../part1-batch-databricks/screenshots/) and [streaming screenshots](../part2-streaming-iceberg/screenshots/) open as a fallback.

If the cluster is stopped or resources are unavailable, don't try an unplanned redeployment during the presentation. Explain that the streaming environment is a local Docker Desktop demo and present the saved evidence instead.

## Walkthrough

### 1. Business goal and architecture

Explain that historical retail CSVs are processed in Azure Databricks while live order events flow through Kafka and Spark Structured Streaming. Both pipelines validate and enrich data into Bronze, Silver, Quarantine, and Gold outputs. Refer to [solution-design.md](solution-design.md).

Be explicit about the boundary: the batch Unity Catalog managed volume and Delta table were verified as ADLS Gen2-backed; the streaming Iceberg and Delta warehouses use a local Kubernetes PVC.

### 2. Batch results

Show `part1-batch-databricks/screenshots/16_adls_gen2_storage_and_batch_counts.png`. Point out the verified Bronze counts (27 products, 42 customers, 161 orders), 104 Silver orders, Delta format, and the redacted ADLS Gen2 confirmation.

### 3. Streaming results

If the Deployment is Ready, show its status and recent logs:

```powershell
kubectl get deployments,pods,pvc -n streaming
kubectl logs -n streaming deployment/nexus-streaming --tail=40
```

For table counts, schema evolution, samples, snapshot history, historical reads, and Delta copies, run the read-only verifier command in the root [README](../README.md) under “Run the Iceberg verifier.” Its expected counts for both formats are 30 Bronze, 12 Silver, 16 Quarantine, and 8 Gold.

The deployed app also persists its 10-minute event-time watermark on the PVC. To show that the state file exists without changing it:

```powershell
kubectl exec -n streaming deployment/nexus-streaming -- cat /lakehouse/watermark/event-time-watermark.txt
```

The current producer sample has already been processed; do not claim that a new late event was demonstrated unless one is intentionally published and its quarantine result is captured.

Use the corresponding saved screenshots if the live verifier is slow or the cluster is unavailable:

- `07_iceberg_row_counts_and_evolved_schema.png`
- `08_silver_orders_and_quarantine_examples.png`
- `09_gold_windows_and_iceberg_snapshot_history.png`
- `10_iceberg_time_travel_and_delta_counts.png`
- `11_kubernetes_deployments_pods_and_pvcs.png`

### 4. Operations and automated validation

Show the Kubernetes Deployment, pod, and PVC evidence. Explain that the rollout preserved the checkpoint/PVC and Kafka offsets; don't perform a rollout, rollback, or pod deletion during this short walkthrough.

The latest published CI evidence is [GitHub Actions run #6](https://github.com/Harshita-codes/Project-Nexus-Retail-Lakehouse/actions/runs/45075261098) for earlier commit `8110d29`. The current local changes have passed focused tests and a Kubernetes rollout but have not yet been pushed for CI.

### 5. Limitations and close

State that Spark runs as a single local-mode pod on Docker Desktop hostpath storage; this is not a production or cloud-durability deployment. The managed volume is ADLS-backed, but a separate customer-managed external location is not configured. Iceberg compaction and snapshot expiration were not demonstrated. See [known-issues.md](known-issues.md).

## Quick readiness checklist

- [ ] Docker Desktop / Kubernetes is available, or screenshots are ready as fallback.
- [ ] Namespace is `streaming`; the Deployment is Ready and the state PVC is Bound if demoing live.
- [ ] Batch and streaming screenshots are open.
- [ ] GitHub Actions run #6 is available.
- [ ] No producer rerun, PVC deletion, cluster reset, or unplanned deployment is part of the walkthrough.
