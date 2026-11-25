# Demo and Operations Guide

This runbook covers the Databricks batch Job, local Kubernetes batch demo, producer, and streaming app. Kubernetes examples use namespace `streaming`; the producer/streaming procedures assume the existing Kafka Deployment and Service are available.

## Azure Databricks batch Job validation

The Databricks notebook remains the primary cloud batch implementation. These steps validate the revised notebook as a parameterized Job; the local Kubernetes batch app below is a separate demonstration.

1. In Azure Databricks, open **Workspace → Repos**, select the project repository, and pull the latest `main`. Alternatively, import `01_bronze_ingestion.scala` and `00_batch_helpers.scala` into the same workspace folder; `%run ./00_batch_helpers` requires that placement.
2. Open **Workflows/Jobs & Pipelines → Create Job → Notebook task**. Select `01_bronze_ingestion.scala` and attach the Databricks cluster that can read the Unity Catalog volume.
3. Configure the notebook task parameters:

   | Parameter | Value |
   |---|---|
   | `batch_input_path` | `/Volumes/nexus/default/retail_nexus_volume/batch` |
   | `target_catalog` | `nexus` |
   | `target_schema` | `retail_nexus_validation` |
   | `run_date` | A valid `yyyy-MM-dd` date, such as `2026-11-23` |

   The notebook overwrites its output tables. Use the new validation schema rather than `nexus.retail_nexus` to preserve the existing outputs. Ensure you have permission to create tables in that schema.
4. Run the Job and confirm all tasks succeed. Verify Bronze counts of 27 products, 42 customers, and 161 orders; final Silver orders should be 104. Check the `sales_channel` and `discount_amount` evolved columns and both Gold tables.
5. Capture the successful Job run page (status, parameters, duration/history), row-count outputs, evolved schema, and table locations in `Testing/evidence/batch/`. Keep the existing screenshots `01`–`16`.

Running the notebook with **Run all** can help debug it, but does not replace the required Job/Workflow run-history evidence.

## Build and deploy

```powershell
docker build -t nexus-kafka-producer:1.0 -f .\Development\Docker\nexus-producer.Dockerfile .

Push-Location '.\Data Ingestion\Streaming'
sbt test
sbt package
Copy-Item .\target\out\jvm\scala-2.12.18\kafkastreamingicebergdemo\kafkastreamingicebergdemo_2.12-0.1.0-SNAPSHOT.jar .\target\app.jar -Force
docker build -t nexus-streaming:1.0 -f ..\..\Development\Docker\nexus-streaming.Dockerfile .
Pop-Location
kubectl apply -f .\Development\Kubernetes\streaming-config.yaml
kubectl apply -f .\Development\Kubernetes\streaming-state-pvc.yaml
kubectl apply -f .\Development\Kubernetes\streaming-deployment.yaml
kubectl rollout status deployment/nexus-streaming -n streaming --timeout=8m
```

If upgrading the existing local deployment, keep `nexus-stream-state` and the Kafka PVC. The new app copies existing Iceberg table contents into Delta paths if the Delta tables do not exist; it uses the existing checkpoint and does not require rerunning the producer.
The ConfigMap sets a 600-second event-time watermark and 600-second Gold windows. The maximum accepted event time is persisted in `/lakehouse/watermark/event-time-watermark.txt` on the same PVC. Events older than the active watermark are still preserved in Bronze and are written to Quarantine; do not delete the watermark state or checkpoint when rolling out the app.

When rebuilding the same `nexus-streaming:1.0` tag for an existing Deployment, applying the manifest alone does not change its pod template. Restart the Deployment to load the rebuilt local image and any updated ConfigMap values:

```powershell
kubectl rollout restart deployment/nexus-streaming -n streaming
kubectl rollout status deployment/nexus-streaming -n streaming --timeout=8m
```

## Check health and output

```powershell
kubectl get deployments,pods,pvc -n streaming
kubectl logs -n streaming deployment/nexus-streaming --tail=100
```

Run the read-only verifier described in the root [README](../README.md#verify-the-running-stream). Preserve its output along with the pod/PVC status.

## Configuration change and rollback

```powershell
kubectl set env deployment/nexus-streaming -n streaming SPARK_SHUFFLE_PARTITIONS=2
kubectl rollout status deployment/nexus-streaming -n streaming --timeout=8m
kubectl exec -n streaming deployment/nexus-streaming -- printenv SPARK_SHUFFLE_PARTITIONS
kubectl rollout undo deployment/nexus-streaming -n streaming
kubectl rollout status deployment/nexus-streaming -n streaming --timeout=8m
kubectl apply -f .\Development\Kubernetes\streaming-deployment.yaml
```

The temporary value should be `2`; after rollback and reapplying the checked-in manifest, it should be `4`.

## Producer scaling demo

The producer Deployment manifest is intentionally configured with zero replicas and a separate `order-events-scaling-demo` topic. Build the producer image, create that topic, and follow the scaling commands in the root [README](../README.md) only during a controlled demo. Scale it back to zero afterward. Do not point it at `order-events` or rerun the one-time producer Job if preserving the current streaming outputs and Kafka offsets.

## Local Kubernetes batch Job

The local batch app processes only the base products, customers, and orders inputs. It does not replace the Databricks notebook's incremental `MERGE` or schema-evolution work. The app writes Delta outputs to the separate `nexus-batch-output` PVC; it does not use or modify the streaming PVC, Kafka topic, or checkpoint.

Build the image and prepare the input CSVs as described in the root [README](../README.md). The input ConfigMap must be named `nexus-batch-data` and contain `products.csv`, `customers.csv`, and `orders.csv`. Do not put credentials in the ConfigMap. If the combined files exceed the 1 MiB ConfigMap limit, mount them from a separate input PVC instead.

```powershell
Push-Location '.\Data Ingestion\Batch'
docker build -t nexus-batch:1.0 -f ..\..\Development\Docker\nexus-batch.Dockerfile .
kubectl apply -f ..\..\Development\Kubernetes\batch-pvc.yaml
kubectl apply -f ..\..\Development\Kubernetes\batch-config.yaml
kubectl delete job nexus-batch -n streaming --ignore-not-found
kubectl apply -f ..\..\Development\Kubernetes\batch-job.yaml
Pop-Location
kubectl get jobs,pods,pvc -n streaming
kubectl wait --for=condition=complete job/nexus-batch -n streaming --timeout=10m
kubectl get pod -n streaming -l job-name=nexus-batch -o yaml
kubectl logs -n streaming job/nexus-batch
```

The logs print Bronze, Silver, Quarantine, and Gold counts plus elapsed time. Save those logs and the Job/Pod/PVC status as evidence. The checked-in `batch-config.yaml` run date is an example; update it to a valid run date before applying. Do not add a CronJob until this one-shot Job succeeds.

For a safe image-pull failure exercise, copy `Development/Kubernetes/batch-job.yaml` to a temporary manifest, change its Job name to `nexus-batch-broken` and its image to `nexus-batch:not-present`, then apply that temporary manifest. Inspect the Pod and events with `kubectl describe pod` and `kubectl get events`; an `ErrImagePull` Pod will not start the batch app or write output. Capture the failure, then delete only the test Job with `kubectl delete job nexus-batch-broken -n streaming`. Kubernetes Jobs are not generally mutable; do not rely on `kubectl set image job/...` to change an existing Job. Keep the successful `nexus-batch` Job and its evidence.

## Pod recovery

```powershell
kubectl delete pod -n streaming -l app=nexus-streaming
kubectl rollout status deployment/nexus-streaming -n streaming --timeout=8m
kubectl get pods,pvc -n streaming
kubectl logs -n streaming deployment/nexus-streaming --tail=100
```

Compare Iceberg and Delta counts with saved pre-restart output. Never delete the streaming PVC as part of this test.
