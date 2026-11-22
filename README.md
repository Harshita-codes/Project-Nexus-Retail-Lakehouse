# Project Nexus — Retail Medallion Lakehouse (Batch & Real-Time Streaming)

End-to-end **Bronze → Silver → Quarantine → Gold** data engineering pipeline built in **Scala**, combining batch Lakehouse governance on **Azure Databricks (Delta Lake)** with low-latency event streaming on **Apache Kafka, Spark Structured Streaming, and Apache Iceberg**.


## Architecture Overview

### Part I: Batch Processing (Azure Databricks & Delta Lake)
* **Catalog & Storage:** Unity Catalog (`nexus.retail_nexus`) backed by Volume `/Volumes/nexus/default/retail_nexus_volume/batch`.
* **Bronze Layer:** Ingests raw `products.csv` (27), `customers.csv` (42), and `orders.csv` (161) with `_ingested_at` audit metadata.
* **Silver & Quarantine Layers:** Enforces schema typing, regex/domain validation, window-based deduplication, and foreign-key referential integrity:
  * **Products:** `21` valid (`silver_products`), `6` rejected (`quarantine_products`)
  * **Customers:** `34` valid (`silver_customers`), `8` rejected (`quarantine_customers`)
  * **Orders:** `88` valid (`silver_orders`), `73` rejected (`quarantine_orders`)
* **Incremental Upserts (`MERGE INTO`):** Processes `orders_updates.csv` (17 raw → 7 quarantined, 10 valid → 6 updates + 4 inserts), bringing `silver_orders` to `94` rows.
* **Schema Evolution:** Ingests `orders_v2.csv` (`mergeSchema = true`), dynamically adding `sales_channel` and `discount_amount` (12 raw → 2 quarantined, 10 valid), bringing `silver_orders` to `104` rows.
* **Gold Analytics:** Builds `gold_category_sales` and `gold_customer_spend` (excluding `CANCELLED` orders).
* **Governance:** Verified via `DESCRIBE HISTORY`, Delta Time Travel (`VERSION AS OF 0` = 88 rows vs. current = 104 rows), `OPTIMIZE ... ZORDER BY`, and `VACUUM`.

### Part II: Real-Time Streaming (Kafka, Spark Structured Streaming & Apache Iceberg)
* **Ingestion:** The checked-in producer sample publishes 30 JSON events to Kafka topic `order-events`, which are ingested into `local.retail_nexus.bronze_order_events`.
* **Stream Validation & Quarantine:** Filters invalid and duplicate events, non-positive quantities/prices, invalid event types, and orphan foreign keys into `local.retail_nexus.quarantine_order_events` (16 events in the verified local run).
* **Stateful Upserts, Late Arrivals & Schema Evolution:** Maintains `local.retail_nexus.silver_order_events` (12 orders in the verified local run), handling updates, cancellations, late arrivals, and the `sales_channel` and `discount_amount` fields.
* **Gold Streaming Window & Snapshots:** Computes 10-minute event-time category revenue in `local.retail_nexus.gold_streaming_sales_window` (8 rows in the verified local run) and records Iceberg snapshots.

---

## Repository Structure

```text
├── part1-batch-databricks/
│   └── 01_bronze_ingestion.scala
├── part2-streaming-iceberg/
│   ├── build.sbt
│   ├── ProjectNexusStreamingApp.scala
│   └── VerifyProjectNexusIceberg.scala
├── producer/
│   ├── build.sbt
│   ├── src/
│   └── data/events/
├── .github/workflows/ci.yml
├── screenshots/
│   ├── part1-databricks/
│   └── part2-iceberg/
└── README.md
```

## Automated Tests and CI

The producer tests run without Kafka and check deterministic JSONL file ordering, blank-line handling, rejection of events without a valid `order_id`, and readability of the checked-in sample events. GitHub Actions runs those tests, assembles the producer, and compiles and packages the Spark streaming application on pushes and pull requests to `main`.

Run the producer tests locally:

```powershell
cd producer
sbt test
```

The workflow is defined in `.github/workflows/ci.yml`. The streaming build is compiled and packaged in CI; the workflow does not start Kafka or run a Kubernetes integration test.

## Kafka Event Producer

The `producer/` module publishes JSON order events to the `order-events` Kafka topic.

### Local prerequisites

- Java 17
- sbt
- A Kafka broker reachable at `localhost:9092`
- Docker optional for the containerized producer

### Run locally

```bash
cd producer
sbt run --once
```

Configuration is environment-based:

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `order-events` | Destination topic |
| `PRODUCER_INTERVAL_MS` | `500` | Delay between events |
| `EVENT_FILE_PATH` | `data/events` or `/app/data/events` | JSONL file or directory |

Examples:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
KAFKA_TOPIC=order-events \
PRODUCER_INTERVAL_MS=100 \
sbt run --once

sbt run --continuous
```

The `--once` argument publishes every event once. `--continuous` publishes the event set repeatedly until stopped.

### Run the producer as a Kubernetes Job

The repository includes a one-pass Job manifest for the existing Docker Desktop cluster. It expects Kafka to be reachable at `kafka:9092` in namespace `streaming`, as in the current local cluster setup.

```powershell
cd producer
sbt assembly
docker build -t nexus-kafka-producer:1.0 .
kubectl create namespace streaming --dry-run=client -o yaml | kubectl apply -f -
kubectl exec -n streaming deployment/kafka -- /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:9092 --create --if-not-exists `
  --topic order-events --partitions 3 --replication-factor 1
kubectl apply -f k8s/producer-job.yaml
kubectl wait -n streaming --for=condition=complete job/nexus-order-event-producer --timeout=180s
kubectl logs -n streaming job/nexus-order-event-producer
```

The Job sends each sample record once and exits. Remove the completed Job before rerunning it:

```powershell
kubectl delete job nexus-order-event-producer -n streaming
```

### Sample event files

The files under `producer/data/events/` include:

- Valid created, updated and cancelled events
- Duplicate event IDs
- Invalid quantities and prices
- Invalid event types
- Orphan customer and product IDs
- Negative discounts and invalid sales channels
- Late or future-skewed events
- Schema-evolution fields

### Container build

Run Docker from the `producer/` directory:

```bash
cd producer
sbt assembly
docker build -t nexus-kafka-producer:1.0 .
docker run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092 \
  -e KAFKA_TOPIC=order-events \
  -e EVENT_FILE_PATH=/app/data/events \
  nexus-kafka-producer:1.0
```

### Kafka verification

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic order-events \
  --from-beginning
```

### Run Spark streaming on local Kubernetes

The local Kubernetes deployment runs Spark 3.5.9 in a single pod using local mode. It connects to the Kafka Service `kafka:9092`, mounts the reference CSV files from `nexus-reference-data`, and stores Iceberg tables and streaming checkpoints on the `nexus-stream-state` PVC.

The local example uses the Docker Desktop `hostpath` storage class. This is suitable for a local demonstration, not a cloud durability guarantee. Do not delete the PVC before saving the table and checkpoint evidence. The local Kafka broker has PLAINTEXT authentication disabled, so this demo has no Kafka credentials to store in a Secret.

First, make sure the reference-data ConfigMap exists. The following PowerShell command creates or updates it from the local sample files:

```powershell
kubectl create configmap nexus-reference-data -n streaming `
  --from-file=products.csv="C:\kafkastreamingicebergdemo\nexus-data\products.csv" `
  --from-file=customers.csv="C:\kafkastreamingicebergdemo\nexus-data\customers.csv" `
  --dry-run=client -o yaml | kubectl apply -f -
```

Build the Scala app and Docker image, then apply the PVC and configuration before the Deployment:

```powershell
cd part2-streaming-iceberg
sbt clean
sbt package
docker build -t nexus-streaming:1.0 .
kubectl apply -f .\k8s\streaming-state-pvc.yaml
kubectl apply -f .\k8s\streaming-config.yaml
kubectl wait -n streaming --for=jsonpath='{.status.phase}'=Bound pvc/nexus-stream-state --timeout=120s
kubectl apply -f .\k8s\streaming-deployment.yaml
kubectl rollout status deployment/nexus-streaming -n streaming --timeout=8m
```

The image uses Java 17 and the Spark 3.5.9 distribution. The pod resolves the Spark Kafka connector and Iceberg runtime through Maven Central at startup; it therefore needs internet access on first launch. Spark's Ivy cache is stored at `/lakehouse/ivy2` on the persistent volume, so dependencies remain available after pod restarts. Kubernetes probes check that the Spark process is alive and that its streaming query has started. Resource requests and limits are defined in the Deployment manifest.

Check the pod and follow the processing logs:

```powershell
kubectl get deployments,pods,pvc -n streaming
kubectl logs -n streaming deployment/nexus-streaming --tail=100
kubectl logs -n streaming deployment/nexus-streaming -f
```

For the checked-in 30-event producer sample, the expected Iceberg counts after processing are 30 bronze events, 12 silver orders, 16 quarantined events, and 8 gold windows. Query them from the running pod:

```powershell
kubectl exec -n streaming deployment/nexus-streaming -- /opt/spark/bin/spark-sql --master 'local[1]' --driver-memory 512m --conf spark.jars.ivy=/lakehouse/ivy2 --conf spark.sql.catalogImplementation=in-memory --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.local.type=hadoop --conf spark.sql.catalog.local.warehouse=file:///lakehouse/warehouse --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.12.0 -e "SELECT 'bronze' AS dataset, count(*) AS rows FROM local.retail_nexus.bronze_order_events UNION ALL SELECT 'silver', count(*) FROM local.retail_nexus.silver_order_events UNION ALL SELECT 'quarantine', count(*) FROM local.retail_nexus.quarantine_order_events UNION ALL SELECT 'gold_windows', count(*) FROM local.retail_nexus.gold_streaming_sales_window UNION ALL SELECT 'silver_snapshots', count(*) FROM local.retail_nexus.silver_order_events.snapshots"
```

Capture the output of the count query, the Deployment/pod status, and the processing logs as evidence. The first run reads the existing `order-events` topic from the earliest offsets; do not rerun the producer unless you intend to add another test batch.

To demonstrate checkpoint recovery after capturing the initial results, delete the streaming pod and wait for its Deployment to replace it:

```powershell
kubectl delete pod -n streaming -l app=nexus-streaming
kubectl rollout status deployment/nexus-streaming -n streaming --timeout=8m
kubectl get pods,pvc -n streaming
kubectl logs -n streaming deployment/nexus-streaming --tail=100
```

The PVC must remain bound: it contains the Spark checkpoint and Iceberg warehouse. After recovery, rerun the count query and compare the results with the captured pre-restart counts.

To demonstrate a temporary runtime configuration change, rollout, and rollback, override the shuffle partition count in the Deployment, verify it in the new pod, then roll back to the manifest-backed ConfigMap value:

```powershell
kubectl rollout history deployment/nexus-streaming -n streaming
kubectl set env deployment/nexus-streaming -n streaming SPARK_SHUFFLE_PARTITIONS=2
kubectl rollout status deployment/nexus-streaming -n streaming --timeout=8m
kubectl exec -n streaming deployment/nexus-streaming -- printenv SPARK_SHUFFLE_PARTITIONS
kubectl rollout history deployment/nexus-streaming -n streaming
kubectl rollout undo deployment/nexus-streaming -n streaming
kubectl rollout status deployment/nexus-streaming -n streaming --timeout=8m
kubectl apply -f part2-streaming-iceberg\k8s\streaming-deployment.yaml
kubectl exec -n streaming deployment/nexus-streaming -- printenv SPARK_SHUFFLE_PARTITIONS
```

The first printed value should be `2`; after rollback it should be `4`, inherited from `nexus-streaming-config`. Capture both values and the rollout history/status as evidence. The local broker uses no authentication, so no credentials are needed in this deployment. If authentication is added later, store credentials in a Kubernetes Secret and reference it through the pod environment or a mounted file; never commit secret values to Git.