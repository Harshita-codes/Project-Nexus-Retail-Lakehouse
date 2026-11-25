# Docker and Kubernetes

Deployment assets are separated from application source so all workload images and Kubernetes resources are easy to find.

| Workload | Source | Image definition | Kubernetes resources |
|---|---|---|---|
| Local batch Spark app | [Batch module](../Data%20Ingestion/Batch/) | [nexus-batch.Dockerfile](Docker/nexus-batch.Dockerfile) | [Batch Job](Kubernetes/batch-job.yaml), [PVC](Kubernetes/batch-pvc.yaml), [ConfigMap](Kubernetes/batch-config.yaml) |
| Kafka event producer | [Producer module](../Data%20Ingestion/Producer/) | [nexus-producer.Dockerfile](Docker/nexus-producer.Dockerfile) | [Producer Job](Kubernetes/producer-job.yaml), [Deployment](Kubernetes/producer-deployment.yaml) |
| Spark streaming app | [Streaming module](../Data%20Ingestion/Streaming/) | [nexus-streaming.Dockerfile](Docker/nexus-streaming.Dockerfile) | [Deployment](Kubernetes/streaming-deployment.yaml), [PVC](Kubernetes/streaming-state-pvc.yaml), [ConfigMap](Kubernetes/streaming-config.yaml) |

Build the batch and streaming images from their respective module directories, using the Dockerfile paths above and each module's directory as the build context. Build the producer image from the repository root because its image includes the checked-in event fixture from `Datasets/streaming/order-events`.

The [root README](../README.md) and [demo guide](../docs/demo-guide.md) contain command sequences, namespace assumptions, and storage cautions. The local streaming PVC/checkpoint is separate from the batch output PVC; do not remove either when preserving evidence.
