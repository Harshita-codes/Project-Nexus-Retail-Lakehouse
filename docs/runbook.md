# Local Docker Desktop Runbook

This runbook operates the producer and streaming app in Kubernetes namespace `streaming`. It assumes the images are built locally and the current namespace contains the Kafka Deployment and Service.

## Build and deploy

```powershell
cd producer
sbt assembly
docker build -t nexus-kafka-producer:1.0 .

cd ..\part2-streaming-iceberg
sbt test
sbt package
docker build -t nexus-streaming:1.0 .
kubectl apply -f .\k8s\streaming-config.yaml
kubectl apply -f .\k8s\streaming-state-pvc.yaml
kubectl apply -f .\k8s\streaming-deployment.yaml
kubectl rollout status deployment/nexus-streaming -n streaming --timeout=8m
```

If upgrading the existing local deployment, keep `nexus-stream-state` and the Kafka PVC. The new app copies existing Iceberg table contents into Delta paths if the Delta tables do not exist; it uses the existing checkpoint and does not require rerunning the producer.

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

Run the Iceberg verifier and Delta count output using the command in the root [README](../README.md). Preserve its output along with the pod/PVC status.

## Configuration change and rollback

```powershell
kubectl set env deployment/nexus-streaming -n streaming SPARK_SHUFFLE_PARTITIONS=2
kubectl rollout status deployment/nexus-streaming -n streaming --timeout=8m
kubectl exec -n streaming deployment/nexus-streaming -- printenv SPARK_SHUFFLE_PARTITIONS
kubectl rollout undo deployment/nexus-streaming -n streaming
kubectl rollout status deployment/nexus-streaming -n streaming --timeout=8m
kubectl apply -f .\k8s\streaming-deployment.yaml
```

The temporary value should be `2`; after rollback and reapplying the checked-in manifest, it should be `4`.

## Pod recovery

```powershell
kubectl delete pod -n streaming -l app=nexus-streaming
kubectl rollout status deployment/nexus-streaming -n streaming --timeout=8m
kubectl get pods,pvc -n streaming
kubectl logs -n streaming deployment/nexus-streaming --tail=100
```

Compare Iceberg and Delta counts with saved pre-restart output. Never delete the streaming PVC as part of this test.
