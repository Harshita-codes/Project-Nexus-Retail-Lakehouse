# Runtime Logs and Evidence

The producer and Spark applications write runtime messages to standard output and errors to standard error. Inspect live Kubernetes output with `kubectl logs -n streaming deployment/nexus-streaming` or `kubectl logs -n streaming job/nexus-batch`.

The repository includes Databricks and streaming screenshots under [`Testing/evidence`](../Testing/evidence/). Separate terminal logs from the previously completed Kubernetes operations were not saved here, so no archived log files are fabricated. Follow the [demo guide](../docs/demo-guide.md) to capture output during a future run.

There is no centralized log-storage service in this local demonstration.
