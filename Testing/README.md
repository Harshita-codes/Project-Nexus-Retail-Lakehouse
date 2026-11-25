# Validation and Evidence

| Check | Test or evidence |
|---|---|
| Batch validation helpers | `Data Ingestion/Batch/test-harness/src/test` |
| Local Spark batch transformation | `Data Ingestion/Batch/src/test` |
| Kafka producer fixture parsing | `Data Ingestion/Producer/src/test` |
| Streaming transformations and recovery state | `Data Ingestion/Streaming/src/test` |
| Databricks batch results | [Batch screenshots](evidence/batch/screenshots/) |
| Streaming and Iceberg results | [Streaming screenshots](evidence/streaming/screenshots/) |
| Hosted checks | [CI workflow](../.github/workflows/ci.yml) |

Run the portable Scala suites from the repository root:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\Testing\validate-project.ps1
```

The script prints an explicit warning and skips the local batch Spark integration suite on Windows when `HADOOP_HOME\bin\winutils.exe` is absent. CI runs that suite in Linux and builds the batch image. The local script does not deploy to Kubernetes or claim live-cluster evidence.
