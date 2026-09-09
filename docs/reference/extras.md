# Extras and dependencies

`dpone` keeps connector dependencies optional. Install only the extras required by your manifests, or use `full` for local all-in-one environments.

## Extras

| Extra | Purpose |
| --- | --- |
| `postgres` | PostgreSQL source, sink, XMin, and CDC support through `psycopg`. |
| `mssql` | SQL Server source, sink, state, and `pyodbc` support. Native fast paths also use external `bcp`. |
| `clickhouse` | ClickHouse source/sink support through `clickhouse-driver` and HTTP/native utilities. |
| `kafka` | Kafka batch source/sink support through `confluent-kafka` and Schema Registry codecs. |
| `gcp` | BigQuery and Google Cloud Storage support. |
| `s3` | AWS S3 object storage staging support through `boto3`. |
| `azure` | Azure Blob Storage staging support through `azure-storage-blob`. |
| `object_storage` | S3, GCS, and Azure Blob staging helpers. |
| `pandas` | DataFrame-based extract/load helpers. |
| `vault` | HashiCorp Vault integration through public `vault-kv-client`. |
| `kubernetes` | Metadata-only Kubernetes inventory and conditional-delete adapters for Airflow runtime Pod retention. |
| `google_ads` | Google Ads API support. |
| `accel` | Optional native-transfer acceleration provider for certified native routes. |
| `full` | All public extras above. |

## Install examples

```bash
pip install "dpone[postgres,mssql]"
pip install "dpone[clickhouse,kafka]"
pip install "dpone[gcp,vault]"
pip install "dpone[object_storage]"
pip install "dpone[kubernetes]"
pip install "dpone[accel]"
pip install "dpone[full]"
```

## Native utilities

Python extras install Python dependencies only. Some production fast paths need native binaries:

| Utility | Used by | Notes |
| --- | --- | --- |
| `bcp` | MSSQL bulk import/export | Recommended for high-volume SQL Server loads. |
| `sqlcmd` | MSSQL diagnostics/bootstrap | Useful for local integration and operations runbooks. |
| ODBC Driver 18 | MSSQL `pyodbc` connections | Default recommended SQL Server ODBC driver. |
| `clickhouse-client` | ClickHouse client fallback paths | Useful fallback for MSSQL -> ClickHouse loads when direct Native TCP is unavailable. |
| `dpone[accel]` | Native acceleration and direct ClickHouse Native TCP ingest | Recommended for certified high-throughput MSSQL -> ClickHouse snapshots. |

Object storage fast paths use Python SDKs from extras, but cloud-side access
also needs IAM/SAS/service-account permissions on the staging prefix.

## Related docs

- [Installation](../getting-started/installation.md)
- [MSSQL guide](../mssql.md)
- [ClickHouse guide](../clickhouse.md)
- [Kafka guide](../kafka.md)
- [Object storage staging](../object-storage-staging.md)
- [Connections and credentials](../connections.md)
