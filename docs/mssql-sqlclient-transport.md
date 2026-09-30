# MSSQL SqlClient bulk transport

`mssql_sqlclient` is the optional Linux x86-64 writer for the bounded
ClickHouse to SQL Server route. It streams sealed native chunks through
`Microsoft.Data.SqlClient.SqlBulkCopy`. BCP remains the default. A granted
attempt never falls back between writers.

The route retains bounded disk usage, durable journals, target-local
aggregate verification, receipt-backed atomic publication, retained custody,
and source-free recovery. It does not read business rows back through Python.

## First successful run

1. [Install and verify the companion](mssql-sqlclient-installation.md).
2. Provision the [generic MSSQL transaction catalog](state.md#generic-mssql-transaction-state).
3. Copy the [seven-day manifest](../examples/native/clickhouse-to-mssql-sqlclient.yaml)
   and bind its logical connection references through a verified runtime
   connection context.
4. Inspect the offline plan:

   ```bash
   dpone plan examples/native/clickhouse-to-mssql-sqlclient.yaml --format json
   ```

5. Run from the deployment runtime with its pinned init-fetch context:

   ```bash
   dpone run examples/native/clickhouse-to-mssql-sqlclient.yaml \
     --interval-end 2026-09-28T00:00:00Z --format json
   ```

The plan reports `live_preflight_required`; it does not claim that the package,
source catalog, target layout, permissions, or capacity were checked. The run
performs those checks before source business-row I/O.

## Choose the backend

```yaml
native_transfer:
  wire:
    mode: typed_binary
    binary_format: mssql_native
  execution:
    import_backend: mssql_sqlclient
    verification_backend: target_local
    layout_version: 2
    chunking:
      mode: bounded_stream
      checkpointing: resumable
      parallelism: 2
```

Layout v1 recomputes the canonical target digest. Layout v2 also persists the
sealed-row hash and a SQL Server mutation watermark, which makes later custody
checks bounded. An invocation never changes backend or layout during recovery.

## Focused guides

- [Install, upgrade, and remove](mssql-sqlclient-installation.md)
- [Manifest, CLI, evidence, and compatibility reference](mssql-sqlclient-reference.md)
- [Architecture and trust boundaries](mssql-sqlclient-architecture.md)
- [Diagnostics and operator decisions](mssql-sqlclient-diagnostics.md)
- [Synthetic certification and limitations](mssql-sqlclient-certification.md)
- [Source-free recovery commands](mssql-native-recovery.md)
- [ClickHouse to MSSQL route overview](source-sink/clickhouse-to-mssql.md)

Private connection coordinates, table identifiers, row values, and benchmark
inputs are outside every public artifact. Public certification uses only
deterministic synthetic fixtures.
