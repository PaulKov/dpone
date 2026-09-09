# Config alias migration

`dpone` keeps legacy flat option aliases for one compatibility window, but new manifests should use nested namespaces. Nested config keeps shared concepts reusable across integrations and prevents one-off fields such as per-tool `*_batch_size` knobs from spreading through the framework.

## Transfer partitioning

| Legacy alias | Canonical path |
| --- | --- |
| `source.options.partition_column` | `source.options.partitioning.column` |
| `source.options.partition_workers` | `source.options.partitioning.export_workers` |
| `source.options.parallel_load_workers` | `source.options.partitioning.load_workers` |
| `source.options.partition_load_workers` | `source.options.partitioning.load_workers` |
| `source.options.lower_bound` | `source.options.partitioning.bounds.lower` |
| `source.options.upper_bound` | `source.options.partitioning.bounds.upper` |

## MSSQL bulk loading

| Legacy alias | Canonical path |
| --- | --- |
| `sink.options.bulk_mode` | `sink.options.bulk.mode` |
| `sink.options.bcp_path` | `sink.options.bulk.bcp.bcp_path` |
| `sink.options.bcp_batch_size` | `sink.options.bulk.bcp.batch_size` |
| `sink.options.bcp_packet_size` | `sink.options.bulk.bcp.packet_size` |
| `sink.options.bcp_timeout_seconds` | `sink.options.bulk.bcp.timeout_seconds` |
| `sink.options.bcp_error_file` | `sink.options.bulk.bcp.error_file` |
| `sink.options.bcp_table_lock` | `sink.options.bulk.bcp.table_lock` |
| `sink.options.bcp_keep_nulls` | `sink.options.bulk.bcp.keep_nulls` |
| `sink.options.bcp_code_page` | `sink.options.bulk.bcp.code_page` |
| `sink.options.field_terminator` | `sink.options.bulk.bcp.field_terminator` |
| `sink.options.row_terminator` | `sink.options.bulk.bcp.row_terminator` |

## ClickHouse direct ingest

| Legacy alias | Canonical path |
| --- | --- |
| `sink.options.clickhouse_bulk_mode` | `sink.options.clickhouse_bulk.mode` |
| `sink.options.clickhouse_insert_settings` | `sink.options.clickhouse_bulk.insert_settings` |
| `sink.options.clickhouse_http_host` | `sink.options.clickhouse_bulk.http.host` |
| `sink.options.clickhouse_http_port` | `sink.options.clickhouse_bulk.http.port` |
| `sink.options.clickhouse_http_database` | `sink.options.clickhouse_bulk.http.database` |
| `sink.options.clickhouse_http_user` | `sink.options.clickhouse_bulk.http.user` |
| `sink.options.clickhouse_http_password` | `sink.options.clickhouse_bulk.http.password` |
| `sink.options.clickhouse_http_secure` | `sink.options.clickhouse_bulk.http.secure` |
| `sink.options.clickhouse_http_timeout_seconds` | `sink.options.clickhouse_bulk.http.timeout_seconds` |
| `sink.options.clickhouse_http_chunk_size` | `sink.options.clickhouse_bulk.http.chunk_size` |
| `sink.options.clickhouse_client_command` | `sink.options.clickhouse_bulk.client.command` |
| `sink.options.clickhouse_client_path` | `sink.options.clickhouse_bulk.client.command` |
| `sink.options.clickhouse_client_host` | `sink.options.clickhouse_bulk.client.host` |
| `sink.options.clickhouse_client_port` | `sink.options.clickhouse_bulk.client.port` |
| `sink.options.clickhouse_client_database` | `sink.options.clickhouse_bulk.client.database` |
| `sink.options.clickhouse_client_user` | `sink.options.clickhouse_bulk.client.user` |
| `sink.options.clickhouse_client_password` | `sink.options.clickhouse_bulk.client.password` |
| `sink.options.clickhouse_client_secure` | `sink.options.clickhouse_bulk.client.secure` |
| `sink.options.clickhouse_client_timeout_seconds` | `sink.options.clickhouse_bulk.client.timeout_seconds` |
| `source.options.native_transfer.snapshot.streaming.target_chunk_bytes` | `source.options.native_transfer.snapshot.streaming.read_buffer_bytes` |

## Runbook

1. Run `dpone plan path/to/manifest.yml --format md`.
2. Check the `warnings` section for deprecated aliases.
3. Replace every legacy alias with its canonical path from this guide.
4. Re-run `dpone plan`; the warning should disappear while the selected fast path and ingest settings stay unchanged.

The BCP streaming alias is route-level compatibility rather than a generic
configuration alias. Its structured warning is published when the MSSQL pipe
route is resolved during execution. The legacy numeric value is not copied to
the canonical field: earlier releases always used a fixed 4 MiB FIFO read, so
migrate it to `read_buffer_bytes: 4MiB` unless a measured value in the supported
`64KiB..16MiB` range is intentionally required.
