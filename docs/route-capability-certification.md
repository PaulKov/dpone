# Route capability certification

`dpone certify route-capabilities` runs a route-capability certification
scenario and writes machine-readable evidence for route selection, quality
checks, load steps, and cleanup. It is meant for live or local certification of
new high-throughput routes before they become production defaults.

The first production use case is the columnar ClickHouse pull route:

```text
MSSQL snapshot
-> Parquet chunks
-> object storage
-> ClickHouse s3/s3Cluster pull
-> staging
-> lineage/DQ/audit/finalize
-> benchmark artifact
```

## CLI

```bash
dpone certify route-capabilities \
  --manifest dpone_workloads/manifests/mssql/account_sales.yaml \
  --scenario work-item_account_sales_benchmark \
  --output test_artifacts/route-capability-certification/account_sales \
  --run-id work-item-account-sales-001 \
  --format json
```

Supported scenarios:

| Scenario | Purpose | Source IO |
| --- | --- | --- |
| `preflight_only` | Validate route capability probes, object-storage access, and ClickHouse read access. | No source data read. |
| `small_live` | Run a small live certification matrix against safe certification targets. | Yes. |
| `work-item_account_sales_benchmark` | Run the work-item `account_sales` route matrix with safe target and object-prefix overrides. | Yes. |

Default benchmark routes:

- `typed_raw_streaming`;
- `typed_binary_streaming`;
- `direct_push_columnar.chunked`;
- `object_storage_pull.chunked`;
- `object_storage_pull.file`;
- `object_storage_pull_s3`;
- `object_storage_pull_s3cluster`.

## Output artifacts

The command writes these files into `--output`:

| File | Contents |
| --- | --- |
| `certification.json` | Full report: selected route, blockers, warnings, safe overrides and artifact index. |
| `certification.md` | Human-readable summary for MR comments and release notes. |
| `route_decisions.jsonl` | One redacted JSON decision per route or preflight probe. |
| `load_steps.json` | Runtime microstep timings such as source read, Parquet write, upload, ClickHouse pull, DQ, lineage, finalize and cleanup when available. |
| `quality_report.json` | DQ gate results by route. |

Secrets, connection payloads, access keys, tokens and presigned URLs are
redacted before any artifact is written.

## Quality gates

A route is selectable only when the route run is green and the quality report is
green:

- source count = target count;
- `NULL counts` match by column;
- distinct counts match for selected key/text/status columns;
- typed hash sample or full typed hash matches where safe;
- Decimal, Float and Unicode fidelity checks pass when supplied by the runner;
- target contains `__dpone__run_id`, `__dpone__load_id`,
  `__dpone__loaded_at`, `__dpone__extracted_at`;
- cleanup reports no stale staging, shadow, backup or object-prefix artifacts.

The fastest route with green DQ and green cleanup becomes `preferred_route_id`.

## Required configuration

Object-storage pull requires two independent credential paths:

```yaml
source:
  options:
    native_transfer:
      snapshot:
        columnar_fast_path:
          mode: auto
          provider: auto
          object_storage:
            uri_prefix: s3://dpone-stage/certification/{run_id}/
            runtime_access:
              connection_type: airflow
              connection_id: s3_dpone_stage_writer
            clickhouse_read_access:
              mode: named_collection
              named_collection: dpone_stage
            preflight:
              require_runtime_write: true
              require_clickhouse_read: true

sink:
  options:
    clickhouse_bulk:
      columnar_pull:
        use_cluster_function: auto
        cluster: dwh
```

`runtime_access.connection_id` is used by the dpone runtime or KPO pod to write,
read, list and delete run-scoped Parquet chunks. `named_collection: dpone_stage`
is used by the ClickHouse server to read those chunks with `s3(...)` or
`s3Cluster(...)` without embedding secrets in SQL.

## Direct columnar push

`direct_push_columnar` is the matching columnar strategy without object-storage
staging and without ClickHouse pull:

```text
MSSQL snapshot
-> local Parquet chunks
-> ClickHouse client or HTTP INSERT FORMAT Parquet
-> staging
-> lineage/DQ/audit/finalize
-> cleanup
```

Use it when object storage or ClickHouse named collections are not available yet,
or when the benchmark shows that the KPO push path is faster than server-side
pull for a specific table. This route still needs a certified Parquet writer and
`clickhouse_bulk.mode: client` or `clickhouse_bulk.mode: http`; it does not need
`runtime_access.connection_id`, `named_collection`, `s3(...)`, or `s3Cluster(...)`.
It appears in the same certification artifacts and competes with the
object-storage routes by DQ-green duration.

## Troubleshooting

| Symptom | Meaning | Action |
| --- | --- | --- |
| `s3Cluster unavailable` or `sink.cluster_pull_unsupported` | ClickHouse cannot use the cluster table function for the planned read. | Use `object_storage_pull_s3` or configure/upgrade ClickHouse cluster support. |
| `sink.auth.named_collection_missing` | ClickHouse cannot resolve the named collection. | Create the named collection or explicitly allow a non-production credential mode. |
| `format.parquet_read_unsupported` | The sink cannot read Parquet through the planned table function. | Use typed binary streaming or fix ClickHouse Parquet support/settings. |
| `source.columnar_writer_missing` | The runtime cannot produce certified Parquet chunks. | Install `dpone[columnar]` or use the best green streaming fallback. |
| `storage.runtime_put_denied` | The runtime writer credential cannot upload the sentinel object. | Fix `runtime_access.connection_id` permissions. |

## Product context

The route follows the high-throughput shape shown in the
[Supermetal SQL Server -> ClickHouse benchmark](https://supermetal.io/blog/sql-server-clickhouse-benchmark):
write columnar files to object storage and let ClickHouse pull them in parallel.
dpone adds route capability probes, GitOps-visible evidence, staging-first
finalization, lineage, DQ and cleanup audit.

ClickHouse table functions and object-storage performance guidance:

- [`s3`](https://clickhouse.com/docs/sql-reference/table-functions/s3);
- [`s3Cluster`](https://clickhouse.com/docs/sql-reference/table-functions/s3Cluster);
- [S3 performance](https://clickhouse.com/docs/integrations/s3/performance).

dlt uses filesystem and object-store staging for load packages. dpone takes that
ergonomics and adds pre-finalize governance and route decision audit:
[dlt filesystem destination](https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem).

Airbyte and Fivetran validate connections before syncs and keep full refresh
safety boundaries. dpone exposes the same readiness as OSS route evidence:
[Airbyte MSSQL](https://docs.airbyte.com/integrations/sources/mssql),
[Fivetran SQL Server](https://fivetran.com/docs/connectors/databases/sql-server).
