# Configuration reference

This page is the central map of manifest and project configuration sections.
Detailed algorithms, operations, and recovery stay in the linked subsystem
guides.

## Top-level sections

| Section | Required | Description |
| --- | --- | --- |
| `name` | yes | Stable process name used by planning, execution, run identity, and operator output. |
| `source` | yes | Source connector, credentials, source object, and extraction options. |
| `sink` | yes | Sink connector, credentials, target object, load strategy, and sink options. |
| `runtime` | no | Runtime storage, work directory, checkpoint, evidence, and native transfer storage contract. |
| `state` | no | Explicit state backend for XMin, CDC, Kafka offsets, run state, and target-atomic MSSQL transaction receipts. Every MSSQL sink route requires it. |
| `quality` | no | Data quality gates, acceptance metrics, and fail/warn behavior. |
| `observability` | no | Run artifacts, OpenTelemetry, Prometheus, and report settings. |
| `performance` | no | Performance advisor options and runtime hints. |

## Project recipe catalog

External Airflow self-service recipes are discovered from one explicitly
trusted, locally materialized catalog configured in `dpone.yaml`:

```yaml
schema: dpone.project.v1
authoring:
  primary_source_policy: one_per_pipeline
  recipe_catalog:
    path: platform/recipes/catalog.yaml
    trusted_catalog_ids:
      - data-platform
```

`path` is a project-relative POSIX YAML path. Traversal, backslashes, symlinks,
and files above 1 MiB are rejected. `trusted_catalog_ids` is a non-empty
allowlist; a catalog id outside it is a security violation rather than a
warning. Catalog lookup is used for scaffold discovery only. Ordinary check,
preview, pack, and runtime paths use the exact artifact path/digest closure
pinned into the primary source. See
[Recipes, profiles, and components](../airflow-recipes.md).

## Project authoring layout

`dpone.yaml` selects one repository layout. Omitted `layout` means the
backward-compatible flat layout.

```yaml
schema: dpone.project.v1
layout:
  mode: domain_first
  root: workloads
  pipeline_id_scope: project
```

`root` is a confined project-relative POSIX path. Absolute paths, traversal,
backslashes, NUL bytes, and symlink escapes are rejected. Domain-first
discovery reads only the exact-depth paths:

```text
workloads/<domain>/ownership.yaml
workloads/<domain>/pipelines/<pipeline-id>/pipeline.yaml
```

Ownership uses `dpone.domain-ownership.v1`:

```yaml
schema: dpone.domain-ownership.v1
domain: crm
owner:
  team: data-crm
  contact: crm@example.com
approvers:
  github_team: data-platform
```

Pipeline ids are project-wide in v1. Each primary source may persist the
effective Airflow policy as `metadata.airflow: true | false`; omitted means
enabled for compatibility. A disabled workload remains statically valid but
does not produce a DAG or preview artifacts.

A generated `dpone.workload-index.v1` contains layout root/scope, ownership
fingerprint, compiled source and semantic fingerprints, logical connection
refs, dependency digests, durable Airflow participation, and a composite
workload fingerprint. Runtime validation recomputes each workload fingerprint
and the project fingerprint before change impact. It is not an authoring file
and must not be committed. See
[Domain-first Airflow project](../getting-started/domain-first-airflow.md).

The public producers write one UTF-8 JSON document to stdout:

```bash
dpone workload index
dpone workload impact \
  --baseline workload-index.json \
  --current .dpone-ci/candidate/workload-index.json
dpone workload promote --help
```

`index` exits `0` only for a complete valid discovery snapshot. `impact`
accepts project-confined, bounded, closed `dpone.workload-index.v1` baseline
and candidate files and exits non-zero for invalid input. Candidate mode emits
raw content digests, semantic fingerprints, `current_source: candidate`, and
`discovery_status: not_run`. Neither read-only command writes a repository
file. `promote` is the only supported baseline mutation: it captures the
approved candidate under the project lock and uses create-only bootstrap or
digest compare-and-swap. Do not redirect directly over the last valid baseline
because the shell truncates it before command validation. Keep
the accepted base/release artifact separate and write the current checkout to
a candidate path:

```bash
mkdir -p .dpone-ci/candidate
tmp="$(mktemp .dpone-ci/candidate/workload-index.XXXXXX)"
dpone workload index > "$tmp" && mv "$tmp" .dpone-ci/candidate/workload-index.json
```

On operational failure, stdout contains a structured error envelope; argument
parsing uses exit `2` and stderr. Run
`workload impact --baseline <accepted> --current <candidate>` before protected
policy invokes `workload promote` with the report's exact digests. Never use
`mv` to activate the baseline. Omitting `--current` is a backward-compatible
interactive rediscovery mode, not the atomic CI handoff.

## Project workload selectors

An optional project-root `selectors.yaml` stores reusable build-plane workload
scopes under `dpone.selectors.v1`:

```yaml
schema: dpone.selectors.v1
selectors:
  finance_active:
    select: [tag:finance+]
    exclude: [tag:deprecated]
```

The public schema is
[`src/dpone/schema/selectors.schema.json`](manifest-schemas.md). The file is
loaded only by project CLI/build operations, is limited to 100 definitions and
100 expressions per definition, and cannot contain executable templates. See
[Workload selectors](../airflow-self-service-selectors.md) for grammar, state,
limits, and command behavior.

## Hermetic pipeline tests

`tests/*.test.yaml` uses the public `dpone.test.v1` schema. It references one
primary pipeline source plus bounded project-confined JSONL fixtures:

```yaml
kind: dpone.test.v1
pipeline: ../pipelines/orders_daily/pipeline.yaml
input:
  fixture: fixtures/orders_daily.input.jsonl
expect:
  rows: 2
  rejected_rows: 0
limits:
  max_bytes: 10485760
  max_rows: 10000
  max_line_bytes: 1048576
  timeout_seconds: 30
```

Limits may only tighten the published hard caps. `full_refresh`,
`incremental_append`, and deterministic `incremental_merge` are the only v1
execution modes. See [Hermetic pipeline tests](../testing/hermetic-pipeline-tests.md).

Domain-first projects colocate the same public test contract with the pipeline:

```text
workloads/crm/pipelines/orders_daily/tests/pipeline.test.yaml
workloads/crm/pipelines/orders_daily/tests/fixtures/input.jsonl
```

`dpone test orders_daily` and `dpone test .` resolve these files through the
canonical project discovery snapshot. Flat projects keep the top-level
`tests/` layout.

## Source configuration

```yaml
source:
  type: postgres
  connection_id: postgres_oltp
  connection_type: env
  table:
    schema: public
    name: orders
  options:
    incremental_column: updated_at
    batch_size: 50000
```

For a PostgreSQL source with an MSSQL sink, do not use
`incremental_column`/`incremental_strategy: column`. Universal validation and
runtime both reject this route because the legacy target-derived
`MAX(column) + >` cursor can skip equal-precision concurrent commits and late
older rows. Use `incremental_strategy: xmin`; add key-snapshot reconciliation
when physical-delete semantics are required.

For a ClickHouse source with an MSSQL sink, `incremental_append` is also
rejected. Its target-derived single-column `MAX + >` cursor cannot represent
equal-precision, out-of-order, late older, `NULL`, or target-rounded values.
Use a complete `full_refresh` or a complete bounded
`replace`/`partition_replace`/`backfill` window. `lookback_days` and
`unique_key` alone are not an atomic cursor contract.

Source types include `postgres`, `mssql`, `clickhouse`, `api`, and `kafka`.

## Runtime storage

```yaml
runtime:
  storage:
    profile: mounted_volume
    work_dir: /mnt/dpone-work
    evidence_dir: .dpone/runs
    checkpoint_dir: .dpone/state
    debug_dir: .dpone/debug
    min_free_bytes: 1GiB
```

Use `runtime.storage.work_dir` for native transfer temporary files instead of
implicit `/tmp`. Read [Adaptive native transfer](../native-transfer-industrial-runtime.md)
for bytes-aware slicing, cleanup, source-impact warnings, and KPO storage notes.

Native transfer transport is configured under source options:

```yaml
source:
  options:
    native_transfer:
      wire:
        mode: typed_raw
        delimiter_profile: ascii_control
      execution:
        transport:
          mode: auto
          prefer_streaming: true
          fallback_to_file: true
        certification:
          mode: auto
          artifact: .dpone/certification/native_transfer_route_certification.json
```

`auto` chooses zero-file stream only when the source, sink, and codec are
stream-safe; otherwise it keeps the pipelined file/object-backed path and
reports the fallback reason in `dpone plan`. Certification `auto` is advisory
for local/dev planning and fail-closed for release/production runner policy.
Use `certified_only` when a production route must stop before source I/O unless
the selected transport has route certification evidence.

`native_transfer.wire` controls the payload shape. For MSSQL -> ClickHouse,
`mode: typed_binary` with `source_native_format: bcp_native`,
`binary_format: native`, and
`sink.options.clickhouse_bulk.ingest_contract: typed_binary_staging` uses SQL
Server `bcp queryout -n` plus dpone's native decoder before ClickHouse
Native columnar staging. Add `acceleration.mode: auto` to use the optional
`dpone[accel]` provider when it is certified, `off` for Python reference
debugging, or `required` to fail before source I/O when the provider is not
available. `binary_format: rowbinary` keeps the v0.26
RowBinary route for compatibility. `source_native_format: odbc_row_stream`
keeps the safe v0.25 fallback when BCP native is not certified for a schema. Use
`mode: source_encoded` only when you intentionally need the legacy direct TSV
behavior.

`native_transfer.snapshot` controls adaptive bulk snapshot execution:

```yaml
source:
  options:
    native_transfer:
      snapshot:
        execution:
          mode: auto
          profile: balanced
          max_parallel_exports: 4
          max_parallel_loads: 2
          adaptive_parallelism: true
        source_governor:
          enabled: true
          max_source_cpu_pct: 60
          max_query_seconds: 3600
          backoff_policy: adaptive
        target_governor:
          enabled: true
          max_inflight_blocks: 4
          max_parts_per_partition: 50
          merge_pressure_policy: throttle
        tuning:
          packet_size: auto
          block_rows: auto
          block_bytes: auto
          compression: auto
          presort: auto
        streaming:
          mode: off
          provider: bcp_pipe
          pipe_mode: fifo
          read_buffer_bytes: 4MiB
          cleanup_policy: eager
          delimiter_safety: certified_only
        scan:
          mode: auto
          heap_policy: single_scan_chunks
          require_index_for_range: true
        physical_chunking:
          mode: auto
          target_chunk_bytes: 64MiB
          max_chunk_bytes: 128MiB
          spool_mode: file
          row_boundary: required
          cleanup_policy: eager
        export_optimizer:
          mode: auto
          candidates: auto
          probe_rows: 100000
          max_probe_seconds: 30
          min_speedup_pct: 15
          bcp_probe_packets: [16384, 32768, 65535]
          odbc_fetch_size: 50000
          cache_policy: route_schema_hash
          rebenchmark_policy: schema_or_source_shape_change
          source_impact_policy: conservative
        materialization:
          mode: auto
          provider: auto
          allow_source_writes: false
          work_connection_ref: null
          work_database: null
          work_schema: null
          table_prefix: __dpone_snapshot_
          ttl_hours: 24
          cleanup_policy: eager
          reuse_policy: never
          min_speedup_pct: 25
          max_materialization_seconds: 3600
          max_work_table_bytes: null
          isolation: inherited
          index:
            mode: auto
            boundary_columns: auto
          update_statistics: auto
    partitioning:
      strategy: stats
      column: id
      target_rows_per_partition: 200000
      max_partitions: 16
      planner:
        mode: statistics
        stats_source: auto
        boundary_type: auto
        bounds_role: filter
        temporal_granularity: auto
        skew_policy: split_hot_ranges
        max_hot_partition_factor: 2.0
        min_partition_rows: 50000
        null_bucket: separate
        low_confidence_policy: conservative
```

For `streaming.provider: bcp_pipe`, `read_buffer_bytes` is the per-read FIFO
buffer (`4MiB` default, `64KiB..16MiB` valid range). It is independent of
`physical_chunking.target_chunk_bytes` and the generic
`execution.transport.stream_buffer_bytes`. String values use whole `KiB` or
`MiB` units; integer values are bytes. The deprecated streaming-only field
`target_chunk_bytes` remains accepted for one dedicated deprecation release;
its value does not alter the historical effective 4 MiB read buffer.

For physical chunking, `target_chunk_bytes` is a soft row-boundary target and
`max_chunk_bytes` is a hard per-file limit. Values must be positive and target
must not exceed max. Numeric values are integer bytes; malformed string values
are rejected rather than normalized. JSON Schema validates decimal-unit string
syntax; the runtime policy parser then rejects values that resolve below one
whole byte before BCP starts. A single encoded row over max fails rather than
being split.

The same contracts are available to Python integrations:

```python
from dpone.runtime.physical_chunking import (
    PhysicalChunkLimitExceeded,
    PhysicalChunkPolicy,
)
from dpone.runtime.streaming_transfer import StreamingTransferPolicy

physical = PhysicalChunkPolicy(
    target_chunk_bytes=64 * 1024 * 1024,
    max_chunk_bytes=128 * 1024 * 1024,
)
streaming = StreamingTransferPolicy(read_buffer_bytes=4 * 1024 * 1024)

try:
    raise PhysicalChunkLimitExceeded(
        chunk_index=3,
        row_bytes=129 * 1024 * 1024,
        max_chunk_bytes=physical.max_chunk_bytes,
    )
except PhysicalChunkLimitExceeded as error:
    safe_metadata = {
        "chunk_index": error.chunk_index,
        "row_bytes": error.row_bytes,
        "max_chunk_bytes": error.max_chunk_bytes,
    }

# Direct-constructor compatibility remains for one deprecation release. The
# deprecated value does not change the effective 4 MiB read buffer.
legacy_constructor = StreamingTransferPolicy(target_chunk_bytes=512 * 1024 * 1024)
assert legacy_constructor.read_buffer_bytes == 4 * 1024 * 1024

# Manifest parsing records the public deprecated path for decision evidence.
legacy_manifest = StreamingTransferPolicy.from_options(
    {
        "native_transfer": {
            "snapshot": {"streaming": {"target_chunk_bytes": "512MiB"}}
        }
    }
)
assert legacy_manifest.deprecated_aliases == (
    "source.options.native_transfer.snapshot.streaming.target_chunk_bytes",
)
```

Invalid physical limits and invalid canonical `read_buffer_bytes` values raise
`ValueError` with the stable configuration code. The deprecated direct Python
constructor field is compatibility-only; manifest validation and deprecation
metadata use `from_options()`. `PhysicalChunkLimitExceeded` contains only the
safe numeric metadata shown above and never stores row contents.

`partitioning.planner.boundary_type` may be `auto`, `numeric`, `date`,
`datetime`, `datetime2`, `datetimeoffset`, or `rowversion`. `bounds_role:
filter` treats manual bounds as a strict dpone extraction window; `bounds_role:
stride` follows Spark JDBC semantics where lower/upper bounds determine
partition stride and edge partitions cover rows outside the stride window.

For ClickHouse Native-interface ingest:

```yaml
sink:
  options:
    clickhouse_bulk:
      mode: native_tcp
      native_tcp:
        enabled: true
        backend: auto
        compression: auto
        host: null
        port: 9000
        secure: false
        connection_pool_size: 2
        query_timeout_seconds: 3600
      ingest_contract: typed_binary_staging
```

`dpone plan` writes `native_transfer_snapshot_optimization` evidence with the
selected backend, `native_tcp_backend`, compression, effective parallelism,
fallback chain, partition planner, statistics confidence, blockers, warnings,
and governor throttling reasons. `backend: auto` prefers a certified direct
provider and otherwise uses the client wrapper with an explicit fallback reason.
When `snapshot.scan.mode: auto` sees a heap/no-index/low-confidence route, the
plan selects `single_scan_chunks` and writes `native_transfer_source_scan`
evidence. When `snapshot.export_optimizer.mode` is enabled, the plan can also
write `native_transfer_source_export_optimizer` evidence with the selected
source provider, current default, measured speedup, source bottleneck and
rejected provider reasons.
`bcp_probe_packets` defines the packet-size mini-matrix for MSSQL BCP probes;
`odbc_fetch_size` controls bounded ODBC array-fetch probes. Both are probe
knobs only: final load still goes through the selected provider and staging
gates.
When `snapshot.materialization.mode` is enabled, `allow_source_writes: true`
is required before dpone may create an MSSQL work table. Plans and run artifacts
write `native_transfer_source_materialization` evidence with the selected
provider, work database, work schema, cleanup policy, expected speedup,
blockers, warnings and source object TTL. For promoted MSSQL workloads, prefer
`work_connection_ref`: the verified environment registry supplies database and
schema atomically, and dpone requires the work and source connections to use the
same SQL Server endpoint. It cannot be combined with `work_database` or
`work_schema`. For legacy physical configuration, `work_database: null`
preserves the current connection database. An explicit value addresses all
snapshot DDL, catalog sweep, export reads and cleanup with a three-part name on
the same SQL Server instance; it does not change the authored source database.
For MSSQL `mssql_work_table`, `snapshot.materialization.cleanup` controls
bounded source cleanup:

```yaml
cleanup:
  lock_timeout_ms: 5000
  retry_attempts: 2
  retry_backoff_ms: 250
  defer_on_lock_timeout: true
```

dpone never waits indefinitely on source metadata locks. If all bounded retries
still hit SQL Server lock timeout, cleanup is deferred with explicit evidence,
and the next materialization run sweeps expired dpone-owned work tables under
the same safe `table_prefix`.
Forced `range_partitioned` with `require_index_for_range: true`
blocks unsafe heap range scans before source I/O.

## Sink configuration

```yaml
sink:
  type: mssql
  connection_id: mssql_dwh
  connection_type: env
  table:
    schema: landing
    name: orders
  strategy:
    mode: incremental_merge
    unique_key: order_id
```

Sink types include `postgres`, `mssql`, `clickhouse`, `bigquery`, and `kafka`.

## Strategy configuration

| Mode | Common required fields | Read more |
| --- | --- | --- |
| `full_refresh` | none | [Load strategies](../load-strategies.md) |
| `incremental_append` | source cursor or bounded source | [Load strategies](../load-strategies.md) |
| `incremental_merge` | `unique_key` | [Load strategies](../load-strategies.md) |
| `replace` | `custom_predicate` or replacement scope | [Load strategies](../load-strategies.md) |
| `partition_replace` | `partition.column` and complete partition slice | [Load strategies](../load-strategies.md) |
| `snapshot_diff` | `unique_key` and row hash comparison | [Load strategies](../load-strategies.md) |
| `scd2` | `unique_key` and SCD2 validity columns | [Load strategies](../load-strategies.md) |
| `cdc_apply` | normalized insert/update/delete events | [Reconciliation and CDC](../cdc.md) |
| `backfill` | chunk definition and resumable state | [Load strategies](../load-strategies.md) |
| `xmin` | PostgreSQL source | [Postgres XMin](../postgres-xmin.md) |
| `cdc` | CDC-enabled source | [Reconciliation and CDC](../cdc.md) |

### Portable PostgreSQL → MSSQL replace scope

For a cross-dialect `replace`, author a SQL-free `portable_scope` instead of
copying one raw predicate into two databases:

```yaml
sink:
  type: mssql
  strategy:
    mode: replace
    portable_scope:
      version: 1
      kind: equality
      column: partition_id
      value:
        type: integer
        value: 1
```

`column` is one exact catalog identifier, not a dotted path. Unicode, dots,
spaces, and punctuation are preserved and quoted as one identifier. Empty,
NUL-containing, unknown, case-ambiguous, renamed, or PostgreSQL-overlength
identifiers fail before state admission and source row access.

Equality supports typed `integer`, `decimal`, `boolean`, `date`, `timestamp`,
`timestamptz`, and `text` literals. Range scopes use `lower`/`upper` bounds with
an optional `inclusive` boolean and do not support text ranges. Both catalogs
must prove exact range, precision, scale, temporal precision, and comparison
semantics. Text equality additionally requires certified PostgreSQL binary and
SQL Server `BIN2` collations; both renderers force binary equality. Values are
DB-API parameters and never become SQL text. Route and operation identity hash
the canonical AST plus its catalog binding, never rendered PostgreSQL or
SQL Server SQL.

Raw `custom_predicate` remains a same-dialect legacy contract. A raw
PostgreSQL predicate on a PostgreSQL → MSSQL replace remains fail-closed.

## Credentials

```yaml
connection_type: env
```

Supported providers are `env`, `params`, `airflow`, and `vault`. Start with [Credentials quickstart](../getting-started/credentials-quickstart.md), then use [Connections and credentials](../connections.md) for all fields and runbooks.

## Schema evolution

```yaml
sink:
  options:
    schema_evolution:
      enabled: true
      mode: widening
      on_breaking: fail
      on_type_change: fail
```

Read [Schema evolution](../schema-evolution.md) before enabling generated new-column mode for incompatible type changes.

## Type inference

```yaml
sink:
  options:
    type_inference:
      enabled: true
      prefer_source_metadata: true
      sample_rows: 10000
      empty_string_is_null: false
      conflict_policy: fail
```

Read [Type inference](../type-inference.md) for precedence, confidence, source
metadata, sampled rows, and empty string vs `NULL` behavior.

## Schema contracts

```yaml
schema_contract:
  enforcement: strict
  columns:
    amount:
      type: decimal
      precision: 18
      scale: 4
      nullable: false
```

Read [Schema contracts](../schema-contracts.md) for explicit logical column
contracts, enforcement modes, and generated variant columns.

## Physical design

```yaml
sink:
  options:
    physical_design:
      apply: online
      indexes:
        primary_key: [order_id]
      storage:
        clickhouse:
          low_cardinality:
            mode: auto
```

Read [Physical design](../physical-design.md) for target DDL controls:
partitioning, indexes, storage, compression, ClickHouse `LowCardinality`, and
BigQuery clustering.

ClickHouse storage also supports cluster-aware DDL:

```yaml
sink:
  options:
    physical_design:
      storage:
        clickhouse:
          engine: "ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')"
          cluster: dwh
          order_by: [event_date, account_id, event_id]
          access_table:
            name: events_all
            engine: Distributed
            sharding_key: cityHash64(account_id)
```

`cluster: dwh` renders `CREATE DATABASE/TABLE ... ON CLUSTER dwh`.
Use `cluster: {name: dwh, ddl_scope: local}` only when DDL should stay local but
the cluster name is still needed for a `Distributed` facade. Legacy
`cluster.on_cluster` remains accepted for older manifests.
`access_table` renders an optional `Distributed` facade in physical DDL plans;
omit it for one-shard replicated clusters.

## Load lineage

```yaml
sink:
  options:
    lineage:
      enabled: true
      preset: standard
      features:
        quarantine: true
```

Read [Load lineage](../load-lineage.md) for canonical `__dpone__*` columns,
`run_id`, `load_id`, row identity, audit lifecycle, and migration guidance for
legacy `meta__*` columns.

For bulk/native routes prefer `bulk_standard` and let the sink project lineage
before finalization:

```yaml
sink:
  options:
    lineage:
      enabled: true
      preset: bulk_standard
      row_identity:
        mode: auto
        unsupported_policy: warn
    load_governance:
      enabled: true
      finalization_phase: pre_finalize
      decision_audit:
        enabled: true
        persist: true
        log_level: warning_on_fallback
        include_successful_decisions: true
      audit:
        mode: standard
        state_schema: etl_state
        loads_table: __dpone__loads
        steps_table: __dpone__load_steps
        clickhouse:
          engine: auto
        resource_metrics: true
        step_metrics: true
        query_log_correlation: true
      cleanup:
        staging_policy: eager
```

`pre_finalize` runs quality gates against projected staging before target
mutation. ClickHouse audit storage is enabled by default for governed loads and
writes `etl_state.__dpone__loads` and `etl_state.__dpone__load_steps` through
the OSS runtime in CLI, Python API, Airflow pack/KPO, Docker and local runs.
For ClickHouse clusters, `load_governance.audit.clickhouse.engine: auto` uses
replicated audit/state tables with the same `physical_design.storage.clickhouse.cluster`
contract as managed runtime artifacts.
`decision_audit` publishes auto/fallback selections to structured logs,
`dpone run --format json`, and `__dpone__load_steps` rows with
`kind: runtime_decision`. Read [Runtime decision audit](../runtime-decision-audit.md)
for reason codes, fallback rules, redaction and troubleshooting.
Set `load_governance.audit.mode: off` or `enabled: false` only when the run
must opt out explicitly.

## Quality gates

Canonical runtime authoring is `quality.gates`. The compatibility dialect
`quality.checks` (with top-level `mode: fail|warn`) is normalized into the same
gate engine; mapped checks today are `min_rows` and `source_target_count`.
Empty loads with `min_rows.threshold >= 1` and `mode: fail` fail the run.

```yaml
quality:
  gates:
    - id: row_count_reconciliation
      type: row_count_reconciliation
      severity: error
      tolerance:
        mode: absolute
        value: 0
    - id: target_min_rows
      type: min_rows
      side: target
      threshold: 1
      severity: error
    - id: typed_hash_sample
      type: typed_hash_reconciliation
      mode: sample
      severity: warning
  # Compatibility alternative (do not combine leftover checks with gates under mode=fail):
  # mode: fail
  # checks:
  #   - type: min_rows
  #     threshold: 1
  #   - type: source_target_count
  #     tolerance_pct: 0
  acceptance:
    enabled: true
    mode: warn_only
    capture:
      source: true
      staged: true
      target: true
    checks:
      row_count: true
      null_counts: all_columns
      distinct_counts: [business_key, status]
```

Read [Source preparation and load governance](../load-governance.md) for
transfer lifecycle gates, including pre-cleanup source/staged/target acceptance
metric snapshots, and [Quality metrics](../quality-metrics.md) for metrics
artifacts.

## Source preparation hooks

```yaml
source:
  options:
    hooks:
      pre_hook:
        - id: refresh_source_mart
          kind: source_refresh
          type: sql
          connector: source
          sql: "EXEC [schema].[p_refresh_source_mart]"
          # For production SQL bodies prefer:
          # sql_file: ../../sql/mssql/refresh_source_mart.sql
          autocommit: true
          mutates_source: true
          execution:
            cli: inline
            airflow: separate_task
      post_hook: []
```

`sql` and `sql_file` are mutually exclusive. `sql_file` is manifest-relative,
repository-bounded, hashed into hook evidence, included in GitOps compact pack
dependencies, and validated by the same mutating-SQL guard as inline SQL. Read
[Source preparation and load governance](../load-governance.md) for the full
hook taxonomy, dependency graph, lineage and Airflow rendering semantics.

## SQL file query source

Use `source.options.query.sql_file` when a workload is a server-side SELECT
transform rather than a table export:

```yaml
source:
  type: clickhouse
  options:
    query:
      mode: sql_file
      sql_file: ../../sql/clickhouse/wide_mart_crm_select.sql
      execution:
        mode: server_side
      render:
        engine: jinja
        context:
          sources:
            mes_task: Example_Datamarts.example__ch__activity_fact
      readonly: true
```

`dpone` resolves the SQL file lexically relative to the manifest, blocks
repository path escape and symlinks, validates that the rendered query is
SELECT-only, and includes the file hash in `airflow-pack.json`. See
[SQL file transform workloads](../sql-file-workloads.md).
