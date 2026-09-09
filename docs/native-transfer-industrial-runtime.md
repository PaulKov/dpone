# Adaptive native transfer runtime

This guide explains the production runtime contract for native, file-based
partition transfer. It is connector-neutral: MSSQL BCP, PostgreSQL `COPY`,
ClickHouse HTTP/client ingest, and future connectors share the same execution
model.

## Problem it solves

Classic native transfer materializes all partition files first, then loads them
into the target. That is good for debugging and benchmarking, but it can fill a
small worker disk before the first target load starts.

Adaptive native transfer separates correctness from local disk pressure:

- `partition` is the logical correctness and resume unit.
- `slice` is the physical export/load/cleanup unit.
- `runtime.storage` decides where temporary files, evidence, checkpoints, and
  debug artifacts live.
- `resource_policy` limits active files and active bytes.
- loaded slices are cleaned eagerly when the sink has accepted them into
  staging.

The target is finalized only after every slice is loaded into staging and all
configured gates pass.

## Range Partitions Vs Physical Chunks

`partitioning.*` describes logical source ranges. It is correct when a source
can seek each range efficiently. On SQL Server heaps, views without reliable
statistics, or columns without a useful boundary index, range partitioning can
turn one full scan into many full scans.

`native_transfer.snapshot.scan` decides the source scan shape:

```yaml
source:
  options:
    native_transfer:
      snapshot:
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
```

When `auto` sees a heap/no-index/low-confidence route, dpone selects
`single_scan_chunks`: one source export process, many small runtime-owned chunk
files, one staging session, and eager cleanup after each chunk load. Evidence is
written as `dpone.native_transfer.source_scan_decision.v1` and
`dpone.native_transfer.physical_chunks.v1`.

The physical target and maximum have different guarantees:

- `target_chunk_bytes` is a soft target. dpone seals after a complete row makes
  the current chunk reach the target.
- `max_chunk_bytes` is a hard file limit. dpone seals before adding a complete
  row that would cross it.
- Both values must be positive and `target_chunk_bytes <= max_chunk_bytes`.
- Numeric values are integer bytes; string values must use one contiguous
  positive number and a supported byte unit.
- One encoded row larger than the maximum fails with
  `physical_chunk_row_exceeds_max_bytes`; rows are never split.

On that failure, the open chunk and FIFO are removed, the BCP process is
terminated and reaped with a bounded kill fallback, and target
finalization/checkpoint advancement do not occur. The physical-chunk evidence
adds `generation_failed` with sizes and the stable code,
but never includes source row data. Successful lifecycle entries remain in the
`chunks` array; failure metadata is the optional top-level
`generation_failure` record. Cleanup tracks exact artifact-owned paths and
never globs another concurrent run's chunk files.

The MSSQL `snapshot.streaming.read_buffer_bytes` setting is separate. It bounds
one FIFO read in the zero-file `bcp_pipe` route; it is not a physical chunk size
and does not inherit `execution.transport.stream_buffer_bytes`. YAML string
values use whole `KiB` or `MiB` units; integer values are interpreted as bytes.

`scan.mode: range_partitioned` remains available, but with
`require_index_for_range: true` it blocks heap/no-index routes before source IO.
Use it only when `perf advise` confirms a seekable boundary and healthy stats.

## Source Export Optimizer

`native_transfer.snapshot.export_optimizer` chooses the source export provider
from measured evidence rather than static assumptions:

```yaml
source:
  options:
    native_transfer:
      snapshot:
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
```

For MSSQL, provider candidates include `mssql_bcp_native`,
`mssql_bcp_character_raw`, `mssql_odbc_array`, `mssql_driver_rowset`,
`range_partitioned`, and `single_scan_chunks`. The optimizer runs bounded
probes over the same projected source query, rejects unsafe or lossy providers,
and only switches when the winner beats the current default by the configured
speedup threshold.
Executable MSSQL probes can test multiple BCP packet sizes and ODBC array-fetch
batches. If BCP startup or file IO dominates a heap/no-index table, the
optimizer can select an ODBC array RowBinary stream while keeping target
staging/finalization unchanged.

The optimizer does not weaken correctness:

- target finalization still waits for staging gates;
- unsafe source-side escaping is rejected unless `source_encoded` is explicit;
- `required` mode fails before source IO if no safe provider is available;
- `benchmark_only` writes recommendations without changing the runtime path;
- decisions are cached by route, query, schema, source shape, provider versions
  and dpone version.

Evidence is recorded as `dpone.native_transfer.export_optimizer.v1` and appears
inside `native_transfer_snapshot_optimization` in `dpone plan`, `dpone perf
advise`, and run artifacts.

## Source-Side Snapshot Materialization

When a source view, heap table, or procedure-shaped query is the bottleneck,
faster ClickHouse ingest cannot recover the lost time. `v0.37` adds controlled
source preparation: materialize the projected source query once into a
run-scoped source work table, then reuse the existing export optimizer,
physical chunks, typed wire, staging gates, and finalizer.

```yaml
source:
  options:
    native_transfer:
      snapshot:
        materialization:
          mode: auto
          provider: auto
          allow_source_writes: true
          work_connection_ref: mssql_dpone_snapshot_work
          table_prefix: __dpone_snapshot_
          ttl_hours: 24
          cleanup_policy: eager
          cleanup:
            lock_timeout_ms: 5000
            retry_attempts: 2
            retry_backoff_ms: 250
            defer_on_lock_timeout: true
          reuse_policy: never
          min_speedup_pct: 25
          max_materialization_seconds: 3600
          isolation: inherited
          index:
            mode: auto
            boundary_columns: auto
          update_statistics: auto
```

`allow_source_writes` is intentionally `false` by default. In `auto` mode,
dpone selects materialization only when the route allows source writes, source
permissions are green, the source shape benefits from a work table, or probe
evidence predicts the configured speedup. `benchmark_only` writes the same
diagnostics without changing the runtime route; `required` fails before full
source export when grants, work schema, safety checks, or cleanup guarantees are
not available.

The first certified provider is MSSQL `mssql_work_table`. It creates a
run-scoped table with:

```sql
SELECT <columns>
INTO [work_database].[work_schema].[__dpone_snapshot_<run_id>]
FROM (<base_query>) AS dpone_src
```

`work_database` is optional. When it is absent, dpone preserves the original
two-part `[work_schema].[table]` behavior in the connection's current database.
When it is present, source reads still use the authored source database while
every work-table operation uses the explicit three-part name. Both databases
must be on the same SQL Server instance and accessible to the same principal.

The export query is then rewritten to `SELECT <columns> FROM <work_table>`.
Optional boundary indexes and statistics are created only when the downstream
planner can use them safely. Cleanup is eager/on-success/retain-on-failure by
policy, and evidence is recorded as
`dpone.native_transfer.source_materialization.v1`.

MSSQL cleanup is deliberately bounded. Before `DROP TABLE`, dpone sets
`LOCK_TIMEOUT`, retries transient metadata-lock timeouts, and then either
records `source_materialization.cleanup` as deleted or defers with reason
`mssql_cleanup_lock_timeout`. A deferred cleanup is not silent success: it is
visible in runtime decision audit and load-step evidence. Before creating a new
work table, the provider also sweeps expired dpone-owned tables matching the
configured `table_prefix` and `ttl_hours`; unsafe prefixes that do not identify
dpone-owned objects are blocked instead of being dropped.

For operators, `ttl_hours` is an age filter for this pre-run sweep, **not an
independent scheduled collector**. A killed worker cannot run eager cleanup, and
a pipeline that never runs again does not trigger its sweep. Monitor residual
tables in the work database and use a separately reviewed recovery procedure;
do not infer that an old table is inactive from age alone. Metadata discovery
errors stop preparation instead of reporting an empty successful sweep. The
cutoff uses SQL Server time, matching `sys.tables.create_date` even when workers
use another timezone.

Prefer `work_connection_ref` for promoted workloads. The verified deployment
registry resolves its database and schema as one environment-owned location,
and dpone requires it to use the same SQL Server endpoint as the source. The
same immutable manifest can therefore select `DWH_Dev.system` in DEV and
`Example_System.dbo` in PROD without a promotion-time rewrite. Explicit
`work_database` and `work_schema` remain available for compatibility but cannot
be combined with the logical ref. The principal needs source `SELECT` plus
`CREATE TABLE` and schema `ALTER` in the resolved work database. Changing the
work location does not clean tables in the previous database.
Source-shape inspection normalizes both `schema` and `database.schema` inputs
and reads catalog views in the business source database, without changing the
connection's work database.

For maintainers, `PreparedSourceArtifact` owns an additional snapshot resource,
so it shares extraction timing with its inner artifact but keeps an independent
terminal decision. A native transport closing its exported file at EOF must not
suppress the later snapshot cleanup. Preparation guards cover failed CREATE,
required index/statistics setup, query rewriting and export assembly before the
top-level owner receives the artifact. With `eager`, these failures attempt exact
snapshot cleanup while preserving the original error; retention policies remain
unchanged. Failed loads also publish cleanup decisions under their load identity.
The shared preparation guard lives in `runtime.source_materialization_preparation`;
it owns failure handling and delegates evidence publication to
`runtime.source_materialization_audit`, which does not own resource lifetimes.

Acceptance requires actual absence of the work table after cleanup; a successful
load or a terminal receipt alone is insufficient. `deferred` still means the
table remains and needs recovery. This snapshot fix does not certify native-file
retention after an unknown target commit, nor independent crash recovery.

## Minimal production manifest

```yaml
runtime:
  storage:
    profile: mounted_volume
    work_dir: /mnt/dpone-work
    evidence_dir: .dpone/runs
    checkpoint_dir: .dpone/state
    debug_dir: .dpone/debug
    min_free_bytes: 1GiB
    transfer_store:
      type: s3
      uri: s3://dpone-stage/native-transfer
      connection_id: dpone_transfer_store
      connection_type: env
      cleanup:
        temp_objects: on_success
        failed_objects: keep
      encryption:
        mode: provider_default
      multipart:
        enabled: true
        part_size: 64MiB
    cleanup:
      temp_files: eager
      failed_files: keep

source:
  type: mssql
  connection_id: mssql_dwh
  connection_type: env
  table:
    database: analytics_staging
    schema: clickhouse
    name: v_dim_counterparty
  options:
    columns: [id, name, updated_at]
    extract_mode: bcp_queryout
    partitioning:
      strategy: auto
      column: id
      bounds: auto
      target_rows_per_partition: 200000
      max_partitions: 4
      export_workers: 4
      load_workers: 2
    native_transfer:
      execution:
        mode: auto
        profile: balanced
        transport:
          mode: auto
          prefer_streaming: true
          fallback_to_file: true
          stream_buffer_bytes: 16MiB
          checksum: rolling
          max_stream_seconds: 3600
        cleanup_policy: eager
        resume_policy: object_store_if_verified
        resource_policy:
          max_active_files: 2
          max_active_bytes: 512MiB
          target_file_bytes: 128MiB
          max_file_bytes: 256MiB
          adaptive_sizing: true
          oversize_policy: split_and_retry
          min_slice_rows: 1000
          max_slice_rows: 250000
          disk_headroom_pct: 20
```

## Runtime storage

`runtime.storage.work_dir` is the local or mounted directory used for temporary
native transfer files. Configure it explicitly for production and KubernetesPod
Operator runs. The fallback to the system temp directory is intended for local
development and advisory workflows.

| Field | Meaning | Production advice |
| --- | --- | --- |
| `profile` | Human-readable storage profile: `worker_local`, `mounted_volume`, `object_backed`, or `custom`. | Use `mounted_volume` for KPO/PVC-backed workers. |
| `work_dir` | Root for temporary transfer files and leases. | Do not rely on `/tmp` for large loads. |
| `evidence_dir` | Run evidence output root. | Keep it on durable storage when evidence is required after pod exit. |
| `checkpoint_dir` | Slice and partition checkpoint root. | Keep it durable when using resume. |
| `debug_dir` | Retained failed file samples when enabled. | Use retention and cleanup policies. |
| `path_template` | Per-run relative path template. Defaults to `{pipeline}/{run_id}/{stage}`. | Keep `{stage}` unless you have a strong reason. |
| `create_dirs` | Create missing directories before execution. | Keep `true` for self-service workloads. |
| `require_writable` | Write a sentinel file during preflight. | Keep `true` for release profile. |
| `min_free_bytes` | Minimum free bytes required before execution. | Size this above one expected max active byte window. |

Legacy aliases still work:

- `source.options.partition_tmp_dir`
- `DPONE_EXPORT_TMP_DIR`
- `observability.artifacts.path` for evidence output

They are kept for compatibility, but new manifests should use
`runtime.storage`.

## Object-backed transfer store

`runtime.storage.transfer_store` is the durable payload store for native
transfer slices. It complements `work_dir`; it does not replace local scratch
for tools such as BCP or `COPY` that require a filesystem path.

| Field | Meaning | Production advice |
| --- | --- | --- |
| `type` | Store adapter: `s3`, `gcs`, `azure`, `minio`, or `local`. | Use cloud/object storage for cross-pod resume. |
| `uri` | Per-workload object prefix. | Use a narrow prefix such as `s3://bucket/dpone/transfers`. |
| `connection_id` | Credentials reference for the store. | Keep credentials in env/Vault/Airflow, not in manifests. |
| `endpoint_url` | Optional S3-compatible endpoint. | Use for MinIO or private S3-compatible stores. |
| `local_root_dir` | Local object-store emulator root. | Use only for tests and local certification. |
| `cleanup.temp_objects` | Object retention after successful runs. | `on_success` for routine loads, `retain` for incident replay. |
| `cleanup.failed_objects` | Object retention after failed runs. | `keep` helps retry and incident analysis. |
| `encryption.mode` | Provider-side encryption declaration. | Prefer provider defaults or KMS where required. |
| `multipart.part_size` | Upload chunk size hint. | Keep `64MiB` or larger for large slices. |

When a transfer store is configured, each slice records a `transfer_object`
evidence block with URI, provider, byte size, checksum, run id, partition id,
and slice id. A retry may hydrate a verified object into fresh local scratch
instead of reading the source again.

### Local CI example

```yaml
runtime:
  storage:
    profile: object_backed
    work_dir: .dpone/work
    transfer_store:
      type: local
      uri: s3://dpone-stage/native-transfer
      local_root_dir: .dpone/object-store-local
```

The local adapter still uses object-style URIs in evidence, while bytes are
stored under `local_root_dir`. This keeps tests deterministic without changing
production manifest shape.

## Transport policy

`source.options.native_transfer.execution.transport` selects the physical
payload transport for each slice:

| Mode | Behavior | Use case |
| --- | --- | --- |
| `auto` | Prefer zero-file `stream` when source, sink, and codec are stream-safe; otherwise use `file`. | Production default. |
| `stream` | Require a bounded byte stream from source to staging sink. Fail fast if the route is not stream-safe. | Weak workers with stream-capable connectors. |
| `object` | Require an object-backed route candidate. Fail fast unless the route has object-store certification. | Cross-pod resume and object-store transfer paths. |
| `file` | Use the v0.20 pipelined file/object-backed path. | BCP, debug, benchmarks, and file-only tools. |

| Field | Meaning | Production advice |
| --- | --- | --- |
| `prefer_streaming` | Allows `auto` to choose zero-file streaming. | Keep `true`; use `mode: file` for explicit file runs. |
| `fallback_to_file` | Allows automatic file fallback when stream is unsupported. | Keep `true` for self-service, set `false` for strict stream certification. |
| `stream_buffer_bytes` | Bounded stream buffer budget used by stream-capable adapters. | Start with `16MiB`; tune only with evidence. |
| `checksum` | Stream evidence checksum mode: `none`, `rolling`, or `sha256`. | Use `rolling` by default; use `sha256` for release evidence. |
| `max_stream_seconds` | Per-slice stream timeout budget. | Align with source and sink command timeouts. |

The resolver records a stream eligibility matrix in `dpone plan`:
source support, sink support, codec support, fallback reason, and connector
reason codes. This is deliberate: fallback must be visible, not hidden behind
performance heuristics.

Initial stream-safe coverage:

- PostgreSQL `COPY (...) TO STDOUT` can produce a bounded byte stream.
- ClickHouse HTTP bulk ingest can consume the stream as an `INSERT ... FORMAT`
  request body.
- ClickHouse client and MSSQL BCP remain file transports until live
  certification proves a stable stdin/stdout contract.
- MSSQL `bcp queryout` is intentionally reported as
  `bcp_queryout_is_file_transport`; use pipelined file/object-backed mode for
  the fastest production MSSQL -> ClickHouse path.

## Typed bulk wire policy

`source.options.native_transfer.wire` controls the logical payload contract
inside the selected transport. It is separate from `execution.transport`:
transport decides whether a slice moves as a file, object, or stream; wire
decides whether source SQL emits sink-specific escaped text or a typed raw
payload that the sink decodes in staging.

```yaml
source:
  options:
    native_transfer:
      wire:
        mode: typed_raw
        text_safety: prove_or_transcode
        delimiter_profile: ascii_control
        null_policy: sidecar
sink:
  options:
    clickhouse_bulk:
      mode: http
      ingest_contract: typed_staging
```

| Field | Behavior |
| --- | --- |
| `mode: auto` | Keeps legacy source-encoded TSV unless the route requests typed staging and the planner can select a safe typed route. |
| `mode: typed_binary` | Uses a sink binary payload contract. For MSSQL -> ClickHouse this can be `source_native_format: bcp_native` for `bcp -n` decode or `odbc_row_stream` for the safe fallback. |
| `mode: typed_raw` | Source exports raw projected columns and partition predicates without ClickHouse TSV `REPLACE` expressions. |
| `mode: source_encoded` | Legacy direct TSV path; MSSQL queryout projects ClickHouse-ready escaped strings in SQL. |
| `mode: driver` | Uses the Python/driver ingest path when native bulk is not suitable. |
| `source_native_format` | `bcp_native` selects SQL Server `bcp queryout -n`; `odbc_row_stream` keeps the v0.25 row-stream fallback; `auto` keeps backward-compatible route selection. |
| `binary_format` | `native` selects ClickHouse `Native` columnar blocks; `rowbinary` keeps the v0.26 row-oriented fallback. |
| `block_rows` / `block_bytes` | Bound ClickHouse `Native` block size for weak-worker memory control and throughput tuning. |
| `acceleration.mode` | `auto` uses a certified optional `dpone[accel]` provider when available, `off` forces Python reference transcode, and `required` fails before source IO if acceleration is unavailable. |
| `text_safety` | Controls whether unsafe text must be proven safe, transcoded, or escaped at source. |
| `delimiter_profile` | `ascii_control` uses `CustomSeparated` control delimiters to reduce delimiter collisions. ClickHouse parses the payload with its CSV escaping rule; MSSQL still exports raw projected values without sink-specific `REPLACE` expressions. |
| `null_policy` | `sidecar` is the future-proof default for typed staging null fidelity. |

For MSSQL -> ClickHouse, the recommended high-throughput route is
`typed_binary` + `source_native_format: bcp_native` +
`clickhouse_bulk.ingest_contract: typed_binary_staging`. The MSSQL `bcp
queryout` SQL should contain only projection and predicates. If `dpone plan` or
runtime evidence reports `mssql_source_escaping: true`, or the generated MSSQL
query contains `REPLACE(`, the route is using the legacy `source_encoded`
fallback.

`dpone plan` writes `native_transfer_bulk_wire` evidence with the selected
route, ClickHouse input format, delimiter profile, schema hash, source escaping
flag, acceleration backend decision, fallback reason, warnings, and blockers.
This makes fast-path changes reviewable in GitOps and release evidence.

## Adaptive snapshot route optimization

`source.options.native_transfer.snapshot` controls the bulk snapshot execution
route after transport and wire decisions are known. It does not replace
`native_transfer.execution.transport` or `native_transfer.wire`: transport moves
the slice, wire defines the payload, and snapshot optimization decides the
runtime route shape.

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
sink:
  options:
    clickhouse_bulk:
      mode: native_tcp
      native_tcp:
        enabled: true
        backend: auto
        compression: auto
        port: 9000
        connection_pool_size: 2
        query_timeout_seconds: 3600
      ingest_contract: typed_binary_staging
```

The stable evidence schema is
`dpone.native_transfer.snapshot_optimization.v1`. Evidence records the selected
backend, protocol backend, compression, packet/block settings, effective
parallelism, fallback chain, release gate, partition planner, statistics
confidence, blockers, warnings, and governor throttling reason codes.

The optimizer evaluates backends in this order:

1. `native_tcp`
2. `client`
3. `http`
4. `python`

`native_tcp` is eligible only for certified typed binary ClickHouse Native
routes (`wire.mode: typed_binary`, `source_native_format: bcp_native`,
`binary_format: native`, `clickhouse_bulk.ingest_contract:
typed_binary_staging`). If the native TCP backend is unavailable, the planner
falls back to `client`, then `http`, then `python`, and records the fallback
reason. In release/production runner policy, uncertified routes stay
fail-closed.

The v0.33 direct backend uses ClickHouse Native TCP without spawning
`clickhouse-client`. The optional `dpone-native-accel` provider opens the Native
protocol connection, sends the native-protocol `INSERT ... VALUES` query, sends
the required empty external-tables `Data` packet, frames existing ClickHouse
Native blocks as insert `Data` packets, and requests LZ4/ZSTD
compression per query. Source adapters still emit typed Native blocks and do not
know whether ClickHouse receives them through the direct protocol or the client
wrapper. When the provider is missing or uncertified, `backend: auto` falls back
to the client wrapper with an explicit `direct_ingest_*` reason; `backend:
direct` remains fail-closed before source I/O.

Direct Native TCP routes must pass type/corner-case certification before being
promoted beyond advisory mode. The certification matrix covers every supported
scalar family: signed/unsigned integers, bool, Float32/Float64 finite extremes,
Decimal128/Decimal256 maximum and minimum scaled values, nullable and
non-nullable decimals, String, FixedString padding and oversize rejection,
Unicode text with tabs/newlines, binary payloads, UUID, Date/Date32,
DateTime/DateTime64 precision, SQL Server `time`/`datetimeoffset`, NULL versus
empty values, and route evidence redaction. `LowCardinality`, composite, and
enum Native targets are not scalar encodings and fail closed. If a type is not
in the certified matrix, the route must fail closed or fall back with an
explicit reason.

## Statistics-aware snapshot planning

`source.options.partitioning.planner` lets routes build partitions from source
metadata instead of expensive boundary scans:

```yaml
source:
  options:
    partitioning:
      strategy: stats
      column: id
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

The stable evidence schema is
`dpone.native_transfer.snapshot_partition_plan.v1`. The generic planner
consumes normalized histogram steps and emits logical partitions. The first
adapter is SQL Server: it reads `sys.dm_db_stats_histogram`, splits hot ranges
when the key domain allows it, and emits a separate NULL bucket when requested.
If statistics are unavailable, stale, or attached to a view route with low
confidence, the plan records `source_stats_low_confidence` and uses conservative
parallelism instead of guessing.

Typed boundary planning also covers Spark-class JDBC partition columns:
numeric, `date`, timestamp-like types, and SQL Server `rowversion`. The
partitioner computes ranges in canonical units and delegates SQL literals to
source-specific renderers, so MSSQL `date` predicates are emitted as typed
literals such as `CONVERT(date, '2024-01-01', 23)` without wrapping the source
column. SQL Server `timestamp` is treated as `rowversion`, not as wall-clock
time.

## Certified route planning

`dpone plan`, `dpone perf advise`, and `dpone run` now build a route-level
decision before source I/O. The decision combines the requested transport,
first-party connector capabilities, codec safety, staging safety, and route
certification evidence:

```yaml
source:
  options:
    native_transfer:
      execution:
        transport:
          mode: auto
        certification:
          mode: auto
          artifact: .dpone/certification/native_transfer_route_certification.json
```

Certification modes:

| Mode | Behavior |
| --- | --- |
| `auto` | Uses `certified_only` for release/production runner policy and `advisory` for local/dev planning. |
| `advisory` | Allows technically valid uncertified routes, but writes warnings to plan and evidence. |
| `certified_only` | Requires route certification for the selected transport and fails before source I/O when evidence is missing or stale. |

The planner evaluates candidates in this order: `stream -> object -> file`.
For each candidate it records source export support, sink staging-load support,
codec safety, staging safety, certification status, blockers, and reason codes.
The stable evidence schema is `dpone.native_transfer.route_decision.v1`.

Example output:

```text
route: postgres -> clickhouse
requested_transport: auto
selected_transport: stream
certification: certified
fallback_chain: stream -> object -> file
release_gate: green
```

Generate route certification evidence with:

```bash
uv run dpone ops route-transport-certification \
  --manifest manifests/postgres_clickhouse_orders.yaml \
  --profile static \
  --artifact-dir .dpone/certification/postgres_clickhouse_orders \
  --format json
```

The command writes:

- `native_transfer_route_certification.json`
- `native_transfer_route_certification.md`

`dpone run` applies the same route decision as `dpone plan`. In
`certified_only` mode, blockers such as
`native_transfer_route_certification.missing` stop the run before runtime
connection hydration, source export, staging creation, or target mutation.
If the manifest route, selected transport, or codec no longer match the
certification artifact, the planner reports
`native_transfer_route_certification.stale_hash`; regenerate the artifact
instead of editing JSON by hand.

## Transport evidence

Every native transfer transport can emit a stable evidence payload with schema
`dpone.native_transfer.transport_evidence.v1`. The evidence shape is shared by
file, object-backed, and stream transports so release gates do not branch per
connector:

| Field | Meaning |
| --- | --- |
| `selected_transport` | Resolved transport: `stream`, `object`, or `file`. |
| `requested_transport` | User policy, usually `auto`, `stream`, or `file`. |
| `fallback_allowed` | Whether policy allowed automatic fallback. |
| `fallback_reason` | Stable reason code when `auto` selected a lower transport. |
| `eligibility` | Source, sink, codec booleans and route-specific reasons. |
| `metrics` | Rows, bytes, checksum, and duration when known. |
| `staging_session_id` | Run-scoped staging marker, if the sink exposes one. |
| `cleanup_status` | Cleanup result such as `eager_deleted` or `retained_for_debug`. |
| `failure_code` | Stable failure taxonomy code, or `null` for success. |

Sensitive diagnostic keys such as passwords, tokens, secrets, credentials, and
keys are redacted before evidence is serialized.

Canonical failure and fallback codes:

- `native_transfer_stream_source_failed`
- `native_transfer_stream_sink_failed`
- `native_transfer_stream_timeout`
- `native_transfer_stream_fallback_file_only_source`
- `native_transfer_stream_fallback_file_only_sink`
- `native_transfer_codec_not_stream_safe`

## Resource policy

The resource policy controls file count, file bytes, and adaptive slicing.

| Field | Meaning | Example |
| --- | --- | --- |
| `max_active_files` | Maximum local slice files at the same time. | `1` for weak workers, `4` for throughput workers. |
| `max_active_bytes` | Maximum bytes held by active local files. | `512MiB` keeps disk pressure bounded. |
| `target_file_bytes` | Desired slice file size. | `128MiB` gives predictable upload chunks. |
| `max_file_bytes` | Hard limit for one slice file. | `256MiB` triggers split/retry before staging load. |
| `adaptive_sizing` | Shrink or grow future slices using observed bytes per row. | Keep `true` for mixed-width tables. |
| `oversize_policy` | `split_and_retry` or `fail` when a file exceeds `max_file_bytes`. | Use `split_and_retry` for bounded key ranges. |
| `min_slice_rows` | Smallest physical slice size in rows. | Prevents thousands of tiny files. |
| `max_slice_rows` | Largest physical slice size in rows. | Caps large-row tables even when estimates are low. |
| `disk_headroom_pct` | Free disk percent to keep untouched. | `20` leaves room for logs and other processes. |

### Example: weak worker

```yaml
native_transfer:
  execution:
    profile: safe_worker
    resource_policy:
      max_active_files: 1
      max_active_bytes: 256MiB
      target_file_bytes: 64MiB
      max_file_bytes: 128MiB
```

Only one file is active. A slice that exceeds `128MiB` is deleted, split, and
retried before any oversized artifact is sent to the sink.

### Example: throughput worker

```yaml
native_transfer:
  execution:
    profile: throughput
    resource_policy:
      max_active_files: 4
      max_active_bytes: 2GiB
      target_file_bytes: 512MiB
      max_file_bytes: 768MiB
```

The worker may export/load several files concurrently, but still has a bounded
disk budget.

## Execution modes

| Mode | Behavior | Use case |
| --- | --- | --- |
| `auto` | Uses `pipelined` when source slicing and sink staging ingest are available, otherwise falls back to `batch`. | Recommended default. |
| `pipelined` | Export one slice, load it into staging, clean it, then continue. | Production native transfer. |
| `batch` | Export all files first, then load all files. | Debug, benchmark, artifact inspection, compatibility. |

The first adaptive slicing release requires numeric partition boundaries for
physical slice splitting. In `auto` mode, non-numeric boundaries fall back to
`batch`. In forced `pipelined` mode, dpone fails fast with
`native_transfer_pipelined_requires_numeric_partition_bounds`.

## Pipelined algorithm

1. Resolve and preflight `TransferWorkspace`.
2. Estimate row width from source metadata and bounded samples.
3. Build logical partitions from `source.options.partitioning`.
4. Split each partition into physical slices.
5. Resolve the physical transport for the slice.
6. For `stream`, open a bounded byte stream and load it directly into staging.
7. For `file`, export one slice into `runtime.storage.work_dir`.
8. For `object`, upload the file to `runtime.storage.transfer_store`.
9. Record transport, rows, bytes, checksum, schema hash, query hash, and slice
   bounds.
10. Mark the slice as loaded to staging.
11. Delete the local file according to cleanup policy when a file exists.
12. Finalize the target only after every slice passes gates.

On failure, dpone cleans the current file, keeps the target unchanged, and
aborts or retains staging according to the resume policy.

## Source impact diagnostics

`dpone plan` and `dpone perf advise` surface source-side risks before runtime:

| Code | Meaning | Typical action |
| --- | --- | --- |
| `source_select_star` | The extract query reads every column. | Configure `source.options.columns`. |
| `source_view_over_view_risk` | The source object looks like a view and may wrap other views. | Inspect the view plan or load from a materialized boundary. |
| `source_boundary_index_unknown` | dpone cannot confirm an index on the partition column. | Add index metadata or ask DBA to verify the key. |
| `source_non_sargable_boundary` | The boundary expression can block index seek. | Partition on a raw indexed column. |

For MSSQL BCP queryout, dpone plans the range predicate against the base query
before ClickHouse-specific TSV/type-fidelity projection. This improves optimizer
pushdown and keeps generated SQL explainable.

## Commands

Render the production plan:

```bash
dpone plan manifest.yaml --format md
```

Inspect performance and source-impact advice:

```bash
dpone perf advise manifest.yaml --format md
```

Safely inspect old runtime storage files:

```bash
dpone runtime storage gc --work-dir /mnt/dpone-work --older-than-seconds 86400 --dry-run
```

Apply cleanup only after reviewing candidates:

```bash
dpone runtime storage gc --work-dir /mnt/dpone-work --older-than-seconds 86400 --apply
```

The GC command only targets known runtime stage directories such as `transfer`,
`leases`, and `debug`.

## KubernetesPod Operator notes

Generated GitOps/Airflow artifacts should treat storage as a runtime contract:

- mount `runtime.storage.work_dir` as an `emptyDir` with `sizeLimit` or as a PVC;
- set `ephemeral-storage` requests and limits when using local ephemeral disk;
- mount evidence and checkpoint directories on durable storage when required;
- configure a transfer store when retry can move to another pod;
- run pod-doctor/preflight checks for path existence, writability, and free
  bytes in release mode;
- do not place credentials in runtime storage paths.

For weak workers, prefer `profile: safe_worker`, a PVC-backed `work_dir`, and
`cleanup.temp_files: eager`.

## Product benchmark

dpone intentionally combines ideas from mature systems while keeping the plan
visible and GitOps-friendly:

| System | Similar capability | dpone addition |
| --- | --- | --- |
| dlt | Working directory and file rotation. | Staging atomicity, per-slice evidence, disk preflight, and explicit KPO storage contract. |
| Airbyte | Platform workspace volumes and state/log storage. | Workload-level disk budget and manifest-owned storage policy. |
| Fivetran | Managed chunk sync and safe target commit. | Transparent SQL plan, source-impact diagnostics, and OSS evidence. |
| Informatica | Partitioning, pushdown, and cache controls. | Generated SQL visibility and source-risk warning codes. |
| Pentaho | Batch/commit knobs and temp directory controls. | Per-run isolated workspaces, typed cleanup, and resume-safe staging. |
| SSIS | Row and byte buffer sizing. | Dual file-count/file-byte limits with adaptive slice feedback. |

## Bounded typed rows and atomic windows

MSSQL typed row streaming uses the existing `source.options.native_transfer.wire`
configuration: `mode: typed_binary`, `source_native_format: odbc_row_stream`, and
`binary_format: rowbinary`. Select `source.options.mssql_export_mode:
odbc_row_stream`, `execution.transport.mode: stream`, and ClickHouse HTTP
`typed_binary_staging`. Optional `wire.block_bytes` bounds encoded rows and batches;
`wire.block_rows` bounds batch cardinality. Oversized rows and mismatched shapes
fail explicitly. These limits do not bound driver allocations or process RSS.

Byte-bounded encoding also rejects fractional integer conversion and timestamp
precision loss. Decimal alias width/scale and pre-epoch timestamp corrections apply
to all encoder callers; historical unbounded timestamp truncation remains unchanged.
This streaming capability does not establish a shared source snapshot or atomic
window recovery. See [Atomic rolling windows](rolling-window.md) for supported
source/target capabilities, composition, and recovery restrictions.
