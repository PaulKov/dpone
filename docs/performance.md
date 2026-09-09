# dpone performance and SLO guide

This guide documents the production-oriented bulk path for large database transfers. The current high-throughput focus is PostgreSQL -> Microsoft SQL Server, ClickHouse -> Microsoft SQL Server, Microsoft SQL Server -> ClickHouse, and SQL Server/PostgreSQL-backed state tables.

## ClickHouse -> MSSQL bulk path

The optimized path keeps source semantics and transport tuning separate:

```text
ClickHouse stream -> one immutable spool -> one verified BCP process
-> raw heap
   -> capacity admitted: one decoded heap -> lossless conversion guard
   -> inline fallback: lossless conversion guard over raw codec expressions
-> one typed/lineage/hash INSERT...SELECT -> atomic finalizer
```

For table extracts this route does not issue a complete preflight row count;
the successfully consumed stream is the source row authority. MSSQL receives
one BCP import per bounded stream instead of one process per source fetch batch.
The bulk-text decoder is physically materialized once per column and row. This
boundary is intentional: SQL Server may inline a CTE or `CROSS APPLY` and
re-evaluate nested `REPLACE` expressions for every conversion predicate.
Rows without the codec marker bypass the replacement chain. Marker detection
uses an explicit binary collation, so the branch is independent of the target
database default. SQL Server populates the empty decoded heap under `TABLOCK`
so eligible recovery models can use the ordinary parallel/minimally logged
heap-insert path.
Native conversion validation then uses one aggregate scan of decoded values,
and lineage plus row hash are projected during the typed insert instead of
subsequent full-stage updates. The decoded heap has the exact raw receipt row
count and is never an evidence authority.

The optimization is capacity-admitted rather than unconditional. The metadata
probe reads the raw heap's reserved pages, including LOB and row-overflow pages,
from
[`sys.dm_db_partition_stats`](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-db-partition-stats-transact-sql)
and current data-file free pages from
[`sys.database_files`](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-database-files-transact-sql)
plus `FILEPROPERTY(..., 'SpaceUsed')`. Only online files in the database's
default filegroup contribute free bytes: the decoded and native heaps have no
explicit `ON` clause and therefore cannot consume capacity from another
filegroup. The probe does not scan business rows or count possible file
autogrowth. Admission requires free data-file bytes of:

```text
raw_reserved_bytes
+ raw_reserved_bytes
+ ceil(raw_row_count / 8) * 9 * 8,192
+ 64 MiB fixed headroom
```

The terms reserve the decoded copy, a raw-sized native payload copy, SQL
Server's full data-page allocation for every possible wide native row, and
operational headroom. The page term rounds rows to complete eight-page extents
and adds one conservative allocation-map page per extent; it does not confuse
the 8,060-byte record payload limit with physical page allocation. This follows
the
[`page and extent architecture`](https://learn.microsoft.com/en-us/sql/relational-databases/pages-and-extents-architecture-guide)
contract; the additional raw-sized component covers variable, row-overflow,
and LOB payload rather than assuming every native row is narrow. At the
admitted peak, the staging database therefore holds the existing raw heap, the
decoded heap, and the native heap simultaneously. If the metadata permission
is unavailable, the evidence is malformed, the raw allocation cannot be
resolved, or free space is below the threshold, dpone logs
`phase=decode_admission status=inline_fallback` and uses the previous inline
decoder. The conversion guards and target boundary are identical; only repeated
decoder evaluation returns. The probe is not a reservation, so a later SQL
Server allocation failure still fails the load closed.

Decoded cleanup is bounded to three idempotent `DROP TABLE IF EXISTS` attempts
with 50 ms and 100 ms backoff. Exhausted cleanup after otherwise successful
processing fails the run. Exhausted cleanup while another exception is already
authoritative preserves that exception and logs
`status=residue_retained attempts=3 table=<exact-qualified-name>`. Recovery is
deliberately exact rather than wildcard-based:

1. Verify that no active run owns the qualified table from the log event.
2. Drop that exact table through the managed MSSQL connection.
3. Rerun the failed load and confirm no `residue_retained` event remains.

Do not automate a prefix sweep: a concurrent run can own another decoded or
native heap with the same human-readable prefix. Full refresh preserves the
existing target object and uses a
table-locked set-based insert after `TRUNCATE`.

Benchmark source fetch size and BCP `-b` independently. Record at least one
warmup and three measured runs for the exact route, strategy, row shape, and
volume. Promotion requires exact count/hash/type/lineage correctness, zero BCP
rejects, preserved physical design, and phase-level evidence; a local CSV to
DuckDB result is not an end-to-end ClickHouse -> MSSQL benchmark.

The runtime does not yet emit a complete versioned ClickHouse -> MSSQL
benchmark artifact or observed source-fetch/phase counters. Collect these
measurements from the Airflow task, ClickHouse query log, BCP output, SQL Server
DMVs, and run receipts; keep route certification `UNVERIFIED`. The runtime log
events `phase=decode_admission` and `phase=decode_materialize` report the chosen
path, capacity values when available, rows, and wall-clock duration for the
added physical boundary. Do not interpret
`Sink_Stream_Materializations: 1` as a source fetch-batch count.

## Baseline bulk path

For PostgreSQL -> SQL Server, prefer native PostgreSQL `COPY TO STDOUT` plus Microsoft `bcp`:

```yaml
source:
  type: postgres
  table: {schema: public, name: orders}
  options:
    export_format: mssql-delimited
    batch_commit_mode: whole

sink:
  type: mssql
  table: {schema: dbo, name: orders}
  strategy: {mode: full_refresh}
  options:
    bulk:
      mode: bcp
      bcp:
        batch_size: 100000
        packet_size: 16384
```

SQL Server loads are staging-first; `bcp` never writes directly to the business
target. `FULL_REFRESH` validates native staging and then atomically truncates
the existing target and performs one table-locked `INSERT ... SELECT` inside
the receipt-backed transaction. This preserves the target object's grants,
indexes, constraints, and references. Other strategies use their documented
set-based finalizer; a shadow swap is not the default because it changes object
identity.

For `PARTITION_REPLACE`, an exact native `date` partition column uses a
deduplicated staging-key join with direct typed equality. This keeps an index on
the target date usable and prevents finalization time from scaling with every
duplicate staging row. Non-certified partition types retain the exact canonical
identity fallback. The runtime decision
`mssql.partition_replace.finalizer` exposes which path ran.

## Range partitioning on certified routes

Range partitioning remains available only on source/sink routes whose
capability contract certifies one consistent source snapshot. PostgreSQL ->
MSSQL currently uses one `COPY` statement and rejects source partitioning
before row I/O: independent exports have no shared MVCC snapshot coordinator.
Do not copy partition settings from other routes into this path.

For routes that do certify parallel reads, numeric, `date`, and timestamp-like
boundaries remain available and the plan records boundary type, precision,
null policy, and generated predicates in run evidence.

SQL Server `timestamp` is not a temporal type. It is `rowversion`; dpone treats
it as a monotonic binary boundary and never as `datetime`.

Date example:

```yaml
source:
  type: mssql
  table: {schema: reporting, name: account_sales_history}
  options:
    partitioning:
      strategy: auto
      column: doc_date
      bounds: auto
      target_rows_per_partition: 500000
      max_partitions: 16
      planner:
        boundary_type: date
        bounds_role: filter
      export_workers: 4
      load_workers: 2
```

## Heap-Aware Single-Scan Chunks

Range partitioning is for source parallelism, not for reducing local file size.
On a heap table, an unindexed boundary column, a view over unknown objects, or a
low-confidence statistics plan, range partitioning can multiply the same full
scan N times. For those routes, prefer one source scan and physical chunks:

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

The runtime builds one source query with projection and source predicates only.
It starts one MSSQL `bcp queryout`, spools the byte stream into row-boundary-safe
chunk files, loads each chunk into the same ClickHouse staging session, and
deletes the chunk immediately after the staging load succeeds. Target tables are
still mutated only after all chunks pass gates.

`target_chunk_bytes` is a soft row-boundary target. `max_chunk_bytes` is an
absolute file limit: the next complete row starts a new file when appending it
would exceed the maximum. A row larger than the maximum fails closed with
`physical_chunk_row_exceeds_max_bytes`; dpone never splits BCP rows. Size the
maximum for the largest valid encoded row as well as worker disk headroom.

Use this when the goal is weak-worker disk safety. Use typed range
partitioning only when the boundary is indexed/seekable and statistics are
healthy. `dpone plan` and `dpone perf advise` surface
`native_transfer_source_scan` evidence with table shape, selected scan mode,
chunk size, and warnings such as `source_heap_range_parallelism_blocked`.

## Altinity-Class MSSQL Pipe Streaming

When the source export itself is the bottleneck, even single-scan physical
chunks still write local files before ClickHouse sees bytes. v0.46 adds an
Altinity-class streaming route for delimiter-safe MSSQL snapshots:

```text
MSSQL bcp queryout -c -> FIFO -> clickhouse-client INSERT FORMAT CustomSeparated
```

The route avoids the monolithic BCP artifact and keeps backpressure between the
producer and ClickHouse consumer. dpone still loads into a run-scoped staging
table first, projects `__dpone__*` lineage columns, runs quality gates, finalizes
atomically and records runtime decisions/progress in audit.

```yaml
source:
  type: mssql
  options:
    native_transfer:
      wire:
        mode: typed_raw
        delimiter_profile: ascii_control
      snapshot:
        streaming:
          mode: required
          provider: bcp_pipe
          pipe_mode: fifo
          read_buffer_bytes: 4MiB
          cleanup_policy: eager
          delimiter_safety: advisory

sink:
  type: clickhouse
  options:
    clickhouse_bulk:
      mode: client
      ingest_contract: typed_raw_streaming_staging
      streaming:
        format: CustomSeparated
        async_insert: true
        wait_for_async_insert: true
```

Safety rules:

- `delimiter_safety: certified_only` is fail-closed until a delimiter probe or
  workload certification proves that text values cannot break row/column
  boundaries.
- `delimiter_safety: advisory` is intended for benchmarks and explicitly
  accepted workloads; it must be paired with count/hash reconciliation.
- Generated MSSQL SQL remains projection plus source predicates only. dpone does
  not inject ClickHouse TSV `REPLACE(...)` escaping in this route.
- `read_buffer_bytes` controls only one FIFO read request. Its default is
  `4MiB`, its valid range is `64KiB..16MiB`, and it is independent of physical
  chunk files, ClickHouse blocks, and the generic transport budget
  `native_transfer.execution.transport.stream_buffer_bytes`.
- The deprecated streaming key `target_chunk_bytes` is accepted for one
  dedicated deprecation release but does not control memory. It keeps the
  historical effective 4 MiB buffer and emits a structured migration warning.
- If `clickhouse-client` is unavailable, use the HTTP streaming fallback only
  after it is certified for the same route; fallback decisions are recorded in
  runtime logs and `etl_state.__dpone__load_steps`.

Compared with the Altinity shell pattern, dpone adds typed route decisions,
staging safety, lineage, quality gates, cleanup and fallback observability.
Compared with physical chunks, pipe streaming optimizes elapsed time and local
disk pressure together; physical chunks remain the conservative fallback when
delimiter safety is not certified.

## Adaptive Source Export Optimizer

When source export is the bottleneck, tuning ClickHouse ingest or worker
parallelism is not enough. The source export optimizer measures bounded probes
for eligible source providers and selects the fastest safe provider only when
the measured gain is meaningful. v0.36 adds executable MSSQL BCP and ODBC-array
probes, so plans can compare real source export speed instead of relying on
static provider preferences:

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

For MSSQL native snapshots the first provider catalog contains:

| Provider | What it tests | When it usually wins |
| --- | --- | --- |
| `mssql_bcp_native` | `bcp queryout -n` source-native binary artifact. | Wide typed routes and schemas certified for native decoding. |
| `mssql_bcp_character_raw` | Raw character export without SQL-side ClickHouse TSV escaping. | Narrow text-safe routes where BCP character output is faster than binary decode. |
| `mssql_odbc_array` | Bounded ODBC array fetch batches. | Small/medium heap tables where BCP startup/file overhead dominates. |
| `mssql_driver_rowset` | Driver rowset streaming fallback. | Debug and compatibility routes where BCP is unavailable. |
| `range_partitioned` | Typed indexed ranges. | Tables with a seekable boundary and healthy statistics. |
| `single_scan_chunks` | One source scan split into physical chunk files. | Heap/no-index/view routes on weak workers. |

Algorithm:

1. Build the same projected source query for every provider; no provider may
   add sink-specific `REPLACE(...)` escaping unless the manifest explicitly
   selects `source_encoded`.
2. Run bounded probes using `probe_rows` and `max_probe_seconds`.
   MSSQL BCP probes test the configured `bcp_probe_packets`; ODBC probes fetch
   bounded row arrays with `odbc_fetch_size`.
3. Measure rows/sec, bytes/sec, first-row latency, temp bytes and sample
   fidelity hash when available.
4. Reject providers that lose type fidelity, require unsafe escaping, exceed
   worker limits or trigger source-impact blockers.
5. Select the fastest safe provider only if it beats the current default by
   `min_speedup_pct`. If the current default could not be probed, `auto` may
   select the fastest measured safe provider and records
   `export_optimizer_default_probe_missing`.
6. Cache the decision by route, query hash, schema hash, source-shape hash,
   provider versions and dpone version.
7. Write `dpone.native_transfer.export_optimizer.v1` evidence into plan, perf
   advice and runtime artifacts.

Modes:

| Mode | Behavior |
| --- | --- |
| `auto` | Uses cached/certified evidence or runs cheap probes, then switches only on safe measured speedup. |
| `off` | Keeps the current provider and records that the optimizer is disabled. |
| `required` | Fails before full source IO when no safe benchmark-certified provider exists. |
| `benchmark_only` | Writes evidence and recommendations without changing the runtime provider. |

`dpone perf advise` surfaces the decision directly:

```text
source_export_optimizer:
  selected_provider: mssql_odbc_array
  current_default: mssql_bcp_native
  measured_speedup_pct: 42
  source_bottleneck: export
  rejected:
    range_partitioned: source_heap_range_parallelism_blocked
    mssql_bcp_character_raw: slower_probe
```

This keeps the product behavior close to industrial systems while staying more
explainable: [dlt SQL database](https://dlthub.com/docs/dlt-ecosystem/verified-sources/sql_database/configuration)
and [ConnectorX](https://sfu-db.github.io/connector-x/) inspire batch/Arrow
extraction benchmarking; [Airbyte MSSQL](https://docs.airbyte.com/integrations/sources/mssql)
and [Fivetran SQL Server](https://fivetran.com/docs/connectors/databases/sql-server)
inspire safe chunking/checkpoint discipline; [SSIS](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/data-flow-performance-features)
and [Pentaho Table Input](https://docs.pentaho.com/pdia-data-integration/pdi-transformation-steps-reference-overview/table-input)
inspire rows/bytes buffer thinking; [Informatica](https://success.informatica.com/videos/support-videos/Nf0gbvac9D8.html),
[Sling](https://docs.slingdata.io/examples/database-to-database), and
[SeaTunnel JDBC](https://seatunnel.apache.org/docs/2.3.9/connector-v2/source/Jdbc/)
inspire split diagnostics. dpone adds route evidence, staging atomicity,
source-impact guardrails and fail-closed provider selection.

## Source-Side Snapshot Materialization

If the fastest export provider is still source-bound, and the source is a
heap/view/low-confidence query, use source-side materialization to avoid
re-running expensive source logic for every downstream export decision:

```yaml
source:
  options:
    native_transfer:
      snapshot:
        materialization:
          mode: auto
          provider: mssql_work_table
          allow_source_writes: true
          work_database: Example_System
          work_schema: dpone_work
          table_prefix: __dpone_snapshot_
          ttl_hours: 24
          cleanup_policy: eager
          cleanup:
            lock_timeout_ms: 5000
            defer_on_lock_timeout: true
          min_speedup_pct: 25
          index:
            mode: auto
            boundary_columns: auto
          update_statistics: auto
```

The MSSQL provider creates one run-scoped work table from the projected base
query, optionally adds a boundary index/statistics when range planning can use
them, and then hands the rewritten table query back to the normal export
optimizer. The target path is unchanged: physical chunks, typed/binary wire,
ClickHouse staging, quality gates, and atomic finalization still apply.
Set `work_database` when the snapshot must live in a technical database while
the source connection remains bound to its business database. Omit it to keep
the previous current-database behavior.

Guardrails:

1. `allow_source_writes: true` is required for real work-table creation.
2. `required` mode fails before export if the work schema, grants, TTL cleanup
   or type safety checks are not green.
3. `benchmark_only` records the expected speedup and required grants without
   changing the runtime route.
4. `keep_on_failure` keeps the source work table for debugging and writes TTL
   evidence; the default `eager` policy drops it after the inner load succeeds.
5. MSSQL cleanup is bounded by `cleanup.lock_timeout_ms`. If SQL Server cannot
   acquire the metadata lock in time, dpone records
   `mssql_cleanup_lock_timeout` as deferred cleanup evidence instead of holding
   DWH locks indefinitely. Unexpected cleanup errors still fail the run.

This is the source-side analogue of staging in industrial ETL tools: SSIS and
Pentaho expose table input/output and buffer knobs, Informatica exposes
pushdown/staging choices, and Fivetran/Airbyte keep initial sync safe. dpone
adds GitOps-visible route evidence and keeps source writes opt-in rather than
implicit.

## Canonical transfer config taxonomy

Use nested option namespaces for new manifests:

| Capability | Canonical path | Notes |
| --- | --- | --- |
| Source partitioning | `source.options.partitioning.*` | `export_workers` controls source artifact writers; `load_workers` controls target artifact loaders. |
| Generic bulk mode | `sink.options.bulk.mode` | Example: `bcp` for MSSQL. |
| MSSQL bcp settings | `sink.options.bulk.bcp.*` | `batch_size`, `packet_size`, `table_lock`, `timeout_seconds`, `field_terminator`, `row_terminator`. |
| ClickHouse direct ingest | `sink.options.clickhouse_bulk.*` | `mode`, `http.*`, `client.*`, `insert_settings`, `query_id`, `insert_deduplication_token`. |

Legacy flat aliases are accepted for migration, but `dpone plan` emits warnings and new examples should not use them. See [Config alias migration](migration/config-aliases.md).

MSSQL BCP commands have a finite 3600-second process deadline by default.
Override `sink.options.bulk.bcp.timeout_seconds` for a route only from measured
partition SLOs. On expiry dpone terminates and reaps BCP, drains its output, and
fails the attempt; it never leaves an unsupervised importer running behind a
failed worker.

The process shape follows the data flow. Synchronous BCP imports use one
bounded `Popen.communicate(...)` call, which owns stdin and both output pipes
without background Python readers. Asynchronous `queryout` starts concurrent
stdout/stderr drainers because the caller must consume its data FIFO before it
can wait for BCP; after child exit, those readers also use finite, bounded
joins. Dpone does not open the next native database session until the prior
command has either completed that lifecycle or failed closed during bounded
abort and resource cleanup.

## Local 15M stress gate

Run against local Docker services:

```bash
PYTHONUNBUFFERED=1 uv run python tools/mssql_stress.py \
  --rows 15000000 \
  --batch-size 250000 \
  --bcp-path /opt/homebrew/bin/bcp \
  --json-output /tmp/dpone-mssql-15m.json
```

Add SLO thresholds when the runner is stable:

```bash
uv run python tools/mssql_stress.py \
  --rows 15000000 \
  --slo-pg-mssql-rps 180000 \
  --slo-mssql-clickhouse-rps 140000
```

The command exits with code `2` when any SLO is missed.

## Recommended x86_64 production benchmark profile

Run the long gate on a real x86_64 Linux host or CI runner with:

- SQL Server 2019+ or 2022 running natively, not under architecture emulation.
- `msodbcsql18`, `mssql-tools18`, and `bcp` installed locally.
- PostgreSQL and ClickHouse on the same network segment as the runner.
- Local NVMe or high-throughput ephemeral SSD for `DPONE_EXPORT_TMP_DIR`.
- Enough free disk for uncompressed TSV artifacts.

Suggested matrix:

| Rows | Partitions | Workers | Expected purpose |
|---:|---:|---:|---|
| 1,000,000 | 1 | 1 | Fast correctness smoke |
| 15,000,000 | 1 | 1 | Baseline bulk path |
| 50,000,000 | 1 | 1 | Long-running consistent-snapshot soak |

## Tuning notes

- Keep PostgreSQL -> MSSQL at one source `COPY` until a shared-snapshot
  coordinator is implemented and certified.
- Tune bounded source predicates, projection width, indexes, network, BCP batch
  and packet sizes, and SQL Server finalization from phase-level evidence.
- Prefer heap loads, `TABLOCK`, and delayed index creation for large initial backfills.
- Use SQL Server simple or bulk-logged recovery during controlled bulk windows when your operational policy allows it.
- Avoid gzip for local PostgreSQL -> SQL Server loads. Compression saves disk but usually costs throughput and cannot be consumed directly by `bcp`.

## Current production path for MSSQL -> ClickHouse

For large MSSQL -> ClickHouse transfers, prefer the typed binary BCP native
route when the schema is certified for the supported v0.26 type set:

```yaml
source:
  type: mssql
  options:
    extract_mode: bcp_queryout
    bulk:
      mode: bcp
      bcp:
        file_format: native
    native_transfer:
      wire:
        mode: typed_binary
        source_native_format: bcp_native
        binary_format: native
        block_rows: 65536
        block_bytes: 64MiB
        acceleration:
          mode: auto

sink:
  type: clickhouse
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

This route exports each slice with SQL Server `bcp queryout -n`, decodes the
source-native binary file inside dpone, and streams ClickHouse `Native`
columnar blocks into staging through the ClickHouse native interface. MSSQL
executes only projection and predicates; generated SQL must not contain
`REPLACE(...)`, `CONVERT(VARCHAR(MAX))`, or ClickHouse TSV escaping.
`acceleration.mode: auto` uses the optional `dpone[accel]` fused provider only
when the route and schema are certified; otherwise the Python reference
transcoder is used with an explicit plan/evidence warning.
Use `acceleration.mode: required` for release routes that must fail before
source IO when the native accelerator is unavailable. Use
`binary_format: rowbinary` to keep the v0.26 row-oriented fallback for
compatibility benchmarks.

`native_tcp.backend: auto` first evaluates the certified direct protocol provider
from `dpone-native-accel` and then falls back to the client wrapper. The v0.33
direct path removes the `clickhouse-client` subprocess: dpone sends
pre-encoded ClickHouse Native blocks as Native TCP `Data` packets with per-query
compression. `backend: direct` remains fail-closed and stops before source I/O
when the provider is missing, unsupported, or uncertified. `dpone runtime
native-accel doctor` reports the selected direct backend, provider version,
protocol revision, supported compression methods, and fallback reason.

### Statistics-aware partition planning

Use statistics planning when a full snapshot should avoid heavy
`COUNT/MIN/MAX` scans for boundaries:

```yaml
source:
  options:
    partitioning:
      strategy: stats
      column: id
      target_rows_per_partition: 200000
      max_partitions: 16
      planner:
        mode: statistics
        stats_source: auto
        skew_policy: split_hot_ranges
        max_hot_partition_factor: 2.0
        min_partition_rows: 50000
        null_bucket: separate
```

For SQL Server, the first adapter reads histogram metadata through
`sys.dm_db_stats_histogram` and converts steps into approximately balanced
ranges. Hot histogram steps are split when the boundary values are splittable;
NULL values can be exported as a separate bucket. If a route points at a view or
histogram confidence is low, dpone emits `source_stats_low_confidence`, lowers
initial parallelism, and lets the governor ramp only after live evidence is
green.

### Adaptive snapshot optimizer

`source.options.native_transfer.snapshot` tunes the full bulk snapshot route:
export concurrency, load concurrency, source safety, ClickHouse merge pressure,
packet size, block size, compression, and fallback chain. The optimizer is
connector-neutral; MSSQL and ClickHouse are the first adapters.

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
```

Use `profile: safe_worker` on weak Airflow/KPO workers; it caps the effective
export and load parallelism to one while preserving eager cleanup and typed
staging safety. Use `profile: throughput` only after live certification shows
MSSQL and ClickHouse governors stay green. `compression: auto` selects LZ4 for
native TCP/client ingestion and keeps compression off for Python fallback; use
ZSTD only when benchmark evidence shows a network bottleneck and enough CPU
headroom.

`dpone plan` and `dpone perf advise` show:

- selected snapshot backend: `native_tcp`, `client`, `http`, or `python`;
- native TCP protocol backend: `direct` or `client`;
- compression, packet/block settings, and effective parallelism;
- partition planner mode and statistics confidence;
- fallback chain: `native_tcp -> client -> http -> python`;
- governor throttling reasons such as `snapshot_source_governor_throttled` or
  `snapshot_target_merge_pressure_throttled`;
- release gate and route certification status.

If BCP native is not certified for a schema, use the `typed_binary` ODBC row
stream fallback by setting `source_native_format: odbc_row_stream` and
`mssql_export_mode: row_stream`. It is safer than source-encoded TSV for
delimiter-heavy text, but it does not match BCP throughput. Use
`typed_raw`/`CustomSeparated` only when route certification proves that source
values cannot collide with the delimiter, row terminator, or quote grammar.

## Current accelerator path

- Install `dpone[accel]` in high-throughput runtime images when the route uses
  MSSQL BCP native -> ClickHouse Native. The base package stays pure Python.
- The optional provider is certified for primitive numeric, decimal, date,
  datetime/datetime2/smalldatetime, UUID, binary, and string layouts that map to
  ClickHouse Native scalar columns. Unsupported source types stay fail-closed or
  fall back according to `acceleration.mode`.
- Treat type coverage as a release gate, not a best-effort smoke. Native binary
  route certification must include max/min Decimal128 and Decimal256 values,
  Float32/Float64 finite extremes, nullable and non-nullable columns, Unicode
  text, tabs/newlines, binary bytes, UUID, Date/Date32, DateTime/DateTime64
  precision, FixedString padding and oversize rejection, and NULL versus empty
  string semantics.
- Run `dpone runtime native-accel doctor --format json` before release to
  confirm the selected backend, fallback reason, and provider version.
- Run `dpone runtime native-accel benchmark --manifest <path> --rows 10000
  --output <dir>` to produce a benchmark plan before live certification.
- Partition manifest persistence for resumable failed-partition retries.
- Auto-bound discovery guarded by a source-count reconciliation step.
- Optional staging-per-partition tables followed by a single final merge for heavily indexed SQL Server targets.

## ClickHouse native ingest modes

For MSSQL -> ClickHouse, prefer HTTP streaming when the ClickHouse HTTP port is reachable from the runner:

```yaml
sink:
  type: clickhouse
  options:
    clickhouse_bulk:
      mode: http
      http:
        host: clickhouse.example.com
        port: 8123
```

Local Docker example:

```bash
uv run python tools/mssql_stress.py \
  --rows 15000000 \
  --clickhouse-bulk-mode http \
  --clickhouse-http-host 127.0.0.1 \
  --clickhouse-http-port 18123
```

`clickhouse-client` is also supported when a client binary is available near the runner:

```bash
uv run python tools/mssql_stress.py \
  --clickhouse-bulk-mode client \
  --clickhouse-client-command "clickhouse-client" \
  --clickhouse-client-host clickhouse.example.com \
  --clickhouse-client-port 9000
```

For local Docker-only checks, `--clickhouse-client-command "docker exec -i dpone-it-clickhouse clickhouse-client"` works, but it adds Docker exec overhead and is not the preferred performance benchmark path.

### Verified local 15M result

On the local Docker/Apple Silicon setup, HTTP streaming avoided Python TSV parsing and improved MSSQL -> ClickHouse throughput from roughly `129k rows/s` to `783k rows/s` for 15M rows.

| Mode | Rows | Throughput |
|---|---:|---:|
| Python TSV parse + native driver | 15,000,000 | 128,972 rows/s |
| HTTP streaming `FORMAT TabSeparated` | 15,000,000 | 782,828 rows/s |

### Verified local 10k, 1M and 10M artifact

The latest local-live benchmark artifact is stored in
[Native transfer benchmark artifact: 2026-06-11](testing/native-transfer-benchmarks-2026-06-11.md).
The previous local-live release suite is stored in
[Native transfer benchmark artifact: 2026-06-09](testing/native-transfer-benchmarks-2026-06-09.md).
Historical benchmark context is kept in
[Native transfer benchmark artifact: 2026-06-08](testing/native-transfer-benchmarks-2026-06-08.md).
It includes separate correctness and throughput tables for:

- `Postgres -> MSSQL` native full-refresh transfer;
- `MSSQL -> ClickHouse` native full-refresh transfer;
- raw JSON artifact paths under `test_artifacts/live_certification/benchmarks/`.

New runs include `phase_metrics` and `transfers.*.artifact` diagnostics so the
operator can separate source export, target load/finalize and reconciliation
time instead of tuning from a single total duration.

### Native transfer tuning matrix

Use `tools/mssql_benchmark_suite.py` when you need a controlled matrix instead
of one-off runs:

```bash
PYTHONUNBUFFERED=1 uv run python tools/mssql_benchmark_suite.py \
  --rows 10000,1000000 \
  --partitions 1 \
  --export-workers 1 \
  --load-workers 1 \
  --batch-size 100000 \
  --bcp-path /opt/homebrew/bin/bcp \
  --optimizer-profile high_throughput_safe \
  --clickhouse-bulk-mode http \
  --clickhouse-http-host 127.0.0.1 \
  --clickhouse-http-port 58123 \
  --output-dir test_artifacts/live_certification/benchmarks/native_tuning_matrix_latest \
  --markdown-output test_artifacts/live_certification/benchmarks/native_tuning_matrix_latest/summary.md \
  --continue-on-fail
```

For release evidence, use the full manual profile:

```bash
PYTHONUNBUFFERED=1 uv run python tools/mssql_benchmark_suite.py \
  --rows 10000,1000000,10000000 \
  --partitions 1 \
  --export-workers 1 \
  --load-workers 1 \
  --batch-size 100000 \
  --bcp-path /opt/homebrew/bin/bcp \
  --optimizer-profile high_throughput_safe \
  --clickhouse-bulk-mode http \
  --clickhouse-http-host 127.0.0.1 \
  --clickhouse-http-port 58123 \
  --output-dir test_artifacts/live_certification/benchmarks/native_release_suite_latest \
  --markdown-output test_artifacts/live_certification/benchmarks/postgres_mssql_native_benchmark_summary.md
```

The suite writes:

- `summary.json` for automation and certification services;
- `summary.md` or the configured `--markdown-output` for human release review;
- one scenario JSON per row/partition/export-worker/load-worker combination.

The manual GitHub workflow `.github/workflows/live-certification.yml` exposes
the same runner through `run_native_benchmark_suite=true`,
`native_benchmark_rows`, and `native_benchmark_partitions`.

PostgreSQL -> MSSQL currently certifies one `COPY` statement per full-refresh
snapshot. Keep `--partitions 1`: independent partition exports do not share an
MVCC snapshot and are rejected before source I/O. Parallel partition tuning is
valid only after a shared-snapshot coordinator is implemented and certified.

Interpretation:

- compare partition/worker matrices only after the source route provides a
  certified shared-snapshot coordinator;
- for the current PostgreSQL -> MSSQL route, tune the single native `COPY` and
  BCP/finalize path instead of increasing export workers;
- lower target concurrency when target part pressure, temp disk saturation or
  lock waits increase on routes that support parallel loading;
- compare only scenarios from the same runner and the same Docker/native service profile.

Postgres -> MSSQL benchmarks often show the target side as the bottleneck:
PostgreSQL `COPY` can export far faster than SQL Server can `bcp` into staging
and finalize. When `postgres_to_mssql.target_load_finalize` dominates, tune SQL
Server first: `bulk.bcp.batch_size`, `bulk.bcp.table_lock`, transaction log
throughput, staging index shape, target lock budget, finalizer policy and
post-load statistics.

`--optimizer-profile high_throughput_safe` maps to:

```yaml
options:
  native_transfer:
    optimizer_profile: high_throughput_safe
```

The profile sets safe defaults for MSSQL bcp packet/batch/timeout values and
ClickHouse HTTP/insert settings. Explicit `bulk.bcp.*` and
`clickhouse_bulk.*` values override profile defaults.
