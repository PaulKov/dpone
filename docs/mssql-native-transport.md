# Bounded native ClickHouse to MSSQL transport

This opt-in Python composition API loads one ClickHouse query through bounded
native BCP files, verifies independent staging tables, and publishes once through
the existing MSSQL transaction finalizer. It supports `full_refresh` and an
explicit UTC `partition_replace` window. Existing character-spool routes retain
their defaults. Live interoperability and performance certification are
**UNVERIFIED**; hermetic tests do not establish a production speed improvement.

## Prepare and configure

The platform owner supplies dedicated connections, durable fenced state, source
DDL exclusion, target writer exclusion, transaction admission, quality checking,
evidence persistence and source checkpoint persistence. A manifest cannot supply
these capabilities. The ordinary runner rejects this mode with
`mssql_native.composition_required` unless its `native_runtime_factory` is set.

The YAML example is for plan inspection; execution requires the platform
capabilities described below. Use [the native example](../examples/native/clickhouse-to-mssql-native.yaml)
and inspect it with:

```bash
dpone plan examples/native/clickhouse-to-mssql-native.yaml --format md
```

Supply an explicit execution `interval.interval_end` in UTC when constructing the
load configuration. The example replaces the complete preceding day, including
an empty source interval. Rows outside `[start, end)` and NULL window values remain
unchanged. The interval never comes from values found in staging. Full refresh
omits `strategy.window` and replaces the complete target under its existing
catalog-preservation policy. Do not combine a native window with legacy
`partition` options.

Only one local plain `MergeTree` table in an `Atomic` database is admitted. The
mandatory source `schema_guard_factory` must exclude ALTER, RENAME, EXCHANGE,
DROP/recreate and relevant access-policy changes from metadata admission until
source cleanup. It does not need to freeze ordinary DML. A no-op context manager
is suitable only for synthetic tests. The source records the nonzero table UUID
and one query ID. A query ID is not a reusable snapshot token.

The finite source types include scalar integers through UInt64, float, Decimal
through precision 38, String, UUID, Date/Date32 and UTC DateTime/DateTime64 through
six fractional digits, with nullable variants. Temporal values travel as integer
microseconds to avoid driver floating-point conversion. Unsupported engines,
views, computed columns, custom SQL, deduplication, CDC and uncomposed hooks fail
admission. All ordinary table columns are extracted; authored column selection is rejected.
NaN, Infinity, numeric overflow and fractional truncation are rejected.
Text decoding is strict. The native encoder can preserve arbitrary bytes for an
already admitted binary wire contract, but the canonical ClickHouse planner maps
`String` to Unicode text. There is currently no authored binary-semantic mapping
from ClickHouse `String` to MSSQL `varbinary`; a physical type override is rejected
as a cross-family conversion. Binary encoder tests therefore do not certify that
end-to-end source route. Its live binary profile remains **UNVERIFIED**.
VARCHAR/CHAR are currently rejected by the importer, including when a collation
is configured.

Bounded delivery reuses frame sizes, projects canonical metadata in the prepared
INSERT and computes business/full digests in one iterator. All four raw checks,
the independent prepared prepublication check and the finalizer target-clock
UPDATE remain. Follow [delivery acceleration](delivery-acceleration/index.md) for
structural evidence, optional observations and measurement instructions. These
changes establish no measured acceleration and do not enable native partition
SWITCH. Existing callers need no manifest or recovery migration.

## Compose the runtime

`NativeMssqlRuntime` accepts these required application capabilities:

| Capability | Responsibility |
|---|---|
| `store`, `target_id` | Durable `WindowStore`, stable physical target identity and fenced CAS |
| `bindings(config, owner, lease, cancelled)` | Target-only restoration of one invocation and `NativeRuntimeBindings` |
| `source(config, binding)` | Owned context manager producing the existing `LoadPayload`; no source access during recovery |
| `preflight(config)` | Local and target capacity, permissions, required service admission |
| `quality(config, handle, lease)` | Configured quality gates against prepared staging before publication |
| `evidence(config, result, context, lease)` | Durable idempotent evidence bound to the exact commit receipt |
| `advance_state(config, result, lease)` | Fenced idempotent checkpoint CAS after evidence persistence |

The binding contains a service, `NativeStageContext` and fresh
`MssqlTransactionAdmission`. Set `context.cancelled` to the supplied event. Restore
stable invocation, query, schema, wire and interval identities before opening the
source. The context journal factory creates
`dpone.adapters.mssql_native_chunks_journal.NativeChunkJournal` against the same
store and lease. Do not create a new generation merely because a response was
lost.

Construct `MSSQLSink` with
`native_staged_load_service_factory=lambda sink: MssqlNativeStagedLoadService(sink,
MssqlNativeStagePreparer(sink, context_factory))`. This constructor injection avoids
mutating an already constructed sink. Use `compose_native_stage_context` from
`dpone.runtime.sinks.mssql_native_composition` to assemble the independent importers,
writer scopes, journal, verification, cleanup and capacity checks. Pass the existing `BcpOptions` class from
`dpone.runtime.connectors.mssql_bulk` as `bcp_options_factory` at the application
composition root. Its target
session must use the configured staging/target database; differing staging and
target databases are not admitted by this helper. The context owns:

- `BoundedNativeChunks` with a context-manager importer factory. Every importer
  owns and closes a separate target connector, native wire columns,
  `MssqlNativeEncoder.encode_row`, and `store.assert_lease`.
- `native_stage_writer_scope` on that same connector, keyed by the stable attempt
  identity. Old writers must settle before retry or removal. The preparation
  scope covers verification through commit and owns an independent same-database
  session created by `target_connector.open_session()`. The context releases its
  lock and closes that session on every exit. Reusing the target connector or
  opening a different database fails before preparation begins.
- Real receipt verification and cleanup callbacks using importer `inspect` and
  `settle`; cleanup checks the table's persisted owner binding.
- Capacity checks before extraction, preparation and publication. Use
  `require_native_target_capacity` to require catalog, volume and log-space
  observations; denied or missing observations are admission failures.
- The existing MSSQL schema preplanner, transaction admission and extraction
  lifecycle. The completed payload persists schema, physical source UUID,
  lifecycle, mutation plan and operation identity for source-free recovery.

Pass the resulting runtime through
`DefaultProcessRunner(native_runtime_factory=lambda process_config: runtime)`.
The factory must select the runtime for the actual route. The executable
[composition-order tests](../tests/test_mssql_native_runtime.py) demonstrate
callback ordering and recovery, and the
[real-finalizer tests](../tests/test_mssql_native_staged_finalizer.py) exercise
the concrete preparation scope with commit, lost acknowledgement and unknown
outcomes using synthetic SQL fixtures. When COMMIT acknowledgement is lost,
the finalizer can close its own connection and confirm the exact receipt while
the preparation lock remains held. A confirmed commit continues to evidence and
checkpoint completion; an unknown outcome retains its existing recovery path.
These fixtures are not deployment authorities.

## Resource limits and observations

`execution.native_chunks` requires `max_total_encoded_bytes` and
`stage_allocated_bytes_stop_threshold`. Defaults are 65,536 rows, 16 MiB per chunk,
1 MiB per business row, two pending chunks and 1,024 staging tables. Parallelism
is 1–64. The pending bound is independent of the cumulative encoded-byte limit.
One prepared table also counts toward the staging-table limit. Rows are limited
to 1–1,000,000; chunk bytes to 1–1 GiB; row bytes must not exceed chunk bytes;
pending chunks and parallelism are 1–64. Required cumulative limits and staging
table counts are positive integers. Booleans and unknown limit keys are rejected.

The local payload bound is `(parallelism + max_pending + 1) * max_bytes`, plus
format/receipt overhead. The executor checks observed filesystem capacity with a
1 GiB reserve before worker startup. Driver allocation and process RSS are
separate observations; encoded-byte limits do not guarantee physical RSS caps. Application preflight also checks capacity
before extraction. SQL allocation counts reserved pages of all `dpone_native_` staging tables in
the configured database, including other invocations. SQL allocation and log
headroom checks are observations, not
exclusive reservations. Already active imports can overshoot an observed SQL stop
threshold; exceeding it prevents successful stage completion/publication.

The journal stores per-phase worker observations, encoded bytes, rows, immutable
file hashes and typed multiset digests. Effective parallelism uses observed worker
active time and wall time. A digest is probabilistic evidence; small correctness
certification cases must also compare exact typed multisets. Do not report the
configured worker count as measured overlap.

## Diagnose and recover

| Persisted state | Action |
|---|---|
| Partial extraction/staging | Settle owned attempts; report `reextract_required`; start a new invocation from a new complete query |
| `stage_complete` | Reverify contiguous receipts and prepare without reopening ClickHouse |
| Publication `preparing` | Reconcile the owned prepared table and rebuild from completed chunks |
| Publication `prepared` | Reverify, run quality, then publish once |
| Publication `publishing` | Inspect the exact target commit receipt first; unknown outcome retains resources and blocks replay |
| Publication `published` | Persist evidence, then advance source state |
| `evidence-complete` | Retry idempotent state advancement |
| `succeeded` | Retry owned cleanup without source reads or republishing |

Chunk phases and publication phases are nested journal records. `stage_complete`
is one CAS after EOF, contiguous verification and completion metadata. A verified
individual chunk does not authorize publication. Up to two import retries reuse immutable bytes after previous-writer settlement
when the importer classifies a failure as `WindowTransientError`. Unclassified
vendor errors stop the run. Cancellation, exhausted
retries and lease loss cannot manufacture EOF or a target success receipt.

For a first deployment, run synthetic NULL, duplicate, Unicode, binary, decimal,
empty-window and outside-window invariance cases in an explicitly approved
disposable environment. Test interrupted imports and lost commit acknowledgements.
Record exact source commit, dependency/server versions and observed resources.
Follow the [approved design](feature-design-clickhouse-mssql-bounded-native-v1.md)
and [route certification standard](connector-certification.md) before promoting a
specific environment. Existing manifests need no migration.
