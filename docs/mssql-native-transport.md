# Bounded native ClickHouse to MSSQL transport

## SqlClient transport development status

Offline plans accept the separately authored `mssql_sqlclient` backend. A
deployment can compose that backend through
`compose_sqlclient_native_runtime` and the closed
`SqlClientNativeRouteDeployment` contract. The deployment must inject the
complete admitted worker, durable state, publication, retirement, custody and
checkpoint capabilities. Missing or inconsistent capabilities fail before
source access, and the SqlClient composer cannot fall back to BCP. Installing
.NET alone does not enable execution. Omit `transport` for the existing BCP
route; unresolved attempts must retain their original backend.

The SqlClient policy requires `input: rows|arrow` and
`max_worker_address_space_bytes` in 8–16 GiB. Optional
`max_input_batch_bytes` defaults to 64 MiB and admits 1–256 MiB. It bounds
accounted retained input buffers, including Arrow buffers and growth overlap,
not process RSS. Row limits and operating-system resource limits remain separate.
This field is rejected for `mssql_python`, whose serialized policy is unchanged.

Plans report the Linux Arm64 candidate and the required companion,
.NET 8.0.31, Microsoft.Data.SqlClient 7.0.2 and, for Arrow input,
Apache.Arrow 23.0.0. These requirements describe implementation admission;
they are not a published installation recipe or route certification. The Python
worker explicitly rejects SqlClient requests rather than selecting its own SDK.

For runtime maintainers, the approved CREATE/departure handoff has a narrower
success condition than a completed load. It must invoke CREATE on the original
fresh attempt, acknowledge six separate helper records, observe the matching
zero process exit and close owned resources within the original deadline.
The records are launch intent, registration, credential intent, result, local
exit and exclusion. Their presence alone does not authorize `Prepared`, a bulk
writer, publication or a checkpoint. See the
[TDS recovery decision](adr/0072-bounded-tds-importer.md) for the authority model.

If this handoff becomes unknown, retain its evidence and owned resources for
reconciliation. Do not replay CREATE on the same attempt, switch its backend,
drop a table based only on its name, or extend the captured containment budget.
Recovery cannot turn a previously observed result into fresh execution authority.
No standalone end-user execution command or public manifest-only activation is
available for this component. Platform code must use the production composition
API and provide every environment-owned capability explicitly.

The internal pre-grant writer boundary now uses a second, separately
credentialed SQL connection in a contained helper process. The helper publishes
its own admitted incarnation before the writer capability is claimed and accepts
one bounded command naming the announced writer session and nonce. The parent
then acknowledges the independent writer observation before recording grant
intent. The complete production composition continues from this boundary
through the one-shot grant, `SqlBulkCopy`, remote settlement and typed
verification. A failed or ambiguous exchange retains bounded cleanup custody
and cannot be replayed as execution authority.

The next internal boundary can consume that acknowledged intent once. It sends
the exact private grant at most once, retains one EOF-confirmed result, records
`RESULT` and exact reaped `LOCAL_EXIT` evidence, acknowledges lifecycle
`Exited`, and confirms closure of all local resources. Only success with exit
`0` or a handled worker failure with exit `1` is admitted; every other pairing
is UNKNOWN and cannot resend. The resulting custody is explicitly *locally*
exited: it does not prove remote SQL settlement, stage contents, publication or
checkpoint completion. Those later facts come only from the matching settlement
and parent route. Manifest-only runtime selection remains unavailable.

The [departure evidence design](delivery-acceleration/sqlclient-departure-evidence.md)
explains strict v1 behavior and the explicit v2 work for SQL Server session-number
reuse. Neither version's evidence alone enables the bulk route.

## Existing BCP composition

This opt-in Python composition API loads one ClickHouse query through bounded
native BCP files, verifies independent staging tables, and publishes once through
the existing MSSQL transaction finalizer. It supports `full_refresh` and an
explicit UTC `partition_replace` window. Existing character-spool routes retain
their defaults. Retained evidence for dpone 0.80.0 at commit
`6ae541d38ac223327d7edb23510859df91173bda` establishes scoped local Docker
correctness and controlled recovery: six non-binary profiles, 64 rows per
fidelity cell, full refresh and explicit UTC partition replacement, with
encoding/import policies 2/1 and 1/2. Production workload performance,
independent source DDL and target-writer governance, and hard-failure recovery
remain **UNVERIFIED**. These results do not certify a new deployment.

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

By default, only one local plain `MergeTree` table in an `Atomic` database is admitted.
Explicit `native_transfer.source_read.mode: raw_single_query` also admits local
`ReplicatedMergeTree`, `ReplacingMergeTree` and `ReplicatedReplacingMergeTree`.
It preserves the rows returned by one physical session with query-level `final=0`;
an inherited `final=1` cannot silently deduplicate the result. Denied overrides
fail. This is neither a deduplicated business snapshot nor a replica-freshness
guarantee. See the [raw composition guide](delivery-acceleration/raw-window-composition.md)
for a synthetic example, exact policy and recovery requirements. The
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

Before creating an affected chunk table or starting BCP, the canonical chunk
importer scans the integrity-bound native file and rejects an `nvarchar(max)`
value consisting of exactly one NUL character with
`mssql_native.single_nul_not_representable`. Synthetic native BCP testing showed
that this value arrives as empty text. Empty strings, SQL NULL and longer strings
containing NUL are distinct values; the gate does not substitute or remove any
characters. Correct the source value only under an explicit data contract, then
start a new delivery attempt. Repeating the same file cannot resolve this error.

Admission adds a local sequential decoding pass and an integrity revalidation
read before BCP. Runtime composition supplies the configured row-byte limit; standalone
importers without that optional limit use the verified file size as their finite
bound. Exact row count, byte count and file hash must agree at EOF. Independent
post-import typed reconciliation remains mandatory. Earlier chunks may already
exist when a later chunk fails; existing ownership and recovery rules still apply.
This check applies to `MssqlNativeChunkImporter`, not generic native-file staging.
Shared native encoding and TDS decoding retain their original value domain;
this BCP check is not evidence of TDS route certification.

Bounded delivery reuses frame sizes, projects canonical metadata in the prepared
INSERT and computes business/full digests in one iterator. All four raw checks,
the independent prepared prepublication check and the finalizer target-clock
UPDATE remain. Follow [delivery acceleration](delivery-acceleration/index.md) for
structural evidence, optional observations and measurement instructions. These
changes establish no measured acceleration and do not enable native partition
SWITCH. Existing callers need no manifest or recovery migration. Explicit raw
mode uses version-2 chunk journals; keep the original mode for recovery and
settle these invocations before downgrading to a version-1-only reader.

## Compose the runtime

The optional SQLClient implementation has an exact authorization root used by
the production composition API. It accepts the original PREPARED `TdsAttempt`, performs
all deterministic dependency and GRANT-material checks before registering the
GRANT owner, then delegates GRANT hold/release/settlement and restricted-writer
VERIFY/settlement to their existing phase services. The VERIFY coordinator is
allocated only after its exact directory reservation. Callers cannot supply a
prepared transition or manufacture the returned `RestrictedWriterVerified`.

Deployments construct that PREPARED authority with
`compose_sqlclient_prepared_attempt_factory` and one admitted
`SqlClientPreparedAttemptDeployment`. The composer pins and verifies the
retained native file, derives stable CREATE operation identities from the exact
attempt, runs CREATE departure and OBSERVE, and returns only after the existing
preparation service acknowledges `PREPARED`. No route-specific callback is
needed. Parent input release uses `SQLiteSqlClientInputCustodyJournal`; an
unknown intent can be reconciled only with an independently observed exact
release receipt and can never repeat the release effect.

Source-free parent settlement is assembled with
`compose_sqlclient_native_parent_capabilities`. The composer binds one schema-v4
`NativeChunkJournal` to its publication view, lifecycle and directory observers,
inspection index, retirement authorization, ordered file-custody release and
checkpoint CAS. Retirement progress uses the same fenced `WindowStore` and
initializes `VERIFIED` only from the exact current lifecycle and directory
snapshots; it never invents or equates their independent revisions. Deployments
still inject the SQL Server retirement effects, rollback observation, evidence
reader and durable checkpoint provider because those capabilities own target
credentials and environment-specific storage.

Assemble one `SqlClientNativeInvocationDeployment` from that parent deployment,
the admitted import capabilities, exact stage plan and target-owned services.
Pass an invocation factory plus source, preflight, quality and evidence hooks in
`SqlClientNativeRouteDeployment` to `compose_sqlclient_native_runtime`. The
composer verifies the shared journal, store, lease, cancellation token, custody,
transport, capacity and implementation identity before returning runtime
bindings. Source credentials remain inside the injected context-manager closure;
they are never stored in the deployment DTO, state or evidence. Schema-v4 parent
settlement owns checkpoint advancement, so the generic runtime checkpoint hook
is deliberately unreachable for this route.

Failed imports use a single nominal settlement capability. It persists the
exact contained predecessor before DROP, reconciles uncertain effects under the
canonical DDL lock, and admits a successor only after exact absence, durable
`RETIRED` state, closed directory admission and actor release. If the process
stops after actor release but before recording capacity, replay reads the
durable terminal prefix first and completes only the capacity receipt; it does
not reacquire invocation custody or repeat the SQL effect. Legacy v1 eligibility
records remain readable audit data and never authorize retry.

Contract tests cover the boundary from a controlled PREPARED producer through
remote writer settlement. Release qualification must additionally exercise the
exact candidate source through the production composer for both a narrow input
and a 100-column input, including local writer exit, independent writer-session
departure, typed stage readback, publication, retirement, custody release and
checkpoint advancement. Preparation baseline admission must be bound to one
pinned SQL Server image/build, database profile and frozen query producer using
two distinct fresh-storage controls, verified TLS, equal reviewed projections
and complete teardown. Evidence from another commit or a component-only P10f
run does not satisfy either gate. Such a qualification does not extend to
another SQL build, collation, platform or managed service. The backend requires
explicit Python composition and remains inactive for manifest-only
configuration. BCP remains the default.

P10f launches a fresh contained management helper after P10e. Its public request
uses the input descriptor stored in writer registration, whose digest must match
the writer-observation binding. The helper performs no retry or publication. It
checks the exact stage identity around bounded readback, computes the same
versioned typed multiset digest as preparation, closes SQL, and returns one
request-bound result. Parent evidence binds startup, request, result, reaped
local exit, implementation and admission hashes before lifecycle `VERIFIED`.
Local descriptor numbers from preparation and the inherited writer process are
not interchangeable identities.

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

`native_transfer.execution.native_chunks` requires `max_total_encoded_bytes` and
`stage_allocated_bytes_stop_threshold`. Defaults are 65,536 rows, 16 MiB per chunk,
1 MiB per business row, two pending chunks and 1,024 staging tables. Parallelism
defaults to 1 and is 1–64. Optional `encoding_parallelism` and
`import_parallelism` under `native_chunks` independently fall back to
`execution.chunking.parallelism`. Authored overrides must be strict integers
1–64; explicit YAML null is invalid. Python keyword-only overrides default to
`None`, retaining all eight historical positional fields. Follow the
[concurrency how-to](delivery-acceleration/concurrency.md) and its
[new example](../examples/native/clickhouse-to-mssql-native-concurrency.yaml)
for resolved planner output and upgrade guidance. The pending bound is independent of the cumulative encoded-byte limit.
One prepared table also counts toward the staging-table limit. Rows are limited
to 1–1,000,000; chunk bytes to 1–1 GiB; row bytes must not exceed chunk bytes;
pending chunks and parallelism are 1–64. Required cumulative limits and staging
table counts are positive integers. Booleans and unknown limit keys are rejected.

The shared retained-work capacity is `C = max(E, I) + max_pending`, where E/I
are the effective encoding/import limits. Import concurrency bounds file
import/verify tasks, not all target connections. The local payload bound is
`(C + 1) * max_bytes`, plus
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

For a first deployment, run admitted synthetic NULL, duplicate, Unicode, Decimal,
empty-window and outside-window invariance cases in an explicitly approved
disposable environment. Record authored binary source mapping as unsupported;
encoder-only binary fixtures do not enable the canonical route. Test interrupted imports and lost commit acknowledgements.
Record exact source commit, dependency/server versions and observed resources.
Follow the [approved design](feature-design-clickhouse-mssql-bounded-native-v1.md)
and [route certification standard](connector-certification.md) before promoting a
specific environment. Existing manifests need no migration.
