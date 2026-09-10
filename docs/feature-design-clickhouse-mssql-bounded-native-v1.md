# Feature design: bounded native ClickHouse to MSSQL transport

- Status: RESEARCHED
- Owner: dpone maintainers
- Target release: unassigned
- Last verified: 2026-09-10

This specification is for framework maintainers and connector authors. It proposes
an opt-in replacement for the complete character spool on bounded ClickHouse to
MSSQL loads. It is not an implemented capability or a production migration guide.
See the [current route guide](source-sink/clickhouse-to-mssql.md) for usable behavior
and the [feature design standard](feature-design-standard.md) for approval.

Independent architecture review resolved the initial admission boundary to one
ClickHouse query snapshot and resumable completed staging. Interrupted partial
extraction requires complete re-extraction. This deliberately does not promise a
reusable ClickHouse snapshot across processes. Track runtime patch removal
separately in the [removal inventory](runtime-patch-removal-inventory.md).

## Executive summary and existing execution path

The audited base is v0.74.36, commit
`3977ca2d04ca5dbcf3338d7c31faff8a1199549e`. The following are code observations,
not measured performance results:

1. `StreamingRowsArtifact.materialize` calls the optional staging manager
   `insert_streaming_rows` method.
2. `MSSQLStagingManager.insert_streaming_rows` calls `insert_rows`, which calls
   `MssqlCharacterSpool.import_rows`. The complete stream is serialized before
   import. Ordinary raw staging uses character types before native projection.
3. `NativeTransferCapabilityPlanner` has a typed row-stream specialization for
   MSSQL to ClickHouse. ClickHouse source and MSSQL sink return
   `clickhouse_stream_export_not_declared` and
   `mssql_stream_staging_not_declared`; RowBinary is a ClickHouse ingest format.
4. Independently, `supports_staged_load` requires `stage_payload`,
   `finalize_staged_load`, and `abort_staged_load`. `MSSQLSink` has none of those
   methods. `PayloadLoader` therefore reports `staged_load_port_unavailable`
   and chooses `legacy_post_finalize`. This warning does not mean the MSSQL
   transaction finalizer itself is non-atomic.
5. The existing `mssql_typed_bcp` codec emits length-prefixed SQLCHAR UTF-8,
   including text representations of numbers and temporal values. It is not a
   native binary value encoder and must not be presented as one.

The [approved throughput design](feature-design-clickhouse-mssql-strategy-throughput-v1.md)
deliberately specifies one complete immutable character spool and one BCP process.
The [bounded streaming implementation](feature-design-bounded-streaming-window-v1.md)
supports MSSQL to ClickHouse typed transport and PostgreSQL to ClickHouse windows.
[ADR 0057](adr/0057-bounded-window-atomic-publication.md) initially admits a local
ClickHouse Atomic target. None of these establishes the requested MSSQL capability.

## Personas and customer journey

| Persona | Goal | Success signal |
|---|---|---|
| Data engineer | Load a wide, complete interval efficiently | Explicit native route and resource limits in the plan |
| Operator | Recover interrupted staging safely | Verified chunk reuse or an explicit re-extraction requirement |
| Data owner | Preserve exact rows and target visibility | Duplicate parity, outside-window invariance, one publication |
| Connector author | Reuse bounded execution | Narrow source, transport, staging and publication contracts |

Journey: discover the route; verify source-snapshot and target-transaction
prerequisites; configure the opt-in policy; inspect admission before extraction;
run synthetic correctness checks; inspect observed phase evidence; recover using
the original invocation; promote only an exact revision with environment-specific
evidence. Existing manifests retain their existing behavior.

## Scope and non-goals

Include bounded concurrent encoding/import, native typed chunks, isolated staging
attempts, prepublication quality, durable recovery and one transactional interval
replacement. Full refresh is a second complete-boundary variation of the same
transport; it has no interval predicate and retains the existing catalog policy.

Exclude CDC and cursor inference, arbitrary independent source snapshots,
deduplication, append/merge route enablement, automatic target index removal,
new database drivers, import-time patches, deployment changes and publication of
a package. A partition loop must not publish individual target intervals.

## Public contract and compatibility

### CLI and Python API

Keep existing commands, exit codes and defaults. An explicit unsupported native
route fails during planning/admission rather than silently selecting character
transport. The composition root supplies a single-query source adapter and
independent worker connector factories; credentials remain outside the manifest.

Expose staged loading through the existing three-method port. Stage returns an
owned prepared handle with verified row authority; finalize accepts that same
handle and invokes one transaction finalizer; abort may remove only unpublished,
settled attempts belonging to the handle. Unknown publication cannot be aborted.
The combined `load` entry point delegates through the same lifecycle so there
are no competing implementations. Unsupported strategies retain their explicit
compatibility behavior; method presence alone must not admit them. Add optional
`supports_staged_load_for(load_config)` admission to existing detection. Sinks
without it retain their current three-method detection. MSSQL admits the new
native path only for full refresh and explicit interval partition replacement;
its legacy configurations retain existing governance selection.

### Manifest and schema proposal

Reuse `source.options.native_transfer.wire` and
`source.options.native_transfer.execution`. Add `binary_format: mssql_native`
to the existing wire vocabulary, with route/type admission. Add
`execution.chunking.mode: bounded_stream`, reusing `parallelism` and
`checkpointing: resumable`. Unlike `bounded_window`, it partitions one acquired
stream into physical chunks and does not open independent interval queries.
Full refresh has no window block; partition replacement requires an explicit
fixed `sink.strategy.window`. Preserve existing `bounded_window` composition
and behavior for other routes. Introduce no top-level transport registry.

Freeze physical limits in a new optional `execution.native_chunks` object:

| Field | Default | Validation and meaning |
|---|---|---|
| `max_rows` | 65536 | Integer 1–1000000, excluding booleans; rows per physical chunk |
| `max_bytes` | 16777216 | Integer 1–1073741824; encoded bytes including framing |
| `max_row_bytes` | 1048576 | Positive integer no greater than `max_bytes` |
| `max_pending` | 2 | Integer 1–64; waiting chunks, excluding active workers |
| `max_total_encoded_bytes` | required | Positive hard cumulative payload limit |
| `max_staging_tables` | 1024 | Positive cap on simultaneously owned stage objects |
| `stage_allocated_bytes_stop_threshold` | required | Observed allocation threshold; not an exclusive reservation |

`parallelism` retains its 1–64 bound. Enforce aggregate spool admission for
`(parallelism + max_pending + 1) * max_bytes` plus known format/receipt overhead.
Check SQL allocation before scheduling imports and after their settlement. A
crossed allocation threshold stops new work and prevents publication; active
imports may overshoot it, and evidence records that overshoot. Count all attempt
and prepared stages. Observe database/log headroom and required permissions
before extraction and publication. These are capacity checks, not reservations
against unrelated writers. Strict physical storage caps require separately
provisioned database/storage limits. Driver allocation and RSS are observed
separately and are not guaranteed by encoded-byte limits.

These proposed fields are not accepted by v0.74.36. Update the schema producer,
parser, plan diagnostics and reference together after approval. Keep the current
one-spool regression test and the current route example as compatibility tests.

### Artifacts and migration

Define `dpone.mssql_native_chunks.v1` receipts binding invocation, source query,
window, schema, wire profile, logical chunk, physical ordinal and owned attempt.
Bind immutable chunk bytes before import, then vendor count, rejects, independent
stage count and typed multiset digest. Record source EOF separately from chunk
completion. Chunk identity never deduplicates business rows. Query identity binds
the extraction and schema but is not a durable source snapshot token.

Keep identity and content evidence deterministic by canonical ordering of logical
chunk and physical ordinal, independently of worker completion. Timings are
observations and do not enter identity. Existing artifact schemas are unchanged.
Migration is explicit opt-in after acceptance; rollback restores the old manifest
and package only after unresolved publication has been reconciled.

## Detailed algorithm

1. Resolve the explicit UTC half-open interval once. Validate complete coverage,
   source types, target catalog, resource reservations and transaction authority
   before row I/O. Empty intervals and unsupported capabilities fail explicitly.
2. Acquire fenced invocation ownership. Admit one directly connected local plain
   MergeTree source table in an Atomic database, with ordinary explicit columns.
   Execute exactly one data-bearing SELECT for the full table or frozen interval.
   Reject views, remote/Distributed tables, custom queries, joins, FINAL,
   sampling, LIMIT/OFFSET, semantic MergeTree variants, unsupported temporal
   representations and source policies that cannot be proven compatible.
   The query owns its snapshot only until EOF; business changes spanning separate
   mutations are not claimed to be an atomic source transaction.
3. Persist the plan using existing durable CAS journal contracts. Derive logical
   physical chunk ordinals from this invocation's observed row order, never
   LIMIT/OFFSET over a reopened query.
4. One producer owns the source iterator. Pull typed frames under backpressure
   and validate range, arity, nullability and value bounds. Explicitly spawned
   processes encode native files; never fork inherited connections. Bound IPC
   frame bytes independently, with the same configured `max_bytes` ceiling.
   Import workers own independent target connections and attempt identities.
   The parent owns scheduling, source lifetime, fencing and journal writes.
   Never write a full-stream character intermediate.
5. Seal each bounded file, persist its receipt, and BCP it into its own typed
   staging attempt. File imports may overlap encoding of later chunks. Do not
   share mutable connection state, reject paths or count-delta authority.
6. Compare vendor count, reject status, server count and typed content evidence.
   Persist verified attempt receipts with fencing before releasing local files.
   Persist `stage_complete` with atomic CAS only after source EOF, contiguous
   ordinal coverage and verification of every attempt. Bind total rows, schema,
   content evidence and ordered receipt digests. Missing or duplicate ordinals fail.
7. Prepare a typed replacement stage using `UNION ALL` semantics and the existing
   native lineage authority. Reject schema drift and authored incompatible quality
   policy. Run source/staging quality before any target publication.
8. Enter the existing MSSQL serializable transaction and target fence. Revalidate
   catalog and receipt identity. For a bounded interval, delete exactly the frozen
   predicate and insert verified replacement rows; an empty source still deletes
   that interval. Preserve NULL-window rows and all outside-window rows. Full
   refresh uses its existing transaction-safe strategy and catalog checks.
9. Write the authoritative target receipt in that same transaction and commit
   once. Never use staging-discovered values as authority for an empty interval.
   Target readers under supported isolation observe a committed old or new result;
   dirty reads are outside this guarantee.
10. Reconcile a lost commit acknowledgement through the target receipt before any
    retry. Persist required evidence, then advance source state with CAS/fencing.
    Cleanup runs only after publication status and owned-resource settlement are
    known. A postcommit evidence failure requires completion of that publication.

```text
admit -> fence -> single query snapshot -> durable plan
  -> bounded native chunks -> isolated verified attempts -> stage_complete
  -> prepublication quality -> one transaction and receipt
  -> reconcile commit -> durable evidence -> source state
```

### State, retries and cancellation

Use planned, staging, verified, publishing, published, evidence-complete and
succeeded states with failed/outcome-unknown branches. Freeze their serialized
representation before implementation. A physical attempt may be reused only
after server identity, rows, digest and invocation binding are reverified.

Allow at most two transient import retries per physical chunk, persisted durably.
Fence and join an old writer before replacing an unverified attempt. Contract,
schema, authentication, quality and capacity failures are terminal. On cancel,
stop pulling, close source iterators, cancel/join workers and retain unresolved
attempt metadata. Cleanup failure does not replace the original error.

Before `stage_complete`, a process restart fences/settles all attempts, proves no
publication began, and discards all extraction stages before starting a new
invocation. Return `reextract_required`; retain the old audit record. Never resume
a mutable source at a row offset. During a running query, retry an import only
from its retained immutable file; never reopen the source to reconstruct a prefix.
After `stage_complete`, reverify stages and resume publication without a source
connection. Missing or altered stages fail closed. After publication intent,
inspect the target receipt first. Published recovery only completes evidence/state.

### Type and edge-case contract

Share the finite native framing authority with `native_wire_mssql_framing` and
the existing MSSQL native decoder; add a separate checked encoder responsibility.
Generate vendor format metadata from exact native target types. Do not infer a
wire representation from generic Python `str` conversion.

Test integer boundaries including UInt64, Decimal precision/scale, binary bytes,
Unicode, UUID order, dates and temporal ticks. NULL, empty text and literal NULL
markers remain distinct. Reject unsupported nested types, NaN/infinity profiles,
overflow and fractional truncation. DateTime64 precision above Python datetime
precision requires a lossless source representation or rejection before extraction;
mapping to `datetime2(7)` alone cannot authorize discarded fractional digits.

Duplicate rows, including duplicates spanning workers, retain multiplicity.
Source NULL-window values lie outside the selected interval. A valid nonzero
interval containing zero source rows replaces that interval with an empty set;
an explicit incompatible min-rows quality gate rejects before publication.

### Proposed synthetic migration fragment

This is a design-review fragment to merge into a future valid manifest. It is
not executable on v0.74.36. It deliberately contains no connection binding;
the composed application supplies connection factories and source authority.
The schema and a complete tested composition example must land before migration.

```yaml
source:
  type: clickhouse
  options:
    native_transfer:
      wire:
        mode: typed_binary
        binary_format: mssql_native
      execution:
        chunking:
          mode: bounded_stream
          parallelism: 4
          checkpointing: resumable
        native_chunks:
          max_rows: 65536
          max_bytes: 16777216
          max_row_bytes: 1048576
          max_pending: 2
          max_total_encoded_bytes: 1073741824
          max_staging_tables: 1024
          stage_allocated_bytes_stop_threshold: 4294967296
sink:
  type: mssql
  strategy:
    mode: partition_replace
    atomicity: target_atomic
    window:
      column: observed_at
      anchor: data_interval_end
      lookback: P7D
      timezone: UTC
```

Use a fixed aware interval end in execution context. Do not silently replace it
with wall time or a target-derived cursor. Future plan output must show actual
admission and the selected transport, not merely echo this request.

## Architecture and ADR extension

| Component | Responsibility |
|---|---|
| Existing bounded executor/journal | Logical ownership, retry budget, durable ordering |
| ClickHouse single-query adapter | One acquired query snapshot and producer lifecycle |
| Native MSSQL encoder | Pure typed value/framing validation |
| Bounded chunk transport service | Backpressure, immutable files, worker lifecycle |
| MSSQL staging service | Independent typed attempts, parity and prepared handle |
| MSSQL staged-load adapter | Existing governance port and transaction finalizer delegation |
| Evidence producer | Stable receipts and observed phase/resource measurements |

Contracts belong in `dpone.contracts`, narrow injected ports in `dpone.ports`,
execution policy in `dpone.runtime`, and vendor I/O in adapters. Reuse existing
factories; no new generic registry. Native framing and staging are separate
variation points, allowing a second typed source with its own snapshot authority.
The existing bounded-window journal supplies CAS/fencing conventions; do not
pretend its independently reread logical-window algorithm matches a single stream.

Extend ADR 0057 with transactional MSSQL publication and target-receipt recovery.
Do not transplant ClickHouse UUID/EXCHANGE recovery. Target transaction locks
must protect the predicate against concurrent DML through commit; DDL/catalog
checks remain under the existing authority. Journal leases alone are insufficient.
Retain existing hard limits from `docs/benchmarks/quality_budgets.yml` and split
modules by responsibility, without threshold exemptions.

## Alternatives and current primary-source comparison

Sources checked 2026-09-10. Adoption choices are design judgments; no comparative
speed claim has been measured.

| System or option | Observation and decision |
|---|---|
| SQL Server native BCP | Native files carry typed values with format constraints. Adopt bounded sealed files and explicit Unicode/type profiles; validate actual driver/server combinations. [Microsoft native-format guide](https://learn.microsoft.com/en-us/sql/relational-databases/import-export/use-native-format-to-import-or-export-data-sql-server?view=sql-server-ver17) |
| SQL Server bulk loading | Parallel heap imports have specific index/locking prerequisites. Use independent attempts rather than assuming concurrent count deltas on one heap are attributable. [Microsoft BULK INSERT reference](https://learn.microsoft.com/en-us/sql/t-sql/statements/bulk-insert-transact-sql?view=sql-server-ver17) |
| dlt | Worker counts, buffered items and file size limits control load work. Adopt explicit bounds, while retaining dpone's separate atomic publication contract. [dlt performance](https://dlthub.com/docs/reference/performance) |
| ClickHouse | A running SELECT holds its snapshot's data parts until query completion. Adopt one query; do not infer durable cross-process snapshot reuse. [ClickHouse 25.6 explanation](https://clickhouse.com/blog/clickhouse-release-25-06) |
| ODBC/OLE DB bulk API | Microsoft documents memory-buffer bulk interfaces. Defer direct buffer transport because it adds binding/lifetime and receipt proof beyond the existing BCP adapter. [Microsoft preparation guide](https://learn.microsoft.com/en-us/sql/relational-databases/import-export/prepare-to-bulk-import-data-sql-server?view=sql-server-ver17) |
| Length-prefixed SQLCHAR | Existing local codec avoids delimiters but still serializes characters. Reject as evidence of a native binary fast path. |
| FIFO into BCP | Defer: it lacks the current sealed-file proof before import. |
| Informatica, Airbyte, Fivetran, Pentaho | N/A for this adapter-format decision; no new connector protocol or product-engine comparison is made. |
| SSIS | SQL Server bulk primitives above are relevant; SSIS orchestration itself is N/A. |
| gusty, Astronomer Cosmos, Apache Beam | N/A: scheduler/DAG/distributed execution changes are outside this bounded adapter change. |

## Evidence and measurable differentiation

Producer output includes exact commit, dependency/server versions, sanitized route
identity, rows, encoded bytes, physical chunks, attempts, BCP processes, configured
workers, observed peak active workers, per-phase active-worker time and wall time.
Effective parallelism for a phase is worker-active seconds divided by that phase's
wall span; do not substitute requested workers. Empty phases record zero work and
an explicit unavailable ratio. Separate encode/spool, transport, stage verification
and publish; overlapping durations must not be summed into elapsed time.

Correctness uses exact typed multiset equality in hermetic and small live cases;
scalable digests record their version and probabilistic limitation. Benchmark the
audited base against the candidate on the same synthetic narrow/wide/NULL/skewed
datasets and resource limits, one warmup plus at least three trials. Record median
and dispersion, RSS and local/server storage high-water marks. Initial acceptance
requires zero fidelity failures, zero partial publications, respected resource
limits and demonstrated worker overlap. Numeric speed claims require those runs;
no production-ready or acceleration claim follows from mocks.

## Test, documentation and rollout plan

Use red-green-refactor for new admission, bounds, native framing, concurrent
attempts, staging governance and recovery contracts. Preserve the existing
one-spool/one-BCP default test. Use barrier-controlled hermetic workers to prove
overlap without flaky time thresholds. Inject failures before/after import ACK,
checkpoint persistence, publication ACK, evidence and state. Test tampered stages,
missing ordinals, pre-EOF re-extraction, source-free post-EOF recovery, cancellation
and exhausted retries.

An approved disposable ClickHouse/MSSQL environment must move real synthetic rows,
verify duplicate/type parity, late changes/deletes, empty windows, outside-window
invariance, retry and target catalog preservation. Missing live infrastructure is
SKIP/UNVERIFIED. Exact-commit evidence is required before route certification.

Run focused tests, `select_checks.py`, ruff, formatting, mypy, import/layer/module
gates, full non-live pytest, docs contracts and strict MkDocs. Keep observed baseline
failures separate from regressions. Update the route tutorial, migration example,
schema reference, performance runbook, ADR and changelog when behavior lands.

The root agent is the sole integrator and shared-file writer. Explorer, architect,
test/certification and docs/UX agents are read-only. A fresh-context reviewer checks
the final implementation before integration. No merge or release is authorized.

## Approval checklist

- [x] Actual route and missing capabilities traced against the named base.
- [x] Resolve source lifetime, strategy admission, resource semantics and worker model.
- [x] Compatibility, public options, evidence and migration impact identified.
- [x] Primary-source alternatives and reproducible validation plan recorded.
- [x] Single integrator and independent read-only reviews assigned.
- [ ] Maintainer marks this concrete extension APPROVED before production edits.
