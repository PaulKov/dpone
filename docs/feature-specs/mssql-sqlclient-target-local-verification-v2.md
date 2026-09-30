# Feature design: MSSQL target-local verification and SqlClient bulk transport v2

- Status: APPROVED
- Owner: dpone maintainers
- Issue: TBD
- Target release: phased minor releases after 0.83.23
- Last verified: 2026-09-28

## Implementation status

| Phase | Status in this change |
|---|---|
| P1: supervised BCP plus target-local verification | Implemented; the [synthetic local Docker receipt](../../test_artifacts/live_certification/mssql-target-local-p1/validation-report.md) is separate from production qualification |
| P2: SqlClient writer, .NET companion, and package extra | Implemented for release 0.88.0; exact-commit certification remains the release authority |
| P4: persisted native hash plus mutation watermark | Implemented as explicit SqlClient layout v2 for release 0.88.0; layout v1 remains compatible |
| Public recovery CLI | Implemented for 0.88.0 with manifest-bound live actions; exact-commit release evidence remains the publication authority |
| Delivery-evidence-v2 contract and certification producer | Implemented for synthetic/private certification. Normal runs persist operational evidence v1 in the state store; a public per-run sidecar remains deferred until a configured evidence-root contract is approved |
| Private seven-day production qualification | Unverified here and never published with corporate identities, data, or measurements |

## Executive summary

The bounded ClickHouse-to-MSSQL native route already has deterministic source
scope, sealed chunk files, isolated target stages, durable receipts,
source-free recovery after EOF, and atomic publication. Its successful path is
still too expensive for large windows because SQL Server business rows cross
back to Python several times for canonical re-encoding and digest comparison.
Before this approved implementation, the only available bulk writer was BCP.

This design removes the reverse business-row transfer from the optimized path
while reproducing the existing probabilistic
`mssql-native-sha256-sum-v1` multiset evidence exactly. Object identity, schema,
row count, and algorithm parity remain exact checks; large-set content equality
remains probabilistic. The design then adds an optional
`Microsoft.Data.SqlClient.SqlBulkCopy` writer behind the current native stage
lifecycle. BCP remains the default. Synthetic wide100 profiling at the exact
candidate commit proved that repeated target-local canonical hashing dominates
the route, so P4 is promoted under the contract below. The measurement is a
design input rather than release evidence; only generated exact-commit
certification artifacts can activate the new layout.

The measurable outcome is a bounded, recoverable seven-day delivery whose
confirmed visibility is below 60 minutes, with a target median of 20 minutes or
less, zero business rows returned during mandatory target verification, and one
atomic publication boundary. Production qualification is private and does not
publish endpoint, object, query, credential, row sample, or customer workload
details.

### P1 BCP proof amendment

The original design required the actual bulk-writer SQL session to acquire a
nonce-derived application lock. The released BCP CLI opens its own SQL session
and provides no supported import-session SQL preamble, so the importer cannot
make that claim truthfully. P1 therefore uses a narrower, fail-closed proof for
BCP: one supervised child remains under grant custody; only positively
acknowledged success from a reaped child can enter a transaction-held
`TABLOCKX, HOLDLOCK` stage barrier and target-local verification. Timeout,
cleanup failure, lost acknowledgement, controller loss, or any uncertain child
or server outcome enters `UNKNOWN`. It never authorizes automatic retry, stage
drop, preparation, or publication.

The SqlClient phase retains the stronger same-session nonce-lock contract. The
two proof capabilities have distinct identity bindings and must never be
interpreted as equivalent. This amendment trades automatic recovery after an
ambiguous BCP outcome for a truthful, safe optimized success path; it does not
weaken data correctness or publication authority.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Data platform operator | Deliver a bounded ClickHouse window to MSSQL quickly and safely | Verification may repeat the full dataset over TDS | Phase timings identify one bounded write and aggregate-only verification |
| Data architect | Select a faster writer without changing correctness or recovery | The current route exposes only BCP | An explicit optional backend has deterministic capability and identity |
| Incident responder | Resume after a crash or lost acknowledgement without duplicating data | A bulk writer can finish after the caller loses the result | Exact-stage observation classifies complete, partial, or unknown outcome |
| Framework maintainer | Evolve the route without regressing existing manifests | A stale prototype created a parallel lifecycle and journal graph | The new writer uses the current contracts and BCP remains compatible |

The operator discovers the capability in the ClickHouse-to-MSSQL route guide,
runs readiness before any source row I/O, installs the optional companion only
when selecting SqlClient, and observes source-read, bulk-write, target-digest,
preparation, publication, and confirmed-visibility timings. Unsupported types,
missing runtime capability, identity drift, and insufficient capacity fail
before extraction. A retry reuses the existing invocation only when the journal
and exact target stage prove that replay is safe. Upgrade preserves all BCP v1
records; rollback selects BCP for a new invocation.

## Scope

### In scope

- Target-local raw and prepared-stage evaluation that reproduces the current
  versioned probabilistic digest and returns only `COUNT_BIG` and fixed-size
  aggregate limbs.
- Removal of business-row readback from the explicitly optimized verification
  path.
- An optional `mssql_sqlclient` writer using `Microsoft.Data.SqlClient` and
  `SqlBulkCopy` with streaming input, explicit column mappings, null
  preservation, bounded memory, and one operation deadline.
- BCP as the unchanged default and compatibility path.
- An opt-in BCP plus target-local P1 path whose positive proof is supervised
  child completion followed by a transaction-held exact-stage barrier.
- Backend-aware durable identity v2 for explicit SqlClient invocations.
- Fail-closed recovery for process failure, timeout, cancellation, lost ACK,
  partial write, target mutation, and unknown commit outcome.
- Synthetic narrow, wide100, and existing wide stress profiles in Docker.
- Private seven-day qualification bound to the exact commit and environment.
- A separately approved persisted-hash layout v2 if profiling meets its
  promotion trigger.

### Non-goals

- Direct unsealed ClickHouse-to-TDS streaming in this delivery.
- CDC, arbitrary queries, unbounded full-table extraction, offset replay, or a
  new publication state machine.
- Changes to dbt, composition activation, Airflow, or Kubernetes contracts.
- Automatic fallback from SqlClient to BCP.
- Publishing private infrastructure, object identities, queries, credentials,
  row samples, or private benchmark receipts.
- Claiming general superiority over other products.

### Assumptions and constraints

- Baseline is exact `v0.83.23` commit
  `54905bce7f93c768e9ff7cc95d6b7067a513cab8`; implementation rebases onto the
  then-current `origin/master` before editing shared files.
- Current `BoundedNativeChunks`, `NativeChunkReceipt`, stage ownership,
  preparation, publication, and checkpoint ordering remain authoritative.
- The first SqlClient capability set is closed and narrower than BCP.
  Version 1 admits only nullable/non-nullable `bigint`, `float(53)`,
  `nvarchar(max)`, and `datetime2(6)`. Unsupported schemas fail before source
  I/O. Type expansion is a later capability version, never silent coercion.
- SqlClient v1 public runtime support is Linux x64 on a glibc-based image with
  .NET 10 LTS. .NET 8 is rejected for GA because its support ends on
  2026-11-10, before a reasonable P2 operating horizon. Docker Desktop on macOS is a supported development host through
  that Linux image. Other OS/architecture pairs remain `UNVERIFIED` until their
  packaging, protocol, and live matrix passes.
- Sealed native files remain the replay boundary. Their file and typed digests
  are checked before the writer receives a stage grant.
- Credentials are injected at the composition root and never enter plans,
  receipts, process arguments, logs, or repository artifacts.
- A skipped live test is `UNVERIFIED`, never a pass.

## Public contract

This section is the approved contract implemented by 0.88.0. The
implementation-status table above distinguishes executable runtime behavior
from certification-only evidence producers.

### CLI

The executable admission sequence is:

1. `dpone doctor --profile mssql --format json` preserves the released BCP
   doctor contract. `dpone runtime mssql-sqlclient doctor --format json`
   checks the optional companion and .NET runtime without database connections.
2. `dpone plan PATH --selector NAME --format json` validates schema and projects
   backend, verifier, identity version, capability requirements, and blockers
   without opening source or target connections.
3. `dpone check PATH --connections --format json` performs the existing bounded
   connection/configuration checks. It does not replace the SqlClient doctor or
   negotiate the companion protocol.
4. `dpone check PATH --live --format json` performs the existing generic live
   preflight. SqlClient package/protocol/layout admission is repeated by the
   native runtime before ClickHouse business-row I/O; no separate disposable
   SqlClient probe is claimed in 0.88.0.
5. `dpone run PATH --selector NAME --format json` executes only after admission
   succeeds again inside the runtime boundary.

The existing `mssql` doctor retains its released exits. The SqlClient doctor
exits `0` when ready and `2` for a companion/runtime capability blocker. It
emits one JSON document to stdout for `--format json`; human output supports
Markdown and also uses stdout. Its closed document contains `schema_version`,
`backend`, `ready`, and `blocker_codes`, plus package/runtime identities when
ready. Missing package (`mssql_sqlclient.optional_package_required`), unavailable
runtime (`mssql_sqlclient.runtime_unavailable`), incompatible protocol
(`mssql_sqlclient.invalid_descriptor`), unsupported type
(`mssql_sqlclient.type_unsupported`), and identity drift
(`mssql_native.identity_mismatch`) fail before ClickHouse business-row I/O.

P2 adds `dpone ops mssql-native-recovery` with `inspect`, `reconcile`,
`resume`, and `retire` actions. Each requires an opaque invocation ID and
durable journal root. Live actions additionally require the original manifest,
its selector, and `--yes`. The runtime resolves target connection, state store,
work root, and expected identity from the manifest and sealed recovery authority
through the ordinary composition roots. Their digests must match before target
I/O; command arguments never contain credentials or duplicate identity authority.
`inspect` is read-only;
`reconcile` proves quiescence and stage state; `resume` is allowed only after
verified EOF; `retire` drops only an exact owned terminal stage. JSON reports
an opaque stage ID, state, permitted next actions, and diagnostic tokens. An
unknown or mismatched state exits `1` and retains custody. The generic
`dpone resume` command is not extended implicitly.

No release command, automatic installation, or implicit backend switch is
introduced.

At verified source EOF the same fenced journal transition persists a private
`recovery_authority_v1` document. It contains the complete current transaction
operation needed to reconstruct `MssqlTransactionAdmission`, plus SHA-256
bindings for the authored route, verification identity, target connection,
state store, and absolute work root. It contains no credential, host, raw work
path, or source row. A recovery process recomputes every binding from the
manifest and composition root before target I/O. Records created before this
authority version remain inspectable, but live actions fail closed with
`mssql_native.recovery_authority_required`.

`reconcile`, `resume`, and `retire` acquire the normal window lease and target
custody before observing or mutating SQL Server. `reconcile` first probes the
generic transaction receipt and exact owned objects. `resume` is source-free
and is admitted only for a complete EOF chain. `retire` is admitted only after
publication exclusion and drops only the recorded SQL Server object ID. A
different work root, manifest, selector, connection, state store, identity,
active lease, or object ID retains custody and performs no mutation.

Recovery preserves the same terminal ordering as a normal run: confirmed
publication receipt, durable delivery evidence, state/checkpoint advancement,
exact-stage cleanup, then custody release. A crash at every boundary is replayed
from the last durable transition; an uncertain publication outcome is observed
before any target mutation and is never blindly repeated.

`CODE` values below are diagnostic codes, uppercase words are durable states,
the command exit code is separate, and only `permitted_actions` may execute.

| Diagnostic code / state | Required operator action | Success observation | Escalation |
|---|---|---|---|
| `mssql_native.writer_ack_lost` / `UNKNOWN` | Inspect the exact attempt; P1 BCP provides no automatic reconcile-to-write authority after custody loss | Read-only incident record; no retry, drop, preparation, or overlap | Retain custody as `INCIDENT_RETAINED`; SqlClient may use live reconcile only when its same-session lock proof is available |
| `mssql_native.partial_stage_proved` / `FAILED_RETIRABLE` | `dpone ops mssql-native-recovery retire ID --journal-root ROOT --manifest PATH#SELECTOR --yes --format json` | Exact owned stage removed; new attempt permitted if publication exclusion also holds | Stop on any identity drift |
| `mssql_native.verified_eof_recoverable` / `VERIFIED` | `dpone ops mssql-native-recovery resume ID --journal-root ROOT --manifest PATH#SELECTOR --yes --format json` | Preparation/publication continues without source read | Stop if receipt chain is not contiguous |
| `mssql_native.pre_eof_reextract_required` / `FAILED_RETIRABLE` | Retire with the complete command above, then run the full authored interval | New source query and invocation | Never resume the vanished stream |
| `mssql_native.publication_outcome_unknown` / `INCIDENT_RETAINED` | Run the complete `reconcile` command above | Existing publication receipt or proved uncommitted state | Never replay target mutation blindly |
| `mssql_native.identity_mismatch` or `mssql_native.server_not_quiescent` / `INCIDENT_RETAINED` | `dpone ops mssql-native-recovery inspect ID --journal-root ROOT --format json` | Read-only incident record | Manual resolution followed by successful reconciliation; no ordinary drop/retry |

### Python API

P2 adds one capability-oriented port with immutable request and receipt contracts:

```python
class NativeStageWriter(Protocol):
    def write(
        self,
        request: NativeStageWriteRequest,
        *,
        deadline: OperationDeadline,
    ) -> NativeStageWriteObservation: ...
```

The request binds the exact owned stage identity, sealed file identity, native
wire layout, explicit writable columns, and attempt identity. It contains no
credential material. The observation reports attempt identity, input rows
consumed, writer/runtime identity, completion status, and non-sensitive phase
metrics. It is not a receipt or content authority: the existing importer creates
`NativeChunkReceipt` only after independent target-local count and digest
verification.

The models are frozen in
`dpone.contracts.mssql_native_stage_writer`; the protocol stays in
`dpone.ports.mssql_native_writer`:

- `NativeStageColumnMapping(ordinal: int, target_name: str, target_type: str,
  nullable: bool)` admits contiguous zero-based ordinals, unique non-control
  names, and the closed v1 types `bigint`, `float(53)`, `nvarchar(max)`, and
  `datetime2(6)`;
- `NativeStageWriteRequest` contains `attempt_id`, `qualified_stage`, opaque
  `stage_id_sha256`, `owner_binding_sha256`, positive `object_id`,
  `schema_sha256`, `file_path`, non-negative `expected_rows` and
  `encoded_bytes`, positive `max_row_bytes` bounded by a non-empty artifact,
  `file_sha256`, `grant_token_sha256`, `proof_capability`,
  `wire_layout_sha256`, and an immutable ordered tuple of mappings;
- `OperationDeadline(expires_at_monotonic, clock)` is process-local and exposes
  only `remaining_seconds()`. It returns a positive finite budget or raises the
  internal deadline signal; it is never serialized and no phase may replace it;
- `NativeStageWriteMetrics` contains nullable finite non-negative
  `launch_seconds`, `write_seconds`, and `dispose_seconds` measured from the
  same monotonic clock. `null` means that phase did not start or could not be
  observed; implementations never manufacture zero for an unavailable phase;
- `NativeStageWriteObservation` contains `attempt_id`, nullable non-negative
  `input_rows_consumed`, one existing writer-outcome classification (`success`,
  `failure`, `timeout`, `lost_ack`, `cleanup_failed`, or `custody_lost`),
  writer/runtime identity SHA-256 values, closed protocol ID, and metrics.
  Only `success` carries a row count, and the count must equal the request.

The coordinator maps `success` to `WRITER_TERMINAL` and proceeds to the
independent quiescence barrier. Every other classification appends `UNKNOWN`
and retains custody. A launcher exception, elapsed deadline, malformed IPC,
unexpected EOF, or process-loss exception is caught inside the writer boundary
and becomes the corresponding closed uncertain observation before the
coordinator returns or raises. No exception path may skip durable `UNKNOWN`.

The shipped P1 `NativeStageBulkWriter.write(grant, rejects_path=...)` callable
remains the BCP compatibility boundary for unchanged P1 composition. P2
introduces `NativeStageWriter` as the canonical capability port. Its BCP
implementation owns a deadline-aware supervised launcher that receives the
same `OperationDeadline` and derives launch, wait, acknowledgement, cleanup,
and disposal bounds from `remaining_seconds()`; it never wraps the legacy
callable while claiming the canonical contract. The old signature does not
change and gains no SqlClient policy. See
[ADR 0076](../adr/0076-mssql-sqlclient-companion-boundary.md).

Existing public BCP imports and `NativeChunkImporter` behavior remain valid.
In P2, the companion is distributed as the optional `dpone-mssql-sqlclient` wheel and
selected through the `dpone[clickhouse,mssql-sqlclient]` extras. It contains a
framework-dependent Linux x64 assembly for .NET 10 LTS. Its closed protocol is
`dpone.mssql-sqlclient.ipc.v1`; the wheel minor version must match dpone and
`Microsoft.Data.SqlClient` is pinned to the certified `6.1.7` patch.
`dpone runtime mssql-sqlclient doctor` verifies `dotnet --list-runtimes` contains
`Microsoft.NETCore.App 10.x`, prints only version/capability IDs, and gives the
remediation `pip install "dpone[clickhouse,mssql-sqlclient]"`. Upgrade changes companion
identity and cannot resume unfinished v2 work. Removal uses
`pip uninstall dpone-mssql-sqlclient` only after
`dpone ops mssql-native-recovery inspect-all --journal-root ROOT --format json`
reports `retained_custody_count: 0`.

#### Companion IPC v1

The supervisor sends exactly one non-secret request frame on stdin and accepts
exactly one result frame on stdout. Each frame is a four-byte unsigned
big-endian length followed by that many UTF-8 JSON bytes. Request payloads are
limited to 1 MiB, result payloads to 64 KiB, credential payloads to 64 KiB, and
captured stderr to 64 KiB. Zero-length, oversized, truncated, duplicate-key,
unknown-field, invalid-UTF-8, non-canonical, trailing-frame, or trailing-byte
input is a protocol failure. Stdout carries no logs. Stderr is diagnostic only,
bounded and discarded after mapping to a closed diagnostic token.

The request is closed JSON with `schema_version`, `protocol`, `attempt_id`,
`qualified_stage`, `stage_id_sha256`, `owner_binding_sha256`, positive
`object_id`, `schema_sha256`, `file_path`, non-negative `expected_rows` and
`encoded_bytes`, positive `max_row_bytes` bounded by a non-empty artifact,
`file_sha256`, `grant_token_sha256`, `proof_capability`,
`wire_layout_sha256`, `deadline_budget_ms`, and `columns`. Each column contains
exactly `ordinal`, `target_name`, `target_type`, and `nullable`. The supervisor
derives `deadline_budget_ms` from the existing deadline immediately before
spawn; the child starts one stopwatch and never replenishes that budget.
The companion rejects any row whose cumulative physical framing exceeds
`max_row_bytes`, so malformed length prefixes cannot turn a bounded artifact
into an unbounded allocation.
`file_path` and exact stage coordinates remain process-local and are forbidden
from shareable evidence.

The result is closed JSON with `schema_version`, `protocol`, `attempt_id`,
`classification`, nullable `input_rows_consumed`, `writer_identity_sha256`,
`runtime_identity_sha256`, and `metrics`. Metrics contain nullable
`launch_seconds`, `write_seconds`, and `dispose_seconds`. Only `success` may
carry rows, and success rows must equal the request. The companion emits a
positive result only after bulk-copy and connection disposal complete.

Credentials use a second length-prefixed frame on a dedicated anonymous
inherited descriptor. The descriptor number is non-secret process metadata;
the credential bytes never enter argv, environment, stdin, stdout, stderr, or
durable files. V1 admits only `sql_password` authentication. Its closed payload
contains `schema_version`, `authentication`, bounded `host`, positive `port`,
bounded `database`, bounded `username`, bounded `password`, `encrypt=true`, and
boolean `trust_server_certificate`. The parent writes once and closes its end;
the child reads once, rejects trailing bytes, builds a non-pooled connection,
then clears its byte/character buffers after connection construction. Token,
integrated, certificate, and Entra authentication require a later protocol.

The session application-lock resource is
`dpone:mssql-native:` plus lowercase SHA-256 of UTF-8
`"dpone.mssql-sqlclient.applock.v1\0" + grant_token_sha256`. The companion uses
`sp_getapplock` with `LockOwner=Session`, `LockMode=Exclusive`, and the remaining
deadline. It checks the exact object ID, `dpone_native_owner` extended property,
and writable schema before mutation. The same non-pooled open connection owns
the lock and `SqlBulkCopy`; reconnect and failover continuation are forbidden.

The Python supervisor starts a new process group, inherits only the credential
read descriptor, closes every parent/child duplicate promptly, drains bounded
stdout/stderr concurrently, and derives every wait, write, cancellation, TERM,
KILL, and reap bound from the original `OperationDeadline`. Timeout or
cancellation kills and reaps the complete group. It returns only the closed
`timeout`, `lost_ack`, `cleanup_failed`, or `custody_lost` observation; raw
exceptions and captured bytes never cross the adapter boundary.

### Manifest/schema

The bounded native execution object gains three optional closed fields. The exact
dotted paths start at
`defaults.source.options.native_transfer.execution` in a batch manifest:

```yaml
native_transfer:
  execution:
    import_backend: bcp  # bcp | mssql_sqlclient
    verification_backend: python_readback  # python_readback | target_local
    layout_version: 1  # 1 | 2; explicit and valid only for mssql_sqlclient
```

Omission means `bcp` plus `python_readback`, preserving v1. `target_local` is
opt-in for BCP and mandatory for `mssql_sqlclient`. SqlClient has no fallback
chain. `layout_version` omission preserves the SqlClient v1 physical layout.
Explicit v2 selects persisted row hashes and mutation watermarks and has no
downgrade. Unknown or incompatible values fail schema validation. Every
non-default selector participates in readiness, plan output, durable identity, recovery,
and evidence. Existing v1 BCP journals are neither rewritten nor interpreted
as v2 journals.

The first release exposes no transport-specific tuning knobs. Batch size,
streaming, mappings, and transaction policy are derived from the sealed chunk
and closed capability profile so that the manifest cannot request an unsafe
combination. SqlClient v1 uses `EnableStreaming`, explicit mappings,
`KeepNulls`, `TableLock`, and `UseInternalTransaction`; `FireTriggers`,
`CheckConstraints`, and `KeepIdentity` are absent. Each internal batch may
commit only to the isolated attempt stage. Final target visibility still occurs
once through the existing publication transaction.

### Prerequisites

| Requirement | SqlClient v1 contract | Admission proof |
|---|---|---|
| Platform | Linux x64, glibc-based image | OS/architecture capability ID |
| Runtime | `Microsoft.NETCore.App` 10.x | `dotnet --list-runtimes` through `doctor` |
| SQL Server | SQL Server 2019+ with database compatibility level 150+ | bounded catalog query before source I/O |
| SQL crypto | `HASHBYTES('SHA2_256', varbinary(max))` on the compiled payload | disposable parity probe |
| Permissions | connect; create/drop owned stage; `ALTER` on staging schema; insert/select owned stage; catalog/extended-property read/write; database application locks; existing strategy publication permissions | least-privilege disposable preflight |

A successful doctor JSON includes `{"backend":"mssql_sqlclient","ready":true,
"protocol":"dpone.mssql-sqlclient.ipc.v1","runtime_major":10,...}`. A missing
runtime returns exit `2` with `ready:false` and
`blocker_codes:["mssql_sqlclient.runtime_unavailable"]`.
Connection and permission blockers are emitted only by `check --connections` or
`check --live`, never by credential-free doctor/plan.

### Artifacts and evidence

Legacy BCP v1 evidence stays byte-identical. Normal application runs write
durable `dpone.mssql-native-operational-evidence.v1` into the configured state
store before advancing the checkpoint. Exact-commit certification builds an
immutable UTF-8 `dpone.mssql-native-delivery-evidence.v2` JSON sidecar through
the closed certification producer. Its schema requires:

- `import_backend`: `bcp` or `mssql_sqlclient`;
- `backend_identity_sha256` and `wire_identity_sha256`;
- `source_read_seconds`, `bulk_write_seconds`, `target_digest_seconds`,
  `preparation_seconds`, `publication_seconds`, and
  `confirmed_visibility_seconds`;
- `business_rows_read_back`, which must be zero for the optimized route;
- `bcp_process_count`, which must be zero for SqlClient certification;
- retry, unknown-outcome, and reconciliation classifications;
- exact commit, package version, dirty flag, and allowlisted environment digest.

All timing values are non-negative decimal seconds from one monotonic clock.
`confirmed_visibility_seconds` spans runtime admission start through the first
successful post-commit visibility observation. Overlapping spans are stored
independently and never summed to derive a total. Missing values are `null` and
make the applicable performance decision `UNVERIFIED`.

The strict public scanner covers shareable JSON/Markdown/log/JUnit artifacts,
their producer stdout/stderr, and private qualification exports. It validates
the closed schema, recursively scans keys and strings, then scans serialized
bytes against forbidden names and in-memory secret/endpoint needles.
Privileged local journals and recovery streams have a separate secret scanner:
they may contain exact object coordinates but never credentials or raw vendor
exceptions. Diagnostics are closed tokens. Exact private counts and timings do
not enter public summaries. Partition/object identities in private exports are
opaque keyed digests; the key is external and is never retained with the
export.

The final public gate scans `git diff --binary APPROVED_BASE...HEAD`, every
tracked file added or changed in that range, untracked files below public
artifact roots, and every generated JSON/Markdown/log/JUnit/stdout/stderr
artifact. A staged diff is insufficient. Any forbidden match is `FAIL`.

The environment digest is domain-separated over allowlisted architecture,
runtime/package/driver versions, capability IDs, and package digests. It
excludes hosts, paths, endpoints, environment variables, and
credential-derived values. Private receipts use an external root outside the
checkout and are not copied into Git, CI artifacts, PRs, or public docs.

The certification sidecar lives below the explicitly supplied certification
evidence root. The immutable payload filename is its SHA-256; an atomic pointer
selects the current revision. Equal content is idempotent and unequal content
creates a new revision. A normal runtime does not infer a filesystem evidence
root or claim this artifact exists.

P2 generates the proposed schemas
`dpone.mssql-native-admission.v2.schema.json`,
`dpone.mssql-native-recovery.v2.schema.json`, and
`dpone.mssql-native-delivery-evidence.v2.schema.json`. Admission requires
`schema_version`, `kind`, `status`, `import_backend`,
`verification_backend`, `identity_version`, `capability_id`, `blockers`, and
`warnings`. Recovery requires `schema_version`, `kind`, opaque
`invocation_id`, `identity_sha256`, durable `state`, namespaced
`diagnostic_code`, `permitted_actions`, and content-addressed `artifact_refs`.
Evidence requires the fields enumerated above under versioned `subject`,
`backend`, `timings`, `resources`, `correctness`, `recovery`, `privacy`, and
`status` objects. Identifiers/digests are strings, facts are booleans, counts
are non-negative integers, seconds/bytes are non-negative JSON numbers or
`null`, and state/status/action fields are closed enums. Unknown fields are
rejected. Contract tests cover success, blocked, and unknown payloads against
these schemas; the operator runbook documents executable command examples.

### Compatibility and migration

- Old manifests select BCP plus Python readback and produce the same v1
  plan/journal/evidence bytes.
- Explicit target-local verification or SqlClient uses identity/journal v2. A
  v1 record cannot resume v2, and either selector change requires a new
  invocation.
- No automatic migration rewrites durable records.
- Operators can roll back by starting a new BCP invocation only when stable
  target custody is clear. Any in-flight v2 invocation must first reach a
  proved terminal or recoverable state that authorizes custody release.

Journal v2 uses key prefix `mssql-native-chunks-v2/`. Its canonical identity
contains the six unchanged v1 `NativeChunkPlan` fields plus `import_backend`,
`verification_backend`, `writer_proof_capability`, companion protocol and package digests,
capability-layout digest, digest algorithm ID, and timeout-policy digest. The
monotonic deadline is process-local and never serialized. Journal v1 decodes
only as BCP plus Python readback. Existing `NativeChunkReceipt` and prepared
snapshot shapes remain unchanged.

The invocation key is lowercase SHA-256 of RFC 8785 canonical JSON for that
identity. Each attempt begins by persisting a 256-bit random nonce; `attempt_id`
is SHA-256 of canonical `[invocation_key, ordinal, nonce]`. The writer grant
contains a fresh 256-bit token and a closed proof-capability identifier; only
the token digest is durable. For SqlClient, the application-lock resource is
derived from that digest. For BCP P1, the digest binds one supervised launch
and must not be described as a SQL-session token.
Pre-release P1 journals that already contain a schema-valid 64-hex binding stay
readable as opaque launch bindings; they are never reinterpreted as raw tokens
or rewritten. Every new grant persists the SHA-256 of a newly generated
256-bit token.
The closed initial proof-capability values are
`bcp-supervised-stage-barrier-v1` and `sqlclient-session-applock-v1`.
The only admitted pairs are BCP plus the former and SqlClient plus the latter;
every other pair fails before source I/O. The capability string is an explicit
field in the canonical invocation-identity JSON and therefore changes its
SHA-256 key.

Journal identity alone cannot prevent a new backend or invocation from ignoring
unresolved custody. A second durable CAS authority uses the stable key
`mssql-native-target-custody-v1/{sha256(target_id)}` in the existing
`WindowStore`; the key excludes run, window, backend, and verifier identity.
Every native runtime, including default v1, checks this authority after target
lease acquisition and before source or writer I/O. A v2 invocation claims it
with its invocation digest before any v2 durable side effect or source I/O;
all of its chunk attempts share that invocation-owned custody. A crash between
claim and the first launch remains conservatively held. It is released only
after the matching invocation has durable publication success and cleanup, or after durable
non-publication proof plus retirement of every owned stage. Lease expiry never
clears it. A matching source-free recovery may inspect or complete already
authorized work but cannot grant a second launch. Current binaries enforce this
guard; older binaries do not understand the key and must be operationally
excluded from targets with any v2 custody record.
The authority payload is closed JSON with `schema_version=1`,
`kind="dpone.mssql-native-target-custody"`, `target_id_sha256`, monotonically
increasing positive `epoch`, `state` (`clear` or `held`), nullable
`holder_invocation_key`, nullable positive `holder_lease_fence`, and a closed
nullable `release_reason`. In `held`, holder and fence are required and
`release_reason` is null. In `clear`, holder and fence are null and
`release_reason` is one of `published_cleanup`,
`nonpublication_all_stages_retired`, or `empty_completion_cleanup`.
`clear -> held` increments `epoch`; `held -> clear` preserves it. Both use `WindowStore.save`
CAS under the current target lease. A held record may be read by matching
recovery but cannot be overwritten by lease replacement, backend change, or a
new run ID. A changed lease fence makes the matching invocation recovery-only:
it may inspect, verify already durable positive terminal authority, retire
proved stages, or finish source-free publication, but it cannot create a new
attempt or writer grant. No deletion or TTL-based release exists.

Writer state is an append-only chain below
`mssql-native-chunks-v2/{invocation_key}/{ordinal:020d}/{attempt_id}/`.
Each UTF-8 canonical JSON event has exactly `schema_version`, `kind`,
`invocation_key`, `ordinal`, `attempt_id`, `sequence`, `previous_sha256`,
`event`, `stage_binding`, `artifact_binding`, `writer_binding`, `observation`,
and `created_at`. Bindings are closed objects containing only opaque IDs,
digests, counts, and bytes; no credentials or coordinates. Allowed events are
`INTENT`, `STAGE_OWNED`, `GRANTED`, `WRITING`, `WRITER_TERMINAL`, `QUIESCENT`,
`VERIFIED`, `PARTIAL_PROVED`, `UNKNOWN`, `FAILED_RETIRABLE`,
`INCIDENT_RETAINED`, and `RETIRED`. The mandatory generated
`dpone.mssql-native-writer-state.v2.schema.json` closes the nested objects:

- `artifact_binding` is required from `INTENT`: ordinal, row count, encoded
  bytes, file SHA-256, and typed digest;
- `stage_binding` is `null` for `INTENT` and thereafter requires opaque stage
  ID, owner-binding SHA-256, positive SQL object ID, and schema SHA-256;
- `writer_binding` is `null` before `GRANTED` and thereafter requires import
  backend, proof-capability ID, protocol/package/capability SHA-256 values,
  grant-token SHA-256, and timeout-policy SHA-256;
- `observation` is `null` through `WRITING`. Later events require writer outcome
  (`success`, `failure`, `timeout`, `lost_ack`, `cleanup_failed`, or
  `custody_lost`), nullable non-negative input
  rows consumed, nullable count/sentinel, nullable overflow flag, either `null`
  or exactly eight non-negative decimal limb strings, quiescence (`unverified`,
  `proved`, or `failed`), and a namespaced diagnostic code.

`sequence` is non-negative; `previous_sha256` is `null` only at sequence zero;
`created_at` is UTC RFC 3339. Event-conditional required/null rules and
`additionalProperties: false` apply at every level. Events must be contiguous,
hash-linked,
idempotent by content, and follow the state machine. Unknown fields, missing
links, duplicate sequence with unequal bytes, or invalid transitions retain
custody and block replay. Recovery reconstructs state only from this chain and
the unchanged receipt/publication authorities.

The normal chain is `INTENT -> STAGE_OWNED -> GRANTED -> WRITING ->
WRITER_TERMINAL -> QUIESCENT -> VERIFIED`. Any state after `GRANTED` may enter
`UNKNOWN`; only reconciliation may append `QUIESCENT`, `PARTIAL_PROVED`, or
`INCIDENT_RETAINED`. `PARTIAL_PROVED` may enter `FAILED_RETIRABLE` after
non-publication proof. Only `FAILED_RETIRABLE` may enter `RETIRED`.
`INCIDENT_RETAINED` has no ordinary destructive transition. Receipt creation
requires terminal `VERIFIED` plus matching artifact and stage bindings.
For `bcp-supervised-stage-barrier-v1`, `UNKNOWN -> QUIESCENT` is allowed only
when the durable chain already contains positive acknowledged-and-reaped
`WRITER_TERMINAL(success)` and the new observation-only recovery acquires the
exact-stage barrier. BCP lost ACK, failed reap, uncertain launch, or custody
loss may append only `INCIDENT_RETAINED`; current stage contents cannot replace
missing writer authority. BCP never enters `PARTIAL_PROVED`. Recovery-only
journal access exposes this closed observation operation but still forbids a
new attempt, grant, launch, source resume, EOF promotion, or publication.
If the source fails before EOF after one or more BCP chunks reached `VERIFIED`,
an invocation-level durable proof that no preparation/publication intent exists
may move those verified stages to `FAILED_RETIRABLE` and then `RETIRED` by exact
owner identity. This retirement path applies only to fully writer-proved
`VERIFIED` stages; any `UNKNOWN` attempt keeps target custody held.

Compatibility is fail-closed. SqlClient plus a missing/old companion blocks
before extraction. Old dpone rejects unknown manifest fields and cannot decode
the v2 keyspace. Protocol mismatch blocks. Unfinished v2 state cannot be
downgraded. No overlapping rollback invocation may start until the prior v2
publication state is proved non-publishing, published, terminal-retirable, or
reconciled to another proved terminal state. `INCIDENT_RETAINED` never
authorizes overlap. Retained raw stages may coexist
only after non-publication is proved and remain excluded from new identity.
P1 verifier rollback starts a new `python_readback` invocation only after the
old attempt has proved non-publication and reached an allowed terminal state;
an unresolved BCP `UNKNOWN` never authorizes overlap. P2 writer
rollback then starts a new BCP invocation. Older binaries preserve but never
interpret v2 records.

## Architecture and detailed algorithm

The normative [architecture and algorithm appendix](mssql-sqlclient-target-local-verification-v2-architecture.md) defines the step-by-step algorithm, persisted-hash layout, state machine, failure semantics, component boundaries, and ADR decisions.

## Market, certification, and rollout

The normative [certification and rollout appendix](mssql-sqlclient-target-local-verification-v2-certification.md) records the market comparison, measurable targets, security and privacy rules, exact test matrix, documentation plan, and phased rollout.

## Agent execution plan

One integrator owns shared semantic files and reconciles all agents. Parallel
writers use separate worktrees and task contracts.

| Agent/role | Owned paths | Read-only paths | Forbidden paths | Dependency |
|---|---|---|---|---|
| Explorer | None | Current native execution, tests, docs, old prototype | All writes | None |
| Architect | None | Contracts, journals, recovery, ADRs | All writes | Explorer map |
| Digest implementer | New digest contracts/port/adapter and focused tests | Encoder/importer/preparer | Shared schemas, changelog, indexes | Approved spec and ADR |
| SqlClient companion implementer | Optional companion package and its tests | Native wire corpus and port | Current runtime/shared files | Approved writer protocol |
| Runtime implementer | Importer/composition integration and focused tests | Companion protocol and current lifecycle | Release/shared docs | Digest and writer contracts |
| Test certifier | Synthetic harness and certification tests | All changed code | Production code | Integrated candidate |
| Docs/UX reviewer | Route docs/runbook/examples review | Public contracts and evidence | Production code | Integrated candidate |
| Integrator | Shared schemas, registries, ADR/index, changelog, dependency locks | Entire scoped diff | Unrelated dbt/composition work | All reports |

## Implementation readiness checklist

The `APPROVED` status records maintainer approval of this phased design. The
unchecked items below are implementation/release gates for the remaining
phases; they do not claim that P1 or P2 has passed final certification.

- [x] User problem and CJM are clear.
- [ ] Algorithm and failure semantics passed independent review.
- [ ] Public contracts and compatibility passed independent review.
- [x] Architecture and alternatives are justified.
- [ ] Relevant market research is fully reproducible from official sources.
- [x] Claimed differentiation is measurable.
- [ ] Tests, evidence, docs, rollout, and rollback passed independent review.
- [x] Path ownership and integration plan are conflict-safe.
- [x] Maintainer changed status to `APPROVED`.
