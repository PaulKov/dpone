# Protected ClickHouse Publication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans for the preserved Native execution choice. Steps use checkbox syntax; root is the sole writer and a fresh independent reviewer checks the final increment.

**Goal:** Implement one protected Python publication from authority-owned candidate creation through verified sealing, one-shot publication and source-free recovery.

**Architecture:** Extend storage through an explicit v2 profile without changing v1 defaults. Keep candidate admission, native transport, typed observation and kernel adaptation separate, with injected narrow dependencies and one execution session across publication. Retain owners and uncertain operations; do not bind the stock ODBC route.

**Tech Stack:** Python, SQLite WAL/FULL, POSIX flock, clickhouse-driver 0.2.10, ClickHouse 24.8.14.39, pytest, owned Docker Desktop Linux volumes; no dependency upgrade.

**Spec:** [Approved protected-publication supplement](../../feature-design-clickhouse-protected-publication.md), [parent authority design](../../feature-design-clickhouse-dpone-only-authority.md), [ADR 0079](../../adr/0079-protected-clickhouse-candidate-lifecycle.md).

**Status:** APPROVED. The maintainer separately confirmed this written plan on 2026-09-30, after approval of the specification. Execution method: Native, already selected. Implementation progress is recorded below; approval is not completion or certification.

**Base:** `f46bdceef120894a5ad1cd8fab82bb4170c601ee`, existing branch `codex/clickhouse-native-publisher-locale-base`, PR #249. Scope and shared ownership are in the [task contract](../../agent-tasks/clickhouse-protected-publication.yml).

## Global Constraints

- One POSIX host, persistent local SQLite storage, one direct ClickHouse node, Atomic database and plain MergeTree.
- SQLite WAL, `synchronous=FULL`, foreign keys and short `BEGIN IMMEDIATE` transactions; no network I/O in a database transaction.
- Explicit `dpone.clickhouse.authority.v2`; existing v1 provision/open defaults and original records stay unchanged. No migration, owner transfer or replacement authority over previously managed names.
- Preserve `dpone.clickhouse.guarded-publication.v2`, the eight-method kernel port and existing publication selection/classification.
- No application/driver/proxy retry, asynchronous insert, arbitrary SQL/settings, ON CLUSTER, replication, Distributed, HA, TTL expiry or independent mutation ingress.
- Candidate CREATE and each INSERT require acknowledged registration, durable send entry, successful native EndOfStream and durable completion before successful join.
- A source exception is not exhaustion. Every accepted request must complete successfully; unsent cancellation cannot silently omit rows.
- Reopened pre-PREPARED operations, including SEALED, permit inspection only. Existing journal state never recreates an invocation or dispatch capability.
- No checkpoint promotion, cleanup, owner release, successor operation, production grant changes, version bump, merge or release.
- Preserve probabilistic typed hash semantics and old producer bytes; never describe hash parity as mathematical equality.
- Source objects, native driver buffers and RSS are outside encoded-byte bounds. The ODBC pre-read byte-admission gate remains separate.
- Use the existing `docs/benchmarks/quality_budgets.yml` and exact-cap ratchets without relaxation. Target each new cohesive module below 300 SLOC; hard limits come only from the checked-in budget file.
- All `uv run` implementation commands below use `--frozen --extra postgres --extra gcp --extra columnar --extra dbt-mssql --extra accel --extra clickhouse`; preserve the existing environment and lockfile.

## Review Focus

### Execution ruling: unchanged technical-column policy consolidation

The initial implementation exposed a missed coupling constraint: the existing
runtime-to-contracts flow was already 214, its current permitted cap. Relocating
shared wire/evidence implementations adds three genuine dependencies (217).
Changing import syntax does not remove them. Budget and baseline relaxation is
not permitted.

The root integrator extends Task 1's internal, behavior-neutral ownership to
`src/dpone/runtime/sinks/clickhouse_physical_types.py`,
`clickhouse_lineage_projection.py`, `clickhouse_production_finalize.py` and the
existing lineage-contract/finalizer tests named in the task contract. Consolidate
the duplicated ClickHouse technical-column name/type decisions in the existing
physical-type policy owner, retaining injected catalogs, exact output names,
types, order, overrides and SQL. No strategy behavior changes or unrelated
incremental-route refactoring is included. Characterization tests precede edits.

The temporal compatibility import may delegate through the existing binary
encoding module, preserving its function object and errors; test its formerly
smaller import path for optional-SDK/import-time-I/O regressions. The combined
graph projection is 214, not proof: run the actual gates after implementation.
Task 5 should genuinely consume port/kernel values rather than add unnecessary
runtime model construction. Its combined public status can belong to the runtime
API; annotation-only imports are not execution dependencies. Recheck actual
coupling and type-hint behavior then. This ruling changes no public behavior,
publication authority or deployment/release scope.

### Original risk review

1. A target name reused as another operation's candidate, including through a different store inventory entry, must block before source access (Task 2).
2. Admission closed while an accepted writer is paused must neither lose its rows nor permit late registration or false sealing (Tasks 2–3).
3. Equal-looking Python values after lossy driver conversion must not hide different binary content, decimal scale or timestamps (Tasks 1 and 4).
4. A valid but foreign/expired/fork-inherited execution session must not authorize scoped dispatch; no nested flock or unlock window is allowed (Task 5).
5. A crash between seal and PREPARED, or after server effect but before durable completion, must never invoke source or mutation again (Tasks 5–6).

## File structure and ownership

All paths are repository-relative. New modules are listed explicitly; do not
introduce additional abstractions or split files mechanically. Root owns every
write; earlier explorer/architecture/test/docs findings are read-only inputs.

| Task | New files and responsibility |
|---|---|
| 1 | `src/dpone/contracts/clickhouse_candidate.py` — immutable candidate/request/status values; `clickhouse_observation.py` — design/profile/catalog/evidence values; `clickhouse_scalar_wire.py` — shared pure scalar encoding primitives; `clickhouse_typed_multiset.py` — existing aggregate implementation; `src/dpone/adapters/clickhouse_observation_profile.py` — strict types/values/limits; `clickhouse_design_grammar.py` — closed CREATE grammar and fixed renderer |
| 2 | `src/dpone/adapters/clickhouse_authority_schema.py` — closed v1/v2 SQL definitions; `clickhouse_publication_journal.py` — shared existing publication CAS engine; `clickhouse_candidate_sqlite.py` — v2 enrollment/lifecycle; `clickhouse_candidate_requests.py` — request transitions on the same injected storage; `clickhouse_candidate_diagnostics.py` — redacted status and atomic export |
| 3 | `src/dpone/adapters/clickhouse_candidate_gateway.py` — serialized candidate requests; `clickhouse_native_candidate.py` — pinned CREATE/INSERT protocol |
| 4 | `src/dpone/ports/clickhouse_observation.py` — protected reader capability; `src/dpone/adapters/clickhouse_native_catalog.py` — fixed catalog/row reads; `clickhouse_authority_observer.py` — protected evidence policy; `clickhouse_candidate_seal.py` — close/join/parity/seal service |
| 5 | `src/dpone/adapters/clickhouse_guarded_backend.py` — kernel bridge; `clickhouse_candidate_lifecycle.py` — explicit runtime-facing adapter; `src/dpone/ports/clickhouse_candidate.py` — runtime-facing candidate/readiness capability; `src/dpone/runtime/sinks/clickhouse_protected_publication.py` — source lifecycle orchestration; `src/dpone/adapters/clickhouse_publication_readiness.py` — deployment inventory and live readiness |
| 6 | `examples/python/clickhouse_protected_publication.py` — explicit runnable composition; three docs pages and scoped unit/live support files listed below |

Existing files changed only for reuse/compatibility:

- `src/dpone/runtime/clickhouse_binary_encoding.py`: retain source-policy/coercion
  behavior and public functions; delegate pure byte primitives to contracts.
- `src/dpone/runtime/clickhouse_temporal_encoding.py`: compatibility re-export
  of the same temporal primitive, preserving import/error behavior.
- `src/dpone/runtime/sinks/clickhouse_window_evidence.py`: compatibility re-export
  of `TypedMultiset`; no changed digest or window semantics.
- `src/dpone/adapters/clickhouse_authority_storage.py`: explicit closed version
  choice, default v1, same file/durability/inode enforcement.
- `src/dpone/adapters/clickhouse_authority_sqlite.py`: thin unchanged v1 public
  API delegating common publication journal behavior; no lifecycle exposure.
- `src/dpone/ports/clickhouse_publication_exclusion.py` and
  `src/dpone/adapters/clickhouse_authority_execution_lock.py`: additive bound
  session capability, existing exclusion API remains usable.
- `src/dpone/adapters/clickhouse_authority_publisher.py`: scoped publisher entry
  points plus unchanged standalone lock-acquiring wrappers.

Adapters never import runtime. Runtime imports contracts/ports, not concrete
adapters. The explicit example is the composition root; no default factory,
registry, compatibility shim policy or service locator is added.

## Task 1: Closed design profile and compatible typed evidence

**Files:** Create the six Task 1 modules above; modify the three encoding/evidence
compatibility modules. Tests: `tests/test_clickhouse_observation_profile.py`,
`tests/test_clickhouse_design_grammar.py`, `tests/test_clickhouse_evidence_compatibility.py`.

**Interfaces:**

- In `contracts.clickhouse_observation`, frozen `CandidateColumn(name: str,
  type_name: str)` and `CandidateDesign(columns: tuple[CandidateColumn, ...],
  sorting_key: tuple[str, ...], primary_key: tuple[str, ...],
  partition_key: tuple[str, ...])`. Empty key tuple means `tuple()`;
  primary key must be a sorting-key prefix. Reject duplicate column names,
  unknown key columns, nullable/non-integer/non-date partition columns and all
  unsupported clauses. Names are exact strings, quoted safely; never interpolate
  caller expressions as SQL. Authority database/table identifiers keep their
  existing simple-identifier restriction.
- Frozen `ObservationLimits(max_columns: int, max_batch_rows: int,
  max_batch_bytes: int, max_row_bytes: int, max_partitions: int,
  max_scan_rows: int, max_scan_bytes: int, request_seconds: float)` requires
  explicit positive finite values, rejecting bools and inconsistent row/batch
  bounds. No implicit unbounded defaults.
- Frozen `MultisetState(count: int, total: int, encoded_bytes: int,
  nulls: tuple[int, ...])` and `TypedTableEvidence(rows: int,
  content_digest: str, partitions: tuple[str, ...], encoded_bytes: int)`.
  Preserve `TypedMultiset.digest()` byte-for-byte, including its current JSON
  serialization. `snapshot_multiset(value: TypedMultiset) -> MultisetState` and
  `restore_multiset(state: MultisetState) -> TypedMultiset` in the shared multiset
  module validate widths/counts; storage uses
  canonical JSON TEXT for the 256-bit sum, not a SQLite integer column.
- `ProtectedObservationProfile(design: CandidateDesign, limits: ObservationLimits)`
  exposes `profile_id = "dpone.clickhouse.observation.v1"`, `profile_digest: str`,
  `design_digest: str`, `encode_row(values: tuple[object, ...]) -> bytes` and
  `validate_batch(rows: tuple[tuple[object, ...], ...]) -> CandidateBatch`.
  Digest canonical sorted compact UTF-8 JSON for design/profile, with non-finite
  JSON values forbidden; include pinned versions, grammar and encoding IDs.
- Frozen `CandidateBatch(rows: tuple[tuple[object, ...], ...],
  evidence: MultisetState, payload_digest: str)` lives in candidate contracts.
  Compute payload SHA256 over length-framed canonical row bytes in batch order.
  Freeze/copy accepted rows; reject mutable/custom scalar values. Batch DTOs are
  data, not admission authority. The gateway recomputes/validates their evidence.
- Define opaque `VerifiedEnrollment` in candidate contracts now, before the
  Task 2 consumer: its internal issuer binds exact request/profile, inventory
  revision/digest, PID/thread and active lifetime. `assert_current(request:
  ProtectedPublicationRequest) -> None` rejects mismatch/expiry; `close() -> None`
  invalidates it. Define `ProtectedPublicationRequest` with the Task 2 fields
  in this same task. Only trusted readiness code issues it; no public constructor
  takes a success boolean. Early contract tests use a trusted fake issuer;
  Task 5 supplies the actual inventory/catalog/grant verifier.
- `render_candidate_create(binding: OperationBinding, design: CandidateDesign) -> str`
  and `parse_table_design(create_sql: str) -> CandidateDesign` implement the
  same closed grammar. The parser consumes the complete statement, recognizes
  object identity separately, rejects unknown clauses and preserves literals.
  No generic SQL parser dependency or regex-based clause deletion.
- Pure `clickhouse_scalar_wire` exposes `pack_integer(value: int, root: str) -> bytes`,
  `encode_temporal_integer(value: int, root: str, scale: int = 0) -> bytes`,
  `encode_decimal(value: Decimal, type_name: str) -> bytes`,
  `encode_string(value: bytes) -> bytes`, `encode_fixed_string(value: bytes,
  width: int) -> bytes`, `encode_uuid(value: UUID) -> bytes` and
  `var_uint(value: int) -> bytes`. Move existing pure algorithms; legacy wrappers
  retain historic coercion before calling them. Do not relocate MSSQL policy
  imports into contracts or duplicate the binary algorithms.

- [x] Write RED tests for the exact approved scalar allowlist, empty/duplicate
  rows, NULL versus empty/zero, negative floating zero, binary NUL/invalid UTF-8,
  FixedString zero suffix, Decimal range/scale, UTC date boundaries and precision
  loss. Assert DateTime64(7), Decimal256, NaN/Inf and nested types fail.
- [x] Pin `test_legacy_evidence_vectors_unchanged` before refactoring: same row
  bytes, digests, imports and exceptions for existing window/native vectors.
  Use injected hash collisions to prove count/null multiplicity is retained,
  not to assert collision-free content equality.
- [x] Write grammar tests for quoted names containing punctuation, escaped
  quotes, empty keys, primary-key prefix, supported partitions and complete
  consumption. Assert DEFAULT/TTL/codecs/projections/indices/comments/SETTINGS,
  arbitrary expressions and trailing SQL are rejected.
- [x] Run the three focused files; retain actual missing-API/behavior failures.
  In `test_float64_nonfinite_is_rejected`, construct a one-column Float64
  profile with explicit limits and pin these assertions:

  ```python
  with pytest.raises(ValueError):
      profile.encode_row((float("nan"),))
  assert profile.encode_row((-0.0,)) != profile.encode_row((0.0,))
  ```
- [x] Implement the interfaces. Strict input types: bool only for Bool, int
  excluding bool for integers, finite float for floating types, bytes or UTF-8
  str for String/FixedString, UUID, Decimal, date and aware UTC datetime.
  Float32 input must round-trip exactly at its declared precision, including
  sign. Reject nonzero submicrosecond extensions and lossy time/decimal coercion.
  Normalize only documented server type aliases, including Decimal width aliases
  rendered as Decimal(9/18/38, scale); do not broaden the approved profile.
- [x] Run focused tests plus existing binary/RowBinary/window tests selected by
  `rg --files tests` for those modules; all old vectors must pass unchanged.
  Run Ruff, mypy and import-rule checks for the new boundary.
- [ ] Commit the scoped green task and immediately update/attach its PR. Do not
  describe the profile as live-certified yet.

## Task 2: Explicit v2 journal, namespace reservations and candidate history

**Files:** Create the five Task 2 modules; modify existing storage/v1 facade
and add lifecycle values to `contracts/clickhouse_candidate.py`.
Tests: `tests/test_clickhouse_candidate_sqlite.py`,
`tests/test_clickhouse_candidate_processes.py`,
`tests/test_clickhouse_candidate_diagnostics.py`; preserve existing v1 tests.

**Interfaces:**

- `AuthorityVersion` is a closed v1/v2 enum in authority schema, not an extensible
  plugin. `AuthorityStorage(path, deployment_id, *, version=AuthorityVersion.V1)`
  and matching `provision` select immutable SQL/version pairs. V2 includes the
  existing publication tables plus lifecycle tables in **one** SQLite file.
- `SQLitePublicationJournal(storage: AuthorityStorage)` owns the existing
  `binding/read/prepare/claim/begin_send/record_terminal/close_without_send/
  transport_state/resolve` implementations with their current signatures and
  semantics. The v1 facade preserves its current constructors/acquire/diagnostics
  API and delegates, rather than duplicating CAS code or exposing v2 operations.
- `SQLiteCandidateAuthority(path: Path, deployment_id: str)` and
  `provision(path: Path, deployment_id: str) -> None` always use v2 explicitly.
  It supplies `execution_identity()` and `binding(operation_id)` for exclusion.
- `enroll(request: ProtectedPublicationRequest, enrollment: VerifiedEnrollment)
  -> CandidateInvocation` atomically reserves both names and creates the owner,
  publication row and candidate lifecycle. Existing IDs always conflict for
  fresh enrollment; inspection remains separate.
- `ProtectedPublicationRequest(operation_id: str, subject: AuthoritySubject,
  candidate: str, design: CandidateDesign, limits: ObservationLimits)` is frozen.
  `VerifiedEnrollment` is the Task 1 capability, issued in production by Task 5, not an
  input bool/digest; it binds inventory revision, exact names/server/profile and
  active invocation. Tests inject a trusted readiness fake, not public flags.
- `CandidateInvocation` is a process/thread/lifetime-bound opaque capability
  with `binding: OperationBinding`, `assert_current() -> None`, `close() -> None`.
  It implements a context manager returning itself and invalidating on exit;
  context exit never releases durable ownership.
  Never serialize/copy it; persist only capability hashes. Return it only after
  commit acknowledgement. Its context belongs to `publish_new`, not recovery.
- Candidate states are `registered`, `loading`, `admission_closed`, `sealed`,
  `retained`. Source exhaustion is an independent monotonic durable fact.
  Request states reuse the four transport states; `closed_without_send` aborts
  candidate success. Store CREATE UUID after acknowledged completion/observation.
  New tables are `name_reservations`, `candidate_operations`,
  `candidate_requests`, `candidate_history` and `candidate_seals`. Every request
  references its operation; every reservation is unique by deployment/server/
  database/physical name irrespective of role. Request identity, history and seal
  are immutable. Lifecycle revision/aggregate and request completion change in
  the same transaction; the publication row remains the existing kernel journal.
- `CandidateRequestJournal(storage)` provides `register(invocation, request:
  CandidateMutationRequest) -> CandidateRequestGrant`, `begin_send(grant) -> None`,
  `record_terminal(grant, completion: CandidateCompletion) -> None`,
  `close_unsent(operation_id: str) -> None`, `requests(operation_id: str)
  -> tuple[CandidateRequestStatus, ...]`. Grants are opaque, never reconstructed.
- Frozen mutation request contains binding, kind (`create`/`insert`), sequence,
  design digest, statement digest, payload digest and `MultisetState`.
  CREATE has sequence 0/zero rows; INSERT starts at 1. Derive query ID from
  canonical `["dpone-candidate-v1", operation_id, kind, sequence]` SHA256 with
  `dpone-candidate-` prefix; no conflict with `dpone-publication-` identity.
  `CandidateRequestStatus(request: CandidateMutationRequest, state:
  TransportState, revision: int, completion_digest: str | None)` is frozen.
  `CandidateRequestGrant` is an opaque issued capability bound to invocation,
  request identity and current revision, with its secret excluded from repr and
  durable plaintext. Define `CandidateCompletion` now with the exact receipt
  fields listed in Task 3; its trusted producer is added in that task.
- Authority exposes `source_exhausted(invocation) -> None`,
  `close_admission(operation_id: str) -> None`, `retain(operation_id: str,
  reason: str) -> None`, `inspect(operation_id: str) -> CandidateStatus`.
  Closing admission is safe from a recovery process and races registration in
  SQLite; it cannot grant a send or assert source exhaustion.
- `CandidateStatus` contains binding, revision, lifecycle/admission state,
  source-exhausted flag, accepted/succeeded/uncertain request identities,
  expected multiset, optional seal, publication state and retained reason.
  Diagnostic export uses `dpone.clickhouse.protected-publication-status.v1`,
  atomic create-without-overwrite outside authority storage, no raw rows/secrets.
  Define these data values in candidate contracts during this task, including
  `CandidateSeal` with the fields specified in Task 4, before any consumer uses
  them. Its producer is not available until Task 4; use `seal=None` beforehand.

- [ ] Write RED tests for v1 defaults and unchanged bytes/history; v1/v2
  cross-open/unknown versions must fail. New v2 provisioning cannot replace
  an existing file or adopt sidecars. Test symlink/inode/permission guards.
- [ ] Write cross-role name collision tests (target/candidate, candidate/candidate),
  concurrent processes and same operation with changed request. Assert no partial
  owner/name reservation survives a rolled-back transaction.
  `test_candidate_name_cannot_be_another_target` uses two valid readiness
  capabilities and requests whose candidate/target names collide:

  ```python
  with authority.enroll(first_request, first_enrollment):
      with pytest.raises(AuthorityConflict):
          authority.enroll(conflicting_request, second_enrollment)
  assert authority.inspect(first_request.operation_id).binding.candidate == first_request.candidate
  ```
- [ ] Write commit-ACK fault tests at enrollment, registration, send entry,
  completion and admission closure. Readback never returns a new capability;
  persisted MAY_HAVE_SENT is retained even when no network call occurred.
- [ ] Write Event-coordinated close-versus-registration tests. Accepted-before-close
  stays recorded; rejected-after-close has no transport entry. Duplicate IDs,
  stale revisions, foreign/forked/expired grants and modified digests reject.
- [ ] Run focused tests RED, then implement the SQL/lifecycle interfaces. Keep
  namespace uniqueness, immutable metadata/history and monotonic state guarded
  transactionally. Recheck exact original binding and request on each transition.
  Successful completion atomically updates expected aggregate once, never twice.
- [ ] Run all new files and existing `test_clickhouse_authority_sqlite.py`,
  `test_clickhouse_authority_processes.py`, codec/kernel tests GREEN. For fresh
  Linux process checks preserve journals and counters; no macOS durability claim.
- [ ] Commit/update PR; v2 is not exposed as an enabled publication backend yet.

## Task 3: Serialized authority-owned CREATE and INSERT

**Files:** Create gateway/native candidate modules. Tests:
`tests/test_clickhouse_candidate_gateway.py`,
`tests/test_clickhouse_native_candidate.py`. Reuse existing endpoint and private
logging isolation without changing global SDK classes/loggers.

**Interfaces:**

- `CandidateGateway(authority: SQLiteCandidateAuthority, requests:
  CandidateRequestJournal, exclusion: PublicationExclusion, transport:
  CandidateTransport, profile: ProtectedObservationProfile)`; persistence and
  transport dependencies use consumer-owned protocols with the exact Task 2
  methods, not concrete type imports in reusable policy.
  Translate uncertain persistence/native outcomes to `PublicationUnknown`
  after retaining the operation; pre-send validation/conflict remains a typed
  `AuthorityError`/`AuthorityConflict` with no hidden retry.
- `create(invocation: CandidateInvocation) -> None` and
  `insert(invocation: CandidateInvocation, batch: CandidateBatch) -> None`.
  No async queue or driver retry; one owned native mutation in flight.
- Gateway-local `CandidateTransport.execute(request: CandidateMutationRequest,
  design: CandidateDesign, batch: CandidateBatch | None) -> CandidateCompletion`.
  Frozen completion binds operation, query ID, kind, statement/payload digests,
  server ID/version/revision and driver version; its canonical receipt profile
  is `dpone.clickhouse.candidate-completion.v1`. Only the trusted transport's
  positively validated return may close a request; DTO construction is not proof.
- `DirectNativeCandidateTransport(endpoint: NativePublicationEndpoint,
  limits: ObservationLimits)` implements that capability using one private
  synchronous connection per request. Fixed renderer owns CREATE and
  `INSERT INTO <candidate> (<exact columns>) VALUES`; no caller statement.

- [ ] Write RED tests: CREATE cannot adopt an existing table; CREATE-before-INSERT;
  exact immutable batch metadata; late writer rejection; accepted paused writer
  plus closed admission; unknown writer prevents sealing; every send has a prior
  acknowledged registration and MAY_HAVE_SENT transition.
  `test_lost_send_ack_never_calls_transport` injects a commit-then-raise fault
  at `begin_send`, with the batch/request registered normally and the transport
  spy reset after the acknowledged CREATE setup:

  ```python
  with pytest.raises(PublicationUnknown):
      gateway.insert(invocation, batch)
  assert transport.calls == []
  assert requests.requests(invocation.binding.operation_id)[-1].state == TransportState.MAY_HAVE_SENT
  ```
- [ ] Write protocol tests using pinned real SDK entry points with fake packets:
  successful EOS versus EOF/exception/truncated/unexpected packet; wrong peer,
  driver/server version, wrong insert sample schema, payload mismatch and raw log
  redaction. Timeout/cancellation and server rejection retain uncertainty.
- [ ] Run both focused files RED, then implement gateway ordering. Acquire
  execution exclusion for request send; registration/close CAS stays the admission
  authority. Never fetch a replacement source batch after uncertain entry.
- [ ] Implement fixed direct protocol: connect once with reconnect disabled;
  validate peer/profile, send fixed query, validate INSERT sample block against
  the approved schema, send the validated bounded data and empty terminator,
  consume successful EOS, disconnect, validate completion and persist it.
  A post-EOS disconnect failure is conservative uncertainty, not replay permission.
  Do not use the generic connector retry loop or a new thread/process queue.
- [ ] Apply `request_seconds` using an injected monotonic clock in transport:
  recheck before each protocol phase and cap each socket timeout by remaining
  budget. Exceeding budget retains work; timeout is not server cancellation or
  proof of closure. Check byte/row limits before encoded batch allocation/send;
  never promise a hard RSS ceiling.
- [ ] Run both files plus existing native publisher/driver/exclusion tests GREEN;
  assert exactly one transport invocation, no sensitive logging and no changes
  to ordinary concurrent driver connections. Commit/update PR.

## Task 4: Actual protected observation and immutable candidate seal

**Files:** Create observation port, native catalog reader, observer and seal
service; extend exclusion port/adapter with the bound session below. Tests:
`tests/test_clickhouse_authority_observer.py`,
`tests/test_clickhouse_native_catalog.py`, `tests/test_clickhouse_candidate_seal.py`.

**Interfaces:**

- Add `BoundExecutionSession(ExecutionSession)` protocol with
  `assert_bound(identity: AuthorityStorageIdentity, binding: OperationBinding)
  -> None`. Local exclusion returns this subtype and validates authority inode,
  exact binding, lock identity, PID/thread and lifetime. Existing unbound
  `ExecutionSession.assert_current` consumers remain compatible. Define and test
  this now because the observer requires it; Task 5 reuses it for publication.
- In observation contracts, frozen `TableCatalog(name: str, uuid: str,
  create_sql: str, columns: tuple[CandidateColumn, ...], engine: str,
  partition_key: str, sorting_key: str, primary_key: str, sampling_key: str,
  storage_policy: str, active_partitions: tuple[str, ...])`.
- Frozen `CatalogSnapshot(server_id: str, server_version: tuple[int, int, int],
  database_uuid: str, database_engine: str, target: TableCatalog | None,
  candidate: TableCatalog | None, mutations: tuple[str, ...],
  dependencies: tuple[str, ...], row_policies: tuple[str, ...])` carries actual
  reader results, not completeness/safety booleans. Reader errors or missing
  visibility never produce empty-success lists. Unknown features reject.
- `ProtectedCatalogReader.snapshot(binding: OperationBinding) -> CatalogSnapshot`
  and `rows(binding: OperationBinding, table: str, columns:
  tuple[CandidateColumn, ...], limits: ObservationLimits)
  -> ContextManager[Iterator[ObservedRow]]`; `ObservedRow(values:
  tuple[object, ...], partition_id: str)` is immutable.
- `DirectNativeCatalogReader(endpoint: NativePublicationEndpoint,
  limits: ObservationLimits)` uses fixed read-only metadata/stream queries and
  private logging isolation. Reject row filtering/incomplete grants during
  readiness; SELECT error is never absence. No user SQL, implicit retries or
  result-limit overflow mode that truncates successfully.
- `ProtectedClickHouseObserver(reader: ProtectedCatalogReader, profile:
  ProtectedObservationProfile, authority_identity:
  Callable[[], AuthorityStorageIdentity])` supplies `candidate(binding, session:
  BoundExecutionSession) -> CandidateObservation` and
  `publication(binding, session, seal: CandidateSeal, intent:
  PublicationIntent | None) -> PublicationObservation`.
  Frozen `CandidateObservation` binds candidate UUID/design/evidence,
  catalog-before/after digests and profile identity, not authority.
  Before and after reads, validate `session.assert_bound(authority_identity(),
  binding)` against the explicitly injected original store identity.
  For publication, construct each table's encoder from its own parsed supported
  design under the same profile rules. Do not encode a differently designed
  target using candidate columns; valid design change must remain eligible for
  the existing EXCHANGE selection. Apply scan row/byte budgets to the complete
  observation across target and candidate, not separately reset for each table.
- `CandidateSealService(authority, requests, exclusion, observer).seal(invocation:
  CandidateInvocation) -> CandidateSeal` closes admission and, under exclusion,
  verifies source exhaustion, CREATE, every accepted completion and candidate
  parity. `CandidateSeal` stores binding, UUID, request frontier/history digest,
  profile/design digests and expected/observed typed evidence.
  `SQLiteCandidateAuthority.persist_seal(invocation, observation,
  expected_revision: int) -> CandidateSeal` atomically rechecks all prerequisites;
  it cannot accept a caller-supplied sealed bool or bypass unclosed requests.

- [ ] Write RED catalog tests: denied SELECT/SHOW, missing system fields, TTL,
  mutation, dependencies, row policy, unsupported engine/storage policy, detached
  or inconsistent objects, UUID/design drift, unknown grammar and incomplete
  stream must reject before a complete observation is returned.
  Extend `tests/test_clickhouse_authority_execution_lock.py` to reject foreign,
  expired and forked bound sessions before any catalog read.
- [ ] Write typed scan tests with actual returned row values, partition IDs and
  budgets. Verify empty inventory, duplicate multiplicity and count; bound each
  partition/row/byte accumulation without truncation. Restore FixedString width
  before canonical encoding. Cross-check active parts excluding zero-row parts.
  Include different supported target/candidate schemas and combined scan-budget
  exhaustion; unsupported target types still reject instead of selecting fallback.
- [ ] Write seal tests: source exception/unsent request/uncertain completion/
  changed candidate cannot seal; normal exhausted empty source can; stale CAS
  cannot replace a seal; closed admission never reopens. No SQL is dispatched
  by the seal service.
  `test_unclosed_insert_cannot_seal` uses a registered MAY_HAVE_SENT INSERT,
  completed CREATE and normal exhaustion, then checks the durable original:

  ```python
  with pytest.raises(AuthorityError):
      seals.seal(invocation)
  assert authority.inspect(invocation.binding.operation_id).seal is None
  assert observer.calls == []
  ```
- [ ] Run focused files RED, implement the interfaces, then run GREEN. Keep
  catalog-before/rows/catalog-after inside one validated session. Physical merge
  part names are not logical evidence; reject inconsistent logical inventory.
- [ ] Probe the pinned server's real SHOW CREATE/catalog shape in an owned
  isolated Docker fixture before broadening any parser rule. Distinguish
  server-rendered syntax from caller clauses without deleting unknown content.
  If the approved grammar cannot represent that shape, stop with the exact
  incompatibility; do not silently weaken the approved profile.
- [ ] Exercise actual CREATE/INSERT from Task 3 and actual reader; no synthetic
  digest/flags qualify as seal evidence. Preserve this component proof separately
  from final exact-commit certification. Commit/update PR.

## Task 5: Session-safe backend and source-free public orchestration

**Files:** Create Task 5 modules; modify the publisher, reusing Task 4's bound
exclusion capability.
Tests: `tests/test_clickhouse_guarded_backend.py`,
`tests/test_clickhouse_protected_publication.py`,
`tests/test_clickhouse_publication_readiness.py`; extend existing exclusion and
publisher tests without weakening their standalone contracts.

**Interfaces:**

- Consume Task 4's `BoundExecutionSession`; do not define a second session type
  or change the existing `ExecutionSession` protocol into a dispatch capability.
- Add `AuthorityPublicationPublisher.execute_in_session(grant: DispatchGrant,
  session: BoundExecutionSession) -> None` and `close_in_session(operation_id:
  str, session: BoundExecutionSession) -> None`. Reject foreign sessions before
  mutation. Old `execute_once`/`close_and_drain` wrappers still acquire their own
  exclusion and keep existing behavior; share a private implementation, not a
  nested public lock call. No requirement is added to third-party legacy session
  objects used only through the existing wrapper interface.
- `SQLiteGuardedPublicationBackend(authority: SQLiteCandidateAuthority,
  exclusion: PublicationExclusion, observer: ProtectedClickHouseObserver,
  publisher: AuthorityPublicationPublisher, invocation: CandidateInvocation | None)`
  implements the existing eight-method port exactly. Consumer protocols expose
  only the used capabilities. Recovery uses `invocation=None` and cannot prepare
  or claim new work. No source is a backend constructor dependency.
- `hold` maintains one active bound session and clears volatile grant/revision
  cache on exit. `read` unwraps an original JournalEntry; `prepare` requires
  current acknowledged seal plus fresh invocation; `claim` caches only the
  exact newly acknowledged DispatchGrant; `execute_once` checks the exact
  intent/grant pair. `resolve` uses original revision/closure/observation.
- V2 publication preparation is exposed only through the gated authority method
  `prepare_publication(invocation, intent) -> JournalEntry`. Its shared journal
  engine stays private to composition; no public v2 `acquire+prepare` shortcut.
- Readiness `DeploymentIngressInventory` is a frozen reviewed inventory of
  deployment/server/direct endpoint, observer/publisher principals, revision,
  managed physical names, historical authority enrollment and mutation-path
  entries (job ID, principal, physical names, authority ID, enabled status).
  Credentials are excluded. `PublicationReadiness(inventory:
  DeploymentIngressInventory, probe: ReadinessProbe)` supplies `verify(request)
  -> VerifiedEnrollment`, which checks the inventory and actual supported server/grants/
  visibility before returning the opaque capability used in Task 2.
  It rechecks before seal/publication; an inventory hash alone is not proof.
  A known enabled bypass path or prior/ambiguous enrollment rejects. The review
  and actual credential routing remain deployment obligations, not SQL proof
  of the absence of every possible external writer.
  Consumer-owned `ReadinessProbe.check(request: ProtectedPublicationRequest,
  inventory: DeploymentIngressInventory) -> None` raises on any missing evidence.
  `DirectNativeReadinessProbe(observer_endpoint: NativePublicationEndpoint,
  publisher_endpoint: NativePublicationEndpoint)` in the same readiness adapter
  performs fixed version/peer/current-user/grant/catalog-visibility probes for
  both explicit principals. It cannot return success on denied introspection or
  unknown role/grant inheritance. No arbitrary query callback is exposed.
- Runtime-facing `CandidateLifecycle` port has `start(request)
  -> ContextManager[CandidateInvocation]`, `create(invocation)`,
  `write(invocation, rows: tuple[tuple[object, ...], ...])`,
  `finish(invocation) -> CandidateSeal`, `backend(invocation)
  -> GuardedPublicationBackend`, `recovery_backend(operation_id)
  -> GuardedPublicationBackend`, `inspect(operation_id) -> CandidateStatus`,
  `retain(operation_id, reason)`. It is implemented by an explicitly composed
  adapter, with readiness/acquisition hidden behind `start`.
- `AuthorityCandidateLifecycle(authority, readiness, gateway, seal_service,
  backend_factory: Callable[[CandidateInvocation | None], GuardedPublicationBackend])`
  in `adapters.clickhouse_candidate_lifecycle` implements that port. The factory
  is an explicit closure at the example composition root, never a registry or
  global lookup. `start` owns readiness/enrollment capability lifetimes;
  `finish` durably records normal exhaustion then closes/joins/seals. Recovery
  inspects the original binding before constructing a backend with `None`.
- `ProtectedClickHousePublication(lifecycle: CandidateLifecycle)` in runtime
  supplies `publish_new(request: ProtectedPublicationRequest,
  source_factory: Callable[[], Iterator[tuple[object, ...]]]) -> PublicationRecord`
  and `recover_existing(operation_id: str) -> ProtectedPublicationStatus`.
  The latter status contains `candidate: CandidateStatus`, `publication:
  PublicationRecord | None`, `observation_kind: Literal["historical", "fresh",
  "not_observed"]`, `safe_to_retry: bool` (false while owner retained).

- [ ] Write RED tests for a foreign but valid session, closed session, fork and
  wrong thread. Assert no transport call and no second flock; retain standalone
  publisher tests with legacy fake sessions.
- [ ] Write an actual-kernel/fake-I/O test for all eight backend methods and all
  four selector outcomes. Assert exact stored revision/grant mapping, continuous
  exclusion during both observations/send, and no permission from copied DTOs.
- [ ] Write source sentinels: existing operation, reserved name, unsupported
  schema/grants/topology/inventory reject with `source_calls == 0`;
  `recover_existing` has no source argument. SEALED-without-PREPARED remains
  retained with zero CREATE/INSERT/publication calls on reopen.
  `test_reopened_seal_without_prepared_never_dispatches` opens a fresh service
  over an original sealed journal without a kernel record:

  ```python
  status = reopened.recover_existing(operation_id)
  assert status.publication is None
  assert status.candidate.lifecycle == "retained"
  assert status.safe_to_retry is False
  assert transport.calls == []
  ```
- [ ] Write source-error and boundary tests: iterator raises after a successful
  batch, empty normal exhaustion, over-limit row, abandoned context and exception
  during source close. Close/retain on failure; never treat any exception as
  normal exhaustion or publish an incomplete candidate.
- [ ] Run focused tests RED, implement the bridge/readiness/lifecycle adapter
  and runtime orchestration, then run GREEN. Candidate request locks may be
  released between completed synchronous requests; closed admission and durable
  ownership exclude later writers. Kernel hold acquires a fresh single session,
  not an outer lock held during source extraction.
- [ ] Use bounded batching in runtime before gateway entry. Mark normal source
  exhaustion durably only after iteration completes, close/join/seal, invoke
  existing kernel. On any exception retain the original operation, invalidate
  invocation capability and close source resources without masking the primary
  failure. After PREPARED only source-free kernel recovery is allowed.
- [ ] Test post-RENAME/EXCHANGE seal validation by UUID transition from frozen
  intent, not by assuming candidate name still holds the sealed table. Terminal
  historical readback is labelled historical, never current target freshness.
  Run all earlier focused tasks plus kernel/v1/publisher regression tests;
  commit/update PR with availability still explicitly staged.

## Task 6: Runnable journey, live faults and final exact-commit proof

**Files:** Create `examples/python/clickhouse_protected_publication.py`,
`docs/tutorials/clickhouse-protected-publication.md`,
`docs/clickhouse-protected-publication.md`,
`docs/runbooks/clickhouse-publication-recovery.md`,
`tests/test_clickhouse_protected_publication_example.py`,
`tests/integration/test_clickhouse_protected_publication.py`,
`tests/integration/clickhouse_protected_publication_support.py`,
`tests/integration/clickhouse_protected_publication_faults.py`.
Update overview, journal/native guides, closure runbook, architecture, ADR index,
spec/plan progress, changelog, navigation and producer-owned metrics as needed.

**Interfaces:** The example explicitly composes concrete adapters with the
runtime entry point from Task 5. Credentials come from environment at invocation,
never module import, CLI examples or retained evidence. Example `main(argv:
Sequence[str] | None = None) -> int` is a standalone demonstration, not a new
`dpone` command. Exit 0 means COMMITTED, 2 means rejected/NOT_PUBLISHED, and 3
means retained/unresolved; JSON status goes to stdout and redacted errors to
stderr. Provide separate `publish`/`recover` modes; recovery cannot
load or reference a source. Require a new enrolled fixture for each demonstration,
never teach a new operation ID as an escape from retained ownership.

- [ ] Write RED example tests for explicit limits, no import-time I/O, missing
  credentials before source calls, expected status output and secret redaction.
  Tutorial fixture values: 1024 batch rows, 8 MiB batch, 1 MiB row, 128 columns,
  128 partitions, 1,000,000 scan rows, 256 MiB scan bytes, 60-second request
  budget. These are explicit example limits, not new production defaults.
  `test_example_recovery_reports_retained_without_source` injects the documented
  retained source-free status into the example's explicit composition fixture:

  ```python
  assert main(["recover", "--operation-id", operation_id]) == 3
  assert source_calls == []
  assert json.loads(capsys.readouterr().out)["safe_to_retry"] is False
  ```
- [ ] Implement the example and tutorial/reference/runbook. Walk discover,
  provision, configure, load, seal, publish, diagnose, recover and upgrade. Explain
  v1 preservation/no migration, retained names/owners, grammar/type rejection,
  source-free recovery and no production route activation; validate the actual
  example rather than pseudocode with `supplied_backend`.
- [ ] Write live cases using actual v2 registration, CREATE/INSERT, observer and
  seal: RENAME, single-partition REPLACE (unpartitioned `all` included),
  multi-partition/stale-partition EXCHANGE, design change, equal-content no-op
  and empty candidate. Compare actual duplicate-preserving rows, schema, UUIDs,
  partition inventory, journal history and dispatch counts.
- [ ] Add Event/fault-relay cases: in-flight accepted INSERT while admission closes;
  late writer; successful effect with lost/truncated response; failure before
  observed effect; lost SQLite ACK; drift between observe/send; crash after seal
  before PREPARED; terminal receipt before resolution. Restart in a fresh Linux
  process and assert source/CREATE/INSERT/publication call counts independently
  outside swallowed exception paths. No synthetic completion/seal flags.
- [ ] Use isolated owned Docker containers/network/named volumes only. Record
  image digest and `SELECT version()` full version, native revision, driver,
  SQLite, Python/platform and inventory scope. Existing user containers remain
  untouched. Local fault tests are not production ingress/TLS/power-loss proof.
- [ ] Run focused live cases with `-m "integration_live and integration_clickhouse"`
  and require zero skips for the requested matrix. Raw logs, JUnit, fault
  activation, original journals, actual rows/UUIDs and source/replay counters
  belong under a new `test_artifacts/clickhouse-protected-publication-<run-id>/`.
  Preserve older evidence directories unchanged.
- [ ] Run the change-aware selector, Ruff/format, mypy, import rules, graph and
  module-size ratchets, full non-live suite, docs/generated references/language
  contracts/strict MkDocs. Use canonical exact-SHA module-size commands from
  AGENTS.md. Run `update-dev-metrics --check`; if new modules require refresh,
  stage them and use the existing producer, never hand-edit metrics. Record
  every SKIP reason separately; no package build unless packaging changed.
- [ ] Commit the final implementation/docs, immediately push/update its PR, then
  freeze the exact head/tree and repeat final focused/live certification against
  that identity. Any subsequent source change invalidates affected exact-commit
  proof; do not substitute a prior candidate's result.
- [ ] Request one fresh-context independent whole-diff reviewer that did not
  implement the feature. Provide spec, plan, base/head, contracts and evidence.
  Fix findings with RED/GREEN checks and obtain follow-up review of changed scope.
  Record reviewed SHA, severity, disposition and verdict; unresolved review is
  UNVERIFIED, not completion. This step does not authorize merge or publication.
- [ ] Archive owned test data/journals before resource cleanup, checksum artifacts
  and verify the checksums. Remove only validated owned resources after evidence
  capture; stopped-volume archives are not certified logical restore/power-loss
  proof. Report remaining deployment and production-route gates explicitly.

## Completion and review handoff

Each task's commit follows a demonstrated RED/GREEN cycle, focused checks and
PR description update; plan progress tracks only actually completed work.
No task can advertise production capability from unit tests or fixture seals.

The final report must include behavior/compatibility, PASS/FAIL/SKIP/N/A checks,
exact evidence paths and limitations, docs/CJM, independent review disposition,
and readiness separately for review, merge and release. Owner release and
ODBC route certification remain unfinished after this plan is implemented.

Plan self-review: all supplement sections map to Tasks 1–6; every Review Focus
item has an owning negative test; public method names and DTOs above are shared
by reference, not redefined per task. File ownership is explicit and Native
single-writer execution is preserved. The maintainer reviewed and approved this
plan before `executing-plans` production-code edits began.
