# ClickHouse Durable Authority Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans or superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Implement the durable, nonexpiring single-host authority and journal transitions needed by the approved dpone-only publication backend.

**Architecture:** Keep immutable values and strict record serialization separate from SQLite transaction machinery and publication-specific state transitions. The new store is explicitly provisioned once and subsequently opened without creation; it grants one volatile dispatch capability after acknowledged CAS, never from a loaded record. Existing kernel, TTL window store and runtime routes remain unchanged.

**Tech Stack:** Python standard library (`sqlite3`, `json`, `hashlib`, `secrets`, `pathlib`), pytest and fresh subprocesses; no dependency changes.

**Spec:** [Approved dpone-only authority specification](../../feature-design-clickhouse-dpone-only-authority.md).

**Status:** APPROVED for Native execution by the maintainer on 2026-09-30. One integrator implements the tasks; a fresh independent reviewer checks the completed increment.

## Scope and global constraints

- This is increment 1: contracts, journal, CAS and transport-state authority. It
  is independently testable and not a complete protected backend.
- Increment 2 will add one-shot native transport and conservative closure;
  increment 3 will add observation, prepublication lifecycle and kernel
  composition. Each must retain the approved spec's behavior and evidence gates.
- `dpone.clickhouse.authority.v1` stores existing
  `dpone.clickhouse.guarded-publication.v2` records without reinterpretation.
- SQLite WAL, `synchronous=FULL`, foreign keys, short `BEGIN IMMEDIATE`
  transactions. No network filesystem, TTL expiry or automatic takeover.
- Standard startup opens existing storage only. Provisioning is explicit and
  create-once. Missing/corrupt/version-mismatched authority fails closed.
- Restored old valid backups cannot be identified by SQLite alone; do not claim
  automatic stale-backup detection. External credential isolation is required.
- No ownership release API, second-operation admission, default router binding,
  CLI flag, manifest change, source fetch or production credential mutation.
- All `uv run` commands below add
  `--frozen --extra postgres --extra gcp --extra columnar --extra dbt-mssql --extra accel`.
- One writer owns the paths in
  `docs/agent-tasks/clickhouse-authority-foundation.yml`. Keep existing user edits
  intact. No module-size/graph budget or baseline changes.

## Review focus

1. Alternate hostnames and changed table UUIDs must not create a new target owner
   (Task 1/2 canonical-subject tests).
2. A claimed record reloaded after lost commit ACK must not recreate a dispatch
   grant (Task 3 ambiguous-claim/subprocess tests).
3. A delayed claimant must lose to durable no-send closure, even after restart
   (Task 3 competing transition tests).
4. Missing, symlinked or replaced journal files must not initialize fresh empty
   authority (Task 2 storage identity tests; stale valid backups stay unsupported).
5. An outcome labelled terminal must not implicitly release target ownership
   (Task 4 retained-owner and second-operation rejection tests).

## File structure

| Path | Responsibility |
|---|---|
| `src/dpone/contracts/clickhouse_authority.py` | Canonical subject, binding, transport states, grant and errors |
| `src/dpone/contracts/clickhouse_publication.py` | Revisioned `JournalEntry` beside existing publication values; no change to method choice |
| `src/dpone/adapters/clickhouse_publication_codec.py` | Strict versioned canonical serialization of existing publication records |
| `src/dpone/adapters/clickhouse_authority_storage.py` | Create-once/open-existing storage, identity and transaction lifecycle |
| `src/dpone/adapters/clickhouse_authority_sqlite.py` | Ownership, immutable preparation, claim/send/close/resolution CAS |
| `tests/test_clickhouse_publication_codec.py` | Serialization invariants and hostile/corrupt payload rejection |
| `tests/test_clickhouse_authority_sqlite.py` | Store lifecycle, owner and state transitions |
| `tests/test_clickhouse_authority_processes.py` | Independent-process race/crash boundaries |
| `docs/clickhouse-authority-journal.md` | Tested Python lifecycle and explicit non-authority/non-production limits |

If storage and state policy cannot remain cohesive within existing budgets, stop
and revise this map before adding arbitrary `part_1` modules. Do not import
runtime classes into adapters; public authority errors and values live in contracts.
Private storage/codec failures stay local and are translated at the authority boundary.

Review amendment (2026-09-30): the integrator also owns
`src/dpone/contracts/clickhouse_publication.py` solely to place the new
`JournalEntry` beside `PublicationRecord`. Existing publication values, method
choice and kernel interfaces remain unchanged. This separates revisioned
publication snapshots from ownership capabilities; no compatibility reexports
or new generic modules are added. Direct invalid `JournalEntry` values raise
`ValueError`, consistent with the publication contract; invalid persisted values
surface as `AuthorityError`. These APIs have not been released.

## Task 1: Immutable identity and strict record codec

**Files:** Create the contracts and codec modules plus codec tests above.

**Interfaces:**

- `AuthoritySubject(deployment_id: str, server_id: str, database: str, target: str)`;
  frozen value, `key: str` property uses canonical JSON of these four nonempty
  values and SHA-256. The endpoint URL and changing target UUID are not key fields.
- `OperationBinding(operation_id: str, subject: AuthoritySubject, candidate: str,
  epoch: int)`; frozen, positive non-bool epoch, distinct simple SQL identifiers
  for target/candidate. Registered `server_id` is a deployment authority input,
  not an arbitrary hostname supplied for each call.
- `DispatchGrant(operation_id: str, epoch: int, secret: str)`; private secret
  excluded from repr; this is a capability issued only by an acknowledged claim,
  not deserialized in diagnostic output.
- `AuthorityError(RuntimeError)` and `AuthorityConflict(AuthorityError)`.
- `TransportState`: `not_started`, `may_have_sent`, `closed_without_send`,
  `closed_terminal`. No transition from either closed state is allowed.
- `JournalEntry(record: PublicationRecord, revision: int)`; frozen snapshot with
  positive non-bool revision, used for exact store CAS. The future backend unwraps
  the record for the unchanged kernel port; it must retain the store revision.
- `encode_record(record: PublicationRecord) -> str` and
  `decode_record(payload: str) -> PublicationRecord`; canonical UTF-8 JSON,
  sorted keys, compact separators, reject NaN, unknown keys/version/types and
  duplicate JSON keys. No pickle, eval, silent coercion or ignored fields.

- [ ] Write `test_codec_round_trip_preserves_full_intent_and_claim_history` with
  fixed UUIDs, 64-character literal digests, prepared/claimed/unknown/terminal
  states and both absent/present target. Assert decoded equality and deterministic
  bytes, never grant creation.
- [ ] Write parametrized corrupt-input tests: bool as row count/epoch,
  noncanonical digest, duplicate keys, unknown field/version, inconsistent claim,
  altered method/reason/partition. Recompute method with existing
  `choose_publication` and reject divergence rather than normalizing it.
- [ ] Write `test_subject_identity_excludes_endpoint_alias_and_table_uuid` and
  `test_binding_rejects_reused_target_as_candidate` using literal expectations.
- [ ] Run `pytest tests/test_clickhouse_publication_codec.py -q`; record RED
  for the missing implementation, then implement the interfaces and repeat GREEN.
- [ ] Run Ruff on the new files; commit the cohesive contracts/codec after review
  and immediately push/update PR #240. Do not claim the backend is complete.

## Task 2: Create-once storage and retained target ownership

**Files:** Add the storage and SQLite adapters plus SQLite tests.

**Interfaces:**

- `SQLitePublicationAuthority.provision(path: Path, deployment_id: str) -> None`:
  exclusive create, restrictive directory/file permissions, initialize and sync
  schema; an existing path is an error, never an idempotent reset.
- `SQLitePublicationAuthority(path: Path, deployment_id: str)`: open existing via
  SQLite URI `mode=rw`; validate deployment/schema, private regular file and
  stable device/inode, verify WAL/FULL/foreign keys per connection. Do not create
  parent directories in normal operation.
- `acquire(operation_id: str, subject: AuthoritySubject, candidate: str) ->
  OperationBinding`: one retained subject owner; exact same binding reopens with
  the same epoch, divergent reuse raises `AuthorityConflict`. Require operation
  IDs prefixed by the registered deployment ID and `:`; never silently rename an
  existing operation. Query-ID uniqueness follows the kernel's existing hash.
- `binding(operation_id: str) -> OperationBinding`: load original or raise;
  no ownership transfer, expiry or release function.
- Private storage helper supplies connection/transaction context managers;
  rollback on `BaseException`, close on every path, never swallow commit errors.

Store schema: metadata (one schema/deployment identity), subjects (unique subject
key, canonical bytes, owner operation, epoch), operations (unique operation/query
identity, binding, immutable intent, revision, record, transport state and hashed
grant), append-only transition history. Constraints and CAS updates enforce the
same invariants; JSON is data, not a substitute for uniqueness constraints.

- [ ] Write tests that missing-file open leaves the path absent, a provisioned
  empty journal opens, and a second provision cannot overwrite it.
- [ ] Test wrong deployment/version, corrupt database, symlink and replaced inode;
  assert `AuthorityError` and unchanged existing records. Do not write a test
  pretending an old valid backup can be automatically distinguished.
- [ ] Test two operations for one canonical subject: first retains ownership,
  second conflicts after store reopen and despite arbitrary elapsed time; exact
  same operation reopens, changed candidate/subject conflicts.
- [ ] Run SQLite tests RED, implement the minimal schema/storage/ownership path,
  repeat GREEN; inspect file descriptors and transaction cleanup on errors.
- [ ] Commit and update the existing PR after focused checks. No legacy store
  migration, temporary global lock registry or unrelated connector edits.

## Task 3: Immutable intent and mutually exclusive send/close transitions

**Files:** Extend the SQLite adapter/tests; add process tests.

**Interfaces:**

- `prepare(binding: OperationBinding, intent: PublicationIntent) -> JournalEntry`:
  verify protected subject, canonical operation/query identity and exact intent;
  create PREPARED once or return exact original PREPARED, never replace it.
- `read(operation_id: str) -> JournalEntry | None`: strictly decode original
  and retain its current revision.
- `claim(entry: JournalEntry) -> DispatchGrant | None`: acknowledged CAS
  from exact original PREPARED, persist CLAIMED/claim history plus hashed secret;
  losing CAS returns None. Commit error raises and never returns a grant.
- `begin_send(grant: DispatchGrant) -> None`: current owner/epoch/secret plus
  `not_started -> may_have_sent` CAS; duplicate/stale/closed transitions raise.
- `close_without_send(operation_id: str) -> None`: competing
  `not_started -> closed_without_send` CAS and irreversible grant revocation;
  repeat of the exact closed state is idempotent, possible-send cannot close here.
- `transport_state(operation_id: str) -> TransportState`: diagnostic original,
  not a capability. Secret hashes/tokens never appear in public JSON or logs.

- [ ] Write `test_claim_is_not_reissued_after_reload`: one grant, second claim
  None, loaded CLAIMED and UNKNOWN history never yield another grant.
- [ ] Inject commit-ACK loss beneath the adapter after durable claim commit;
  assert caller receives no grant and reopened CLAIMED cannot begin a send.
  Injection belongs in tests, not production failpoint APIs.
- [ ] Write two-process races sharing one real SQLite file: one claim winner;
  then paused claimant versus no-send closer. Assert exactly one of
  `may_have_sent` or `closed_without_send` wins and losing send never invokes a
  test-side transmission sentinel.
- [ ] Kill a subprocess after `begin_send` returns; fresh process must see
  `may_have_sent`, retain owner and reject no-send closure or another operation.
- [ ] Run SQLite/process tests RED, implement acknowledged transactional grants
  and CAS, repeat GREEN. Tests use real SQLite, not fake-store return values.
- [ ] Commit and update PR after focused compatibility tests against the existing
  publication kernel; do not change its eight-method port.

## Task 4: Terminal evidence persistence, diagnostics and complete verification

**Files:** Extend store/tests; add journal guide; update approved spec progress,
publication overview, CHANGELOG and MkDocs; main integrator owns shared files.

**Interfaces:**

- `record_terminal(grant: DispatchGrant, completion_digest: str) -> None`: exact
  owner/epoch/grant and `may_have_sent -> closed_terminal` CAS; require canonical
  64-hex digest. This store persists a trusted future publisher's receipt, it
  does not itself authenticate EndOfStream. Do not expose it to workers/CLI.
- `resolve(entry: JournalEntry, state: PublicationState,
  observed: PublicationObservation) -> JournalEntry`: exact revision/intent
  CAS, require closed state, preserve claim history and immutable observations.
  This persistence boundary accepts a future trusted backend's classification;
  it must not claim to produce independent ClickHouse observations itself.
  Every mutation increments revision, including transport changes. Callers reload
  after closure; a stale snapshot cannot resolve even if record values match.
- `diagnostics(operation_id: str) -> dict[str, object]`: redacted versioned
  ownership/record/transport history, no grant/secret/rows/credentials.
- `write_diagnostics(operation_id: str, destination: Path) -> None`: UTF-8 JSON,
  atomic create without overwriting an existing output; output is never authority.

- [ ] Write tests that terminal recording with a forged/stale/closed grant fails;
  resolution with no closure fails; reopened terminal state can resolve without
  another grant; intent/claim bytes remain immutable.
- [ ] Write `test_committed_resolution_keeps_target_owned` and a lost resolution
  ACK/reopen case: historical outcome is retained, second operation still refused.
- [ ] Test diagnostics redaction, existing-output preservation and valid JSON
  after injected write failure. Explain that caller-provided completion digests
  are trusted composition inputs, not proof of transport safety.
- [ ] Run focused tests RED, implement remaining persistence/diagnostic behavior,
  rerun GREEN; add tested Python provision/acquire/reopen examples to the guide.
- [ ] Run change-aware check selection and all applicable AGENTS Python gates:
  Ruff lint/format, mypy, import rules, layer metrics, exact-commit module-size,
  complete `pytest -m "not integration_live" -n auto --dist loadfile`.
- [ ] Run docs links/generated parity, language contracts and strict MkDocs.
  Inspect rendered navigation and the first-use/recovery path.
- [ ] Run the process suite additionally inside a new owned Linux Docker Desktop
  runner with a named local volume, no credentials and no existing containers
  changed. Record source SHA/image/SQLite/OS/volume identity, JUnit and zero skips.
  This is process/storage behavior, not power-loss or ClickHouse certification.
- [ ] Obtain an independent fresh-context review of the actual final diff and
  exact candidate SHA; resolve findings, rerun affected checks and record verdict.
- [ ] Commit/push/update PR #240; rerun exact-commit checks where needed. Keep
  the overarching backend spec APPROVED, not IMPLEMENTED; report increments 2/3
  and all ODBC release gates still incomplete.

## Handoff and evidence

Use a create-once artifact directory under `test_artifacts/` for RED/GREEN runs,
fault logs, JUnit, selected checks, Docker identity and review. Never hand-edit a
producer's receipt. Missing Docker capability is UNVERIFIED, not PASS. Cleanup
only the new owned runner/volume after exporting evidence; preserve user data.

Self-review: all authority-foundation requirements map to Tasks 1–4. Transport,
physical observation, writer join/seal and actual kernel composition are explicit
later increments, not omitted completion claims. No independent production
release, cleanup, HA or automatic unknown-outcome recovery is authorized here.

Maintainer confirmed this scoped plan and Native execution before implementation.
