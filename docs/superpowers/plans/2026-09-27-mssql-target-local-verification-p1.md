# MSSQL Target-Local Verification P1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in, target-local MSSQL native verifier that returns aggregate digest data instead of business rows while preserving the released BCP plus Python-readback path byte-for-byte.

**Architecture:** A closed digest compiler reproduces the admitted native bytes for the initial four-type capability and framework prepared columns, and a decoder folds eight SQL word sums into the existing digest envelope. A versioned policy and journal identity isolate optimized invocations from v1. The current importer, preparation, publication, receipt, and checkpoint services remain authoritative.

**Tech Stack:** Python 3.12, T-SQL, pyodbc connector abstractions, pytest, JSON Schema, local Docker SQL Server.

**Spec:** `docs/feature-specs/mssql-sqlclient-target-local-verification-v2.md`

## Global Constraints

- Base implementation on the current `origin/master`; preserve all changes after 0.83.23 when rebasing.
- Omitted selectors remain `bcp` plus `python_readback` and retain v1 plan, journal, evidence, and behavior.
- P1 adds `target_local`; it does not add the SqlClient writer.
- Large-set content evidence remains probabilistic; exact claims apply only to identity, schema, count, algorithm parity, and small fixture comparisons.
- No corporate endpoint, object, query, credential, workload identity, or private measurement may enter the repository or public artifacts.
- Production code follows red-green-refactor; each task records the failing test before implementation.
- Shared schemas, planning projections, changelog, MkDocs navigation, and task integration belong to the integrator.

## Review Focus

- Nullable native framing: SQL must emit the same missing prefix and present length prefix as `MssqlNativeEncoder`.
- SQL Server byte order: integer, float, and datetime limbs must match Python on boundary vectors.
- Count overflow: `expected_rows + 1` must fail closed and never be reported as an exact larger count.
- Compatibility: old manifests and journal v1 payloads must remain byte-identical.
- Recovery: a target-local invocation must never be decoded or resumed as v1.

---

### Task 1: Target-local digest kernel

**Files:**
- Create: `src/dpone/runtime/sinks/mssql_native_target_digest.py`
- Create: `tests/test_mssql_native_target_digest.py`

**Interfaces:**
- Produces: `build_target_digest_sql(qualified_stage: str, contract: SourceNativeWireContract, expected_rows: int) -> str`
- Produces: `decode_target_digest_row(row: Any, *, expected_rows: int) -> TargetDigest`
- Produces: immutable `TargetDigest(rows: int, typed_digest: str, typed_sum: int)`

- [ ] Write failing tests for four admitted raw types, nullable framing, 100-column bound, count overflow, malformed driver rows, limb carry, and no business-row projection.
- [ ] Run `uv run pytest tests/test_mssql_native_target_digest.py -q` and record the expected import/behavior failures.
- [ ] Implement the minimal closed compiler and decoder. Quote only validated identifiers; use one temporary hash heap, one SHA-256 per row, eight `decimal(38,0)` sums, and the existing `native_multiset_digest` envelope.
- [ ] Run the focused test to green and refactor without broadening the type registry.
- [ ] Commit the task in its writer worktree.

### Task 2: Verification policy and journal identity v2

**Files:**
- Create: `src/dpone/contracts/mssql_native_verification.py`
- Create: `src/dpone/adapters/mssql_native_chunks_journal_v2.py`
- Create: `tests/test_mssql_native_verification_policy.py`
- Create: `tests/test_mssql_native_chunks_journal_v2.py`

**Interfaces:**
- Produces: `NativeVerificationBackend` closed enum and `NativeVerificationIdentityV2`.
- Produces: `NativeChunkJournalV2` with a distinct key prefix and strict target-local identity; existing receipt/publication contracts are reused.

- [ ] Write failing policy tests for omission/v1, explicit target-local/v2, invalid values, and fail-closed identity drift.
- [ ] Write failing journal tests for distinct keys, canonical identity, strict event shape, hash links, invalid transitions, pre-EOF restart rejection, and v1 isolation.
- [ ] Run both files and record RED.
- [ ] Implement immutable contracts and v2 journal with the approved closed state machine. Do not edit v1 journal.
- [ ] Run focused tests to green and commit in the writer worktree.

### Task 3: Runtime raw and prepared verification integration

**Files:**
- Modify: `src/dpone/runtime/sinks/mssql_native_import.py`
- Modify: `src/dpone/runtime/sinks/mssql_native_prepared_digests.py`
- Modify: `src/dpone/runtime/sinks/mssql_native_prepare.py`
- Modify: `src/dpone/runtime/sinks/mssql_native_composition.py`
- Modify: `src/dpone/runtime/mssql_native_chunks.py`
- Modify: `tests/test_mssql_native_staged_import.py`
- Modify: `tests/test_mssql_native_integrity_readbacks.py`
- Modify: `tests/test_mssql_native_composition.py`
- Modify: `tests/test_mssql_native_chunks_execution.py`

**Interfaces:**
- Consumes: Task 1 digest kernel and Task 2 backend identity/journal.
- Produces: opt-in aggregate-only raw/import/inspect/preparation/reverify behavior; default Python readback remains unchanged.

- [ ] Add failing tests proving zero business-row iterators for target-local raw and prepared verification, aggregate mismatch rejection, repeated prepublication verification, and unchanged default reads.
- [ ] Run the named focused tests and record RED.
- [ ] Inject verification policy at composition, select journal v2 for target-local, and route raw/prepared checks through the kernel without changing receipt/publication ordering.
- [ ] Run focused tests to green, including the existing BCP lifecycle tests.
- [ ] Commit the integrated runtime task.

### Task 4: Public manifest, planning, schemas, evidence, and self-service docs

**Files:**
- Modify: `src/dpone/manifest/mssql_native_policy.py`
- Modify: `src/dpone/readiness/mssql_native_planning.py`
- Modify: `src/dpone/schema/etl-config.schema.json`
- Modify: `src/dpone/schema/etl-batch-manifest.schema.json`
- Modify: `examples/native/clickhouse-to-mssql-native.yaml`
- Create: `examples/native/clickhouse-to-mssql-target-local.yaml`
- Modify: `docs/mssql-native-transport.md`
- Modify: `docs/source-sink/clickhouse-to-mssql.md`
- Modify: relevant focused manifest/planning/docs tests

**Interfaces:**
- Consumes: Task 2 selector/identity and Task 3 runtime behavior.
- Produces: schema-valid explicit `verification_backend: target_local`, plan projection, blockers, compatibility documentation, and examples.

- [ ] Add failing schema/policy/planning/example tests for omission, explicit selection, invalid values, and v2 projection.
- [ ] Run focused tests and record RED.
- [ ] Implement the two generated schemas, readiness projection, examples, and focused self-service docs. Keep existing example behavior unchanged.
- [ ] Run schema, docs, and compatibility tests to green.
- [ ] Commit the public-contract task.

### Task 5: Docker differential certification and release evidence

**Files:**
- Create or modify focused files under `tests/integration/mssql/` for target-local parity.
- Create versioned generic fixture descriptors under `tests/fixtures/`.
- Modify: `docs/feature-specs/mssql-sqlclient-target-local-verification-v2.md` only after evidence exists.
- Modify: `CHANGELOG.md` at integration time.

**Interfaces:**
- Consumes: Tasks 1-4 integrated candidate.
- Produces: narrow and wide100 parity evidence on local Docker SQL Server; no private data.

- [ ] Add live tests for all admitted raw/prepared layouts, NULL/empty/Unicode/NUL/boundaries/duplicates, count overflow, mutation rejection, and repeated digest stability.
- [ ] Run the live tests against disposable local Docker SQL Server and record exact commit/environment-bound generic evidence.
- [ ] Run `uv run python tools/agent_policy/select_checks.py --base-ref origin/master`, all selected checks, and the normal broad Python/docs gates.
- [ ] Obtain fresh-context correctness, compatibility, data-loss, recovery, docs, and evidence review; fix and re-review all blockers.
- [ ] Mark P1 evidence in the specification, update changelog, and commit.

## Self-review result

- Spec coverage: P1 covers opt-in target-local raw/prepared verification, identity isolation, public selection, compatibility, Docker parity, docs, and evidence. SqlClient writer, private seven-day qualification, persisted hashes, and direct streaming remain later phases.
- Interface consistency: Task 1 kernel and Task 2 identity are independent; Task 3 is their only runtime integration point; Task 4 owns shared public schemas; Task 5 certifies the integrated result.
- File conflicts: Tasks 1 and 2 are disjoint. Tasks 3-5 execute sequentially under the integrator after cherry-picking Tasks 1 and 2.
- Review-focus coverage: each listed risk has an explicit Task 1, 2, 3, or 5 test.
