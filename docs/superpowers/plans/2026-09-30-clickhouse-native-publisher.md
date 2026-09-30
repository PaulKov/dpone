# ClickHouse Native Publisher Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans for the preserved Native execution choice. Steps use checkbox syntax for tracking; one fresh independent reviewer checks the completed increment.

**Goal:** Add one-shot native publication, per-target execution exclusion and conservative transport closure to the existing durable authority.

**Architecture:** A runtime publisher consumes the original authority grant through narrow injected ports. A local lock adapter binds exclusion to the existing authority and canonical subject; a dedicated native adapter sends one fixed statement and positively consumes successful EndOfStream. Neither component becomes the full guarded backend or changes route selection.

**Tech Stack:** Python, SQLite, POSIX `flock`, pinned `clickhouse-driver==0.2.10`, pytest, Docker Desktop Linux with ClickHouse `24.8.14.39`; no dependency changes.

**Spec:** [Approved dpone-only authority specification](../../feature-design-clickhouse-dpone-only-authority.md).

**Status:** APPROVED for Native execution by the maintainer on 2026-09-30 with an explicit `approved` response to this written plan. One integrator implements the four tasks; independent final review remains mandatory.

**Base:** `343dec2597b1360dbeac8823c48f7241a242e442`, PR #244. Preserve its foundation evidence and the unrelated upstream SqlClient work. Task ownership is in [the task contract](../../agent-tasks/clickhouse-native-publisher.yml).

## Global Constraints

- One deployment host, one direct ClickHouse node, Atomic databases and plain MergeTree tables.
- SQLite WAL, `synchronous=FULL`, foreign keys and short `BEGIN IMMEDIATE` transactions. Network I/O never occurs inside a SQLite transaction.
- Preserve `dpone.clickhouse.authority.v1`, `dpone.clickhouse.guarded-publication.v2` and `dpone.clickhouse.authority-diagnostics.v1`; no persisted schema migration.
- Before writing any SQL bytes, acknowledge `not_started -> may_have_sent`. Lost acknowledgement means no send, even if protected readback finds the transition.
- No application/driver/proxy retry, arbitrary SQL/settings, multi-statement, asynchronous insert or distributed queue.
- Positive completion requires consuming successful EndOfStream. Server exceptions, partial packets, timeout, disconnect and process death are not positive completion.
- No TTL expiry, automatic takeover, ownership release, successor admission, route activation, source reads or production grant changes.
- No observer, candidate-writer join/seal, complete `GuardedPublicationBackend`, checkpoint advancement or cleanup in this increment. These remain later work under the approved specification.
- Local Linux persistent authority storage is the supported deployment profile. macOS tests are development checks, not Linux storage certification; a host bind mount is not a certified authority volume.
- A valid old backup is not automatically detectable. External credential isolation before restore remains an operational prerequisite.
- All commands written as `uv run` below add `--frozen --extra postgres --extra gcp --extra columnar --extra dbt-mssql --extra accel --extra clickhouse`; keep the existing environment extras and lockfile.
- One integrator writes in the existing isolated worktree. Preserve unrelated changes and old evidence. No budget/baseline relaxation, driver upgrade, workflow or release mutation.

## Review Focus

1. A `begin_send` commit whose ACK is lost must emit zero network calls while retaining possible-send state (Task 3).
2. Two threads/processes, endpoint aliases and fork-inherited objects must not bypass the same canonical target exclusion (Task 1).
3. Progress, logs, empty data and EOF must never substitute for successful EndOfStream (Task 2).
4. An EXCHANGE that took effect before response loss must never be repeated on restart (Task 4).
5. A delayed claimant or lost terminal-write ACK must not recreate a grant, infer no-send, or admit another owner (Tasks 3–4).

## File structure and dependency direction

| Path | Responsibility |
|---|---|
| `src/dpone/contracts/clickhouse_authority.py` | Add immutable local storage identity; retain existing grant/state semantics |
| `src/dpone/contracts/clickhouse_native_publication.py` | Frozen request and successful completion material, safe transport error |
| `src/dpone/ports/clickhouse_publication_transport.py` | Minimal authority, exclusion/session and synchronous transport protocols |
| `src/dpone/adapters/clickhouse_authority_storage.py` | Validated read-only storage identity access |
| `src/dpone/adapters/clickhouse_authority_sqlite.py` | Expose validated execution identity without new schema or grants |
| `src/dpone/adapters/clickhouse_authority_execution_lock.py` | Stable private per-subject lock files and invocation-scoped exclusion |
| `src/dpone/adapters/clickhouse_native_publication.py` | Explicit native connection, one send and packet completion |
| `src/dpone/runtime/sinks/clickhouse_authority_publisher.py` | Original-intent validation and send/closure ordering |
| `tests/test_clickhouse_authority_execution_lock.py` | Thread/process/inode/session tests |
| `tests/test_clickhouse_native_publication.py` | Request, driver packet and lifecycle tests |
| `tests/test_clickhouse_authority_publisher.py` | Protected intent, CAS ambiguity, rendering and closure tests |
| `tests/integration/test_clickhouse_native_publication.py` | Actual server effects and restart/fault evidence |
| `tests/integration/clickhouse_native_publication_support.py` | Owned fixture and test-only fault relay; no shared fixture edits |
| `docs/clickhouse-native-publication.md` | Python integrator reference and bounded first success |
| `docs/runbooks/clickhouse-publication-closure.md` | Inspect original operation, quarantine and escalation |

Adapters import contracts/ports, never runtime. Runtime imports only contracts/ports and existing pure publication logic. Do not reuse generic connector retry code or add a registry. If a cohesive module cannot meet existing budgets, revise the responsibility map before adding files; do not split mechanically.

## Task 1: Authority-bound execution exclusion

**Files:** Modify the three authority modules above; create the transport port, lock adapter and lock tests. Integrator owns shared contracts.

**Interfaces:**

- `AuthorityStorageIdentity(path: str, device: int, inode: int, deployment_id: str)` is frozen, local-only, not diagnostic/recovery authority.
- `SQLitePublicationAuthority.execution_identity() -> AuthorityStorageIdentity` checks existing storage identity/settings before returning its original absolute location and inode. It never provisions storage.
- `PublicationAuthority` protocol exposes only existing `binding`, `read`, `transport_state`, `begin_send`, `close_without_send`, `record_terminal` plus `execution_identity`; use the exact existing signatures when declaring these members.
- `ExecutionSession.assert_current() -> None` rejects use after exit, from a different PID or thread, or after protected identity/binding replacement.
- `PublicationExclusion.hold(operation_id: str) -> ContextManager[ExecutionSession]`.
- `LocalPublicationExclusion(authority: PublicationAuthority)` implements that port. No caller-provided lock path or lock key.

- [ ] Write `test_same_subject_threads_and_spawned_processes_conflict`: one holder wins, another receives `AuthorityConflict`, and independent subjects can hold concurrently. Use Events/barriers, not sleep-based ordering. Resolve subjects from the original bindings, not caller endpoint aliases or table UUIDs.
- [ ] Write `test_lock_identity_and_session_fail_closed`: symlink/hardlink/non-private lock, replaced authority or lock inode, inherited PID, wrong thread and expired session are rejected. Killing a holder releases only the OS mutex; existing owner still blocks a second operation.
- [ ] Run `uv run pytest tests/test_clickhouse_authority_execution_lock.py -q`; expect missing new API failures, retain the red result.
- [ ] Implement identity access and the adapter. Derive a sibling filename `<authority-filename>.execution-<subject.key>.lock` under the existing private authority directory. Create/open with no symlink following, mode `0600`, validate regular file, owner, link count and descriptor/path identity; fsync newly created file and directory. Acquire a fresh descriptor using `LOCK_EX | LOCK_NB`, revalidate authority and binding after acquisition. Contention has no hidden retry. Normal release closes/unlocks but never unlinks the file. The protected directory is a deployment trust boundary, not protection against its owning OS user.
- [ ] Run the new tests plus `tests/test_clickhouse_authority_sqlite.py` and `tests/test_clickhouse_authority_processes.py`; expect PASS, no schema or prior-state changes.
- [ ] Commit only owned paths after verification; immediately update the existing PR description, or create/attach the appropriate PR if its lifecycle has changed. Never force-push or manufacture a merge commit.

## Task 2: Pinned synchronous native transport

**Files:** Create native contracts, native adapter and native tests; extend the transport port only with the interface below.

**Interfaces:**

- `NativePublicationRequest(binding: OperationBinding, intent: PublicationIntent)` is frozen and carries no caller SQL or settings. It verifies matching operation/subject/candidate, supported method and method-specific fields. The publisher additionally verifies selector consistency from the protected original. `statement: str | None` is a deterministic property for the four approved methods; no-op returns `None`.
- `NativePublicationCompletion(operation_id: str, query_id: str, server_id: str, statement_digest: str, server_version: tuple[int, int, int], server_revision: int, driver_version: str)` is frozen. Its `digest: str` hashes canonical sorted compact UTF-8 JSON with receipt profile `dpone.clickhouse.native-completion.v1`. Creation by a caller is not authority; only the injected trusted adapter's return is accepted.
- `NativePublicationError(RuntimeError)` has `safe_to_retry=False`; public messages contain no credentials, source rows or raw vendor exception text.
- `NativePublicationTransport.execute(request: NativePublicationRequest) -> NativePublicationCompletion` is synchronous, one request per invocation, no background work or application retries.
- `NativePublicationEndpoint` is adapter-owned frozen configuration: registered `server_id`, literal IPv4 `host`, valid integer `port`, `user`, repr-hidden `password`, positive finite `connect_timeout`/`send_receive_timeout`, `secure=True`, optional `ca_certs` and `server_hostname`. TLS verification cannot be disabled. Explicit `secure=False` is limited to the documented isolated local test deployment. No environment discovery or arbitrary settings mapping.
- `DirectNativePublicationTransport(endpoint: NativePublicationEndpoint)` implements the port; optional SDK import occurs on execution, not base import/help.

- [ ] Write `test_fixed_statements_and_no_arbitrary_sql`: backtick-quoted simple identifiers, exact EXCHANGE/RENAME/REPLACE statements, stable original query ID and no-op zero driver calls. REPLACE always uses `PARTITION ID`, including `'all'`; validate canonical partition IDs against `[A-Za-z0-9_-]+` for this bounded transport profile, rejecting others before send entry. Never infer `tuple()` from `'all'`.
- [ ] Write `test_native_requires_explicit_successful_eos`: progress/profile/log/empty-data packets may precede EOS; server exception, unexpected packet, nonempty result data, EOF, truncated packet and timeout never produce completion. Check receipt fields and deterministic digest.
- [ ] Write `test_one_connection_one_query_no_reconnect`: validate version, literal IPv4, exact peer, no alternates/pool, one connect and at most one send; partial send failure has no second client, query or fallback. Base import works without the optional SDK; malformed config fails before socket use.
- [ ] Run `uv run pytest tests/test_clickhouse_native_publication.py -q`; expect missing API failures.
- [ ] Implement fixed rendering and field validation in the request contract without runtime/SDK imports; existing selector consistency remains the runtime publisher's responsibility. Keep native lifecycle in the adapter: privately create one pinned `Client` with the protected database, `compression=False`, `disable_reconnect=True` and no alternate hosts/round robin, whose constructor initializes `connection.context`; retain its one connection and never call `Client.execute`, `force_connect` or `get_connection` again. Call `connect()` once, check actual peer and server identity/version metadata, assert connected, send one `send_query(statement, query_id=..., params=None)`, then `send_external_tables(None)`. Consume native packets until successful EOS; disconnect in `finally`. No connection/client escapes the adapter. Require driver `0.2.10` and server version `(24, 8, 14)`; record actual revision and Docker build/image identity separately. A handshake cannot attest deployment `server_id`; registered endpoint/credentials remain an external prerequisite. The driver's implicit connect inside `send_query` must remain unreachable by exclusive connection ownership; tests count actual connect calls.
- [ ] Implement the explicit DDL packet allowlist: END_OF_STREAM succeeds; EXCEPTION fails; PROGRESS, PROFILE_INFO, LOG, PROFILE_EVENTS and TIMEZONE_UPDATE are drained; DATA is accepted only with zero rows and zero columns. All other decoded packet types, including TOTALS/EXTREMES, fail closed. Record EOS only for this invocation, never from `is_query_executing=False` or generic return success. Per-socket timeouts are not advertised as a total wall-clock deadline.
- [ ] Run the new module and `tests/test_clickhouse_exchange_retry_safety.py`; expect PASS without modifying generic connector behavior. Pin actual SDK interaction in tests as well as fake packet sequences.
- [ ] Commit the verified task and refresh the PR description with scope and evidence, not a production-readiness claim.

## Task 3: Grant-consuming publisher and conservative closure

**Files:** Create the runtime publisher and its tests. No change to the existing eight-method backend port or publication kernel.

**Interfaces:**

- `AuthorityPublicationPublisher(authority: PublicationAuthority, exclusion: PublicationExclusion, transport: NativePublicationTransport)` uses explicit constructor injection.
- `execute_once(grant: DispatchGrant) -> None` reopens the protected binding and journal; there is no caller intent, arbitrary SQL or grant cache.
- `close_and_drain(operation_id: str) -> None` only closes transport under the same exclusion. Missing proof raises existing `PublicationUnknown` with `safe_to_retry=False`; this does not classify physical publication outcome.

- [ ] Write `test_lost_begin_send_ack_sends_nothing`: inject failure after real SQLite commit, assert zero transport calls and persisted `MAY_HAVE_SENT`. Readback cannot authorize dispatch or change it to no-send.
- [ ] Write `test_delayed_claimant_loses_to_close`: pause a claimant before lock acquisition, durably close from another process, then resume; assert zero sends and retained owner. Reject foreign/stale epoch/grant and divergent protected identity before transport.
- [ ] Write `test_terminal_ack_loss_never_replays`: fail before and after terminal commit separately. Later closure succeeds only for the persisted original terminal; otherwise it raises unknown. Repeated closure does not append duplicate history. A different completion digest is rejected.
- [ ] Write `test_noop_and_closure_state_matrix`: NOT_STARTED closes without send; both closed states are idempotent; MAY_HAVE_SENT raises unknown. No-op performs zero socket/SDK calls. Every result retains target ownership and blocks a successor. Cross-thread/fork session use fails.
- [ ] Run `uv run pytest tests/test_clickhouse_authority_publisher.py -q`; expect missing API failures.
- [ ] Implement this order under one exclusion session: reopen binding/entry, require claimed original and matching operation/epoch, construct validated request, assert current session, acknowledge `begin_send(grant)`, call transport exactly once, validate returned completion identity/profile/statement digest, persist `record_terminal(grant, completion.digest)`, release local mutex. All network activity occurs after acknowledged send entry; even a connect failure may therefore quarantine the operation. For no-op use no-send closure, never `begin_send`. Propagate interruption without false success; other post-entry failures become sanitized unknown. Lost terminal-write ACK is resolved only by a later protected read, not a second send.
- [ ] Implement closure exactly as the state matrix; no KILL, query-log proof, PID/timeout inference, source reads or outcome observation. No pending background queue exists; any late entrant must still lose the durable CAS.
- [ ] Run new tests with all foundation, codec and kernel tests; expect PASS. Verify no SQLite transaction spans a transport callback and no grant appears in repr, diagnostics or exception messages.
- [ ] Commit verified paths and update the PR with the conservative availability cost.

## Task 4: Actual Docker faults, documentation and integration gate

**Files:** Create the integration test/support modules and two documentation pages listed above. Integrator updates `docs/clickhouse-authority-journal.md`, `docs/clickhouse-publication-methods.md`, the approved spec's progress/contradictory backup wording, this plan, task contract, `CHANGELOG.md`, `mkdocs.yml` and producer-generated `docs/quality-metrics.md` only when required by freshness checks.

**Interfaces:** Tests exercise Tasks 1–3 without a complete backend. Already prepared observations and candidate fixtures are explicitly synthetic registration inputs, not certification of sealing or observation. Use `integration_live` and `integration_clickhouse` markers and explicit environment opt-in.

- [ ] Write actual-server tests for single partition replacement, unpartitioned canonical ID `'all'`, EXCHANGE for empty/stale-multipartition snapshots, RENAME into absent target, no-op, server exception and reopen. Assert exact duplicate-preserving rows, target/candidate UUID behavior and retained owner. A separate raw `tuple()` fixture probe may show server equivalence; it is not another publisher mode.
- [ ] Write `test_exchange_effect_then_response_loss_is_never_replayed`: a test-only relay proves fault activation after forwarding the mutation and before delivering EOS, an independent reader verifies the swap, then a fresh process reopens and refuses replay. Assert one actual publication send and unchanged swapped result after recovery. Test truncated packet and termination after MAY_HAVE_SENT but before bytes; absence from query log is diagnostic only.
- [ ] Run the new opt-in suite to observe the missing fixture/implementation failures; preserve red output. Provision only newly named owned containers/network/volumes in approved Docker Desktop `desktop-linux`, not existing user services. Use server `24.8.14.39`, record resolved image digest, native-driver version, Linux/Python/SQLite versions and exact source SHA/tree. Authority data uses a named Linux volume, not a macOS bind mount. Temporary secrets stay unlogged and uncommitted.
- [ ] Implement fixture/relay support; use explicit Events/control channels and prove every injected boundary was reached. Do not replace actual native traffic with a fake result. Record direct success separately from test-relay faults; the relay is not a supported production proxy. Archive diagnostics, actual row/UUID assertions and logs before cleaning only exact newly owned Docker resources.
- [ ] Run `uv run pytest tests/integration/test_clickhouse_native_publication.py -m 'integration_live and integration_clickhouse' -q` in the owned Linux runner with explicit fixture opt-in; require executed cases with zero unexplained skips. TLS, power-loss durability, deployment ingress, observer/seal and whole-route certification remain UNVERIFIED unless independently exercised; do not extrapolate plaintext fixture results.
- [ ] Write the native reference and closure runbook: purpose, prerequisites, actual API/signatures/errors, tested synthetic offline example, real local-server test entry point, success/unknown diagnostics, source-free inspection, quarantine, upgrade and remaining gates. Closure is not COMMITTED, owner release or route success. Library stdout/stderr and CLI/YAML changes are N/A. Never show raw vendor errors or a diagnostic import as recovery.
- [ ] Correct existing documentation: label the overview's future recovery binding explicitly unavailable; replace automatic stale-backup-refusal wording with external restore-isolation evidence; make the journal temporary example print/assert redacted results inside its scope and use sibling `authority/` and `reports/` directories. Link overview → journal → native reference → closure runbook, with backlinks. Keep overall specification APPROVED, not fully IMPLEMENTED.
- [ ] Run the focused suite, change-aware selector, Ruff, mypy, import/layer/module gates, full non-live pytest, documentation/language/generated-reference checks and strict MkDocs from repository instructions. Use a fresh platform-native pytest temporary root, not repository-local temporary fixtures. Inspect rendered navigation. Do not edit budgets to obtain PASS.
- [ ] Obtain one fresh independent correctness/compatibility/data-loss/docs/evidence review, resolve actionable findings, then commit and update/attach the PR. Freeze the candidate and rerun source-bound Docker evidence if runtime/test code changed. The completion report records exact SHA, command, duration, PASS/FAIL/SKIP/N/A/UNVERIFIED and artifact paths. No merge, version bump, tag or publication follows automatically from this plan.

## Evidence and rollout boundary

New producer-generated evidence belongs under `test_artifacts/clickhouse-native-publisher-<date>-<run-id>/`: commands, raw logs/JUnit, source/image/environment identity, fault activation, redacted original diagnostics, assertions and checksums. Never rewrite the completed foundation evidence directory or hand-edit a result into PASS. Do not commit runtime secrets or raw data; test rows are synthetic.

This increment can establish one-shot transport and conservative closure only. Later work must implement protected observation, writer admission/join/sealing, complete backend composition, durable controlled owner release, method-aware cleanup and checkpoint/evidence finalization. The ODBC route additionally still needs pre-read byte admission, bounded Arrow/Parquet/RSS and exact-commit MSSQL/object-storage/ClickHouse certification. A supplemental ADR is required before production binding; ADR 0078 remains scoped to the unbound kernel.

## Source verification and self-review

Read-only mapping and architecture/test/docs review used base `343dec2597b1360dbeac8823c48f7241a242e442` on 2026-09-30. The pinned driver's local `client.py` SHA-256 is `8984b3d832d00beb7a44b9ad589bed1e811604016d28321a7e5f3be2dcf679bb`.

- [Pinned driver client source](https://github.com/mymarilyn/clickhouse-driver/blob/0.2.10/clickhouse_driver/client.py) initializes settings/context; [connection source](https://github.com/mymarilyn/clickhouse-driver/blob/0.2.10/clickhouse_driver/connection.py) decodes packets and shows implicit-connect/host iteration boundaries. The plan avoids the generic execute path and owns a single literal-address connection.
- [ClickHouse native server protocol](https://clickhouse.com/docs/resources/develop-contribute/native-protocol/server) identifies EndOfStream. Requiring successful observed EOS is the approved dpone policy, not a claim that ordinary driver execution has an early-return bug. Inspection confirmed its fallback returns `True` for other packets.

Self-review: specification steps 5–8 map to Tasks 1–4; earlier foundation and later observer/sealing/finalization obligations are explicitly excluded, not dropped. All five review hazards have named tests. Request/completion, lock-session and publisher interfaces are consistent across tasks. No authority schema change or partition-expression inference is introduced. Real transport tests cannot prove exclusive production credentials or a sealed candidate. Maintainer review of this written plan is complete.
