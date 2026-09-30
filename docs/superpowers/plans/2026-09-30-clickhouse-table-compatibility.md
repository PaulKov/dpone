# Self-service ClickHouse Table Compatibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans for the preserved Native execution choice. Steps use checkbox syntax. Root is the sole writer; independent agents remain read-only reviewers.

**Goal:** Finish the protected single-operation Python publisher with automatic settings preservation, explicit compatibility policy and source-free recovery.

**Architecture:** Separate configuration identity, method compatibility and desired post-state. Resolve settings from protected facts, freeze a resolved plan at enrollment, then atomically bind selection to the seal and PREPARED record in an explicit v3 authority. Reuse version-neutral native transport and durability mechanisms without reinterpreting historical records.

**Tech Stack:** Python, SQLite WAL/FULL, POSIX flock, clickhouse-driver 0.2.10, ClickHouse 24.8.14.39, pytest and owned Docker Desktop Linux; no dependency upgrade.

**Spec:** [Approved table-compatibility amendment](../../feature-design-clickhouse-table-compatibility.md), [protected-publication parent](../../feature-design-clickhouse-protected-publication.md), [ADR 0080](../../adr/0080-clickhouse-table-compatibility-policy.md).

**Status:** APPROVED; Task 1 structural feasibility FAILED. The maintainer separately approved this written plan and its path contract with `approve` after commit `9553698e8374df4e4af0c5c2a4e4cc0f4cd9acda`. Native execution remains selected; root is the sole writer. Dependent production implementation has not started. The execution record below distinguishes observed failure from hypothetical projections.

**Base:** `5075a1e50ff7d1f3d670423fe196f970ab65e873`, branch `codex/clickhouse-native-publisher-locale-base`, PR #249. The [new task contract](../../agent-tasks/clickhouse-table-compatibility.yml) takes precedence only for this increment after plan review.

## Global Constraints

- One POSIX host, persistent original private authority, direct node, Atomic database, plain MergeTree and dpone-only mutations; no production ingress enforcement claim.
- Preserve committed parent Tasks 1–2 and unfinished Task 3 files. This plan replaces the remaining work in [the prior plan](2026-09-30-clickhouse-protected-publication.md), not its historical progress or evidence.
- Old v1/v2 provision/open defaults, schemas, wire bytes, selector and recovery remain unchanged. New enrollment explicitly uses `dpone.clickhouse.authority.v3`; no migration, replacement store or retained-name adoption.
- New families are `dpone.clickhouse.table-descriptor.v1`, `dpone.clickhouse.compatibility-plan.v1`, `dpone.clickhouse.observation.v2` and `dpone.clickhouse.guarded-publication.v3`.
- Default `design_change="preserve"`, `settings={}` and `on_unverified_compatibility="block"`. Explicit `warn` excludes an unknown method; it never authorizes it or bypasses shared safety checks.
- No source access before acknowledged enrollment, registered CREATE completion and independent candidate readback. No retry, grant reconstruction, owner release, cleanup or checkpoint promotion.
- A source failure is not exhaustion. Pre-PREPARED restart is inspection-only, even when SEALED. At/after PREPARED recovery uses frozen intent, transport closure and protected observations, never source or SQL replay.
- No stock ODBC activation, CLI/manifest changes, dependency/version changes, merge, tag or release in this increment.
- Keep the current scalar/type/key restrictions. Encoded-byte bounds do not certify upstream ODBC allocation, driver buffers or RSS.
- Budgets come from `docs/benchmarks/quality_budgets.yml` and existing strict graph gates. No threshold/baseline relaxation, hidden imports, proxy-only modules, arbitrary merges or artificial graph padding.
- Commands below abbreviate the environment as `uv run`; execute with `--frozen --extra postgres --extra gcp --extra columnar --extra dbt-mssql --extra accel --extra clickhouse`. Do not alter `uv.lock`.
- Each implementation task ends with focused evidence, a scoped commit and an immediate update to existing PR #249. Never stage all current changes indiscriminately.

## Review Focus

1. An omitted historical default must not be inferred from a newly restarted server with identical-looking globals; test incarnation, reload and coverage loss in Tasks 1–2.
2. A competing operation can use this target as its candidate before enrollment; test two-name locking, cross-role races and uninterrupted binding in Task 3.
3. A known incompatible REPLACE fact must not conceal a shared unknown; test verdict precedence, strict/warn alternatives and late rejection in Tasks 2 and 6.
4. Equal content under different configurations must not become NOOP, and REPLACE must preserve the target's actual configuration; test all four explicit after-states in Tasks 2 and 5.
5. A lost seal/PREPARED/claim acknowledgement, expired session or copied journal must not recreate permission; test fresh-process recovery and exact fault activation in Tasks 3–7.

## Boundaries, estimates and feasibility gate

The current unfinished tree measures clustering `0.18341116714046293` and
strict cross-layer ratio `0.300842835894893`; both gates fail. These are diagnostic
observations, not passing evidence. A read-only contraction analysis found no
honest narrow Task-3 merge that fixes them: candidate enrollment plus request
persistence is already 444 SLOC; combining gateway and transport crosses the
orchestration/I/O boundary and worsens clustering.

Task 1 must therefore produce a measured whole-plan dependency projection before
dependent production changes. The estimates below are planning envelopes, not a
claim of PASS. Count every actual import/annotation using the repository producer.
Do not pretend that a capability interface removes consumers' real type edges.
If a cohesive implementation within these paths cannot satisfy the gates, retain
the failed projection and revise this plan before continuing; do not silently
expand scope or defer this known failure to final certification.

The first v3 policy/codec/enrollment consumers alone are estimated to add
12–18 genuine cross-layer dependencies, before the remaining observer/runtime
composition. That estimate predicts additional pressure, not headroom; Task 1
must account for the entire graph, not subtract hypothetical erased imports.

All paths below are repository-relative; the task contract is the exhaustive
write list. Existing helper names listed without a directory are adapter files.

| Owner and files under `src/dpone/` | Responsibility and estimated SLOC per file |
|---|---|
| `contracts/clickhouse_table_descriptor.py` | Immutable descriptor, settings facts and epoch references; 160–260 |
| `contracts/clickhouse_compatibility.py` | Closed settings registry, desired-design resolution and pure per-method policy; 220–350 |
| `contracts/clickhouse_publication_v3.py` | Request, preview/resolved/selected plan and explicit frozen intent/after-state; 180–300 |
| `ports/clickhouse_deployment_epoch.py`, `ports/clickhouse_protected_publication.py` | Injected provenance and session-scoped lifecycle capabilities; 40–120 each |
| `adapters/clickhouse_deployment_epoch.py` | Protected epoch journal verification against injected platform observations; 180–300 |
| `adapters/clickhouse_table_descriptor.py` | Complete settings-aware catalog decoding; 150–260 |
| `adapters/clickhouse_publication_codec_v3.py` | Closed canonical v3 wire and semantic validation; 200–320 |
| `adapters/clickhouse_candidate_v3.py`, `adapters/clickhouse_publication_journal_v3.py` | v3 enrollment/lifecycle and atomic selected PREPARED/publication CAS; 220–350 each |
| `adapters/clickhouse_candidate_requests_v3.py` | v3 request-context validation around shared request CAS; 120–220 |
| `adapters/clickhouse_authority_transitions.py` | Extracted version-neutral ownership/request/send CAS primitives, not a new policy registry; 160–280 |
| Existing `clickhouse_authority_schema.py`, `clickhouse_authority_storage.py` | Explicit new schema selector and unchanged old storage validation; measure existing SLOC before growth |
| Existing `clickhouse_authority_execution_lock.py` | Original-file two-name session, pre-enrollment then operation-bound; target below 300 |
| Existing `clickhouse_observation_profile.py`, `clickhouse_design_grammar.py` | Additive v2 profile and explicit-settings renderer/parser; current 169/174, target below 300 each |
| Existing `clickhouse_candidate_gateway.py`, `clickhouse_native_candidate.py` | Keep request sequencing separate from pinned native I/O; current 102/206, target below 300 each |
| `adapters/clickhouse_native_catalog.py`, `adapters/clickhouse_authority_observer.py`, `adapters/clickhouse_candidate_seal.py` | Fixed reads, protected before/after observation and irreversible seal; 150–300 each |
| `adapters/clickhouse_publication_readiness.py`, `adapters/clickhouse_candidate_lifecycle.py`, `adapters/clickhouse_guarded_backend.py` | Inventory verification, explicit composition and v3 port adapter; 120–280 each |
| `runtime/sinks/clickhouse_guarded_publication_v3.py`, `runtime/sinks/clickhouse_protected_publication.py` | v3 kernel and source orchestration; 120–240 each |
| `adapters/clickhouse_publication_diagnostics.py` | Immutable redacted external diagnostics; 120–220 |

Adapters never import runtime. Pure policy imports only contracts/stdlib. The
explicit example is the composition root. No default factory or automatic
provenance provider is registered. Refactoring old persistence is permitted only
to extract the same version-neutral mechanisms for genuine v2/v3 reuse, with
old golden tests unchanged; old semantic policy and codec remain read-only.

## Task 1: Prove historical defaults and structural feasibility

**Files:** Create `tests/integration/clickhouse_table_compatibility_probe.py`,
`tests/integration/clickhouse_table_compatibility_support.py` and
`tests/integration/test_clickhouse_table_compatibility_live.py`. Record generated
artifacts under `test_artifacts/clickhouse-table-compatibility-<run-id>/`.
No production implementation precedes this prerequisite's result.

**Interfaces and trust boundary:** The concrete certification provider is
`OwnedDockerEpochSource` in the support module. It implements the future
`DeploymentEpochSource.observe(subject: AuthoritySubject, table_uuid: str | None)
-> DeploymentEpochEvidence` capability. `DeploymentEpochEvidence` is an immutable
descriptor-contract value with producer/profile ID, original journal identity,
deployment/endpoint/server/version/incarnation, epoch revision/head digest,
complete canonical bootstrap configuration, captured globals and acknowledged
CREATE/ATTACH coverage for an existing database/table UUID. No caller field such
as `trusted=True` substitutes for evidence.

The production-facing verifier will be `VerifiedDeploymentEpochSource(journal:
Path, platform: DeploymentEpochSource)` in `adapters/clickhouse_deployment_epoch.py`.
It verifies the original protected append-only epoch journal and live platform
facts. `observe` returns evidence or raises a typed provenance error; it does not
provision the deployment. The Docker implementation is certification-only, not
a production Docker dependency. The platform owner remains responsible for
exclusive privileged access, complete configuration inputs and controlled
creation. This does not prove resistance to a malicious same-user administrator.

- [ ] Write the probe's negative-first test
  `test_omitted_defaults_require_precreation_epoch`: an existing table without
  acknowledged CREATE/ATTACH coverage is rejected, even when current globals
  match. Never backfill proof for existing old test containers.
- [ ] Run that test with `uv run pytest -m integration_live
  tests/integration/test_clickhouse_table_compatibility_live.py -k precreation -vv`;
  record the expected missing-producer failure separately from a live PASS.
- [ ] Implement controlled bootstrap in the support producer: exclusive private
  original epoch journal before a fresh owned incarnation, pinned image and all
  configuration inputs, then server/version/globals readback and acknowledged
  activation. Frame records with sequence, checksum and durable append; reject
  incomplete tail, replacement, truncation and invalidation. Do not create a
  generic journal framework or put credentials in evidence.
- [ ] Create a target with `index_granularity=4096`, leaving another supported
  setting omitted, only through that producer. Record positive server completion,
  database/table UUID and acknowledged coverage; then open the proof in a separate
  runtime process and establish the inherited effective values. Lost CREATE or
  journal acknowledgement must leave coverage unusable.
- [ ] Probe the exact server's persisted defaults, adaptive/nonadaptive settings,
  joint bounds, Compact/Wide parts, local storage policy and REPLACE constraints.
  Save actual CREATE/SHOW CREATE, system tables/columns/settings/parts, image,
  driver/Python/SQLite identities and server responses. Restart/reload/config
  change invalidates the old epoch, including an identical-looking restart.
- [ ] Run positive and negative probe cases; emit `prerequisite-probe.json` with
  explicit PASS/FAIL/UNVERIFIED per condition and the source tree identity. No
  successful mocked probe substitutes for the real existing-table positive case.
- [ ] Use the repository architecture producers to project the complete file
  map above, including v3 consumers and common-CAS extraction. Record exact
  before/proposed imports, SLOC envelopes and both graph calculations in
  `architecture-projection.json`; inspect each consolidation's responsibility.
  Proceed only on a defensible within-budget projection. No new imports or files
  may be introduced merely to change graph denominators.
- [ ] Commit the probe and its reviewed conclusions, preserve raw immutable
  artifacts, and update PR #249. Task 2 is blocked if either prerequisite fails.

### Task 1 execution record: structural prerequisite not passed

The structural branch was checked before creating the live provenance fixture.
The repository's actual strict tests returned two FAIL and two PASS in 19.90 s.
The failed tests are the current-repository clustering and pre-release
cross-layer-budget checks in `tests/test_architecture_fitness_gate.py`.

| Graph | Clustering | Cross-layer ratio | Runtime-to-contracts edges |
|---|---|---|---|
| Observed preserved working tree | 0.1834111671 | 0.3008428359 | 214 |
| Hypothetical named-module implementation | 0.1853563479 | 0.3034645206 | 218 |
| Hypothetical shared-CAS extraction | 0.1852286488 | 0.3034943070 | 218 |

The observed values exceed the existing 0.182 clustering and 0.300 strict ratio
limits. Projections use the repository producers with an explicit 21-module
import map; they are not measurements of unwritten code, a universal lower
bound or proof that every possible architecture fails. They do not establish
the required passing whole-plan design. The proposed CAS extraction alone does
not repair the plan. Future SLOC envelopes remain estimates.

Local reproducible evidence is in
`test_artifacts/clickhouse-table-compatibility-20260930-structural/`:
`architecture_projection.py`, `architecture-projection.json` and
`architecture-baseline.log`. The JSON identifies HEAD plus every working-tree
Python source hash, including the preserved uncommitted candidate work.

Task 1 is incomplete. Docker Desktop availability was checked, but no new
container was created and no live probe was run: provenance is SKIP/UNVERIFIED,
not PASS. No settings-aware production source, thresholds, baselines or old
evidence were changed. The next implementation decision needs a concrete
cohesive boundary redesign and amended exact path ownership; changing a module's
layer label or allowing two more imports alone is not a demonstrated solution.

## Task 2: Typed settings, descriptors and deterministic policy

**Files:** Create the three contract modules and descriptor/epoch adapters above,
`ports/clickhouse_deployment_epoch.py`; modify grammar and observation profile.
Tests: `tests/test_clickhouse_table_descriptor.py`,
`tests/test_clickhouse_compatibility_policy.py`,
`tests/test_clickhouse_compatibility_plan.py`,
`tests/test_clickhouse_deployment_epoch.py`; extend existing grammar/profile tests
without changing v1 vectors.

**Interfaces:**

- `TableDescriptor` separates normalized columns/keys/engine/settings and
  `configuration_digest` from UUID/database identity, provenance and content.
  `ManagedSettings` is an immutable sorted tuple of registry-defined names and
  strictly typed values; no mutable caller mapping survives construction.
  This module also owns `CatalogSnapshotV2` and `ObservedRow` catalog facts.
- `ProtectedPublicationRequestV3(operation_id: str, subject: AuthoritySubject,
  candidate: str, design: CandidateDesign, limits: ObservationLimits, *,
  settings: Mapping[str, int | bool | str], design_change: Literal["preserve",
  "replace"] = "preserve", on_unverified_compatibility: Literal["block", "warn"]
  = "block")` freezes settings; the public default for `settings` is empty.
- `resolve_design(request: ProtectedPublicationRequestV3, target:
  TableDescriptor | None, epoch: DeploymentEpochEvidence) -> TableDescriptor`
  is pure. Existing empty tables are present; absent targets use certified
  defaults. Explicit change overlays overrides on preserved target settings.
- `evaluate_methods(before: PublicationObservationV3, desired: TableDescriptor,
  policy: CompatibilityPolicy) -> tuple[MethodVerdict, ...]` produces exactly
  `compatible`, `incompatible`, `unverified` or `not_applicable`, with reason and
  scope. `select_publication(before, desired, policy) -> SelectedPublication`
  yields a method, explicit `ExpectedPostState` and actual fallback warnings.
- `CompatibilityPolicy` is the built-in immutable registry/selector revision
  plus the request's frozen mode, not user-supplied rules. `PublicationObservationV3`
  contains subject, epoch, target absence or descriptor/content, and candidate
  descriptor/content. `ExpectedPostState` explicitly records each name's absence
  or UUID/configuration/content; it is not a replacement `design_digest` shortcut.
- `CompatibilityPlan` has preview/resolved/selected variants, policy ID/digest,
  mode, descriptor and provenance digests, method verdicts and diagnostics.
  Preview/resolved variants cannot contain final content, a selected method or a
  fabricated after-state. `SelectedPublication` binds a complete observation.
- `ProtectedObservationProfileV2(descriptor: TableDescriptor, limits:
  ObservationLimits)` preserves existing row encoding; profile identity includes
  the full configuration and policy/version. Old profile v1 is unchanged.
  Keep `design_digest` as the existing columns/keys design identity. Expose full
  settings-aware identity separately as `configuration_digest`; never reinterpret
  that old field in a request, record, decoder or legacy consumer.

- [ ] Write RED typed-registry tests for all seven supported names, reordered
  SETTINGS, duplicates, unsupported clauses, bool-as-int, negative/overflow and
  unknown explicit/global deviations. `index_granularity=0` is invalid;
  `index_granularity_bytes=0` is the supported nonadaptive mode. Test certified
  joint constraints, not a blanket zero ban. Cover 4096 and 8192 equivalently.
- [ ] Write RED policy tests for omitted/explicit defaults with and without
  verified epoch, preservation/equivalent overrides, explicit design changes,
  absent versus empty target, and incompatible storage/configuration.
- [ ] Add a parameterized selection matrix: RENAME to absence; NOOP only with
  same desired configuration and content evidence; conservative REPLACE only
  with equal resolved managed settings and complete single-partition coverage;
  otherwise verified EXCHANGE where allowed. Exercise `tuple()`, stale extra
  target partitions and nonempty-to-empty snapshots.
- [ ] Test strict/warn truth table: applicable REPLACE unknown blocks by default;
  warn selects verified EXCHANGE only when the missing fact is irrelevant to it;
  shared unknown and no verified alternative block in both modes. Known REPLACE
  incompatibility may use verified EXCHANGE in either mode. Not-applicable is
  not unknown. Noop/rename shared requirements cannot be bypassed by warn.
- [ ] Run `uv run pytest tests/test_clickhouse_table_descriptor.py
  tests/test_clickhouse_compatibility_policy.py tests/test_clickhouse_compatibility_plan.py
  tests/test_clickhouse_deployment_epoch.py -q`; retain RED results.
- [ ] Implement the interfaces using one closed registry with typed bounds,
  provenance requirements and machine-readable reason metadata. Parse complete
  statements and render every resolved managed value explicitly for the
  candidate. Never delete unknown clauses or compare raw SHOW CREATE strings.
  Add separately named settings-aware grammar entrypoints sharing the existing
  lexer/core; the old parser continues to reject SETTINGS exactly as before.
- [ ] Run the same tests plus old design/profile/evidence compatibility vectors;
  require unchanged v1 bytes and positive verified inheritance. Run actual
  import/graph/module gates against the Task 1 projection; commit and update PR.

## Task 3: Original v3 journal and uninterrupted namespace session

**Files:** Create `clickhouse_candidate_v3.py`,
`clickhouse_publication_journal_v3.py`, `clickhouse_publication_codec_v3.py`,
`clickhouse_authority_transitions.py` and `ports/clickhouse_protected_publication.py`.
Modify authority schema/storage/execution lock and existing candidate/journal
persistence only for characterized common-mechanism extraction. Tests:
`tests/test_clickhouse_candidate_v3.py`,
`tests/test_clickhouse_publication_codec_compatibility.py`,
`tests/test_clickhouse_namespace_session.py`; extend existing process tests.

**Interfaces:**

- `NamespacePublicationExclusion.hold_subject(subject: AuthoritySubject,
  candidate: str) -> ContextManager[SubjectExecutionSession]` acquires both
  physical-name locks in canonical sorted order on the original authority file.
  `SubjectExecutionSession.bind(invocation: CandidateInvocation) ->
  BoundExecutionSession` validates acknowledged enrollment and returns a bound
  view over the same held descriptors, without unlock/relock. Sessions bind
  storage inode, PID/thread, both names and active lifetime. Recovery acquires
  the same two names from the original binding; never reconstructs invocation.
- `SQLiteCandidateAuthorityV3.enroll(request: ProtectedPublicationRequestV3,
  resolved_plan: CompatibilityPlan, verified_readiness: VerifiedReadinessV3,
  session: SubjectExecutionSession) -> CandidateInvocation` atomically reserves
  names and persists exact request/resolved-plan/provenance/policy bytes. Only
  acknowledged fresh enrollment issues the process-local capability.
- `VerifiedReadinessV3` is opaque, issued only by Task 6's trusted verifier and
  bound to the exact request, plan, inventory revision and session. Tests use
  an explicit trusted fake issuer, never a public success flag.
- `prepare_selected(invocation: CandidateInvocation, sealed_observation:
  SealedObservationV2, selected_plan: CompatibilityPlan) -> JournalEntryV3`
  atomically appends selected plan, seal reference and PREPARED v3 intent. It
  cannot overwrite the resolved plan or prepare twice with different bytes.
- `PublicationRecordV3`, `PublicationIntentV3`, `JournalEntryV3` retain explicit
  method, before/expected-after, policy, mode, actual warnings, seal, state and
  claim acknowledgement. `encode_record_v3(record) -> str` and
  `decode_record_v3(payload: str) -> PublicationRecordV3` accept only the closed
  canonical v3 schema and verify immutable relationships with v3 policy.
  `SealedObservationV2` and `ProtectedPublicationStatus` are immutable values in
  the same v3 contract module, not constructors of execution authority.

- [ ] Pin old v1/v2 golden files, default open/provision, unknown-version and
  wrong-store rejection before edits. Require rejected opens to leave bytes,
  inode and ownership unchanged.
- [ ] Write RED tests for pre-enrollment target/candidate cross-role races,
  sorted acquisition, no phantom owner on preflight rejection, inode replacement,
  copied store, thread/fork inheritance and expired/bound-session mismatch.
- [ ] Write RED transaction tests for resolved enrollment without future content;
  selected+PREPARED atomicity; seal/profile/epoch mismatch; lost enrollment,
  selected, claim and completion acknowledgements; no grant from readback.
- [ ] Run `uv run pytest tests/test_clickhouse_candidate_v3.py
  tests/test_clickhouse_publication_codec_compatibility.py
  tests/test_clickhouse_namespace_session.py -q`; record expected RED failures.
- [ ] Implement fixed v3 schema selection and canonical codec. Extract existing
  version-neutral binding/ownership CAS, request sequencing and send-state
  transitions into the common transitions owner; keep version-specific record
  validation/selection in each semantic journal. No generic plugin registry,
  subclass override of old `_entry`, schema migration or second sidecar database.
- [ ] Implement the two-name session and enrollment-to-binding transition.
  Preview must not create locks/reservations; real preflight may leave persistent
  private lock metadata, but no owner on a proven pre-enrollment failure.
- [ ] Run new tests plus all existing authority/candidate/process tests on macOS
  and owned Linux. Recheck graph/size gates after extraction; commit and update PR.

## Task 4: Versioned candidate creation and source admission

**Files:** Finish existing dirty gateway/native-candidate files; extend
`tests/test_clickhouse_candidate_gateway.py`,
`tests/test_clickhouse_native_candidate.py`; create
`adapters/clickhouse_candidate_requests_v3.py` under `src/dpone/` and
`tests/test_clickhouse_candidate_v3_transport.py`. Add the immutable context to
the already owned `contracts/clickhouse_publication_v3.py`.

**Interfaces:** Preserve old gateway wrappers. Add
`CandidateGatewayV3.create_in_session(invocation: CandidateInvocation,
session: BoundExecutionSession) -> None` and
`insert_in_session(invocation, batch: CandidateBatch, session) -> None`.
Inject the exact v2 observation profile. Define immutable
`CandidateMutationContextV3(request: CandidateMutationRequest, resolved_plan:
CompatibilityPlan, profile_digest: str)` in `contracts/clickhouse_publication_v3.py`.
It carries an unchanged old request, not an extended old wire: `design_digest`
still means columns/keys and old query-ID construction is preserved. The v3
journal serializes the context under the explicit authority-v3 schema and binds
its separate full configuration/profile/plan identities to original enrollment.
`CandidateRequestJournalV3.register(invocation, context:
CandidateMutationContextV3) -> CandidateRequestGrant` validates that context,
then uses common registration/send/completion CAS with the existing grant.

`DirectNativeCandidateTransport.execute_v3(context: CandidateMutationContextV3,
batch: CandidateBatch | None) -> CandidateCompletion` independently rebuilds the
v2 profile from the resolved descriptor and injected limits; verifies both the
old design identity and separate configuration/profile/plan identities; and
checks rendered SQL and batch bytes. It rejects non-resolved or foreign context.
The existing `execute(request, design, batch)` remains v1-only. CREATE's statement
digest binds all explicit resolved settings; no field is invented on the old DTO.

- [ ] Write RED tests for explicit CREATE settings, correct profile identity,
  registered-before-send ordering, only sequence-zero CREATE, matching immutable
  batch evidence and no internally reacquired flock in in-session calls.
  Assert old request bytes/query IDs/design digests remain unchanged; swapping
  only context configuration, profile or resolved-plan identity is rejected.
- [ ] Test wrong-profile requests, peer/server/version mismatch, actual positive
  EndOfStream, source-free CREATE failure, lost CREATE/INSERT completion,
  cancellation and handshake/request timeouts. Verify no reconnect/retry.
- [ ] Run `uv run pytest tests/test_clickhouse_candidate_v3_transport.py
  tests/test_clickhouse_candidate_gateway.py tests/test_clickhouse_native_candidate.py -q`.
- [ ] Implement additive version-aware request validation and reuse the existing
  pinned native I/O. Preserve binary/decimal/temporal conversion checks, private
  logger handling and bounded batch checks; no raw SDK errors in diagnostics.
- [ ] Run GREEN tests and unchanged old transport vectors. Confirm source factory
  is still unopened until Task 6's independent catalog readback. Check quality
  gates; commit and update PR.

## Task 5: Protected catalog, seal and v3 publication recovery

**Files:** Create native catalog, authority observer, candidate seal, guarded
backend and runtime v3 kernel from the boundary table. Extend
`adapters/clickhouse_authority_publisher.py` only with explicit v3 in-session
dispatch/closure paths. Tests: `tests/test_clickhouse_native_catalog.py`,
`tests/test_clickhouse_authority_observer.py`,
`tests/test_clickhouse_candidate_seal.py`,
`tests/test_clickhouse_guarded_publication_v3.py`,
`tests/test_clickhouse_guarded_backend.py`; retain old kernel/publisher tests.

**Interfaces:**

- `DirectNativeCatalogReader.snapshot(binding: OperationBinding) -> CatalogSnapshotV2`
  returns exact endpoint/server/database/table identities, complete supported
  design/settings, partition/storage facts, pending mutations and dependency/
  row-policy facts. `rows(binding, table: str, columns:
  tuple[CandidateColumn, ...], limits: ObservationLimits) ->
  ContextManager[Iterator[ObservedRow]]` yields binary-preserving values with
  actual partition IDs through fixed queries, not caller SQL.
- `ProtectedObserverV2.observe(binding, session: BoundExecutionSession,
  resolved_plan: CompatibilityPlan) -> PublicationObservationV3` captures
  metadata/epoch before and after bounded content reads, using each table's own
  supported schema. Reject drift or incomplete facts; no server count-only seal.
- `CandidateSealServiceV3.seal(invocation, session) -> SealedObservationV2`
  irreversibly closes admission, joins every accepted request, verifies normal
  source exhaustion and CREATE completion, independently compares expected and
  observed typed evidence, then durably acknowledges the immutable seal once.
- `GuardedPublicationV3.publish(operation_id: str) -> PublicationRecordV3` and
  `recover(operation_id: str) -> PublicationRecordV3` use the v3 port in
  `ports/clickhouse_protected_publication.py`. The backend implements read,
  observe, prepare-selected, claim, execute-once, closure and resolve against
  the same supplied session; it never acquires an inner lock.
- `AuthorityPublisherV3.execute_in_session(grant: DispatchGrant, session:
  BoundExecutionSession) -> None` rechecks frozen intent and exact before-state,
  spends the durable send permission, then uses existing fixed
  `NativePublicationRequest` and native transport. Recovery invokes closure and
  observation only. It never calls the old selector for v3 records.

- [ ] Write RED observer tests for complete metadata and per-table schemas,
  adaptive/history facts, real partition IDs, scan-budget exhaustion, late
  settings/UUID/epoch drift, dependency uncertainty and binary content parity.
- [ ] Write RED seal tests for paused accepted writers, late registration,
  source exception/close failure, incomplete or ambiguous requests and lost seal
  acknowledgement. No readback restores a seal-producing capability.
- [ ] Test explicit after-states for RENAME, NOOP, EXCHANGE and REPLACE. REPLACE
  preserves target UUID/configuration and leaves candidate unchanged; EXCHANGE
  swaps exact identities; RENAME consumes only candidate name. Require closure
  before classification. NOOP is COMMITTED only with acknowledged claim and
  unchanged protected state; unclaimed is NOT_PUBLISHED.
- [ ] Test source-free zero-SQL recovery for all methods, unknown third/partial
  state, wrong policy/version, lost ACK, PREPARED crash and terminal historical
  readback. Distinct proven before-state is NOT_PUBLISHED; ambiguous state stays
  UNKNOWN. No alternate method is tried after any mutation attempt.
- [ ] Run `uv run pytest tests/test_clickhouse_native_catalog.py
  tests/test_clickhouse_authority_observer.py tests/test_clickhouse_candidate_seal.py
  tests/test_clickhouse_guarded_publication_v3.py tests/test_clickhouse_guarded_backend.py -q`.
- [ ] Implement fixed catalog reads, observer/seal, backend and v3 kernel using
  Task 2's selected post-state. Keep the old kernel/codec/selector unchanged.
  Require epoch and session validity at observation, seal and pre-send.
- [ ] Run GREEN plus old guarded-publication/native-publisher suites and graph/
  module gates. Commit only this cohesive increment; update PR #249.

## Task 6: One-call API and actionable diagnostics

**Files:** Create readiness, lifecycle, runtime protected-publication and
diagnostics modules; create tests for readiness, protected publication,
compatibility diagnostics and the runnable example. Create
`examples/python/clickhouse_protected_publication.py`.

**Interfaces:** `ProtectedClickHousePublication(lifecycle:
ProtectedPublicationLifecycleV3)` exposes
`plan_new(request: ProtectedPublicationRequestV3) -> CompatibilityPlan`,
`publish_new(request: ProtectedPublicationRequestV3, source_factory:
Callable[[], ContextManager[Iterator[tuple[object, ...]]]]) -> PublicationRecordV3`
and `recover_existing(operation_id: str) -> ProtectedPublicationStatus`.
The lifecycle port owns protected session/readiness/enrollment/observation/seal/
publication capabilities; the runtime owns source iteration/exhaustion/closure.
Status contains candidate/publication state, observation kind and retry safety,
not capabilities. Credentials and concrete adapters enter only at composition.

The port's exact methods are `preview(request) -> CompatibilityPlan`,
`open_new(request) -> ContextManager[PublicationInvocationV3]`,
`create_and_verify(invocation) -> None`,
`write(invocation, batch: CandidateBatch) -> None`,
`source_exhausted(invocation) -> None`, `seal(invocation) -> SealedObservationV2`,
`publish(invocation, seal: SealedObservationV2) -> PublicationRecordV3`,
`retain(invocation, reason: str) -> None` and
`recover_existing(operation_id: str) -> ProtectedPublicationStatus`.
`PublicationInvocationV3` is an active process-local wrapper around acknowledged
candidate invocation, bound session and resolved plan; never decode it from
JSON. The runtime receives the profile's immutable batch-validation capability
through this wrapper as `batch_validator: CandidateBatchValidator`, whose
`validate_batch(rows: tuple[tuple[object, ...], ...]) -> CandidateBatch` method
is declared in the same lifecycle port, without importing a concrete adapter.

- [ ] Write RED preview tests: zero source calls, SQL mutations, reservations,
  authority provisions or lock-file creation; advisory warnings have no selected
  alternative. Publish recomputes under the two-name session and cannot reuse
  preview as permission.
- [ ] Write RED sequence tests asserting `source_calls == 0` for rejected
  preflight, enrollment ambiguity, CREATE failure and readback mismatch; the
  first source call follows verified candidate UUID/configuration. Hold the
  session continuously through source exhaustion, seal, selection and closure.
- [ ] Test `test_late_block_cannot_switch_to_warn`: unresolved post-load
  compatibility retains the operation, and recovery cannot change its mode,
  reopen source or dispatch a fallback. Add source-close failure and interrupted
  diagnostics tests, retaining the true publication outcome.
- [ ] Test example `plan|publish|recover`: JSON-only stdout, redacted stderr,
  plan exit 0/2; publish/recover 0 COMMITTED, 2 proven pre-enrollment rejection or
  NOT_PUBLISHED, 3 retained/unresolved. A warning-bearing COMMITTED result is
  exit 0 with actual `unverified_method_skipped`, not whole-route certification.
- [ ] Run `uv run pytest tests/test_clickhouse_publication_readiness.py
  tests/test_clickhouse_protected_publication.py tests/test_clickhouse_compatibility_diagnostics.py
  tests/test_clickhouse_protected_publication_example.py -q`.
- [ ] Implement the lifecycle and runtime interfaces. Readiness checks original
  inventory, role/name coverage, past ownership and grant visibility; opaque
  `VerifiedReadinessV3` is bound to the same current session/plan. No caller
  assertion establishes dpone-only ingress.
- [ ] Implement versioned diagnostics with method/reason/settings disposition,
  mode, actual warnings, missing fact, next action and retained ownership.
  Unknown owner/source values are null, not false. Retry is safe only after a
  proven unenrolled rejection has been corrected. Export private UTF-8 JSON
  atomically outside the authority directory without overwrite; export failure
  yields null path plus warning, never false publication failure or success.
- [ ] Run GREEN, privacy and base-import tests; measure the one-call self-service
  path's manual-action count. Check architecture gates; commit and update PR.

## Task 7: Documentation, fault certification and independent review

**Files:** Create `docs/clickhouse-protected-publication.md`,
`docs/tutorials/clickhouse-protected-publication.md`,
`docs/reference/clickhouse-table-compatibility.md`,
`docs/runbooks/clickhouse-publication-recovery.md`;
create `tools/generate_clickhouse_compatibility_reference.py` and
`tests/test_clickhouse_compatibility_reference.py`. Update existing native
publication, methods, authority-journal and closure guides, architecture,
changelog and MkDocs navigation. Extend the live test/support files from Task 1
and create `tests/integration/clickhouse_table_compatibility_faults.py`.

- [ ] Write the generated-reference freshness test against Task 2's registry
  and reason metadata; run `uv run pytest
  tests/test_clickhouse_compatibility_reference.py -q` and retain RED output.
- [ ] Implement the deterministic reference producer. Generate supported values,
  default/provenance rules, modes, versions, reason codes and exact diagnostics
  without a second hand-maintained settings table. Keep conceptual explanation
  and recovery advice authored, linking to the generated reference.
- [ ] Complete discover → platform preparation → one-call publish → optional
  preview → outcome/diagnostics → original-operation recovery → upgrade journey.
  Separate platform/bootstrap responsibilities from a data engineer's zero-copy
  settings path. Show block, successful verified warning alternative, shared
  unknown rejection and late retained operation. Explain optional preview versus
  final selection and all four publication methods; link from existing guides.
- [ ] Run focused example/reference/docs tests. State staged Python-only scope
  everywhere; no claim that ODBC parallelism, recurring refresh or cleanup is
  available. Update parent plan's remaining-task pointer without rewriting its
  completed historical ledger or evidence.
- [ ] Produce the change-aware validation plan using
  `uv run python tools/agent_policy/select_checks.py --base-ref origin/master`.
  Run Ruff lint/format, mypy, import rules, layer metrics, strict graph tests,
  exact-base/head module-size ratchet and full non-live pytest per AGENTS.
  Refresh `docs/quality-metrics.md` only through its existing producer and check
  freshness; budget and debt-baseline files remain read-only.
  Record failures/skips, including pre-existing immutable artifact issues;
  never edit old evidence to make a gate green.
- [ ] Commit the candidate before final certification. On that exact commit,
  use only fresh task-owned Docker Desktop containers/network/volumes with labels,
  pinned image and explicit limits. Run actual default/nondefault/adaptive/
  nonadaptive/Compact/Wide cases and all four methods, verifying rows, UUIDs,
  complete settings and expected old/candidate resources.
- [ ] Activate actual lost-response, paused writer, competing cross-role owner,
  changed configuration/epoch and crash/fresh-process cases. Include empty and
  stale partitions, warn alternative and late strict rejection. Record original
  journal, phase, fault receipt and mutation/source counters; recovery must have
  zero source and zero mutation replay. Mock-only checks cannot pass this gate.
- [ ] Save `summary.json`, JUnit, source commit/tree identity, image/server/driver/
  SQLite identities, protected probe evidence, journal snapshots, fault receipts,
  counters and checksums in the new artifact family. Mark production ingress,
  TLS, host power-loss and full ODBC route UNVERIFIED, not failed or passed here.
- [ ] Run docs links, generated references, language contracts and one isolated
  strict MkDocs build. Inspect rendered navigation, approval/state notices,
  self-service entry point, warning examples and recovery links.
- [ ] Obtain fresh-context independent correctness/compatibility/data-loss/test/
  docs/evidence review. Fix findings with regression tests; any source correction
  invalidates exact-commit certification and requires rerun on the new commit.
- [ ] Update PR #249 with scoped PASS/FAIL/SKIP/N/A/UNVERIFIED evidence and remaining
  gates. Do not merge or release. Completion requires both architectural gates
  and the approved scoped live evidence, not only a green focused suite.

## Self-review and handoff

Coverage: provenance and structural feasibility → Task 1; public defaults,
registry, identities and policy → Task 2; versions, ownership and durability →
Task 3; CREATE/INSERT → Task 4; seal/selection/recovery → Task 5; self-service and
diagnostics → Task 6; documentation/certification/rollout → Task 7. Each Review
Focus case has an owning regression test. No old evidence becomes new evidence.

The specification and this plan are approved. Execute Native with root as sole
writer. Task 1 is an explicit
feasibility gate: provenance and graph compliance are not yet certified, and a
failed result must not be silently treated as permission for later tasks.
