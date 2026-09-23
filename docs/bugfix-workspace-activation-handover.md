# Recoverable workspace activation handover

Status: BLOCKED for design review. The originally approved local-intent approach
does not survive complete ephemeral-cache loss and is superseded below. Only an
uncommitted historical-readback backend slice remains; end-to-end integration is
not ready. No database migration, commit, push or release is authorized here.

The replacement proposal is the
[durable handover feature specification](feature-design-durable-workspace-handover.md),
with [ADR 0072](adr/0072-durable-workspace-handover-witness.md). Its RESEARCHED status
does not authorize the protected-state implementation.
This note is for platform maintainers and operators diagnosing
repeat activation against an already-owned physical database.

The normative behavior is the protected occurrence protocol in the
[approved workspace design](feature-design-dbt-multi-project-release.md#protected-activation-occurrence-and-reservation-protocol).
This change completes its retirement and recovery integration. It does not
authorize a new release, route certification, manual guard mutation, or a weaker
admission policy. See the [dbt overview](dbt.md) for the public authoring journey.

## Reproduction and impact

`DeploymentCacheMaterializer.promote` calls `prepare_occurrence`, commits the
pointer, and calls `activate_occurrence`. It never retires the pointer's prior
occurrence. A second fresh activation UUID against the same database therefore
encounters the first occurrence's HELD guard and fails with `guard_conflict`.
The public wrapper reports `DPONE_DBT_WORKSPACE_ADMISSION_UNAVAILABLE`.
Reactivating even the same immutable deployment reproduces the bug; deployment
identity is not activation identity.

There is a second recovery defect: the application coordinator rebuilds the
original request from a new physical observation before retirement. An admitted
workload can legitimately create or replace catalog objects. The new observation
then changes the request digest, and retirement fails with `occurrence_rebuild`.
Deleting guards or ignoring observation equality would discard authority.

The focused regression module uses the real release/projection fixtures,
materializer, cache saga, and application coordinator. Its stateful admission
double enforces ownership, epochs, exact replay, allowed transitions and terminal
closure. It does not emulate SQL concurrency or certify a live database.

## Superseded local-cache algorithm

The following sequence explains the initial proposal, not an accepted durable
cluster protocol. A fsynced file inside an ephemeral cache is lost with that
cache. The experimental runtime integration and both local journal modules were
removed from the worktree before review. The cache materializer is unchanged
from the base commit. Do not enable this sequence as a completed recovery fix.

1. Validate the candidate and sealed projection, authorize the promoter, acquire
   the existing promotion lock, and validate local and remote CAS. Identify the
   predecessor by its exact pointer activation UUID as well as deployment,
   release, environment and sealed projection. A deployment-ID match alone
   cannot identify an occurrence.
2. Persist a bounded handover intent before closing predecessor admission. Bind
   the exact predecessor pointer and projection, successor activation UUID,
   candidate identity, original previous deployment ID, and selected authority.
   Use atomic durable publication and confinement. The intent is recovery input,
   never proof of protected database state. Reject a competing intent.
3. Read the predecessor's occurrence from the protected authority. Verify its
   coordinates, source inventory, immutable runtime-context identity, complete
   write/guard ownership and epochs. Do not derive historical ownership from a
   fresh catalog observation. Transition ACTIVE to RETIRING; exact RETIRING or
   RETIRED readback supports replay. A missing, foreign or ambiguous occurrence
   fails closed. Runtime must reject new attempts once RETIRING is durable.
4. Finalize only after every admitted attempt has durable terminal closure and
   the existing connector-specific quiescence evidence. RUNNING and COMMIT_UNKNOWN
   always block, even when an unknown outcome has a non-null receipt. A terminal
   state without a receipt also blocks. Keep the predecessor pointer and guards
   while blocked; emit a recovery-required, retryable result. Do not poll forever
   while holding the cache lock or infer quiescence from elapsed time.
5. Release only the predecessor's exact epochs and read back RETIRED. The
   predecessor is now intentionally unavailable for new work. There is a bounded
   availability gap; code must not claim an atomic old-to-new database handoff.
6. Build the successor request from a fresh complete observation. Retain its
   exact bounded request bytes in the intent before reservation, then reserve
   and read back PREPARED. Reject any unrelated owner acquiring a guard in the
   gap. Revalidate applicable authority immediately before pointer mutation.
7. Commit the pointer with the intent's successor UUID, then activate and read
   back that exact occurrence. Mark the intent complete only after durable
   ACTIVE and consistent pointer/audit observation. No success may be inferred
   from pointer files or a local journal alone.

After interruption, reacquire the same lock and verify CAS against both the
intent's predecessor and successor coordinates. Reconcile protected state before
continuing any transition. The original previous-deployment coordinate stays
fixed across replay, even if pointer metadata was partially written. A retry
resumes the journaled UUID; an unrelated new promotion cannot replace it.

This is a focused recovery continuation, distinct from a newly requested ordinary
recovery, which retains the approved fresh-UUID contract. A new recovery must
first reconcile any pending intent rather than orphaning its guards. Audit-only
repair continues to require exact ACTIVE readback and does not reserve again.

## Historical observation and durable identity

Retirement needs a capability to read historical ownership independently of
current physical observations. The existing protected tables retain occurrence
coordinates, request/resource fingerprints, write membership and fencing epochs,
but not the complete original observation or request payload. Those fingerprints
cannot reconstruct the original request after catalog drift.

The preferred minimum is an additive lifecycle identity/readback capability
using those existing protected rows. It validates the immutable source and
runtime identity from the predecessor's sealed projection, then consumes stored
request/resource subjects and exact owned epochs for retirement. It does not
change or recompute the persisted subjects. Fresh successor preparation still
observes and validates the entire physical closure. The existing request-based
API can retain compatibility while delegating retirement validation to the same
identity/readback policy.

A local copy of the original request is useful for new-operation replay, but is
not sufficient as the sole migration strategy: earlier occurrences never wrote
one. A solution that only adds a sidecar would leave already-active occurrences
unrecoverable after legitimate catalog changes. Do not silently recreate an old
request from today's observation or treat local bytes as database authority.

The implementation must specify the typed lifecycle identity and exact readback
checks before adding this port. New database schemas are not assumed necessary;
any such need returns to the integrator for review.

## Failure and retry matrix

| Interruption or conflict | Required behavior |
| --- | --- |
| Before intent or predecessor transition | Preserve old ACTIVE and pointer; no new reservation |
| RETIRING with incomplete attempts | Preserve old guard epochs; no successor pointer; retry after durable terminal evidence |
| Retirement commit acknowledgement lost | Exact protected readback on a fresh session; never guess rollback |
| RETIRED before successor PREPARED | Resume the same intent; do not reactivate old epochs |
| Successor PREPARED, pointer untouched | Retain ownership and intent; exact retry reuses its UUID and epoch |
| Pointer metadata/audit partially committed | Reconcile pointer components and protected PREPARED/ACTIVE using the same intent |
| Pointer changed, ACTIVE acknowledgement lost | Fail closed until exact ACTIVE readback; do not create a fresh competing reservation |
| Physical observation changed after old workload | Retire stored historical ownership; freshly observe successor |
| Third party claims released guards | Stop at admission conflict; never overwrite the third party |
| Finalizer replay after successor acquired epochs | Return old RETIRED evidence without touching successor ownership |
| Missing/tampered/foreign recovery input | Stop with actionable recovery-required error; no guard edits |

Automatic abort of PREPARED and direct PREPARED-to-RETIRED transitions are outside
this bug fix. Recovery completes the pinned saga. Abandonment needs a separately
specified authority and transition; a pointer failure alone is not permission to
release guards or restore retired authority.

## Compatibility, user journey and validation

Workload manifests, immutable release/deployment IDs and workspace wire v2 remain
unchanged. Preserve singleton v1 behavior and composed activation dispatch; do
not borrow workspace-only retirement authority for a composed parent. A removed
workspace still needs predecessor retirement when its successor is a legacy
release; source dispatch alone cannot decide whether retirement is needed.

The operator continues using the existing reconcile/promotion entrypoint. Normal
success performs retirement automatically. A blocked result identifies the
activation and phase with content-free reasons, explains that admitted attempts
must reach durable terminal closure, and directs an exact retry. No SQL guard
editing, force flag, credential disclosure, or manual callback is introduced.
Update the operator recovery guide and changelog when implementation lands.

Phase-1 checks on base `abb9de012`, before the backend prototype:

- FAIL, expected regression: second fresh occurrence encounters `guard_conflict`.
- FAIL, expected regression: old occurrence retirement after observation change
  encounters `occurrence_rebuild`.
- PASS: RUNNING, COMMIT_UNKNOWN and missing terminal receipt prevent retirement
  and successor pointer mutation.
- PASS: durable terminal closure allows the next epoch; finalizer replay does
  not release successor ownership.
- PASS: pointer preparation failure retains PREPARED; a foreign UUID conflicts;
  exact same-UUID retry succeeds without advancing its epoch.
- UNVERIFIED: SQL transaction isolation, connector quiescence and live recovery.

Focused evidence is executable in
`tests/test_dbt_workspace_cache_handover.py`. Run with the current source trees
for both the core package and `dpone-airflow-pack`; an older installed pack can
fail pytest initialization before tests execute. After implementation, extend
this module for restart at every boundary, partial pointer writes, repeated
deployment identities, legacy predecessor/successor dispatch, foreign intent,
concurrent claim, and commit-unknown readback. Run change-aware selection and
the mandatory architecture, typing, regression and documentation gates. Fresh
context review precedes integration; no live readiness claim follows from these
fixtures.

## Writer boundaries

This phase owns only the new regression module and this note. Existing runtime,
services, adapters, ports, contracts, schemas, factories, shared test fixtures,
release metadata and workflows are read-only. The integrator owns expansion of
that scope. Phase 1 is ready for design review, not merge or release.

The integrator subsequently authorized workspace-specific backend integration
and one lifecycle contract module. The current machine-readable boundary is
`test_artifacts/agent-policy/workspace-handover-task-contract.yml`. Broader
database or desired-state wire changes still require a separate decision.

## Durable authority gap and proposed review contract

This section is a proposal for independent review, not approval to add a second
desired-state source of truth. The remote desired deployment remains the sole
selector of immutable source/release/deployment content.

What existing authorities actually retain:

- `desired.source.occurrence_id` already supplies the successor activation UUID.
  Native reconciliation must continue using it across replicas and restarts.
- `desired.previous` supplies only opaque remote revision and deployment ID. It
  does not supply the exact predecessor activation UUID. Repeated activation of
  the same deployment makes that distinction essential.
- The desired-state checkpoint contains channel coordinates and activation UUID,
  but the current filesystem checkpoint is also inside the ephemeral cache.
- Protected occurrence rows retain immutable coordinates, original request and
  resource subjects, write membership and guard epochs. Those suffice for exact
  historical retirement/readback. They do not identify a global deployment
  channel, an authorized predecessor-successor transition, or a full original
  request payload.

Consequently, a cache-local intent cannot justify automatic recovery after loss
at RETIRING, RETIRED, successor PREPARED, or pointer-written/ACTIVE-unconfirmed.
Searching for any ACTIVE occurrence with the same deployment ID is not a safe
substitute: another replica or same-deployment reactivation can own that ID.

### Typed handover witness

Any durable solution must recover one exact immutable witness containing:

| Field | Semantics |
| --- | --- |
| `channel_sha256` | Fingerprint of versioned trusted desired-object identity plus registry scope, source project/ref and environment; never pod or local-cache path |
| `desired_state_sha256` and `remote_revision` | Exact already-verified remote authority; revision is an opaque equality/CAS token, never a sequence |
| `successor_activation_id` | Exactly `desired.source.occurrence_id`; all replicas observing that desired identity converge on it |
| `predecessor_activation_id` | Exact existing occurrence UUID, or explicit bootstrap absence; never inferred only from deployment ID |
| `predecessor_deployment_id` | Must match both verified remote predecessor and protected historical occurrence |
| `predecessor_witness_sha256` | Link to the previous completed durable handover, when this mechanism owns the channel |
| `successor_release_id`, `successor_deployment_id` | Exact immutable artifacts selected by remote desired authority |
| `source_inventory_sha256`, `runtime_context_sha256` | Exact verified immutable input authority used by admission |
| `witness_sha256` | Canonical digest of the complete immutable witness |

If a retained remote previous revision plus its independently verified immutable
payload already provides the predecessor binding, prefer deriving this witness
from that authority. Merely copying a local checkpoint to a database does not
prove it was authorized. A migration/adoption rule is required when no durable
predecessor witness exists; ambiguous existing state must remain blocked.

Independent review found two additional constraints. The existing authority can
select distinct `desired_state_uri` objects for the same registry scope, source
project/ref and environment. Those channels must not collide: bind the canonical
trusted desired-object identity, or enforce one-object uniqueness explicitly.
Do not use the entire publication-authority fingerprint as the stable channel
key, because that authority includes `watcher_identity` and would separate
replicas. Also, the existing reconcile policy supports skipped desired revisions.
The immediately previous published occurrence therefore need not be the applied
predecessor. Guard-owner discovery does not fill that gap when write sets are
disjoint or the old occurrence is already RETIRED.

### Conditional DDL semantics, only if existing remote authority is insufficient

One possible protected ledger uses existing occurrence rows as the execution
authority and adds only durable handover witnesses. It is not a new mutable
desired pointer. Proposed SQL Server semantics, not an applied migration:

```sql
CREATE TABLE [dpone_control].[dbt_workspace_handovers] (
    channel_sha256 varchar(71) NOT NULL,
    successor_activation_id uniqueidentifier NOT NULL,
    predecessor_activation_id uniqueidentifier NULL,
    witness_sha256 varchar(71) NOT NULL,
    predecessor_witness_sha256 varchar(71) NULL,
    witness_json nvarchar(max) NOT NULL,
    prepared_request_sha256 varchar(71) NULL,
    prepared_request_json nvarchar(max) NULL,
    completed bit NOT NULL CONSTRAINT [df_workspace_handover_completed] DEFAULT (0),
    CONSTRAINT [pk_workspace_handover]
        PRIMARY KEY (channel_sha256, successor_activation_id),
    CONSTRAINT [uq_workspace_handover_witness] UNIQUE (witness_sha256),
    CONSTRAINT [ck_workspace_handover_witness_bytes]
        CHECK (DATALENGTH(witness_json) <= 262144 AND ISJSON(witness_json) = 1),
    CONSTRAINT [ck_workspace_handover_request_pair]
        CHECK ((prepared_request_sha256 IS NULL AND prepared_request_json IS NULL)
            OR (prepared_request_sha256 IS NOT NULL AND prepared_request_json IS NOT NULL)),
    CONSTRAINT [ck_workspace_handover_request_bytes]
        CHECK (prepared_request_json IS NULL OR
            (DATALENGTH(prepared_request_json) <= 16777216 AND ISJSON(prepared_request_json) = 1))
);
CREATE UNIQUE INDEX [uq_workspace_handover_pending_channel]
ON [dpone_control].[dbt_workspace_handovers] (channel_sha256)
WHERE completed = 0;
CREATE UNIQUE INDEX [uq_workspace_handover_predecessor]
ON [dpone_control].[dbt_workspace_handovers] (channel_sha256, predecessor_activation_id)
WHERE predecessor_activation_id IS NOT NULL;
CREATE UNIQUE INDEX [uq_workspace_handover_bootstrap]
ON [dpone_control].[dbt_workspace_handovers] (channel_sha256)
WHERE predecessor_activation_id IS NULL;
```

The DDL alone is insufficient. A protected transaction API must enforce immutable
witness bytes, complete closed parsing, digest equality, exact predecessor chain
membership, and write-once request bytes before PREPARED admission. Updates may
only fill the request pair once and change `completed` from false to true after
exact durable successor ACTIVE readback. Runtime principals must not be able to
modify/delete witness or ownership rows outside that API. Schema validation and
adoption/migration policy are part of the proposed contract, not optional setup.

Use SERIALIZABLE transactions plus a stable channel application lock when
claiming/progressing a witness. Identical successor UUID and witness digest are
idempotent across replicas. Same UUID with any changed immutable field is a
conflict. A different pending successor cannot preempt the first. Completion
cannot make an old occurrence ACTIVE again after a later handover retired it.
Same-deployment reactivation advances through distinct UUIDs and links the exact
prior UUID; deployment hash equality never grants permission to retire it.

An alternative to the append-only witness ledger is one protected channel row
with exact applied UUID, pending immutable transition and monotonic CAS revision.
It stores execution progress, not desired-state selection. That alternative may
be smaller, but still requires an attested bootstrap mapping of existing rows to
the trusted desired object and an agreed remote supersession protocol. Neither
DDL alternative is approved by this note.

### Remote CAS races remain a required design decision

A database transaction cannot atomically compare a remote object-store revision.
Perform the existing independent remote verification before claiming a witness
and again at the pointer publication boundary, but do not describe those checks
as a cross-store atomic CAS. If remote desired authority changes between checks,
the pending witness remains pinned; it cannot silently take the new desired UUID
or free guards. A replacement pod recovers protected state, not pointer evidence.

There are two unresolved ways to close the liveness gap when a pinned handover is
superseded after predecessor retirement or successor PREPARED:

1. An existing remote publication/receipt protocol may already pin the in-flight
   occurrence until protected activation completes. Verify that protocol and its
   retained predecessor revision before adding a duplicate ledger.
2. Otherwise, a separately approved supersession/cancellation protocol is needed.
   In particular, PREPARED has no currently approved direct RETIRED transition.
   Neither marking it ACTIVE without the pointer boundary nor releasing its
   guards merely because a newer remote object exists is allowed.

Until this decision is resolved, adding a handover table alone does not establish
safe automatic recovery. Do not treat a newer remote revision, expired TTL,
missing pod, or empty cache as release authority.

### Required independent recovery evidence

Rebuild a completely new cache while retaining only the approved durable remote
and database authorities at each boundary: before retirement, RETIRING with
RUNNING/COMMIT_UNKNOWN, RETIRED, successor PREPARED, pointer-written, and ACTIVE
with missing local acknowledgement. Run two independent caches on the same
desired occurrence and on competing occurrences, then repeat with identical
deployment hashes and distinct activation UUIDs. Verify exact final epochs,
absence of foreign retirement, pinned replay, and no success without protected
ACTIVE. The removed local-intent prototype passed only same-cache tests; that is
not evidence for these cases.

Current scoped backend checks: 39 focused historical-contract, SQL-admission and
coordinator tests PASS. The historical backend has not passed the full repository
gate or live SQL validation. Runtime integration was removed; the second native
activation regression intentionally remains FAIL until durable authority is
resolved. The branch is not ready for merge, release or a recovery-readiness claim.
