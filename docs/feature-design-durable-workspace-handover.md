# Feature design: durable workspace activation handover

- Status: APPROVED for scoped implementation on 2026-09-23; no live migration,
  release, or production-readiness authorization.
- Owner: dpone maintainers; parent task is the integrator.
- Target release: unassigned; no publication authorization.
- Last verified: 2026-09-23.

## Problem and intended result

Native workspace activation currently reserves a new occurrence before retiring
its predecessor. Repeated activation against the same physical database fails.
Adding a cache-local retirement journal does not solve loss of an entire watcher
pod or coordination between independent caches. A local pointer also cannot prove
which occurrence is applied across replicas.

Introduce a shared protected **execution witness**, colocated with existing SQL
admission state. Remote desired state remains the only source selecting desired
content. The witness records which exact selected occurrence is applied and which
exact transition was claimed. Existing activation, attempt and guard rows remain
the sole physical ownership and quiescence authority. No second guard namespace
or independent fencing epoch is introduced.

Success means two independent caches can resume the same durable transition
after complete cache loss, without changing activation identity, retiring a
foreign occurrence, losing attempts, or reporting unconfirmed ACTIVE as success.
This supersedes the local-intent proposal in the
[bug-fix investigation](bugfix-workspace-activation-handover.md) and extends the
[existing activation protocol](feature-design-dbt-multi-project-release.md#protected-activation-occurrence-and-reservation-protocol).

## Personas and customer journey

| Persona | Task | Observable result |
| --- | --- | --- |
| Pipeline author | Publish a later workspace revision | Existing authoring and release commands remain unchanged |
| Platform operator | Provision or explicitly adopt an existing channel | Exact channel registration and protected predecessor readback |
| On-call operator | Replace a failed watcher with an empty cache | Next reconcile resumes the claimed UUID automatically |
| Maintainer/auditor | Investigate incomplete handover | Durable channel revision, claim identity, admission state and safe error code |

Discover this behavior in the desired-state operator guide. Prepare the new
protected schema before upgrading watchers. Register the trusted channel or use
the privileged adoption procedure below. Execute the existing `dpone airflow
desired-state reconcile` entrypoint. Observe convergence only after protected
ACTIVE readback and local projection verification. If attempts are incomplete,
finish their existing terminal-recovery procedure and retry reconcile. Pod
replacement, TTL expiry and pointer deletion never authorize guard release.
Upgrade readers/adapters before enabling the witness requirement; retain all
original occurrence history and immutable source artifacts.

## Scope and constraints

In scope: native workspace v2 channel claim, historical readback, exact
retirement, durable request replay, cold-cache recovery, same-desired replicas,
same-deployment reactivation, skipped desired publications, and operator evidence.

Not in scope: new connectors, cross-database transactions, distributed atomic
SQL/object-store publication, speculative cancellation, guard force-clear,
PREPARED abandonment, release publication, or changes to runtime credential
projection and deployment payload wires. The design consumes current and later
projection formats through the existing validated projection interfaces.

All runtime SQL authority for one channel uses the same protected control store
as its existing admission tables. Different physical authorities cannot share a
channel. A channel is explicitly native-workspace managed; composed-parent
handover requires its own complete authority and remains rejected before native
retirement. Standalone singleton v1 and existing composed flows remain unchanged.
Moving a managed channel to a non-workspace release requires explicit retirement
through this witness and a separately approved authority-mode migration; do not
silently downgrade a channel to an unguarded path.

## Public contracts and identity

### Channel identity

The canonical `dpone.dbt-workspace-channel.v1` object has exactly `schema`,
`desired_state_uri`, `registry_scope_id`, `environment`, `source_project`,
`source_ref`, and `channel_sha256`. Compute the digest from all fields except
itself. Use the trusted desired URI parsed and re-rendered from exact bucket/key,
without filesystem normalization, case folding or percent-decoding guesses.
`registry_scope_id` retains the existing certified-endpoint-bound registry
identity. The object URI is necessary: two configured desired keys can share all
other coordinates. Exclude watcher identity, hostname, pod, cache path and the
complete publication-authority fingerprint; replicas must share one channel.

New witness-document fingerprints use the existing
`dbt_contract_validation.canonical_fingerprint`: sorted keys, compact separators,
ASCII-escaped finite JSON, UTF-8 bytes, and lowercase `sha256:` digest. Keep
existing desired-byte, activation-request and physical-resource fingerprint
algorithms unchanged; they are separately validated nested contracts. SQL and
Python golden vectors must include non-ASCII strings and escaped nested JSON.
The existing activation/resource fingerprint also normalizes backslashes in
string values to slashes. Validate that normalized fingerprint while retaining
the exact original request and guard identifiers; an equal fingerprint is not
permission to substitute different original bytes. SQL comparisons/replacement
must be binary and independent of the database's width/case sensitivity.

`desired.source.occurrence_id` is always the successor UUID. Never generate a
replacement UUID for a durable claimed transition. Exact UUID reuse with changed
desired bytes, source/runtime subjects or channel is a conflict.

### Claim and durable readback

A closed `dpone.dbt-workspace-handover-claim.v1` object contains exactly:

- `schema`, complete `channel`, `claim_sha256`;
- `expected_channel_revision`, `claim_revision`;
- `predecessor_activation_id`, `predecessor_deployment_id`, and
  `predecessor_request_sha256`, all null only for registered-empty bootstrap;
- `successor_activation_id`, `successor_release_id`, `successor_deployment_id`;
- `desired_state_sha256`, exact `desired_state_json`, `observed_remote_revision`;
- `source_inventory_sha256`, `runtime_context_sha256`;
- `authorization_subject_sha256`, identifying the independently verified
  immutable promotion/authority evidence used when claiming.

The digest covers the entire object except itself. The desired JSON is the
bounded exact verified UTF-8 object, not a reserialized partial summary.
`claim_revision = expected_channel_revision + 1`. The applied predecessor comes
from protected channel readback and its exact activation rows, never from the
immediately prior published desired revision. Skipped publications are valid.
The existing previous-publication relationship is checked as publication
lineage and reported separately from the applied predecessor.

The complete original successor activation request is persisted once, in the
same protected transaction that reserves PREPARED. Bound it to this claim's
UUID, environment, deployment/release, applied previous deployment, source and
runtime subjects. Request bytes retain original physical observation subjects;
hashes alone are not a replay payload. Once PREPARED exists, recovery uses this
exact saved request and protected historical readback, not today's observation.

### CLI, Python and evidence

Keep the existing native reconcile CLI flags and JSON success schema. A
non-converged cycle must not emit its normal success report or successful health
status. Safe failures use existing structured error output with one of:
`DPONE_WORKSPACE_CHANNEL_UNREGISTERED`, `DPONE_WORKSPACE_CHANNEL_CAS_CONFLICT`,
`DPONE_WORKSPACE_HANDOVER_WAITING_ATTEMPTS`,
`DPONE_WORKSPACE_HANDOVER_COMMIT_UNKNOWN`,
`DPONE_WORKSPACE_HANDOVER_CONTINUATION_REQUIRED`, or
`DPONE_WORKSPACE_CHANNEL_AUTHORITY_MISMATCH`, or
`DPONE_WORKSPACE_REGISTRATION_PROOF_INVALID`. Preserve the existing nonzero
reconcile-error exit policy; no new force or bypass flag.

Add a separate local diagnostic `workspace-handover-status.json` with schema
`dpone.workspace-handover-status.v1` and exactly `schema`, `channel_sha256`,
`channel_revision`, `current_activation_id`, `pending_activation_id`,
`phase`, `remote_relation`, `converged`, and `code`. Phase is one of `IDLE`,
`CLAIMED`, `RETIRING`, `RETIRED`, `PREPARED`, `ACTIVE`, `UNKNOWN`;
`remote_relation` is `matches_claim`, `superseded`, or `unavailable`.
`converged` is true only after the final checks below. Persist atomically with
existing confined-file helpers; replacement is allowed, deletion loses only
diagnostics. Never use this file as authority. Include no raw request bytes,
credentials or database diagnostics in logs/status.

Python composition injects a `WorkspaceHandoverStorePort` with narrowly scoped
operations: `read_channel(channel)`, `claim(validated_claim, expected_revision)`,
`begin_retirement(claim)`, `finalize_retirement(claim)`,
`prepare_successor(claim, proposed_request)`, and `complete(claim)`.
Readbacks are closed immutable typed values. Store methods operate on fresh
connections and exact claim revision; application services own remote validation
and progression. Existing request-based admission APIs remain compatible.
Their signatures and unmanaged behavior remain compatible, not their ability to
bypass a managed channel: a request-only managed mutation is denied. The SQL
permission migration described below is a required deployment change.

## Protected schema and transaction semantics

Two additive tables are proposed; no migration has been applied:

```sql
CREATE TABLE [dpone_control].[dbt_workspace_channels] (
    channel_sha256 varchar(71) NOT NULL PRIMARY KEY,
    channel_json nvarchar(max) NOT NULL,
    revision bigint NOT NULL,
    current_activation_id uniqueidentifier NULL,
    pending_activation_id uniqueidentifier NULL,
    registration_id uniqueidentifier NOT NULL UNIQUE,
    registration_input_sha256 varchar(71) NOT NULL,
    registration_sha256 varchar(71) NOT NULL,
    registration_json nvarchar(max) NOT NULL,
    CONSTRAINT [ck_workspace_channel_revision] CHECK (revision >= 0),
    CONSTRAINT [ck_workspace_channel_json]
        CHECK (DATALENGTH(channel_json) <= 65536 AND ISJSON(channel_json) = 1),
    CONSTRAINT [ck_workspace_registration_json]
        CHECK (DATALENGTH(registration_json) <= 67108864
            AND ISJSON(registration_json) = 1)
);
CREATE TABLE [dpone_control].[dbt_workspace_handover_claims] (
    channel_sha256 varchar(71) NOT NULL,
    activation_id uniqueidentifier NOT NULL,
    claim_revision bigint NOT NULL,
    claim_sha256 varchar(71) NOT NULL,
    claim_json nvarchar(max) NOT NULL,
    request_sha256 varchar(71) NULL,
    request_json nvarchar(max) NULL,
    completed bit NOT NULL DEFAULT (0),
    CONSTRAINT [pk_workspace_handover_claim] PRIMARY KEY (channel_sha256, activation_id),
    CONSTRAINT [uq_workspace_handover_claim_uuid] UNIQUE (activation_id),
    CONSTRAINT [uq_workspace_handover_claim_revision] UNIQUE (channel_sha256, claim_revision),
    CONSTRAINT [fk_workspace_handover_channel] FOREIGN KEY (channel_sha256)
        REFERENCES [dpone_control].[dbt_workspace_channels] (channel_sha256),
    CONSTRAINT [ck_workspace_handover_claim_revision] CHECK (claim_revision > 0),
    CONSTRAINT [ck_workspace_handover_claim_json]
        CHECK (DATALENGTH(claim_json) <= 524288 AND ISJSON(claim_json) = 1),
    CONSTRAINT [ck_workspace_handover_request_pair]
        CHECK ((request_sha256 IS NULL AND request_json IS NULL)
            OR (request_sha256 IS NOT NULL AND request_json IS NOT NULL)),
    CONSTRAINT [ck_workspace_handover_request_json]
        CHECK (request_json IS NULL OR
            (DATALENGTH(request_json) <= 33554432 AND ISJSON(request_json) = 1))
);
CREATE UNIQUE INDEX [uq_workspace_handover_pending]
ON [dpone_control].[dbt_workspace_handover_claims] (channel_sha256)
WHERE completed = 0;
```

The SQL byte limits account for UTF-16 storage. Application UTF-8 limits are
32 KiB channel, 256 KiB claim, and 16 MiB activation request; desired bytes remain
at the existing 72 KiB and opaque revision at 1024 bytes. Preserve the existing
8192 write/resource bounds. Closed parsing, canonical digest checks, exact foreign
key targets and schema validation are mandatory; `ISJSON` alone is insufficient.

Protected mutation permits only: creation of a registered channel; claim insertion;
request pair null-to-exact-value; completion false-to-true; channel CAS transitions.
No API updates immutable channel/registration/claim/request bytes or deletes
history. Registration input and receipt digests are distinct: the first provides
exact idempotency before the database assigns actor/time; the second protects the
full persisted receipt. Current readback resolves either an adopted registration
baseline or a completed claim by exact UUID, never by deployment ID.

### Enforceable SQL capability boundary

The current adapters issue direct SQL; there is no existing admission procedure
gateway to reuse. Introduce fixed, same-database, owner-chained stored procedures.
Procedure bodies and protected tables have the same non-runtime owner. Runtime
principals receive object-specific EXECUTE and necessary read permissions, **no
direct INSERT/UPDATE/DELETE** on either witness table, the five workspace tables
(`dbt_workspace_activations`, `dbt_workspace_activation_guards`,
`dbt_workspace_activation_write_subjects`, `dbt_workspace_attempts`,
`dbt_workspace_attempt_guards`), or `semantic_refresh_guards`. No broad schema
EXECUTE grant, dynamic caller-supplied SQL, SESSION_CONTEXT authorization flag,
runtime ALTER/CONTROL/IMPERSONATE, ownership, db_owner or sysadmin membership is
permitted. The separate registrar can execute registration procedures but also
has no direct DML. Only the offline schema installer owns/changes modules and
grants; that explicitly trusted administrative boundary already controls tables.
Use disjoint managed-runtime, legacy-workspace and registrar EXECUTE roles: the
managed-runtime credential cannot execute legacy workspace prepare/activate or
registration procedures, and must not inherit their roles. Existing unmanaged
controllers may receive the legacy gateway role; its procedures still reject
managed UUIDs. A semantic caller receives only its three guard procedures, never
workspace mutation procedures. Reusing a broad credential across these roles
fails the enablement permission audit.

The gateway validates closed payloads, exact digests, complete subjects and all
transition predicates in SQL before mutation; Python validation is additional,
not the enforcement boundary. It implements the six workspace port operations,
exact lifecycle/attempt readback, attempt admit/terminalize, and request-based
legacy prepare/activate/begin-retirement/finalize-retirement. Legacy procedures
reject any UUID bound by a claim or adopted registration, even after retirement.
They cannot create channel registrations or claims. Claim insertion and legacy
prepare serialize on the successor UUID and reject an already-existing unbound
occurrence; registration/adoption takes the inventory barrier below. Managed
attempt admission requires the exact ACTIVE shared current with no pending
retirement; terminalization allows existing attempts during retirement, without
guard release. No public procedure accepts an arbitrary expected-state/next-state
pair or an unconditional guard release.

Preventing a direct shared-guard bypass requires a bounded compatibility slice,
not conversion of every semantic-refresh authority table. Exactly five existing
write sites in four adapters need gateway calls:

| Existing adapter | Gateway operation replacing direct guard DML |
| --- | --- |
| `semantic_refresh_mssql_activation.py` | Two guard INSERT paths: validated initial/restored `guard_seed` |
| `semantic_refresh_mssql_state.py` | Guard acquisition UPDATE: `guard_acquire_semantic` |
| `semantic_refresh_mssql_workflow_summary.py` | Terminal success release UPDATE: `guard_release_semantic` |
| `semantic_refresh_mssql_failure.py` | Terminal failure release UPDATE: `guard_release_semantic` |

These three procedures participate in the existing caller transaction: they do
not independently commit. They preserve exact resource/epoch/owner/workflow and
terminal evidence checks from the existing path. Seed is insert-if-absent only,
with the existing validated initial/restoration epoch proof, never reset/update.
Acquire permits only AVAILABLE/RELEASED at exact expected epoch, increments once,
and rejects a new `dbt-workspace:` owner. Release checks the complete expected
guard set/epochs plus terminal closure and rejects any `dbt-workspace:` owner,
regardless of caller-provided workflow ID. Workspace owners can be acquired or
released only inside the workspace gateway's validated occurrence transition.
Read-only guard adapters and other semantic tables retain their current APIs.

The workspace activation, attempt, retirement and historical-lifecycle adapters
must route mutations through this gateway. Unmanaged legacy request behavior,
singleton v1 and composed behavior remain unchanged, but an old binary with
direct-DML guard writes cannot run under the migrated role. Upgrade these four
semantic callers and workspace callers before permission cutover; stop/drain old
writers. Effective permission probes must include inherited roles, alternate
credentials and callable owner-chained modules, not merely inspect named grants.
Any remaining runtime path able to directly change these eight tables blocks
enablement. Do not create a parallel guard store to avoid this migration. This
is a required, separately reviewable compatibility slice, not an already-working
or isolated local bug fix. Full migration of unrelated semantic authority logic
and a generic trigger interception framework are rejected as unnecessary scope.

All mutation runs SERIALIZABLE with a transaction-owned application lock named
from `dpone:workspace-channel:<channel_sha256>` and bounded zero-wait contention.
Lock order is store inventory barrier (shared during ordinary mutation, exclusive
during registration), channel, sorted occurrences, sorted physical guards,
attempts. The barrier is a transaction-owned application lock scoped to the exact
database/control schema; existing semantic guard callers acquire its shared mode
before their first row lock, so registration cannot race a partial transaction.
The five DML sites above are not the entire file-change budget. Bounded
transaction-owner touchpoints also include
`semantic_refresh_mssql_worker_admission.admit_run` (before
`_require_activation_authority`, not only when the guard helper is reached) and
`semantic_refresh_mssql_activation_transaction._transaction` (before its first
activation lock). Standalone state admission, workflow summary, and both failure
transaction entries acquire the shared barrier at transaction start as well.
Thus this compatibility slice touches the four guard-writer modules and these
two transaction-owner modules; it does not migrate unrelated semantic authority
logic. Calling a gateway helper after earlier UPDLOCKs is not sufficient.
In `admit_run`, preload and validate external attempt/context/pod identity through
the injected attempt authority before opening the protected transaction or
inventory barrier. The current call chain can reach `pod_uid_reader` after an
activation row lock; moving only guard DML leaves that violation intact. Pass the
immutable preloaded input into the transaction and recheck durable predicates
there. A test double that fails on any external I/O after transaction start must
cover new admission, replay and continuation. This is not permission to omit
fresh protected DB readback or to perform remote checks under a lock.

Workspace attempt admit, terminalize and replay deliberately change their SQL
lock acquisition order: resolve immutable attempt coordinates without taking an
out-of-order update lock, then lock channel/occurrence, sorted guards and attempts,
and reread exact attempt coordinates under those locks before deciding. Current
attempt-first `_attempt_row` code cannot simply be wrapped by a procedure.
Statement-order tests and live registration-versus-admission/terminalization
contention tests must verify the barrier and order, including idempotent replay
and lost acknowledgement. Deadlock/lock timeout returns bounded non-success and
fresh exact readback; never downgrade isolation or bypass the barrier.
No SQL transaction holds remote HTTP or filesystem I/O. Existing admission
operations must accept the same injected transaction/session for this path;
nested independent commits would break the atomic witness/admission boundary.

`claim` locks the channel, requires exact expected revision/current UUID, rejects
another pending UUID, inserts the immutable claim and advances channel revision
to claim revision while setting pending UUID. Identical claim replay returns the
same record. `begin_retirement` and `finalize_retirement` require that exact pending
UUID/revision and verify complete protected historical ownership on each call.
`prepare_successor` atomically saves exact request bytes and complete PREPARED
ownership; a competing same-claim proposal reads the already-saved request.
`complete` atomically verifies PREPARED/exact epochs, transitions successor ACTIVE,
sets current to successor, clears pending, advances channel revision once more,
and marks the claim complete. Exact already-complete replay is read-only; it
cannot reactivate an occurrence later retired by another claim.

## Detailed algorithm and remote supersession

1. Validate trusted channel authority and read protected channel state. A missing
   registration is an actionable error. If pending exists, load the complete
   durable claim before consulting local cache status; local files cannot select
   its predecessor or successor.
2. With no pending claim, read/validate latest remote desired bytes and immutable
   artifacts using existing trust checks. Observe channel current and historical
   ACTIVE, and independently recheck the remote revision. If the latest exact
   UUID/body already matches current's immutable completed claim or adoption
   baseline, verify/rebuild only the local pointer and continue to step 8: do not
   claim, increment channel revision/epochs, or retire current. A reused UUID with
   different bytes fails. Otherwise claim through channel CAS. A race losing
   channel CAS returns readback and replans, never retires the occurrence observed
   before that race.
3. A durable claim is bounded authorization to finish that exact transition.
   Remote publication after the claim does not replace its UUID or revoke its
   completion authority. **Complete the claimed saga first, then reconcile the
   latest desired state.** This explicit new semantic avoids unsafe PREPARED
   abandonment. Source/promotion signatures, channel binding, retained artifact
   integrity and current credential/capability revocation are still enforced;
   replay does not require the claimed historical Git SHA to remain today's head.
4. Begin exact predecessor RETIRING, closing new attempts. Existing admitted
   attempts retain their epochs and may terminalize. Finalize only after every
   attempt is durably terminal and connector-specific quiescence holds. RUNNING,
   COMMIT_UNKNOWN (even with a receipt), and missing terminal receipt block.
   Return a bounded waiting result; do not sleep while holding a SQL/cache lock.
5. After durable RETIRED, observe successor physical closure and call atomic
   `prepare_successor`. Retry unknown commit by exact claim/request readback on a
   fresh connection. Once saved, the winner's request is immutable across caches.
6. Materialize the claim's immutable artifacts into the local cache and commit
   its pointer with the claimed UUID. Then `complete` obtains exact SQL ACTIVE and
   channel-current readback. This preserves pointer-before-ACTIVE for the process
   completing the claim. A pointer failure retains PREPARED; a replacement process
   can install the same pointer from durable claim/request and resume completion.
7. If SQL completion was durable but all cache bytes vanished, rebuild the local
   projection/pointer from shared current and its completed claim or adopted
   baseline without reserving
   again, advancing epochs or retiring current. Recheck shared revision/current
   after pointer commit; if another claim advanced it, never return stale current.
8. Read latest remote desired again. `converged=true` and ordinary success require
   shared current ACTIVE, verified local pointer, and latest observed desired
   occurrence/body matching that applied claim. This is last-observed convergence,
   not a linearizable claim about subsequent remote writes. If superseded, begin
   the next claim using the applied predecessor. At most two claims progress per
   cycle; then report CONTINUATION_REQUIRED and let the next existing watcher
   cycle continue. Remote unavailable means convergence UNKNOWN/non-success,
   even if the claimed occurrence was safely completed.

There is no cross-store transaction: remote may change between its recheck and
SQL claim, or after completion. Such a claim may temporarily apply a superseded
but previously authorized revision. This is the chosen availability/ordering
tradeoff. A security requirement forbidding that transient revision requires a
different shared publication fence and is outside this specification. No API
mislabels this temporary applied state as latest desired success.

```text
read shared channel
  pending -> recover exact durable claim
  idle    -> verify latest desired -> same current: replicate only
                                  -> new UUID: remote recheck -> channel CAS claim
claim -> old ACTIVE -> RETIRING -> terminal/quiescent RETIRED
      -> atomic request + PREPARED -> local pointer -> atomic ACTIVE + shared current
      -> recheck shared current + latest remote -> converged | continue | blocked
cache loss anywhere -> read shared channel/claim, never generate a replacement UUID
```

Concurrent replicas observing the same desired occurrence share one claim and
request. Only one wins each CAS; others read the same protected result. Replica
cache files are independent. Different desired UUIDs cannot run transitions
concurrently in one channel. Different channels retain shared physical guard
collision checks and can never retire each other's current occurrence. Identical
deployment hashes still require distinct exact UUID transitions. Missing input,
partial schema, foreign channel, changed immutable bytes, unsupported composition,
or unrecoverable authority ambiguity fail before further mutation.

## Registration, adoption, compatibility and rollback

Registration is an explicit privileged platform operation, not an automatic
inference from a cache. Its authority is the separate registrar's SQL EXECUTE
capability and explicit attestation; a digest or self-signed JSON is not proof of
operator authority. The complete attestation and adopted baseline are immutable
inside `dbt_workspace_channels.registration_json`, not a local file or a hash-only
external reference.

### Closed registration input, inventory and receipt

The exact `dpone.dbt-workspace-registration-input.v1` fields are `schema`,
`registration_id` (canonical UUID idempotency key), `channel`, `mode`
(`empty` or `adopt_active`), `reason` (1–1024 UTF-8 bytes), `operator_reference`
(1–256 bytes, a secret-free change reference), `inventory`, `empty_attestation`,
`adopted_current`, and `registration_input_sha256`. The digest covers every field
except itself. All fields are required; the inactive mode member is null. Unknown
keys, duplicate JSON keys, noncanonical UUIDs, coercions and extra enum values are
rejected. Input UTF-8 size is at most 31 MiB, including the nested exact request;
the full database receipt is limited to 32 MiB, leaving bounded actor/time overhead.

`inventory` has exactly `schema` (`dpone.dbt-workspace-registration-inventory.v1`),
`occurrences`, and `inventory_sha256`. The last is the canonical fingerprint of
the first two. `occurrences` is the complete, UUID-sorted list from this protected
store, with at most 8192 entries; overflow blocks registration, never truncates.
Each entry has exactly `activation_id`, `request_sha256`, `state`, and
`bound_channel_sha256` (null for unbound legacy). Include PREPARED, ACTIVE,
RETIRING and RETIRED records, across all environments/channels/resources, including
disjoint guards. Binding is derived from the protected claim or adopted baseline,
not supplied by a caller. These fields detect changes in inventory and lifecycle;
the adopter additionally rereads complete guards, writes and attempts under the
same transaction. Large historical stores exceeding this registration limit need
a separately reviewed inventory mechanism; history must not be deleted to fit.

`empty_attestation` has exactly `assertion` and `retired_predecessor_ids`.
Assertion is `channel_never_applied` (the list must be empty) or
`all_channel_predecessors_retired` (a nonempty, distinct UUID-sorted list).
The registrar explicitly attests that no other prior applied occurrence belongs
to this exact channel. For every declared predecessor, require protected RETIRED,
complete historical closure and no retained ownership or incomplete attempt.
It must not be bound to another channel. This attestation fills a missing legacy
mapping; SQL cannot discover that mapping from publication history or resources.

The exact `dpone.dbt-workspace-registration-receipt.v1` fields are `schema`,
`input` (the complete validated input above), `database_actor`, `registered_at_utc`,
and `registration_sha256`. Actor has exactly `original_login`, `database_user`,
and `database_principal_id`; the procedure derives them from the authenticated
connection (`ORIGINAL_LOGIN()`, `USER_NAME()`, `USER_ID()`), never a caller's actor
string. Time is database UTC with six fractional digits. Owner chaining does not
change execution context. The receipt digest covers all fields except itself;
the database computes/validates canonical UTF-8 fingerprints, using the same
golden vectors as the Python renderer. Stored `registration_input_sha256` and
`registration_sha256` must match their respective nested values.

### Representable adopted-current baseline

For `adopt_active`, `adopted_current` is the closed
`dpone.dbt-workspace-adopted-current.v1` object with exactly `schema`,
`activation_id`, `state` (only `ACTIVE`), `desired_state_json`,
`desired_state_sha256`, `observed_remote_revision`, `request_json`,
`request_sha256`, `guard_epochs`, `authorization_subject_sha256`, and
`baseline_sha256`. Its digest excludes only itself. Desired/request members
preserve exact original UTF-8 JSON bytes, with the same bounds and strict schemas
as ordinary claims/activation requests. Desired occurrence UUID must equal the
adopted UUID; desired and request must bind the exact release/deployment,
environment, source/runtime and channel. Previous deployment in the original
request remains unchanged; do not rewrite it from later publication lineage.
`guard_epochs` is the complete guard-ID-sorted array of closed objects containing
exactly `guard_id`, `resource_sha256`, `fencing_epoch`, and `write_subjects`
(sorted unique hashes). Verify the original request digest against the protected
activation, the complete resource/write partition, and every currently owned
epoch. The authorization digest identifies the verified immutable promotion
evidence retained with the desired release; the offline validator must resolve
and verify those artifacts exactly as for a claimed transition.

This is an immutable **baseline**, not a fictitious completed handover claim.
`read_channel` returns a discriminated current snapshot: `kind` is
`adopted_baseline` or `completed_claim`, with exact `activation_id`,
`snapshot_sha256`, and the full corresponding stored object. Registration starts
at revision zero, current is the adopted UUID and pending is null. A future claim
uses this exact baseline/request as predecessor; cold cache replication uses its
desired/request payload without new reservation or epochs. A UUID already bound
to any other registration or claim is rejected under the inventory barrier.
The baseline remains readable after its occurrence is retired.

Missing original request bytes or exact historical desired UUID/evidence makes
adoption unavailable. Fresh physical observation, inferred previous deployment,
or a request with a different digest cannot replace that evidence. This version
also refuses adoption of PREPARED/RETIRING, mixed/ambiguous mappings, and ACTIVE
with RUNNING/COMMIT_UNKNOWN/missing terminal receipts. Drain existing attempts and
retry. If original payloads cannot be recovered, remain blocked; a separately
approved historical retirement/migration followed by an attested empty channel
and a newly published desired UUID is the only prospective alternative, not an
automatic operation or a force-clear procedure defined here.

### Registration transaction and offline tooling

`register_empty(input)` and `adopt_existing(input)` are fixed registrar-only
procedures. They take the exclusive store inventory barrier before any row lock,
validate the input and recompute the complete inventory in SERIALIZABLE, with
range locks against phantom activations. An existing identical registration ID
and input digest returns its original immutable receipt, including actor/time;
changed input, another registration of that channel, or reused UUID conflicts.
For a new registration, inventory must match byte-for-byte canonical readback.
After all mode checks, insert channel and receipt in one transaction. Read exact
registration ID/input digest through a fresh connection after ambiguous commit;
never issue a new registration UUID just because acknowledgement was lost.

`register_empty` additionally rejects **every unbound non-RETIRED occurrence in
the store**, even if its write set is disjoint from the new deployment. Thus an
old applied occurrence cannot be silently orphaned by guessing an empty channel.
Adopt known legacy ACTIVE occurrences first; retire/resolve other legacy records
using their existing authorized protocol, not this registration API. Bound
foreign channels may remain active because their exact mapping already exists.
The explicit empty attestation is still required when the inventory is empty or
all unbound records are RETIRED. `adopt_existing` allows other unbound occurrences
to remain for subsequent explicit adoption, but requires the administrator's
exact mapping and exclusive ownership of this adopted UUID. Neither mode changes
old activation state, shared guards, epochs, attempts or receipts.

Required tooling is an offline-first operator flow, implemented only after
approval. `WorkspaceRegistrationProofRenderer` takes a trusted channel, a typed
read-only inventory snapshot, operator mode/reason/reference and exact archived
adoption artifacts, and returns deterministic closed input bytes. It never
inserts placeholder evidence, infers a mapping or generates an occurrence UUID.
`WorkspaceRegistrationProofValidator` is a pure closed-schema/digest/closure
validator with injected existing artifact-signature verification; its result is
a typed validated input, not registration authorization. `WorkspaceRegistrarPort`
accepts that input through the separate platform connection and returns the
database receipt. A read-only inventory port exports the exact sorted snapshot.
The renderer's registration UUID is pinned in the reviewed input, so replay after
tool or cache loss uses the stored receipt or identical reviewed input.

The migration runbook must provide this ordered flow: stop/drain old writers;
install reviewed gateway/schema and audit effective permissions; export inventory
and archived artifacts; explicitly choose empty attestation or exact ACTIVE
mapping; render/validate and review the input; invoke the registrar; verify the
durable receipt/readback from a fresh connection; start compatible replicas and
exercise cold-cache readback. Refused/missing proof stops before registration.
The complete receipt is the auditable operator artifact, stored in SQL first;
an exported file is a replica, not a prerequisite for recovery. No live
registration, migration or DDL is authorized by this document.

Install the additive schema, gateway and compatible workspace/shared-guard
adapters; cut over permissions, register/adopt channels, then switch all replicas
of a managed channel to the witness-aware runtime. Mixed direct-DML/new writers
are unsupported and are blocked by effective SQL permissions, not merely by
deployment policy. Old immutable deployment wires remain readable through normal
interfaces. Do not roll back to a runtime that cannot honor an existing pending
witness. Halt new claims and repair/complete through a compatible runtime; rolling
back code does not undo committed data. Retain claims and occurrence histories
for all retained deployments and attempts; no automatic garbage collection in
this feature.

## Architecture and alternatives

Pure contract modules own closed channel, claim and readback types. A workspace
handover application service owns progression and remote/latest status. A narrow
store port owns atomic witness/admission transitions; the MSSQL adapter reuses
existing activation/attempt/guard tables and historical readback. The native
desired-state adapter composes the trusted channel and routes pending recovery
before any pointer-only fast path. Runtime cache materialization remains an
artifact/pointer replica operation. Composition roots inject every dependency.

The service supports cold bootstrap and same-deployment reactivation; it also
supports disjoint write closures without discovering predecessor through guard
overlap. Those are real policy variations, not a new generic plugin framework.
Split channel identity, SQL witness transactions, historical admission and cache
replication by responsibility. Each new module obeys
`docs/benchmarks/quality_budgets.yml`; no growth of existing module-size/coupling
debt. Shared schemas, release metadata and factories remain integrator-owned.

Rejected: local-only journals (ephemeral loss); remote previous-publication as
applied state (skips); guard-owner discovery (disjoint targets/retired gaps);
whole publication-authority channel digest (splits replicas); an independent
guard/epoch namespace (double ownership); TTL reclaim and PREPARED cancel (lack
approved quiescence/transition authority). ADR 0072 records the selected witness
and complete-claimed-before-latest semantics.

## Research and measurable acceptance

Official sources checked 2026-09-23:

- S3 `If-Match` checks one object's ETag and rejects mismatches; concurrent writes
  can conflict. Adopt exact revision comparison, not cross-store atomicity.
  [AWS conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).
- SQL Server SERIALIZABLE protects qualifying row ranges until transaction end;
  transaction-owned application locks provide a named mutual-exclusion boundary.
  Adopt bounded channel locking, retaining per-resource guards.
  [Isolation](https://learn.microsoft.com/en-us/sql/t-sql/statements/set-transaction-isolation-level-transact-sql?view=sql-server-ver17),
  [application locks](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-getapplock-transact-sql?view=sql-server-ver17).
- SQL Server same-owner module chains permit access through a granted procedure
  without granting callers direct table DML. Adopt fixed same-database modules
  and object-specific EXECUTE; do not infer that ordinary adapter transactions
  already provide this security boundary.
  [Ownership-chain tutorial](https://learn.microsoft.com/en-us/sql/relational-databases/tutorial-ownership-chains-and-context-switching?view=sql-server-ver17).
- Cosmos documentation describes dbt execution on workers or isolated containers
  and task retry modes. Preserve that orchestration boundary; do not infer durable
  physical deployment ownership from task retry. This source does not establish
  equivalence with the proposed witness protocol.
  [Cosmos execution modes](https://astronomer.github.io/astronomer-cosmos/guides/run_dbt/execution-modes.html).
- dlt, Informatica, Airbyte, Fivetran, Pentaho, SSIS, gusty and Apache Beam: N/A for
  this narrow dpone protected activation/cache protocol; no ingestion, commercial
  orchestration or streaming capability is replaced or ranked.

Measured axis: compared with dpone base `abb9de012`, recover the exact desired
occurrence after total cache loss at every durable phase, with two independent
replicas. Target: zero duplicate successor reservations, zero foreign retirement,
zero released incomplete-attempt epochs and zero false convergence. Produce
`workspace-handover-certification.json` with exact commit, profile, phase, cache
replacement, claim/request digests, final epochs and PASS/FAIL/SKIP status.
Synthetic evidence proves contracts only; it is not live SQL certification.

## Tests, operations, docs and rollout gate

| Layer | Required cases |
| --- | --- |
| Unit/contracts | Closed fields/digests/bounds; watcher-independent channel; different URI isolation; same UUID mutation rejection; complete historical subjects/epochs |
| Stateful integration | Cold cache at CLAIMED, RETIRING, RETIRED, PREPARED, pointer-written and ACTIVE; two roots same desired; idle warm/cold same-current no-op with unchanged revision/epochs; adopted-current cold replication; same deployment/new UUID; skipped revisions; disjoint and foreign channels |
| Failure/concurrency | Lost claim/retirement/prepare/completion acknowledgement; stale channel CAS; request-write race; remote supersession at each boundary; unavailable remote after completion; partial pointer and whole cache loss |
| Safety | RUNNING/COMMIT_UNKNOWN/missing receipt refusal; source/runtime drift rejection; live observation drift during old retirement; no PREPARED cancellation; no success from pointer/journal alone |
| Migration/compatibility | Full empty attestation; disjoint unbound ACTIVE blocks empty registration; missing/altered proof and stale inventory rejected; exact old ACTIVE baseline adoption and cold replay; duplicate adoption/registration UUID and lost registration acknowledgement; actor derived by SQL; old direct-DML binaries denied; unchanged legacy gateway behavior; v1/composed or explicit mode-migration rejection; old/new projection readers |
| Live certification | Disposable SQL Server transactions, lock contention, process kills/cache replacement and independent replicas under approved credentials; unavailable environments report UNVERIFIED |
| Performance/security | 8192-bound closure/inventory and 16 MiB request; canonical SQL/Python hash vectors; secret-free diagnostics; runtime direct DML/DDL/IMPERSONATE and registrar access denied; old request APIs cannot mutate managed occurrence; semantic guard procedures cannot forge/release workspace owners; inherited/alternate credential bypass probes; no lock held over remote/file I/O |

Run focused red-green tests, change-aware check selection, mandatory type/style,
import/layer/module budgets, offline regressions and strict documentation gates.
Maintain historical-readback backend tests. Add a first-success platform guide,
reference for channel/claim/status, migration/adoption runbook, supersession and
pod-replacement recovery guide, ADR and changelog before integration.

Roll out only when independent review, all offline gates and the scoped live
profile agree. No release, live DDL or production-readiness claim is authorized
until implementation and required proof are complete. Implementation begins with
closed contracts and a pure progression planner. That foundation slice does not
enable runtime handover or implement the SQL gateway; the full feature remains
incomplete until protected mutation, integration and live evidence are delivered.

## Agent plan and approval

The workspace handover writer owns this spec, ADR 0072, focused tests and the
already-authorized historical backend. The parent integrator owns shared schemas,
factories, release/changelog/navigation and approval. Another writer owns ADR 0071
and credential projection; consume its normal interfaces without editing them.
Independent architecture review is required before changing status to APPROVED.

- [x] Failure and empty-cache problem reproduced; local-only approach rejected.
- [x] Channel identity excludes watcher and includes the trusted desired object.
- [x] Protected witness, exact epochs and remote supersession semantics specified.
- [x] Public behavior, registration/adoption and validation scope recorded.
- [x] Independent review of complete-claimed semantics and migration proof complete.
- [x] Integrator approval of protected schema and implementation boundaries;
  transaction-owner/lock-order/external-I/O review conditions recorded above.
- [x] Status changed to APPROVED for implementation only.
