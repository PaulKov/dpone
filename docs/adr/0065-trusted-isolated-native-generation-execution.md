# ADR 0065: Trusted isolated native generation execution

Status: Accepted

Date: 2026-09-15

For platform engineers and maintainers: this decision defines the execution and
recovery boundary of the native dbt self-service path. Accepted for implementation
against baseline `72d38091d0d074761910c5166f6d65b7cb9d15de`; acceptance does not
establish complete route implementation or qualification. The core primitives and
original-subject changes are implementation slices, not complete route evidence.

The maintainer approved specification SHA-256
`606f8c200b3b8cbb64d9e72c8a64261bb7d0b5f57b6f22bc0ea37e35bb858fea`.
This record promotes the approved decision without changing its supported profile.

## Context and decision

A unique generation is owned by one platform-prepared pinned dbt/runtime invocation.
The initial supported sqlserver path excludes writers outside that invocation,
external mutation of its dependency footprint, arbitrary asynchronous work and
bypasses. Accept qualified positive completion of that supported invocation, then
fresh restricted final-quality tests and bounded export. Do not require a universal
SQL login/session observer, LOGON/audit infrastructure, new adapter plugin or server
remote fence. Same-backend generation/attempt state and original authentication remain.

The qualification artifact must bind exact dbt-core, dbt-sqlserver, driver, Python,
dpone distributions/runtime image and project package/macros, command forms and
configured parallelism. Observed local versions1.12.3/1.11.1/1.24.5 are not selected
production pins or a successful qualification artifact. No arbitrary version range
is admitted. Provisioning establishes isolated write footprint and restricted roles.

Retain ADR0023 descriptor-confined authoring transactions, certified native atomic
exchange, durable journal and recovery semantics. A cooperative filesystem lock is
not a correctness fallback. New v4 native policy/intents bind immutable project,
package and dbt dependency-lock bytes; that dependency lock is not a replacement
filesystem concurrency lock. This decision allows no runtime package resolution
or mutation of the reviewed dependency lock.
Full canonical selected v4 policy bytes anchor PLATFORM originals without selfhash.

## Enumerated first supported operation profile

| Phase | Supported effect | Rejected effect before dispatch |
|---|---|---|
| Build | Locked selected SQL model and supported SQL-model dependency footprint; synchronous adapter-generated create/insert/alter within owned unique schema; approved managed table materialization | Seeds/snapshots (detected and rejected in this first profile), undeclared writes, extra executables, arbitrary hooks, job queues, delayed autonomous writes, linked servers, DTC, cross-generation reuse |
| Managed storage | NONE/ROW/PAGE or ordinary columnstore from explicit model_storage policy; catalog checked after normal build | Archive compression, caller ALTER text, arbitrary post-build mutation |
| Quality | Exact locked read-only data tests of frozen generation and protected dependencies; pass or policy warn | store_failures, mutating tests/hooks, undeclared object reads, reused artifacts, empty membership, skipped/success/no-op admission |
| Export | Exact authenticated generation query, bounded native or lossless text BCP, positive held child completion and file/layout validation | Query substitution, ambient credentials, unbounded diagnostics/spool, reopening by PID, live layout regeneration |
| Stage | Closed owned-helper operations under STAGE registration/claim; fully parsed acknowledgements, qualified replication and exact all-replica content | Fire-and-forget/async insert mode, retries after possible send, untracked distributed fanout, free caller SQL |
| Publish | Closed full-refresh EXCHANGE or cold single-table RENAME, or complete declared1..64 day/month partitions, exact target identity | Arbitrary SQL, implicit empty-discovery clear, inverse EXCHANGE on uncertainty, global multipartition atomicity claim |
| Cleanup | Only recorded owned objects after positive completion/reconciliation and release eligibility | TTL drop, blanket schema deletion, release of charged UNKNOWN capacity, conflicting later publication |

“Approved managed” denotes the managed materialization allowlist in the approved
implementation specification. Each supported command still requires qualification
of its successful-return and synchronous-completion semantics; no arbitrary SQL classifier can establish this from text alone. Project
and macro bytes are immutable allowlisted inputs. Full write-footprint accounting
does not authorize extra resource types: seeds/snapshots remain unsupported under
the pinned first SQLserver profile and must be rejected before dispatch. Unsupported effects fail before
credential use. If discovered after dispatch, record uncertainty and retain resources.

## Failure, concurrency and serving consequence

One acknowledged local admission permits one send/run only; serialize actual local
launch with close/cancel, not just a stale flag read. Driver/client/proxy retry layers
must not replay possible mutations. UNKNOWN retains source and target scope guards,
charged capacity and originals until positive resolution; admission backpressure
is preferable to unbounded accumulation or unsafe cleanup. Some UNKNOWN outcomes
may require indefinite manual investigation. Recovery never creates launch authority.

The selected serving behavior permits temporary old/new visibility during switching.
Success requires independently confirmed completion on all three enrolled replicas.
A uniform serving gate is outside this decision and must not be inferred from
terminal observations. Exact
whole-row multiset comparison preserves duplicates; complete partitions are not
one globally atomic transaction. Empty declared partitions clear only that scope.

## Migration and compatibility

All current published v1–v3 policies, public import facades and old read paths retain
existing behavior. The new v4 native path is opt-in and does not reinterpret old
published evidence. Speculative universal SourceWriterSettlement/source session
records were never a released accepted contract in this programme: preserve them
as historical design, reject their tags in new trusted-completion decoding.

Trusted invocation completion uses its explicit version1 tag and authenticated
qualified profile. Never convert an old settlement object into a successful trusted
record by renaming fields. Legacy source evidence stays with its original version,
subject and authority. New final execution v2 reader propagation covers DEV,
promotion/XCom and airflow-pack together; old successful build v1 cannot populate
the final native workflow/campaign slot. Unsupported mixed readers fail clearly.

Actual published compatibility matrix and rollout tests must validate these claims
at the approved implementation commit. SourceOriginalPublisher/BCP consumer methods
retain existing callback compatibility as explicitly mapped; diagnostics bypass
removal requires the previously planned migration note. No release threshold changes.

## Qualification and rollout acceptance

Before enabling the profile, run installed pinned packages through actual dbt builds,
separate quality, native/text exports, storage modes, full and empty partitions,
three-replica staging/publication and independent DEV values/count checks. Include
negative effects, failure/cancellation/ACK loss, local launch-close barriers, repeated
UNKNOWN capacity exhaustion, positive cleanup and restart with no replay. Mocked
checks cannot create the profile's qualification original. Ordinary isolated synthetic
validation in the available local environment is authorized. Complete route
qualification remains UNVERIFIED; individual checks must report their actual
results. Corporate endpoints/data and production or shared-infrastructure mutations
are outside this authorization.

The user journey begins with a prepared project and installed package, generates
files without source checkout, and shows exact run/DAG references. Source BUILD/
QUALITY failures state target unchanged when target dispatch was impossible;
delivery uncertainty states unknown with no safe retry. Errors identify owner,
phase, safe next step and nonsecret original reference. Human4/5 first-time success
in15minutes remains separate UNVERIFIED usability acceptance.

The specification is approved. Shipping still requires scoped worktree task
contracts, complete implementation, focused and broad gates, fresh independent
review, and scoped R1–R9 evidence from the exact candidate commit.
External release controller remains sole ordinary publisher. No version is reserved,
no existing release is republished, and neither this ADR nor a readiness GO changes
publication authority. Rollback disables new admissions while retaining uncertain
attempts; it never rewrites original evidence or blindly reverses target operations.

## Related contracts

- [Architecture decisions](../adr-index.md)
- [Confined authoring transactions](0023-confined-authoring-transaction-semantics.md)
- [Workspace release authority](0052-dbt-workspace-release-source-authority.md)
- [Validated character-file staging](0064-validated-file-clickhouse-staging.md)
- [Native contract primitives](../native-contract-primitives.md)
- [Original-subject reference](../native-original-subjects.md)

## Storage policy implementation boundary

Original subjects, bindings and native storage metadata retain the public
`contracts.native_originals` owner. Reusable S3 policy validation belongs in
`contracts.s3_artifact_store_policy`; the existing adapter reexports its policy
class with historical constructor and pickle compatibility. Native exact-field
and capability checks delegate to that policy before authenticated store
resolution. This dependency direction keeps provider adapters out of contracts
and avoids duplicating provider validation. See the
[storage policy reference](../native-original-storage.md) for the exact schema.

## Shared versioned artifact I/O

The shared `adapters.versioned_artifact_s3` implementation owns conditional PUT,
exact-version HEAD/GET, bucket-capability checks and response-body lifecycle.
`adapters.versioned_artifact_s3_proof` owns pure response/authority validation,
including complete native version-history entries before exact-key filtering.
This separates provider I/O from proof policy without duplicating the legacy
network algorithm. `ports.versioned_artifact_store` owns the structural policy,
typed client/store capabilities and `VersionedArtifactIoBudget`.

The budget has exact positive `max_bytes` and `chunk_bytes` integers, with chunk
size no greater than the byte cap, and a finite float `deadline_monotonic`.
The caller passes one absolute deadline through the whole logical operation;
it is not renewed for individual requests or proof steps. Native methods require
that budget and an actual GET VersionId. Complete bounded consumption, EOF,
protected metadata, content digest, unambiguous history and body closure are all
required before success. A malformed history entry is incomplete proof, even
when another entry appears to identify the expected version.

The existing `S3CreateOnlyArtifactStore` remains a compatibility adapter with its
historical constructor and method signatures. Shared internal operations preserve
its unbudgeted read and omitted-GET-VersionId fallback; neither establishes native
bounded-provider qualification. The legacy structural policy import preserves
class/pickle identity, and `retention_datetime` keeps its old import path. Native
composition must select the explicit bounded adapter, not the compatibility mode.

Cooperative checks cannot interrupt a blocking SDK call. Authenticated application
composition must additionally configure finite SDK connect/read/retry limits and
provide both clocks. No helper-level retry can replace an uncertain write version.
Native subject-to-key adaptation is implemented as described below; authenticated
SQL binding and complete provider composition/qualification remain pending. See the
[bounded artifact reference](../versioned-artifact-store.md) for API use and limits.


## Native original object adapter

`adapters.native_original_store.NativeOriginalStore` implements the writer/reader
ports over the bounded provider. Canonical payload bytes are stored directly under
`{artifact_prefix}/native-originals/v1/{subject_sha256_hex}/{kind}/{payload_sha256_hex}`.
The hash segments cover the complete canonical subject and exact payload bytes.
This concrete spelling is the persistent v1 representation; reads recompute it
and do not accept prefix-only membership or implicit relocation.

Construction checks authority-reference digest agreement and snapshots the
authenticated inputs supplied by composition. That check is not authentication.
Publication permits one create and read-only reconciliation on conflict/lost ACK,
then independently verifies all six provider coordinates and exact canonical bytes.
Read narrows the stored attempt allowance and keeps its absolute deadline. Failure
retains uncertain objects; it does not create a binding or authorize dispatch.

Reusable primitive object-coordinate projection and closed-kind validation remain
in `contracts.native_originals` and are shared with binding encoding. No fictitious
binding or replacement provider reference type is introduced. See the
[native original-store guide](../native-original-store.md) for inputs and recovery.

### Native publication application ownership

The unpublished implementation places the publish–bind–resolve–readback use case
in `dpone.services.native_original_publication`. It coordinates injected
capabilities and implements no execution engine or source/target dispatch.
`BoundNativeOriginalPublisher` belongs to `dpone.ports.native_originals`;
application composition binds the service and injects this exact callback into
runtime consumers. This supersedes the proposed runtime owner in the planning
consumer map while retaining the algorithm and public callable signature.
No released import requires a runtime-to-services compatibility facade.

### Protected native-original SQL index

An explicit administrator migration installs additive V1 objects in the existing
`dbo`-owned native control schema. It registers an already authenticated authority
reference with the actual runtime database principal ID and SID, without runtime
self-enrollment or replacement of retained authority. Caller-context static
procedures use an ownership chain and check this registration transactionally.
The runtime principal has execution access but cannot mutate the tables directly
or alter the schema. Incompatible existing schema/procedure definitions fail
installation rather than being silently replaced.

The index uses a unique locator hash for lookup and full binary identity plus
length comparison for proof. It stores complete canonical binding bytes. Bind
creates if absent under key-range locking; exact replay preserves the row and
conflicting tuples fail. Commit precedes independent fresh-connection resolution.
Ambiguous execute/commit results permit only read-only reconciliation; cleanup
does not establish transaction absence. Automated deployment-member selection
and platform command wiring remain unfinished application integration.

### Initial protected generation admission

Generation reservation reuses the complete existing workspace attempt and P
guard rather than acquiring another physical lease. Canonical request bytes bind
the generation, profile, command, complete attempt and capacity request. SQL
recomputes the attempt fingerprint and verifies every write subject because guard
membership alone cannot distinguish different subsets within one database.
Capacity registration is keyed by the physical guard and shared across trusted
profiles. Runtime cannot enroll an account, reset charges or release uncertain
ownership.

The first executable slice implements reserve, the approved writer-bind signature
and custody read. It installs `native_source_writer_bind_v1` and
`native_source_custody_read_v1` under the existing protected control schema.
Only a fresh acknowledged CAS can return normal writer-bind success. Duplicate
or uncertain binds independently inspect retained state and raise without granting
redispatch. The generation remains charged. Authenticated outcome persistence and
subsequent build/freeze/read/seal transitions remain separate unfinished work;
the partial provider does not claim to implement the complete source-ledger ports.
See [native generation admission](../native-generation-admission.md) for composition,
provisioning, failure semantics and the exact validation scope.

### Pinned activation for original verification

Native original references retain their three immutable release, deployment and
pack coordinates. They do not identify an activation occurrence: reactivating the
same deployment creates a new occurrence. The native verifier therefore receives
an existing `AirflowDeploymentIdentity` from the pinned launch and an
invocation-bound `require_active` callback instead of the general coordinator.
The local constructor amendment adds no persisted fields and preserves
`resolve(refs)`.

The application callback is invoked only after complete policy, project and
source preflight. It reads the durable activation row by the pinned UUID, checks
the exact release, deployment, environment, source inventory and runtime authority,
and obtains the recorded previous deployment before calling the existing
coordinator's `require_active`. Missing or inactive rows fail closed. The mutable
current pointer and a default previous deployment cannot supply these values.
The verifier compares the returned active request with its earlier validated
observations. This lookup is read-only and neither activates nor prepares a
workspace; execution still requires the separate protected admission sequence.

The callback is wired through the production composition and covered by focused
component checks. Native caller integration and live activation-route qualification
remain unfinished; constructor and component evidence alone do not qualify a runtime.

The application also supplies an explicit local `release_root`, constructed from
its configured cache root and pinned release identity. Activation projections
contain deployment files; immutable release archives and packs reside in a
separate release tree. The verifier must not infer a cache root from projection
ancestors or escape its confined reader. Both roots remain local locators; exact
bytes, content identities and source membership bind the observations.


## Native execution and settlement ownership

The optional execution lifecycle capability separates reporting a dbt outcome
from terminalizing its physical workspace attempt. Ordinary execution keeps its
existing immediate terminal transition. Native execution reuses the exact retained
request and RUNNING receipt and independently verifies current physical ownership;
it retains P until the outer native delivery settlement owner acts. Its local
outcome is ephemeral and cannot authorize completion or settlement. No synthetic
terminal receipt, weakened SQL ownership predicate or persisted-schema migration
is introduced. See [Native workspace attempt lifecycle](../native-workspace-attempt-lifecycle.md)
for dependency injection, failure handling and the remaining validation boundary.


## Execution evidence ownership

`DbtExecutionOutcomeWriter` owns the complete execution-evidence projection, final
timestamp, persistence and returned outcome. `DbtExecutionService` supplies the
execution facts and orchestrates commands and attempt ownership. The same injected
clock and evidence writer are retained; serialized fields, timestamp ordering,
write count and post-build exception chaining are unchanged. This replaces a
split between evidence construction in the service and persistence in a separate
helper. The existing persistence helper remains compatible. This ownership change
does not qualify a runtime or make the architecture gates pass by declaration.


## Native project document ownership

The native archive adapter owns both capture and bounded reading of generated
project members through the existing injected archive/file ports. The native
document contract module owns pure policy admission, selection checks, descriptor
derivation and generated intent bytes. The former runtime producer import is a
compatibility re-export of the same adapter class. Actual source immutability,
asset-path admission, archive verification and round-trip order remain unchanged;
a comparison against the prior implementation verifies complete archive-byte
equality. This refactor does not change runtime qualification or delivery policy.


## Invocation authentication ownership

`contracts.native_generation_invocation` owns the authenticated plan value and
pure command, qualification, root-identity and argument-template decisions.
`adapters.native_generation_invocation_auth` owns fresh original acquisition,
ordered decoding and filesystem observations through the existing injected
original ports. The runtime recorder retains sequencing, admission locks,
deadlines, latched failures and completion publication. Its historical
authentication module re-exports the same four public objects for compatibility.

Each boundary still checks generation and command before reading originals, then
reads command, toolchain and qualification. Qualification and invocation checks
precede the three root reads; all roots are acquired before role and distinctness
validation. Argument checks precede filesystem observations. No read cache,
credential resolution, new retry or dispatch authority is introduced. Path
observations remain point-in-time checks: the actual bootstrap must hold the
roots throughout execution. This ownership change neither issues runtime
qualification nor changes metadata-only freeze recovery.


## Positive build bridge result

`GenerationBuildReceipt` is the successful bridge return, containing exact
generation, reservation, guard epoch, primitive executor invocation identity and
authenticated build-evidence, inventory and trusted-termination references. The
bridge must verify the actual writer completion before returning it; constructing
the record proves no custody or dispatch authority. Failed and uncertain builds
use their explicit error/evidence channel and cannot return a partial positive
receipt. This applies the trusted positive-completion contract to the earlier
bridge design without recreating superseded per-session terminal records. No new
persisted original kind or codec is introduced for this in-process return value.


## Native profile directory lifetime

`NativeDbtProfileLease` preallocates a private profile directory before constructing
native command originals and the positive evidence writer. Bootstrap holds its
outer context through recorder closure and evidence authentication, and passes
the lease as the bound build's existing profile-store dependency. Materialization
writes one bounded mode-0600 credential file through the held directory descriptor;
the inner context removes that exact file while retaining the directory identity.
Outer cleanup removes only the owned empty directory and closes its descriptors.

Root/ancestor links and replaced identities fail closed. Cleanup checks the opened
file identity before unlinking and never recursively removes unknown content;
cleanup errors retain any original execution error as context. These checks assume
the approved isolated runtime excludes outside writers; they are not a universal
filesystem lease against privileged concurrent mutation. Ordinary
`TemporaryDbtProfileStore` behavior is unchanged. The new store grants neither
credentials, command admission nor runtime qualification; the bridge still
validates its exact recorder before rendering a profile.


## Native execution public facade

The approved `runtime.native_generation_execution` import exposes the actual
reserved build bridge and recorder through identity-preserving re-exports. The
complete recorder implementation lives in `runtime.native_generation_invocation_recorder`;
the bridge imports that canonical owner directly, avoiding a facade/bridge cycle.
No recorder method, timing, locking, dispatch or evidence behavior changes. Public
import order is checked in fresh processes. This exposes implemented constructors;
it does not create a final-quality placeholder or qualify the bootstrap.


## Managed physical catalog transport

The managed physical provider uses nine closed, individually acquired result
kinds with explicit version, object identity, ordinal and count fields. Empty
collections require one zero marker; a missing resultset cannot establish absence.
HEADER, TABLE and COUNT are singletons. Fixed ordered schemas and exact primitive
representations let both dbt and Python reject partial or unexpected results.

Creation and modification timestamps cross this boundary as canonical ASCII
`YYYY-MM-DDTHH:MM:SS.fffffff` strings, retaining SQL's seventh fractional digit.
UUID and bit values require explicit acquisition conversion to exact Python UUID
and bool. The decoder never coerces values, truncates excess rows, or returns a
partial result after rejection. Positive row and definition-byte budgets are
injected; acquisition must enforce its own bounds before materializing tuples.

Immutable transport records are distinct from authenticated physical observations.
Decoding does not prove metadata visibility, equal before/after headers, actual
COUNT_BIG execution, current P/G, model authority, transaction commit, or generation
completion. Direct DTO constructors remain unchecked; use the decoder for external
rows. Legacy runtime imports and ordinary dbt materializations are unchanged.

See the maintained [v1 wire reference](../dbt-mssql-physical-catalog-wire.md).
The SQL provider, enrolled model plans and principal bridge remain unfinished;
this decision records transport semantics without claiming route qualification.


## Physical plan identity graph and enrollment boundary

Physical plan preparation must not introduce a cycle in existing admission v1.
While holding the existing physical owner P, allocate generation and invocation
UUIDs, observe the predecessor read-only, and derive immutable model plans.
Publish the plan-set original before producing the command that cites it; then
publish the admission request containing that command, reserve G, bind the exact
executor, and enroll authenticated plan bytes with that separate executor value.
Revalidate predecessor/namespace facts after G and independently read enrollment
before dispatch. No model mutation occurs before both P and G are held.

The command's reserved physical vars carry preallocated generation/invocation
identifiers, plan-set reference and a provisioned registration locator. They
exclude their own command or reservation reference. SQL admission resolves the
complete enrolled binding. Plans exclude this build's future executor, receipt,
completion and enclosing reference; spec/plan digests each omit only their own
digest field. Metadata replay cannot restore launch entitlement.

Plan codecs validate closed data shapes and canonical identities. They do not
prove qualification, graph membership, predecessor receipt authenticity or
registered database equality. Those require explicit authenticated admission.
The finite original kind is registered only with its actual admission consumer.
Legacy parsers, command bytes and generation-admission procedures remain intact.

The planned physical-control registration preserves metadata and build principal
ID/SID separation. An observer may share an identity only through an explicitly
reviewed permission contract. A registration UUID alone grants no authority.
Protected cross-database access on the same instance requires caller-preserving
provisioned permissions; broad cross-database trust is not an alternative.
The local model receipt shares its model transaction. Session/transaction values
are local observations, not durable child-session registrations or restart rights.
SQL enrollment, signing, model publication and their live qualification remain
unfinished; this decision fixes the implementation boundary without certifying it.
