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
