# ADR 0052: A dbt workspace has one complete immutable release authority

## Status

Accepted design, 2026-08-28, under the maintainer's delegated implementation
authority recorded in the [approved specification](../feature-design-dbt-multi-project-release.md).
This is not an independent human review or environment certification.
Implementation and rollout remain **IN PROGRESS**: reader, mirror and discovery
coverage does not prove aggregate compilation or live multi-project delivery.

Amends the singleton scope of [ADR 0034](0034-native-dbt-self-service-multi-repo.md),
while preserving its existing producer defaults and runtime contracts.

## Context

Repositories can contain independent dbt projects with different delivery needs.
The singleton producer assigns every project the same runtime object paths;
concatenating its outputs would overwrite sources and bind workflows to the
wrong manifest. Selecting only Git-diff projects for a desired-state release
would also omit unchanged workloads.

SQL changes must not require rebuilding the runtime toolchain image. The code
executed, manifest used for selection, source reviewed in promotion, and bytes
retained for retry must identify the same release. An audit Git mirror is not
runtime source authority.

## Decision

Cache installation explicitly selects the validated producer wire and verifies
the complete captured workspace tree before immutable publication. Projection
validation carries that observed wire from its existing integrity read. Reading
v2 does not authorize activation: promote, recovery and audit restoration fail
closed until protected physical-target admission and runtime finalization are
implemented and certified. No optional callback or caller flag grants workspace
activation. Legacy v1 activation remains unchanged. See the
[reader-first boundary](../dbt-workspace-source-verification.md#cache-installation-is-not-activation).

Discover the complete workspace from standard `dbt_project.yml` files and each
project's existing local publishing policy. Directory/domain names do not select
capabilities. Policy presence means a publishing candidate; resolved dbt manifest
metadata selects actual publishing models. Non-publishing projects retain their
dbt-quality/Cosmos consumer lane without becoming dpone delivery workloads.
Discovery/check are bounded and offline, with no implicit dbt execution,
dependency installation, credential lookup or output publication.

Retain generic `dpone.release-set.v2`. Introduce explicit dbt producer wire
`dpone.dbt-airflow-self-service.v2` and complete
`dpone.dbt-source-snapshot.v2`; do not reinterpret the v1 wire. Project archives,
manifests and selection locks use typed content-addressed IDs and paths. Every
workflow owns one ordered project/manifest/selection trio. IDs, byte lengths,
hashes, media types, execution-pack semantics and source membership must agree.
Global project/workflow/DAG/workload collisions fail without automatic renaming;
dbt node IDs remain project-scoped.

Workspace execution-pack v2 separates the rendered invocation/base target from
the effective model target. Existing `profile.database/schema` retains its
meaning; mandatory `invocation_target` is fingerprint-bound and used only to
render the runtime invocation profile. Preflight still requires the exact
effective relations and graph. This preserves supported dbt custom schemas
without silently reinterpreting shipped v1 packs. See the versioned contract in
the approved specification. Local producer/reader/runtime support and real
offline parse/preflight regression checks are implemented; release, physical
target certification and live rollout remain pending.

The pure `contracts.dbt_invocation` module owns the rendered base target together
with the invocation context. Profile acquisition and selection can use that
identity before execution-pack assembly without loading the pack or SDKs.
`contracts.dbt_execution_pack.DbtInvocationTarget` remains a reexport of the same
class; validation codes, mapping, bounds and pack fingerprints are unchanged.

The workspace planner compiles each publishing project once into typed inputs.
One writer assembles and validates the complete release before atomic publication.
Bounded file acquisition is an injected capability shared by workspace writers,
source readers and mirror verification; composition roots select the concrete
no-follow reader. New services do not construct their filesystem dependency.
Publication conflicts and uncertain durable outcomes belong to shared contracts,
with legacy exception imports retained as reexports. Both payload versions use
one projection implementation; human-readable artifact encoding remains distinct
from compact fingerprint encoding.
Each project projection captures route receipts once as immutable bytes, bound
to the full checked inputs (including fields omitted by the public report).
Singleton and workspace assemblers consume detached copies of that snapshot;
post-check input mutation and conflicting receipts for the same route variant
fail before publication. This local binding neither changes the release wire nor
replaces cryptographic certification verification.
Captured source/projection types and the full report-identity encoder live in
the pure project-artifact contract; existing service imports remain reexports.
That contract also owns the per-workflow release plan: capture runtime options,
derive selection arguments, then construct the selection lock and execution pack.
The service retains source acquisition and the actual selection callback, in the
original order. Timeout validation precedes selection; lock validation precedes
the v2 invocation-target requirement; threads and quality follow selection.
Cleanup still precedes graph ownership and mixed-authority checks. Original model
order, singleton defaults and v1/v2 artifact bytes remain unchanged.
Selection adapters share `contracts.dbt_selection.DbtSelectionPlan` for exact
FQN selectors, stable manifest comparison and graph/result completion. The CLI
adapter still owns installed-toolchain checks, actual parse/ls, bounded reads
and temporary cleanup. Preview remains non-authoritative. Preview ancestor
closure and semantic-refresh exact-root closure remain distinct; compatibility
exports preserve their existing adapter imports and errors.
Pure workspace assembly consumes an exact framework-fingerprint map and schema
bytes supplied by its service, verifies complete metadata/file membership and
checks every descriptor body through one source-plan index. The service retains
actual pack fingerprint and JSON-schema verification. Receipt identity is checked
after external verifiers return, before release assembly; no metadata constructor
substitutes for framework verification. Singleton selection multiplicity and
artifact/provenance ordering remain backward-compatible.
Singleton descriptor construction and final materialized-byte checks likewise
live in `contracts.dbt_release`. DAG descriptors are captured before external
verification; each pack fingerprint stays paired with the original checked
bytes. Final descriptor checks reject replacement of the caller's DAG/pack map
entries during verification rather than hashing new bytes under old trust.
Source verification and runtime preflight share a pure selected-graph observation
contract for logical target, exact graph policy, graph digest and expected results.
Runtime still performs actual parse/ls and compares the observed selection before
any SQL build; a source-manifest observation is not execution evidence.
Pure workload/archive and DAG interpretation lives in
`contracts.dbt_release_workload_binding`. The service wrapper retains confined
file reads and the external Airflow-pack fingerprint verifier. Existing service
error/result imports remain reexports of the same types. Archive interpretation
uses one exact bounded regular member without extracting it to a filesystem.
The complete-source reader builds one detached immutable artifact index for
membership, canonical paths and resource limits, including nested workload-trio
references. Each runtime descriptor is validated once, not once per workflow
or again during tree traversal. The pure source plan joins project names,
execution/selection locks, DAG ownership and write inventories; the service owns
file reads, YAML decoding, bundle extraction and external pack verification.
Selection-byte decoding and V2 execution/trio interpretation belong to that same
source plan. Readers bound/hash-check bytes and verify framework fingerprints
before invoking it; the plan never silently downgrades a workspace pack to V1.
Only compact semantic observations survive between projects, not all source
archives or transfer manifests. Canonical file/byte verification remains a
separate mandatory pass, and every subsequent byte read is checked again.
Metadata indexes are not signature or artifact-byte certification.
The source inventory also binds the exact project-to-archive byte map and
structural directories for audit-mirror staging. It checks hashes and shared
resource ceilings, charging identical archive content only once. Filesystem
traversal, extraction and archive-content verification remain in the mirror
service; a bound byte map alone does not verify an archive or grant authority.
It does not publish singleton releases first or patch generated fingerprints.
An identical output is an idempotent no-op; conflicting existing content is an
error. A failed project cannot produce a successful partial release. Explicit
compilation of an empty publishing inventory fails rather than retiring all
workloads accidentally.

Whole-workspace source closure is verified before signing, evidence finalization
and promotion. A task fetches only its own trio from the already authenticated
release and checks execution-pack ownership; it does not claim to inspect all
other projects. DEV evidence must cover every required workflow. Source checks
do not replace [deployment-scoped attestation](0035-airflow-production-artifact-attestation.md)
or exact provider/runtime compatibility.
The DEV-evidence coordinator acquires files and checks complete coverage; the
existing workflow verifier owns each dbt evidence row's exact identity, attempt,
node inventory and canonical byte observation. Its coordinator import remains
available. Runtime's legacy plan-order repair is pure contract policy and cannot
replace strict execution-pack/source validation or repair v2 references.
`contracts.dbt_release_expectations` owns the legacy metadata/selection expectation
plan and shared expected-workflow values. The loader retains bounded acquisition,
SDK verification and mandatory complete-source verification for workspace v2.
Legacy diagnostic precedence remains metadata, DAG bytes, source descriptors,
selection, execution binding, then DAG membership. V1 is not silently upgraded
to require project/manifest byte reads. Existing result/error imports are retained.
The same expectation contract checks release/deployment/DAG/attempt binding for
already validated Airflow evidence. The coordinator retains sequential envelope
validation, complete-coverage checks and exact full-envelope ASCII serialization.
This separation does not turn a decoded envelope into signature or live proof.

Logical collisions are checked both during assembly and whole-source verification:
all selected model targets, the pinned SQL Server intermediate/backup slots, and
embedded transfer destinations participate. Rehashing a conflicting candidate
does not turn it into an admissible release. The verified reader result retains
this immutable logical write inventory for subsequent environment binding; it
does not claim physical disjointness. Physical-target preflight is
environment-bound: injected resolution proves actual server/database/schema/
relation identity under certified comparison semantics, including transfer
destinations. Different connection aliases are not proof of distinct databases.
Missing authority or unsupported comparison semantics blocks activation before
data writes. No new identity fields are imposed on analysts.

Workspace promotion uses `dpone.dbt-prod-promotion.v3`; existing v2 already means
campaign-bound singleton promotion. One bot-owned mirror subtree, snapshot and
descriptor are staged and installed using the shared lock and recoverable
three-destination journal. Bootstrap requires all destinations absent; updates
require valid prior ownership metadata. Author-owned trees are never adopted
implicitly. Verification holds the same lock and rejects pending recovery without
mutating the repository. This is serialized, rollback-safe replacement, not
simultaneous visibility to arbitrary unlocked filesystem readers.
The mirror service owns its replacement-admission guard in the same module;
the shared transaction invokes that guard under its lock, before no-op detection
or staging. The independently reusable candidate reader and read-only verifier
remain separate components.
The shared promotion contract owns singleton pinned-source validation and the
v1 verification report; its service still rebuilds the actual bundle and compares
both digest and byte count. Metadata failure never starts a rebuild, and malformed
or unavailable observations remain failed reports with nullable diagnostics.
Legacy acceptance of unrelated descriptors does not apply to the strict workspace
inventory. These metadata rules neither verify signatures nor authorize deployment.

## Compatibility and consequences

- Existing singleton commands and v1 wire remain unchanged. New workspace
  producers are explicit; readers must be deployed before emitting v2.
- Publish one immutable release and bind it separately to each environment.
  Runtime image rebuilds are for toolchain/runtime changes, not SQL model changes.
- Git diff chooses quality work, not the complete desired release inventory.
- Runtime payload count/size and archive safety bounds are retained across the
  whole workspace, including deduplication; limits do not multiply per project.
- Preserve original release/deployment/attempt bindings for retries. Deployment
  rollback restores code and bindings, not already committed database data.
  Project A's committed work is not undone because B fails; incomplete campaign
  evidence blocks promotion and existing commit-unknown policy governs recovery.
- Self-consistent source metadata and a successful local mirror are not signature
  verification, independent approval, route certification or PROD readiness.
- Multi-project publishing does not introduce dbt Mesh, cross-project `ref`, new
  certified connector routes or non-root migration.

## Validation and rollout

Require v1 compatibility, complete two-project compilation, stable reversed
discovery order, project-local node-ID reuse, global/physical collision failures,
no partial publication, bounded payloads, and failure injection at every mirror
commit boundary. Prove complete DEV evidence, unchanged-release PROD promotion,
old-attempt retry and retained-deployment rollback in the exact runtime before
claiming live readiness. CI latency requires its separate comparable-run cohort;
local unit tests are not an SLO measurement.

See [workspace authoring](../dbt-workspace-authoring.md),
[source verification](../dbt-workspace-source-verification.md),
[mirror promotion](../dbt-workspace-promotion.md), and the
[dbt integration hub](../dbt.md) for current implementation limits.
