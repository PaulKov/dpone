# Feature design: multi-project dbt releases

- Status: APPROVED
- Owner: repository maintainer; Codex is the delegated implementation integrator
- Target release: next certified release; no version reserved by this document
- Last verified: 2026-08-28
- Implementation baseline: `5a2e6c06272805f5dcb3069d23a2be181bb8e17b`

This is a design, not a statement that multi-project publishing is available.
See the [dbt integration hub](dbt.md) for currently supported behavior.

## Executive summary

A repository can already contain several dbt projects, but current dpone
publishing assigns every project the same release-relative project and manifest
paths. Consumer CI therefore rejects more than one publishing project. Removing
that check would overwrite sources and would leave promotion/evidence readers
bound to the wrong project.

Publish all participating projects as one immutable release, with one deployment
binding and a complete source inventory. SQL, resolved packages, manifests and
selection locks are release artifacts, not runtime-image contents. Model-only
changes must not rebuild a toolchain image. Existing single-project wire behavior
remains supported; new workspace publishing uses an explicit new dbt wire.

## Personas and customer journey

| Persona | Goal and first-success path | Observable result |
| --- | --- | --- |
| Analyst | Add a standard dbt project and project-local publishing declarations; run workspace check | Project is discovered without editing central domain/CI maps |
| Data engineer | Compile two independent projects; inspect the plan before publishing | One release lists both projects and each workflow's exact source trio |
| Platform engineer | Promote the accepted release without rebuilding SQL | Same release digest, new environment-bound deployment digest |
| Operator | Retry an old attempt after a new release; roll back deployment | Original attempt uses its pinned bytes; rollback restores the retained deployment |

Discovery is offline. Authors first run the certified dbt toolchain's `deps` and
canonical manifest preparation where required, then check and compile. The CLI
explains the selected projects, excluded non-publishing projects, collisions and
missing artifacts. Missing dependencies are not installed implicitly. A dbt-only
project remains in the consumer's dbt-quality lane without becoming a dpone
delivery project. Project-local publishing configuration is the opt-in.

## Scope and constraints

In scope: discovery, typed aggregate compile inputs, project-local diagnostics,
content-addressed payloads, one source inventory/release, readers, complete DEV
evidence, promotion, transactional audit mirror, and consumer integration.

Non-goals: cross-project `ref`/dbt Mesh/dbt-loom, distributed database transactions,
changing existing DAG/workflow IDs or schedules, new connector certification,
root-to-non-root migration, or weakening exact producer/provider compatibility.
Independent projects may use different admitted profiles, but this feature does
not certify a new toolchain/route. Current publishing remains constrained by its
exact certified SQL Server policies. dbt-only ClickHouse projects are not silently
converted into dpone publishing projects.

The release is a complete desired inventory, not only the Git diff. Diff selects
quality work; it must not delete unchanged projects from a release. If a project
is intentionally removed, the complete inventory makes removal explicit in the
plan and existing deployment reconciliation handles it. No implicit tombstones
are inferred from an absent/failed manifest.

## Public contract

### CLI and Python API

Add `dpone dbt workspace discover|check|compile` without changing the existing
single-project commands. Each accepts `--root` (default current directory) and
`--format text|json`. Discovery returns sorted repository-relative project paths,
project names and publishing capability/reason. It never runs dbt or reads secrets.
Check validates every publishing project and workspace-wide identities. Compile
accepts `--output-dir` and emits one complete release tree, never a partial set.

The workspace CLI uses success `0` and validation failure `2`; the existing
singleton check's blocker exit `1` remains unchanged. Machine-readable output goes to stdout and diagnostics
to stderr. Existing publishing error objects carry code/path/remediation. New
workspace error codes must be added to the canonical catalog and reference.

The discovery report is `dpone.dbt-workspace-discovery.v1`, with exactly
`schema`, `passed`, `projects`, and `blockers`. Each project has `project_path`,
`project_name` (null when invalid), `publishing`, `profiles_path` (project-relative
or null), `manifest_path` (project-relative or null), and `reason`:
`policy_present`, `not_configured`, or `invalid`. A regular project-local file
at exactly one of `DEFAULT_PROFILE_PATHS` opts the project into publishing;
model-level intents are subsequently validated by the canonical compiler, not
guessed by scanning SQL. Absence of that file is explicitly `not_configured`,
not a claim that models contain no publishing intent. Discovery does not read
policy contents, credential profiles or manifests. Invalid project metadata or
ambiguous policy files fail discovery rather than silently excluding a project.
Standard `packages-install-path` and `target-path` values must be literal,
confined relative paths; their directories are excluded from discovery.
Non-ignored symlinks fail closed, including in-root aliases; this prevents both
escape and duplicate project ownership. Directory traversal is descriptor-based.

The check report is `dpone.dbt-workspace-check.v1`, with exactly `schema`,
`passed`, `discovery`, `projects`, and `blockers`. Project rows have
`project_path` and the existing `dpone.dbt-publish-compile.v2` `report`.
Check preserves all publishing-project results even when one fails; global
workflow, DAG and workload collisions name both owners without renaming them.
No manifest preparation, dependency installation, output publication or database
execution is implicit. Neither a successful discovery nor check report certifies
physical-target disjointness or authorizes deployment.
Workspace compiler acquisition uses the existing canonical decoders through
confined reads; YAML inputs retain a 1 MiB bound and graph manifests retain the
existing 16 MiB bound. Both regular and semantic graph checks consume the
successfully acquired manifest snapshot and reject a changed file before graph
evaluation. Ordinary malformed-input types become project validation failures,
not internal failures that discard other project results.

Python callers use `DbtWorkspaceService.discover(root: Path)`,
`check(root: Path)`, and `compile(root: Path, *, output_dir: Path)`,
returning immutable discovery/check/compile report models. Construct the service
with injected project discovery, compiler, bundle/selection operations and
tree publisher. CLI and Python use the same planner and writer. No CLI scraping,
private `_build_tree` calls, or runtime SDK imports in discovery/help paths.

The compile report is `dpone.dbt-workspace-compile.v1`, with exactly `schema`,
`passed`, `check`, `output_dir`, `release_id`, `source_snapshot_sha256`,
`subject_sha256` and `blockers`. The complete check report is retained, including
all completed project diagnostics. The output directory is an absolute local
path in the report only; it never enters release identity. Success requires a
nonempty passed check, no blockers and three canonical non-null digests.
`subject_sha256` hashes the exact published `release-subjects.sha256` bytes.
All three digests are null on failure. The writer is acquired only for compile
after a successful nonempty check; discover/check/help do not construct execution
dependencies. Compile may run authoritative offline dbt parse/selection, but
never installs dependencies, executes model SQL, signs or activates a deployment.

`--output-dir` is required for compile. Empty publishing inventory, typed
validation failures and output conflicts return exit 2. A durable-publication
failure returns exit 5, retaining the check report and null identities:
output may already be visible, so retry identical inputs or verify it; never
infer permission to delete it. Unexpected writer failures are redacted into
the same compile-report shape with exit 5. The existing generic internal-error
envelope is used only when a complete check report could not be obtained.

Compile accepts optional `--dbt-profiles-dir`: one standard dbt parse-profile
directory for all projects, resolved once against the invocation working
directory. Without it, each project's own `profiles.yml` is used explicitly.
This is not a publishing-policy override. Existing policy
`runtime.dbt_profile` / `dbt_target` names select entries; no new domain map.
Missing, unsafe, oversized (1 MiB), malformed or missing named profile/target
inputs fail with `DPONE_DBT_WORKSPACE_PARSE_PROFILE_INVALID` and the owning
project path. Read each selected file once with confinement; keep the exact
bytes in the existing private temporary profile store (directory 0700/file 0600)
through parse and selection. A shared file is captured once for the workspace.
Cleanup runs on success and exceptions. Profiles never enter release artifacts
or reports. Use non-secret parse-only credentials: `isolated_v1` remains
unchanged, without ambient profile-directory or secret-variable fallback.
Canonical manifest comparison proves graph/logical-target compatibility,
not physical-server or credential identity; deployment binding remains separate.

Default discovery ignores dependency, generated, VCS and environment directories;
configured package-install paths are also excluded. Use the existing
`DEFAULT_PROFILE_PATHS` project-local defaults. An inherited
`DPONE_DBT_PUBLISH_PROFILES` or a workspace-wide profile override is an error:
do not silently apply one project's publishing policy to every project.
Symlinks escaping the root,
nested publishing roots and duplicate project names/paths fail with actionable
diagnostics. Discovery is bounded to 100,000 visited entries, 64 project roots
and depth 32, checked while traversing rather than after collecting everything.
Skip `.git`, `.worktrees`, `.venv`, `venv`, `node_modules`, `target`,
`logs`, `dbt_packages`, `.dpone-cache` and `.ci` directories by default.
Release payload limits can reduce the number of publishable projects further;
64 discovered projects is not a promise of 64-project runtime capacity.
A project located directly at the workspace root is represented by `project_path:
"."` and is supported only as the sole project. It must not be silently excluded
by discovery. Root-plus-descendant publishing projects are an overlap error.
Paths are NFC-normalized POSIX relative paths (at most 1024 UTF-8 bytes and 32
components); reserved VCS/worktree names and overlap checks are case-insensitive.

### Wire, payloads and identity

Keep generic `dpone.release-set.v2` and its descriptor structure. Add the dbt
producer wire `dpone.dbt-airflow-self-service.v2`; preserve the v1 value and
semantics, including existing single-project output and identifier ordering.
Do not simply redefine the old constant to mean v2. Unknown versions fail closed.

V2 runtime descriptors use canonical lowercase content hashes:

| Kind | ID | Release-relative path |
| --- | --- | --- |
| Project | `dbt_project_sha256_<hex>` | `runtime/dbt/objects/<hex>/project.tar.gz` |
| Manifest | `dbt_manifest_sha256_<hex>` | `runtime/dbt/objects/<hex>/manifest.json` |
| Selection | `dbt_selection_sha256_<hex>` | `runtime/dbt/objects/<hex>/selection-lock.json` |

`<hex>` is exactly 64 lowercase hexadecimal digits for the bytes of that object.
Selection IDs hash the exact serialized lock bytes, not the semantic
`selection_sha256` fingerprint. ID, kind, path, media type, digest and length
must agree. Identical same-kind
objects may be deduplicated; equal IDs with different descriptors/bytes fail.
Every workflow references exactly one ordered project/manifest/selection trio.
Mixing v1/v2 IDs in a trio, reordering it, foreign selections, missing/extra
objects and a correct hash under the wrong kind are rejected before execution.

Keep task-local `dbt-project` and target paths unchanged.
Runtime validates payload IDs against the actual project/manifest/selection bytes,
then checks the execution pack's existing project digest, manifest digest,
selection lock and workflow ownership. Content-addressing is not a substitute
for these semantic checks.

### Invocation target versus materialized relations

Use explicit `dpone.dbt-execution-pack.v2` for workspace wire v2.
Keep shipped execution-pack v1 bytes and singleton defaults unchanged.
In both versions, `profile.database/schema` remains the effective logical target
of the admitted model relations. V2 adds mandatory `invocation_target` with
exactly `database` and `schema`: the base target used by dbt to parse the
captured source. Both fields are nonempty validated tokens of at most 256
characters, matching the published schema, and participate in
the execution-pack fingerprint. V1 rejects this field; v2 rejects its absence,
unknown fields and downgrade/mixed wire-pack versions.

The builder derives the base target from the exact installed dbt toolchain's
rendered profile, using the same captured profile bytes, profile/target names,
canonical vars and isolated environment as parse/selection. Do not infer it by
stripping suffixes or reading unrendered YAML strings. A bounded isolated
build-plane adapter returns only non-secret database/schema identity; it must
not emit credential objects, raw profile contents or unsanitized SDK exceptions.
Discovery and check remain SDK-free; only explicit compilation acquires this
capability. No extra field is required from analysts.

Runtime obtains the invocation profile through one canonical pack method:
v1 returns the existing profile, while v2 returns an immutable copy with the
base target. The existing profile renderer still verifies resolved connection
identity exactly. Before SQL mutation, runtime parse/selection must reproduce
the effective target and graph locked by the original pack. Evidence continues
to identify the effective target; its required workload-pack digest binds the
base target too. Neither successful offline parsing nor a matching credential
alias proves a physical server identity.

This distinction follows the documented dbt behavior: by default, model
`+schema` is appended to the profile's target schema. For example, base
`alpha` and custom `alpha` produce `alpha_alpha`; feeding that result back as
the base would produce the wrong relation on the next parse.
[dbt custom schemas](https://docs.getdbt.com/docs/build/custom-schemas) and
[standard profiles](https://docs.getdbt.com/docs/local/profiles.yml), checked
2026-08-28. Preserve supported custom schemas; do not remove them from user
projects or relax preflight to make tests pass.

Required regression evidence: actual runtime profile renderer followed by actual
certified dbt parse/ls preflight with dummy credentials and no SQL/build;
base-only and effective-only tampering; missing/extra target fields;
unknown/mixed/downgraded versions; identical v1 bytes. Keep semantic-refresh
template readers on their existing v1 path. Reader support must be rolled out
before v2 emission or consumer activation.

### Source inventory and ownership

Retain the release sidecar path `_dbt/dbt-source-snapshot.json`. Add
`dpone.dbt-source-snapshot.v2` containing `schema`, `projects`, and
`snapshot_sha256`. `projects` is nonempty and sorted by `project_path`;
workflow entries are sorted by `workflow_id`. Each project entry has
`project_path`, `project_name`, `project_bundle_sha256`, `manifest_sha256`,
`toolchain_sha256`, and sorted `workflows`. Each workflow entry has `workflow_id`,
`dag_id`, `workload_id`, ordered `runtime_payload_ids`, and
`selection_lock_sha256` (exact byte digest, equal to the selection descriptor's
`sha256`, not the lock's semantic `selection_sha256`). Exact key sets are
validated; duplicate JSON keys,
nonfinite values, duplicate entries, unsafe paths and inconsistent membership
are rejected. The snapshot digest is the canonical fingerprint excluding itself.

Publication, promotion and DEV-evidence readers load the confined sidecar,
recompute its canonical fingerprint and
compare it with the release's `provenance.source_snapshot_sha256`. Every
referenced project/manifest/selection descriptor and byte digest must match.
`project_name` must equal both bundled `dbt_project.yml` name and manifest
metadata name. Each workflow's DAG/workload/trio/toolchain must match the
release DAG and workload descriptors and its execution pack, not just a name
in the source inventory.
Missing sidecar or wire/snapshot version mismatch fails; readers may not
reconstruct a missing snapshot by trusting current checkout contents.
Per-task runtime does not fetch this whole-workspace sidecar or other projects'
SQL. Its authority is the signature-verified release and deployment, validated
release membership, and the selected trio bound to the execution pack. Full
workspace source closure is enforced before release signing/publication and
again by promotion and campaign verification; runtime verifies the selected
workload and bytes without pretending it inspected the other projects.
The release's existing `provenance.source_snapshot_sha256` binds this complete
inventory. Its selection fingerprint continues to bind all selections and route
certifications. The source snapshot's membership must equal the release's dbt
workload membership, not merely be a subset. Volatile local paths/timestamps do
not enter identity. Database credentials never enter the source inventory.

Workflow, DAG and workload IDs remain globally unique without auto-renaming.
dbt node `unique_id` is project-scoped: repeated node IDs across projects are not
by themselves collisions. Logical target conflicts are checked at compilation;
environment-bound physical target conflicts are checked after connection/binding
resolution. Do not infer distinct databases solely from different alias strings.
The collision set includes all selected materialized dbt relations and every
transfer destination. A deployment preflight service receives an injected target
identity resolver, producing `(connector_kind, server_authority,
database, schema, relation)` under that connector's certified identifier
comparison policy. Server authority identifies the resolved physical service,
not its connection alias or credential version. SQL Server comparison requires
the admitted server/database collation policy; generic string lowercasing is
not a substitute. Missing physical authority, unsupported comparison semantics
or inability to prove disjointness blocks activation before any data writes.
Do not add a new config field every analyst must populate: platform binding
resolution owns this evidence and retains only non-secret identity digests.

At compilation, reject duplicate literal `(connector family, connection_ref,
database-or-default, schema, relation)` coordinates across all selected model
writes and transfer destinations, including within a single workflow. Compare
exact identifier strings; do not simulate SQL collation with case folding.
Different aliases, a default versus named database, or differently spelled
identifiers remain unresolved for physical preflight, not certified disjoint.
Names come from the locked manifest and generated transfer manifest, never from
domain directory names. A conflict reports both project/workflow/resource owners
as `DPONE_DBT_WORKSPACE_TARGET_COLLISION`; incomplete coordinates report
`DPONE_DBT_WORKSPACE_TARGET_INVALID`. Both return compile validation exit 2 with
all check rows retained, no published identities and no output replacement.
Discovery/check remain offline and do not claim authoritative selected-write
closure. No new wire fields, central registry or analyst configuration is added.

The declared write footprint includes fixed destructive adapter slots, not only
final targets. For the pinned SQL Server macro authority, let `A` be the resolved
model alias and `U` the unit-test name plus `_v{tested_model.version}` when its
version is not null (including version 0):

| Selected resource | Reserved relation names in its resolved database/schema |
| --- | --- |
| table | `A`, `A__dbt_tmp`, `A__dbt_backup`, `A__dbt_tmp__dbt_tmp_vw` |
| view | `A`, `A__dbt_tmp`, `A__dbt_backup` |
| incremental | `A`, `A__dbt_tmp`, `A__dbt_backup`, `A__dbt_tmp_vw`, `A__dbt_tmp__dbt_tmp_vw` |
| unit test | `U__dbt_tmp`, `U__dbt_tmp__dbt_tmp_vw`, in the attached model's database/schema |
| data test | No relation writes under the admitted no-stored-failures policy |

The temp and intermediate incremental slot is the same coordinate: count it
once. Do not add speculative CTAS slots to views. Reject another selected
resource or transfer occupying any slot, including within one workflow. This
projection is valid only under the existing exact graph/macro authority; it must
not infer another adapter's lifecycle. Explicitly include the Python-dispatched
`drop_relation` root and its transitive `get_drop_sql` closure, plus the reachable
`rename_relation`, `get_columns_in_relation`, and `list_relations_without_caching`
roots, in the generated macro authority: the materialization's manifest
dependencies alone omit these execution paths. This tightens the existing exact-body contract, changes policy
and graph digests on both release wires, and requires recompilation; it does not
establish live dependency completeness. These slots participate in environment
binding too. The pinned adapter can recursively delete dependent views when
dropping a view; that dynamic dependency set requires physical preflight and is
not certified by the fixed-name inventory. Missing dependency authority blocks
activation. Offline checking does not certify unrelated existing database objects.

Whole-source verification re-derives this inventory from locked manifests and
embedded transfer YAML. A transfer archive has exactly its canonical regular
manifest member, at most 1 MiB, and at most 8 MiB compressed; execution-pack
archives retain their 2 MiB compressed/1 MiB member bounds. Enforce canonical
base64 and cap the complete expanded TAR stream at the member limit plus two
10 KiB TAR records before parsing extension metadata. Reject extra members,
links, PAX metadata and malformed compression as validation errors, not internal
failures. These checks do not alter artifact wire fields or accept foreign
runtime commands as proof of execution.

### Evidence, promotion and compatibility

#### Environment-bound SQL Server catalog observation

Approved implementation detail (2026-08-29): add a read-only runtime observer
before implementing the protected admission decision. Its input is the complete
source-derived write subset for one resolved connection/database, the exact
release and loader-verified runtime-context subject digests, an existing registry-authored
`MssqlDatabaseAuthorityPin`, the resolved default database, the complete bounded
set of invocation databases, and bounded limits.
It must not discover projects, resolve secrets a second time, or invent a new
analyst-maintained registry. The caller owns signature/context validation;
passing a typed request is not evidence of that validation.

The observer returns immutable internal observations, not a new signed wire,
admission token, `certified` flag, or activation permission:

- exact input write rows and input subject digests;
- observed database pin, server/topology fingerprint, effective/original login
  SID fingerprints and database-principal SID fingerprint;
- observed engine version and server/database/catalog collations;
- every input slot's SQL-defined equivalence class and existing object identity,
  including absent schema/object slots;
- bounded incoming potential view-drop dependencies with raw referenced-name
  coordinates and resolved local view identities. They are potential effects,
  not a claim that every runtime branch actually drops every listed view.

Algorithm and failure contract:

1. Validate every input and limit before connecting, including nonempty tuples,
   supported MSSQL writes, exact current macro-authority digest, canonical
   digests and identifiers of at most 128 UTF-16 code units. Resolve defaults
   from the already resolved runtime connection, never the authoring checkout.
2. Open a dedicated connection in the pinned database using existing bounded
   connection and query-timeout capabilities. Check the authored pin against
   the live catalog; never learn or rotate a pin from that observation. Verify
   the resolved default database, every invocation database, and every
   named/default write database resolve to this same database through SQL
   Server. Cross-database batch execution is
   explicitly unsupported by this first observer, not silently reinterpreted.
3. Read one metadata header. Require visible database `VIEW DEFINITION`,
   dependency-catalog `SELECT`, valid original/effective/database principal SIDs,
   exact database continuity, and a non-contained SQL Server database. Record
   actual catalog semantics; do not hardcode a particular CI/CS collation.
   Reject unavailable metadata and unsupported impersonation, not empty-success.
4. Send bounded JSON parameters; classify all slots with SQL `DENSE_RANK` over
   schema/relation under `COLLATE CATALOG_DEFAULT`. Include absent slots and
   retain their original owners. Class numbers are local observations, not
   durable physical resource IDs. Never classify endpoints as independent using
   different aliases, server names, principal SIDs or database GUIDs alone.
5. For dbt slots, observe the incoming recursive-view predicate pinned by
   `sqlserver__get_drop_sql`: database/schema/entity equality and referencing
   object type `V`. Do not replace it with outgoing `referenced_id` traversal,
   filter out cross-server references, or include NULL database-name rows.
   Recurse with the returned local view names and original database argument.
   Check original and returned names for lossless database-codepage roundtrip,
   because the pinned macro emits non-Unicode literals. Reject literal/identifier
   forms the pinned macro cannot represent safely. Transfer slots participate
   in slot observations, not in an invented dbt lifecycle for transfer code.
6. Bound slots, distinct view nodes, raw dependency rows, traversal depth, input
   JSON bytes and elapsed time. Fetch `remaining + 1` sentinel rows and fail on
   overflow; cycles and incomplete traversal are failures, never truncation.
   Roll back the dedicated read transaction and close the connection on every
   path. Failed cleanup or exceeded deadline cannot yield an observation.

Defaults are 8,192 slots, 8,192 view nodes, 32,768 raw dependency rows, depth 32,
8 MiB JSON and 60 seconds elapsed, with connection/query caps at 10 seconds.
The slot budget includes intermediate/backup/helper relations: a 197-model
project must not be mistaken for only 197 writes or exceed a 256-slot toy limit.
These are internal platform observation budgets, not new user config fields.
Errors are detached/content-free; credentials and raw driver exceptions never
enter reports. No live catalog query runs during module import, offline compile,
or `AirflowDeploymentProjectionService.materialize`.

Validation must run the actual observer against a disposable SQL Server: CI/CS
and absent slots; explicit/default DB; fully qualified incoming chain/diamond
and two-part-reference control; permission denial; Unicode roundtrip; exact and
exceeded budgets; read-only state and cleanup. Retain unit tests for malformed
rows, injected timeouts/cleanup errors and cycles that SQL Server refuses to
construct. A skipped live case is not proof. Extend the existing candidate
certification runner rather than dispatching a parallel certification workflow.

This observation does not solve cross-endpoint physical service identity,
preflight-to-write races, concurrent DDL, runtime reservations, post-task SQL
quiescence, or ClickHouse destinations. Those remain mandatory parts of the
same admission implementation; workspace activation remains denied until they
are complete. This is not a narrower replacement for the final contract.

#### Protected activation occurrence and reservation protocol

Approved implementation detail (2026-08-29): workspace activation uses an
injected platform authority, never an optional success callback. The immutable
request binds one canonical UUID `activation_id`, environment, release and
deployment IDs, previous deployment ID, complete source/write inventory,
loader-verified runtime-context subject, the complete per-binding catalog
observation set and a canonical physical-resource closure. Every physical
resource has a platform-issued service authority, connector, target authority,
observation subject and collision-namespace guard ID. Caller aliases, host text,
database GUIDs, SIDs and local equivalence classes are observations, not global
service identity.

The runtime-context authority subject is workload-neutral: it binds the exact
environment, release/deployment and release, deployment, binding-set,
connection-registry and credential-runtime descriptor digests. The loader also
retains the separately verified init-fetch-plan digest for each workload
attempt. A plan digest is not mislabeled as the shared binding snapshot because
the plan includes a workload-specific pack and payload selection; conversely,
the shared context subject is never evidence that a particular workload plan
was loaded.

The authority persists and reads back an exact occurrence through this saga:

1. Under the existing promotion lock, after immutable projection validation and
   local/remote CAS, reserve the sorted complete guard set in one protected
   SERIALIZABLE transaction. Persist `PREPARED`, exact request/closure subjects
   and monotonically advanced fencing epochs. Commit-unknown remains PREPARED or
   unknown and blocks pointer mutation; it is reconciled by exact readback on a
   new non-pooled session, never inferred as rollback.
2. Read back the exact durable PREPARED occurrence before filesystem pointer
   commit. A stale, foreign, incomplete or differently fenced receipt fails.
3. Commit the pointer with the same `activation_id`. Then transition that exact
   occurrence to `ACTIVE` and read it back. If acknowledgement is unknown, the
   pointer may already have changed, but runtime remains fail-closed until exact
   durable ACTIVE readback succeeds. Recovery reconciles this state; it never
   manufactures a success receipt from pointer bytes.
4. Ordinary recovery creates a fresh activation UUID and reservation after its
   reviewed CAS. Audit-only repair does not reserve again: it requires the
   pointer's existing occurrence to be ACTIVE with identical release,
   deployment, environment and guard closure before appending audit evidence.
5. Each dbt or transfer attempt obtains occurrence-bound fencing authority before
   opening a writer session. The execution path rechecks ACTIVE state and guard
   epochs at its mutation boundary. Mapped, inline and transfer paths use the
   same authority; no best-effort cleanup branch is accepted as coverage.
6. Retirement/finalization moves ACTIVE to RETIRING, waits for durable terminal
   closure of every admitted attempt and connector-specific quiescence, then
   releases only the exact owned epochs and records RETIRED. TTL, current-tip
   change, missing pod or task-state observation alone never releases a guard.

The guard namespace must collide with the existing semantic-refresh physical
resource namespace. A second unrelated workspace-only guard table is forbidden:
it would allow both systems to own the same target. All connectors in the source
write closure need service authority, reservation, attempt fencing and terminal
quiescence support. Until SQL Server and every emitted transfer destination meet
this contract, `release_activation_failure()` keeps workspace V2 denied.

Required regressions cover: authority absent; stale occurrence; partial guard
closure; PREPARED/ACTIVE commit-unknown; pointer failure after reservation;
runtime pointer without ACTIVE occurrence; fresh recovery UUID; audit exact
readback without new reservation; concurrent semantic-refresh collision;
mapped/inline/transfer attempt fencing; finalizer crash/replay; and exact-epoch
release only after complete terminal/quiescence proof.

Primary semantics checked 2026-08-29:
[catalog/database collations](https://learn.microsoft.com/en-us/sql/relational-databases/databases/contained-database-collations?view=sql-server-ver17),
[dependency metadata and permissions](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-sql-expression-dependencies-transact-sql?view=sql-server-ver17),
[SQL literal types](https://learn.microsoft.com/en-us/sql/t-sql/data-types/constants-transact-sql?view=sql-server-ver17).
The market comparison above remains applicable; these internal observations do
not establish an unmeasured performance or safety advantage over another tool.

#### Reader-first cache installation and activation boundary

Local release installation explicitly dispatches the validated dbt producer
wire; the historical default producer check remains V1 for callers that have
not opted into workspace reading. Unknown/malformed producers never fall back.
The exact installed dpone version is required for both wires. V2 installation
preserves the complete canonical tree, including the source inventory and
existing checksum subject, rather than copying only descriptor-referenced files.
It verifies canonical input closure and the captured bytes in a private stage
through the complete-source reader and integrity verifier before publication.
Missing verification capability is a failure, not a reduced validation mode.
Legacy singleton installation keeps its existing layout and behavior.

The cache integrity validator carries the producer wire from its already
validated release read into the projection observation. It must not infer the
wire from environment variables, deployment/index versions or caller hints.
Reading/materializing a V2 release is not physical admission: until the protected
admission provider and runtime reservation/finalizer are implemented and certified,
workspace activation fails closed with
`DPONE_DBT_WORKSPACE_ADMISSION_UNAVAILABLE`. No opt-out flag or synthetic success
provider is exposed. Ordinary promote, recovery and audit restoration cannot
turn successful offline validation into permission to execute V2 writes.
Denial preserves current/pointer/audit bytes; snapshot preparation may still
leave a sealed, inactive cache tree. Legacy activation remains unchanged.

This migration boundary implements the existing missing-authority rule; it is
not a replacement for physical-target admission. The occurrence-specific signed
transport, persistent resource reservation, protected post-task finalizer and
complete transfer-destination coverage remain required before enabling V2
activation. No new public evidence wire is introduced by reader-first support.

Reuse existing immutable release/deployment IDs, dbt execution evidence and DEV
campaign completeness contracts. Evidence must resolve every workflow to its own
project/manifest/selection; one successful project cannot stand in for the other.
Existing v1 evidence and promotion continue to read through explicit v1 dispatch.
Any changed report shape receives a distinct version, never optional fields that
silently reinterpret old evidence. Existing `dpone.dbt-prod-promotion.v2`
already means campaign-bound singleton promotion; workspace promotion uses a
new v3 descriptor, not a second interpretation of v2.

V3 has exactly `schema`, `promotion_id`, `release_id`, `mirror_root`,
`source_snapshot_path`, `source_snapshot_sha256`, `dev_deployment_id`,
`dev_evidence_ref`, `dev_evidence_subject_sha256`,
`dev_evidence_artifact_name`, `dev_evidence_producer_workflow`,
`dev_evidence_source_commit`, `dev_evidence_set_id`, and
`dev_evidence_campaign_request_sha256`. Both campaign identity fields are
mandatory. Preserve all existing trust validation/signature requirements and
compute `promotion_id` as the canonical fingerprint excluding itself.
Install each project at `<mirror_root>/<project_path>`; paths must be confined,
nonoverlapping and repository-relative. Snapshot and descriptor remain outside
the mirror subtree. Removed projects disappear only inside this bot-owned tree.

Add `dpone dbt workspace verify-promotion --root <checkout>
--descriptor <path> --release-set <path> --format json`. The descriptor pins
the mirror root and snapshot. Require v3 descriptor, dbt wire v2 and source
snapshot v2; reject mixed modes. Existing singleton verification stays unchanged.
The new `dpone.dbt-workspace-promotion-verification.v1` report has exactly
`schema`, `status`, `passed`, `code`, `release_id`,
`source_snapshot_sha256`, and `projects`. Each project row has
`project_path`, `expected_project_bundle_sha256`,
`observed_project_bundle_sha256`, `passed`, and `code`.
Rows cover the entire pinned inventory in project-path order. Unavailable
observations are null and failed, never omitted or passed. This source-verification
report does not replace the complete cryptographic promotion gate.

Promotion validates all source projects against the pinned aggregate snapshot and
release. The audit mirror installs a complete workspace under one bot-owned root;
no sequential per-project mutation of the active mirror. Stage all projects and
metadata, verify hashes and confinement, then use one recoverable transaction.
The transaction has three possible destinations and at most three replacements:
one entirely bot-owned workspace subtree, snapshot and descriptor. It does not
add one journal entry per
project: existing journal recovery admits at most three replacements. Crash
recovery and concurrent-owner conflicts use the existing lock/journal boundary,
with no writes outside owned paths. This means rollback-safe, serialized
installation, not simultaneous visibility to arbitrary unlocked filesystem
readers. Verifiers hold the same lock; the bot commits the Git mirror only after
the transaction succeeds. Airflow never executes SQL from this mutable audit
mirror. No author-owned tree may become the aggregate mirror destination.
Recovery restores the three output destinations, not exact parent-directory
topology. Empty metadata parents may remain after interrupted bootstrap; recovery
must not guess which pre-existing empty directories it is allowed to delete.

Workspace preparation is an explicit new boundary, not a change to singleton
`prepare-prod-mirror`: `dpone dbt workspace prepare-prod-mirror` accepts
`--root`, `--compiled-root`, `--mirror-root`, `--source-snapshot-path`,
`--descriptor-path`, `--expected-release-id`, the existing mandatory DEV
deployment/reference/trust fields, both mandatory campaign identity fields,
and `--format text|json`. Its Python service receives the same inputs and
injected source reader, bundle operations and transaction. The
`dpone.dbt-workspace-mirror-prepare.v1` report has exactly `schema`,
`release_id`, `promotion_id`, `mirror_root`, `source_snapshot_path`,
`descriptor_path`, `projects`, and `no_op`; `projects` is the sorted list of
project paths. It prepares a local audit mirror only; neither its report nor
a self-consistent descriptor authenticates the DEV campaign. Existing signature
verification and independent comparison of every expected DEV identity are
required by the promotion gate before activation.

Bootstrap requires all three destinations to be absent. An update requires an
existing valid v3 descriptor at the requested descriptor path, with the same
mirror root and snapshot path, and a matching v2 snapshot fingerprint. These
establish the prior bot-owned layout, not cryptographic authorization. Missing
or malformed ownership metadata, partial bootstrap or an existing unowned tree
fails without replacement; adoption/migration is a separate explicit operation.
Drift inside an established bot-owned mirror may be replaced. All three paths
must be pairwise disjoint (including equality, ancestry and portable case
collisions), outside VCS/worktree and transaction control paths, with no symlink
components. A missing mirror may be restored only from valid ownership metadata.

The shared lock-only primitive has no repository recovery side effects.
Workspace verification holds that lock and rejects a pending transaction journal
with a recovery instruction, without recovering or deleting it. Installation
retains the existing lock-and-recover wrapper. The lock file may be created in
the operating-system temporary directory; read-only means no repository writes.
The entire workspace tree is checked, including unexpected files/directories
outside project roots and ignored/generated files inside them; these cannot be
hidden by the project bundle builder's ignore rules.

Roll out readers before emitting v2. Old readers explicitly reject v2. Exact
wire dispatch is authoritative: dbt wire v2 requires source snapshot v2 and
exclusively content-addressed referenced payloads; v1 retains the historical
inventory and IDs. Runtime obtains the wire from the already verified release
artifact in its fetched plan, not an untrusted environment variable or an ID
guess. The existing plan carries that release artifact, so no new plan field is
needed. Preserve historical order healing only for v1. V2 reordered references
are rejected, even if their set is correct. Exact
producer/provider pin checks are retained; a reader's ability to parse v1 does
not waive an exact runtime version requirement. Retain the old runtime image and
deployment for rollback. The legacy compile command remains v1 unless a separate
approved migration changes its default.

## Detailed algorithm and failure semantics

1. Confine and discover project roots; classify publishing capability from each
   project's existing declarations. Fail duplicate/nested/ambiguous roots.
2. Freeze the complete input inventory. Acquire each canonical manifest and
   resolved source bundle using existing validation and bundle ports. Verify
   exact toolchain/route policy and missing package errors before publication.
3. Compile every project once into a typed intermediate representation. Validate
   project/workflow/DAG/workload/target ownership across the whole workspace.
4. Derive each content-addressed trio. Construct the aggregate snapshot and one
   release from all validated pack, DAG, runtime and schema bytes. No generated
   pack mutation or post-hoc fingerprint patching is permitted.
5. Validate the complete tree with schema, integrity, membership and reader
   checks. Atomically publish it. Identical output is an idempotent no-op;
   different existing output is a conflict, not permission to delete it.
6. Bind the release to the environment once, resolve physical target identities,
   sign through existing deployment-scoped authority, publish and reconcile.
7. Execute each workflow from its pinned release trios. Validate complete dbt
   results before transfers under the existing workflow contract. Record actual
   attempts and results; never label skipped/unavailable checks as passed.
8. Finalize the DEV campaign only when every required workflow passes. Promote
   unchanged release bytes; validate and transactionally install the audit mirror.

```text
discover -> validate complete inventory -> compile all -> validate identities
         -> build one release -> verify complete tree -> atomic publish
         -> environment binding -> execution/evidence -> promote same release
any pre-publication failure -> no active output change
any execution failure -> failed/incomplete campaign, no automatic promotion
```

Compile cancellation, timeout or process crash leaves at most owned staging
files, never a partly active release. Concurrent output writers are fenced by
the publisher; conflicting content is an error. Retry identical compilation is
idempotent. Empty publishing discovery is a reported no-op for discovery/check;
explicit compile fails instead of publishing an empty release that could remove
live workloads. Project removal requires a reviewed nonempty desired inventory
or the existing explicit retirement procedure.

Data execution is not an all-project database transaction: if A committed and B
failed, A stays committed and campaign status is failed. Retry follows existing
workflow idempotency and commit-unknown rules; do not replay A blindly. Preserve
run/deployment/interval/attempt binding when retrying after a new release.
Deployment rollback restores code/bindings, not already committed data. Data
repair is a separate explicit operator action.

Keep current compact producer limits: 64 unique runtime payloads, 256 MiB per
payload, 512 MiB aggregate. Limits apply to the whole release after safe
deduplication, not independently per project; do not multiply the effective limit.
Staged extraction must retain existing archive confinement and expansion bounds.

Workspace v2 emits only canonical descriptor paths, `release-set.json`, the
fixed source snapshot and `release-subjects.sha256`. Singleton aliases,
intermediate transfer YAML and its compile-evidence report are not copied into
the workspace release. Transfer packs already embed their runtime manifests;
they do not fetch the omitted intermediates. Compile diagnostics belong to the
workspace report, outside the immutable release. Validate this exact file set
before creating the checksum subject, so a checksum cannot bless an orphan.
V1 retains its existing complete output layout. The legacy semantic-refresh
template branch has no executable DAG and is not admitted by workspace v2;
reject it before source capture rather than emitting an incomplete inventory.

## Architecture, alternatives and quality

| Component | Responsibility |
| --- | --- |
| Pure dbt payload/source-inventory contracts | Version dispatch, IDs, membership and canonical fingerprints; no I/O/framework imports |
| Workspace discovery adapter and application planner | Confined local discovery and complete typed input plan |
| Existing compiler/bundle/selection ports | Project compilation and authoritative source/selection validation |
| Workspace release writer | One aggregate tree and atomic publication through injected publisher |
| Compact/runtime/evidence/promotion readers | Consume the same canonical contracts, no copied regex/identity algorithms |
| Mirror transaction | Stage, validate, commit/recover the complete workspace |
| Consumer CI | Diff quality selection, canonical manifests, one public compile call and unchanged-artifact promotion |

Contracts cannot import services, runtime, CLI or optional SDKs. Composition roots
construct adapters. New modules respect `docs/benchmarks/quality_budgets.yml`;
existing module-size/coupling debt must not grow. Split the writer by coherent
project projection versus aggregate publication, not mechanical file fragments.
An ADR is required for the versioned wire and source-inventory semantics.

Rejected alternatives: concatenate independent release JSON (invalid identity
and last-writer wins); per-domain hardcoded payload directories (manual central
registration); SQL in runtime image (model changes require image delivery);
remove the one-project guard first (false support); one project per deployment
(does not meet the required coherent release boundary).

## Market comparison and measurable acceptance

Sources below were checked on 2026-08-28. These are capability comparisons, not
claims that dpone outperforms the products overall.

| System/version context | Observation and decision |
| --- | --- |
| Astronomer Cosmos, current OSS docs | [Multi-project guide](https://astronomer.github.io/astronomer-cosmos/guides/multi_project/multi-project.html) separates project configurations and supports external references through dbt-loom. Adopt project isolation; cross-project references remain outside this release feature. Pre-generated manifests avoid parsing-time dbt execution. |
| Astro Remote Execution Agents, current hosted docs | [Deployment guide](https://www.astronomer.io/docs/astro/remote-execution-deploy-dbt) documents separate dbt-code delivery approaches. Use source artifacts independent of the toolchain; do not claim identical execution or trust guarantees. |
| Apache Airflow 3.3.1 | [Versioned DAG bundles](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-bundles.html) preserve a run's code version. Adopt pinned retry semantics; local/S3 bundle delivery alone is not version-pinning evidence. |
| dbt, current artifact docs | [Manifest](https://docs.getdbt.com/reference/artifacts/manifest-json) describes project resources; [state caveats](https://docs.getdbt.com/reference/node-selection/state-comparison-caveats) warn against overwriting comparison artifacts. Preserve distinct project manifests and immutable state. |
| dlt, Informatica, Airbyte, Fivetran, Pentaho, SSIS, Apache Beam | N/A for this narrow dpone artifact-wire migration; no connector/managed-ELT/stream processing capability is being replaced or compared. |
| gusty | N/A for the runtime artifact and promotion boundary; this change does not replace YAML DAG authoring. |

Measured axis: compared with current dpone 0.74.28's rejected two-project compile,
two independently named publishing projects must yield exactly one release and
one deployment, zero missing/cross-wired payloads, zero central CI domain-map
edits and zero runtime image builds for SQL-only changes. Repeat with reversed
discovery order, same node names, one project changed, one failed workflow,
restart, retry of the old version and rollback. Preserve machine-readable
`dbt-workspace-certification.json` with exact commits, image digests, attempts,
test results and links. Unit/fixture results do not establish live readiness or
the separate >=30-root CI SLO.

## Security, tests, documentation and rollout

Secrets remain deployment connection references. Reject path traversal,
symlink/archive attacks, source mutation during snapshotting, digest spoofing,
wrong-kind substitution, duplicate JSON keys and incomplete inventories. Retain
existing trust verification, exact pins and output redaction. No permissive
`require_certified_routes=False` default for publishable production artifacts.

| Layer | Required proof |
| --- | --- |
| Unit | v1/v2 dispatch, ordered trios, canonical paths, hash/length/kind mismatch, duplicate/missing/extra memberships, resource limits |
| Contract | v1 outputs unchanged; old readers reject v2; schemas generated from producers; SDK-free base import/help |
| Integration | Two projects, same local node IDs, global workflow collisions, physical alias collisions, stable reversed order, atomic output/no-op/conflict |
| Failure/recovery | Project B compile failure leaves no partial output; execution B failure blocks campaign; mirror crash at each commit boundary; concurrent writer fencing |
| Runtime | Both projects fetched into isolated task roots, complete result/transfer evidence, exact provider/image/source identities |
| Promotion/rollback | Complete mirror verified; tampered/missing project rejected; retained deployment and old-attempt retry validated |
| Performance | Compile once per selected source snapshot; no second private writer invocation; SQL-only change produces no runtime image build |

Extend existing publish atomicity, compact payload limits/order, runtime identity,
init-fetch, DEV evidence, promotion verification and mirror tests. Add CLI
discovery/check/compile tests. Run change-aware check selection, full mandatory
architecture/type/unit gates and documentation checks. Live validation must use
the already-authorized scoped environment and cannot run business workloads
outside the explicit certification fixtures.

Documentation deliverables: dbt hub links, first-two-project tutorial,
CLI/Python/schema reference, architecture ADR, operator retry/rollback runbook,
compatibility matrix, generated schemas and changelog. Consumer docs show that
authors configure only standard project-local files, not CI routing registries.

Rollout slices: canonical contracts and readers with v1 preserved; workspace
producer and schema/docs; scoped two-project certification; exact provider/runtime
upgrade; consumer public-API integration and guard removal; DEV acceptance then
PROD promotion and rollback. A failed identity/evidence/recovery check halts
promotion, not weakens the gate. No default v2 emission until required readers
are deployed. The complete feature is not done after the reader-only slice.

## Agent execution and approval

Codex is the sole writer/integrator in this worktree. Shared schemas, changelog,
navigation and composition roots remain integrator-owned. Read-only architecture,
test and UX review may inspect all related paths and must not change files,
dispatch workflows or certify live behavior. A fresh-context review is required
before integration.
