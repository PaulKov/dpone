# Feature design: deployment-owned credential projection

- Status: APPROVED
- Owner: dpone maintainers
- Target release: TBD; no publication is authorized by this document
- Base: `5b91dfd5cf2a7dde2cef7670f366b3d81e433e0b` (`origin/master`, including PR #199)
- Last verified: 2026-09-23

This proposed design is for platform engineers, provider maintainers, and
operators. It closes the path from a verified deployment's logical connection
requirements to credential files in native dbt runtime Pods. Architecture
and wire allocation and exact implementation paths were approved on 2026-09-23.
Root-owned integration hooks remain an explicit completion gate. The accompanying tests are
intentionally red evidence of existing gaps, not release acceptance.

Overview: [Airflow pack/provider](../airflow-pack-provider.md).
Related authority: [ADR 0027](../adr/0027-single-runtime-connection-authority.md).
Decision: [ADR 0071](../adr/0071-deployment-owned-credential-projection.md).
Next step: acknowledge implementation ownership and implement the matrix below.

## Problem and customer journey

Native dbt packs correctly contain `connection_projection: {}`: portable release
bytes must not embed an environment's credential topology. Environment deployment
materialization rewrites source `airflow_connection` registry entries into
`kubernetes_secret_volume` resolvers. However, the rewritten registry loses the
original Airflow connection ID and Secret key. The provider has no corresponding
deployment-owned mount inventory. A native Pod can therefore be constructed
without the target or control credential files its runtime resolver requires.

A source connection ID change currently produces identical rewritten runtime
snapshots when all other fields remain unchanged. The credential source identity
must remain part of deployment authority even though credential values do not.

| Persona | Required outcome | Observable success |
| --- | --- | --- |
| Data engineer | Declare target refs once in native source | No domain-specific Pod or control-connection configuration |
| Platform engineer | Bind target and control refs once per environment | Deployment contains exact reviewed Secret keys and paths |
| Operator | Diagnose missing credentials before SQL mutation | Stable failure code identifies the missing projection capability |
| Maintainer | Preserve identity and scheduler trust | Tampered or stale projection is rejected before Pod creation |

Journey: discover native delivery prerequisites; provision the namespace-local
Secret; configure environment bindings and protected workspace authority; compile
the unchanged portable release; build and inspect the environment projection;
activate only after watcher credentials are available; inspect the generated Pod
and run the workflow; diagnose missing keys from Pod events without dumping Secret
values; repair provisioning and retry the same deployment when references are
unchanged. A mapping change requires a new deployment. Upgrade readers before
producing the new projection contract.

## Scope and constraints

In scope: existing Airflow operator-bridge sources, native dbt target credentials,
workspace control credentials, exact alias-to-registry-to-connection-ID mapping,
shared Secret keys, sealed projection identity, deterministic base-container
mounts, missing/tampered authority rejection, and a reusable pure projection
planner for both build and platform tooling.

Non-goals: SQL admission/handover changes, polling changes, new credential
backends, changing workload authorization, reading Airflow Connections at parse
time, storing Secret values in artifacts, modifying native release packs,
automatically creating Secrets, or patching provider functions from user DAGs.
The native target and control remain distinct logical refs. Sharing a physical
credential key is represented honestly; it is not proof of least privilege.

Two supported variations are required: different target/control connection IDs,
and distinct aliases sharing one source connection ID/Secret key. Unused registry
entries must not expand a workload's mount closure. Vault entries continue to use
their existing runtime authentication and must not be converted into files by
this feature.

## Existing path and demonstrated gaps

`AirflowDeploymentProjectionService.materialize` reads the complete environment
binding-set, registry and credential-runtime. It calls
`runtime_connection_snapshots` without a project/domain projection. That function
rewrites all source bridge entries using an identity map of registry refs.
Consequently, domain defaults do not control the sealed snapshot rewrite.

`DbtAirflowExecutionPackBuilder` emits correlation env only.
`compact_provider_execution` preserves placement, image-pull references and base
resources, and discards arbitrary env/volumes. Provider strict allowlists reject
author-controlled credential env and mount overrides. Pod composition chooses
the existing bridge only from a nonempty pack projection. A native empty
projection has no such path. These restrictions should remain intact.

The red tests in
`tests/test_native_deployment_credential_projection_gap.py` prove lost source
identity and acceptance of native control refs without authenticated mount
authority using current public producer/composer functions. They do not require
a future schema or fabricate a successful projection.

## Proposed public contract

### Environment ownership and APIs

The existing protected desired-state authority v2 is the sole authored source
of `workspace_authority_connection_ref`. Do not add a second editable control-ref
field to binding-set, project defaults, domain catalogs or native release packs.
The existing binding-set still maps this logical ref to a registry ref; it does
not select which connection is the control authority.

Today `airflow_desired_state_publish_cmd` loads that authority for preparation
and publication, and `AirflowDesiredStatePreparationService.candidate` seals its
`publish_authority_sha256`. Reconciliation derives the current pointer's control
ref from the same protected authority. In contrast, `airflow_deployment_build_cmd`
and `build_deployment_result` do not currently receive it. The existing
`runtime_authority_ref` build argument is a different runtime-access contract,
not a workspace-control selector, and must not be repurposed.

Proposed integration: the native deployment build composition root loads the
same protected file selected by `DPONE_AIRFLOW_DESIRED_STATE_AUTHORITY_FILE`, then
injects a validated typed authority input into the projection planner. Python
callers supply the equivalent typed capability through the composition boundary;
the pure planner does not read ambient environment variables. It checks environment
and registry scope, derives the control ref and records the existing publication
authority fingerprint. Native workspace build fails closed when this capability
is absent or contradictory. Legacy/non-workspace delivery keeps its contract.

Preparation/publication must verify that the deployment projection's authority
fingerprint matches the authority already loaded there. Activation verifies the
same identity and ref before updating current; provider/runtime verify that the
pointer-derived control ref equals the sealed projection's derived ref. These
immutable copies are consistency evidence, not independently editable inputs.
The runtime Pod need not receive the protected publisher authority file. Domain
authors do not repeat the control ref, and no new binding-set field is required.

Existing CLI materialization and Python materialization entrypoints remain the
entrypoints; no command accepts raw Secret values, arbitrary Pod mounts, or an
unverified projection document. Their existing failure envelope/nonzero exit
behavior is preserved. Successful materialization remains immutable/create-only.
Proposed failures use the `DPONE_RUNTIME_CREDENTIAL_PROJECTION_*` prefix, with
`REQUIRED`, `INVALID`, `MISMATCH`, `UNSUPPORTED`, and `LIMIT_EXCEEDED` variants.
Messages explain remediation without connection strings, secret paths from other
backends, or secret contents.

The reusable planner accepts verified release requirements and environment
documents through typed arguments; it returns an immutable secret-free model.
The provider consumes a dependency-light validated model, not core runtime
services or Airflow BaseHook. Runtime keeps `BindingCredentialResolver`.

### Artifact and exact mapping

Propose one artifact, `credential-projection.json`, with schema
`dpone.runtime-credential-projection.v1`. It contains:

- environment and release ID;
- binding-set SHA-256, original source-registry SHA-256, and rewritten
  runtime-registry SHA-256;
- derived workspace control ref and its protected publication-authority SHA-256
  for workspace-enabled deployments;
- sorted source entries with registry ref, source connection ID, Secret name,
  exact Secret key, absolute mount directory, and relative field filename;
- sorted per-workload membership linking logical ref to registry ref and role
  (`workload` or `workspace_control`).

For the existing bridge, derive Secret key once using the canonical
`airflow_conn_env_name(source_connection_id)` helper. Never derive it from a
logical or registry alias. Validate an explicitly configured bridge key against
the existing bridge policy. For example, logical `warehouse` bound to registry
`warehouse_runtime` and source ID `sql_target_login` means key
`AIRFLOW_CONN_SQL_TARGET_LOGIN`, directory
`/run/secrets/dpone/airflow-connections/warehouse_runtime`, and file `uri`.
A second registry alias can have another directory with the same source key.
This is fan-out, not a duplicate conflict.

The source registry owns the source connection ID. A platform-owned bridge
configuration owns Secret name and namespace placement; the canonical default
remains available. Domain lists cannot replace the deployment closure. The
planner must reject ambiguous keys, conflicting entries for one registry ref,
case-normalization collisions between distinct source IDs, absolute field names,
path traversal, mount ancestor collisions and unsupported resolver combinations.

The artifact contains no usernames, passwords, tokens, URI payloads or Secret
resource data. Source IDs and key references are restricted deployment metadata;
normal reports expose counts and digests rather than the full inventory.

### Fingerprints and trust

Projection content is canonical UTF-8 JSON, deterministically sorted and bounded.
The SHA-256 and byte length become an exact descriptor in the deployment and
its Airflow index, then the runtime init-fetch plan. Changing a source ID, Secret
key, path, membership or control ref changes projection and deployment identity.
The portable release ID does not change. Duplicate input ordering does not
change output. Credential value rotation does not require a new deployment.

Avoid a hash cycle: projection references release and environment input hashes,
not the deployment ID; deployment references the projection descriptor. The
verified index mirrors that descriptor. Workload projections must be proven
subsets of the full deployment artifact. Provider cache loading verifies bytes
and mirrors before creating any operators; runtime verifies the fetched
descriptor again before resolving credentials.

The runtime plan carries a descriptor rather than an unbounded inline inventory;
its existing size limit remains unchanged. The provider obtains the verified
local projection artifact alongside the other indexed metadata. Do not perform
network calls to obtain it during DAG import.

Adding executable mount authority requires new deployment/index/init-fetch wire
versions that old readers reject. The integrator approved v6 for each wire and
v1 for the new artifact. Do not silently add fields to existing strict schemas
or make old readers ignore required authority. Existing runtime registry resolver
schema, v1/v2 native release wire and fingerprints are preserved.

### Exact version allocation and minimality

Verified against `origin/master` at the base above. Deployment/index v2-v5 use
`additionalProperties: false`; their descriptor fragments are exact three-field
objects. Provider `init_fetch_wire` checks exact root keys; core
`runtime_init_fetch_plan_codec` checks exact plan keys. There is no documented
extension bag. The writer's `additional_files` is an internal file-set capability,
not permission to add executable authority to an old wire.

| Contract | Allocated version | Required change |
| --- | --- | --- |
| Deployment | `dpone.deployment-set.v6` | Required exact `credential_projection` descriptor |
| Airflow index | `dpone.airflow-deployment-index.v6` | Required identical descriptor, verified local bytes |
| Init-fetch plan | `dpone.airflow-runtime-init-fetch-plan.v6` | Required identical descriptor, included in fetched artifacts |
| Projection artifact | `dpone.runtime-credential-projection.v1` | Closed mapping and membership document described above |
| Ready manifest | Existing `dpone.runtime-fetch-ready.v1` / `.v2` | No new fields; exact plan/deployment transitively bind projection |
| Promotion evidence | `dpone.airflow-deployment-promotion.v1` | Root-owned neutral family: exact projection descriptor, derived control ref and publication-authority fingerprint |

Use the existing descriptor shape `{artifact_ref, sha256, bytes}` without a new
descriptor version. Place its content at
`cache://runtime-credential-projections/sha256-<projection-digest>/credential-projection.json`.
This independent content address is calculated before the deployment ID; neither
the artifact body nor its URI depends on the deployment ID. Add it to publication,
cache verification and init-fetch artifact inventories, not release payloads.

V6 is not a copy of development-only v5. It requires credential projection for
both permitted trust tiers while retaining production attestation requirements.
MSSQL outlet projection remains optional. Existing development authorization is
orthogonal: absent for ordinary deployments; when required, retain the existing
closed Secret-volume or immutable-payload source and all admission checks. Do
not introduce another source mode, generic feature bag or independently editable
workspace-authority field. The v6 plan keeps existing v4 behavior for externally
mounted confidential development authority and v5 behavior for immutable payload;
its optional fields are conditionally exact for these two existing cases.

Ready-wire preservation requires a regression proving that a tampered/missing
projection is rejected after init-fetch and again before base execution, using
the verified deployment descriptor and existing plan SHA. The runtime locates it
under the existing `payload/<cache-relative-key>` rule, just as connection
snapshots do. No new ready key, implicit success or unverified directory scan is
allowed. If this cannot be proven, stop for a separate version decision.

Desired-state preparation has no artifact registry reader and must not acquire
one or perform secret I/O merely for parity. The new neutral promotion evidence
is emitted from independently verified deployment/projection bytes by the
canonical promotion producer. It carries the exact projection descriptor,
derived control ref and existing publication-authority fingerprint. Preparation
compares these subjects to its loaded protected authority and binds the evidence
digest into the existing candidate. Activation/base still verify actual bytes.

Fail-closed migration: when protected authority has a workspace control ref,
preparation/publication require this projection-bearing evidence. Legacy evidence
is allowed only when that ref is absent. This is a coordinated producer,
publisher, watcher, provider and native-v6 lane migration; do not enable protected
workspace authority while its producer still emits legacy evidence. Legacy
authority-v1 lanes retain their behavior. No schema-name alias or missing-field
fallback crosses the boundary. Root owns the new evidence contract and its
producer/consumer integration; feature completion requires these hooks.

## Algorithm and failure semantics

1. Verify the immutable release and complete native payloads. Derive logical
   workload connection requirements with canonical existing source readers;
   include target/state/proxy/auxiliary refs only when the workload requires
   them. Reject unknown capability instead of recursively guessing fields.
2. Read and validate environment inputs and the injected protected authority.
   Add its control ref to native workspace workload closures; bind its existing
   publication-authority fingerprint. Require exact logical bindings and
   registry entries. Resolve metadata only; never resolve credential values.
3. Build the source mapping before rewriting registry resolvers. Join membership
   through logical ref -> registry ref -> source ID -> Secret key. Compute both
   original and rewritten registry subjects. Require runtime mount paths and
   field filenames to match the planned projection exactly.
4. Enforce deterministic limits: at most 256 selected refs per workload, 1024
   deployment entries, and 1 MiB canonical projection bytes. Retain existing
   deployment limits when stricter. Reject overflow; never truncate closure.
5. Atomically publish projection and deployment through existing immutable
   publication. A changed destination conflicts; identical bytes are a no-op.
   Crash leaves an inactive incomplete candidate, never a successful deployment.
6. During preparation/publication and activation compare protected authority
   fingerprint, sealed derived control ref and pointer identity. Watcher uses
   platform-provisioned credential files at the same
   planned paths. It must fail before admission when required files are absent.
7. Provider verifies exact projection bytes from the sealed local cache and
   selects only the requested workload's membership. Group file items by Secret
   source; create deterministic reserved volume names and readOnly mounts on
   `base`. Materialize shared-key fan-out explicitly. Require same namespace as
   deployment workload identity; reject user-supplied collisions.
8. Do not mount target/control values in artifact-fetch init containers or XCom
   sidecars. Those retain their existing independent credentials. Do not add
   credential values to env, commands, task params, logs, XCom or evidence.
9. Runtime validates projection and registry agreement, then uses existing
   workload-scoped resolution. Kubernetes enforces required Secret/key presence.
   Missing files fail before target/control connections or dbt mutation.
10. Retry with unchanged mapping reuses the same projection subject. Changing
    mapping builds a new deployment. Secret values use existing workload-start
    resolution semantics; this design does not promise immutable Secret values.

```text
verified release requirements + environment bindings + protected control ref
  -> exact source-ID/key mapping -> deterministic credential projection
  -> immutable deployment/index descriptor -> verified local provider context
  -> selected base Pod mounts -> verified runtime files -> existing resolver
```

Empty native requirements, missing control binding, null coordinates, unsupported
resolvers and contradictory mappings fail before Pod creation. An unused
registry entry does not confer mount authority. A conflicting Secret/key during
rotation needs provisioning repair, not fallback to a similarly named alias.
Concurrent deployment builders use existing immutable publication/CAS. No new
global mutable map or expiring credential lease is introduced.

## Architecture and alternatives

| Component | Owner | Responsibility |
| --- | --- | --- |
| Canonical projection contract | dpone contracts | Closed model, identity, bounds and membership |
| Projection planner | dpone application/service | Join verified requirements and environment metadata |
| Deployment builder | dpone build composition | Inject existing protected authority, snapshot rewrite, immutable artifact/descriptors |
| Lightweight provider reader/composer | Airflow package | Verify artifact and materialize bounded Pod mounts |
| Runtime context loader | dpone runtime | Recheck descriptor and resolver path agreement |
| Secret provisioning and watcher Pod | Platform repository | Values, namespace/RBAC and static watcher mount |

A pure planner is injected at build composition; provider validation must remain
available without importing dpone core or Airflow. No provider monkeypatch,
service locator or parse-time secret lookup is allowed. Keep modules cohesive and
below the canonical budgets in `docs/benchmarks/quality_budgets.yml`; expected
new units are contract/codec, planner, reader and Pod projector. Shared schema
and DTO edits remain integrator-owned. An ADR is required because the deployment
becomes explicit owner of credential mount authority.

Build-only source acquisition is separate from the pure metadata join. Native
execution profiles use their bounded single-member contract. Mixed ordinary
transfers, flows and batches use the existing bounded multi-file archive reader,
private temporary staging and pinned runtime-manifest digest. The metadata-only
manifest loader disables ambient source registries; the canonical runtime
connection-closure policy covers every compiled process, including active state,
object-storage and materialization authorities. Disabled/reused state contributes
no extra ref. No credential is resolved or process executed. Unsafe or incomplete
sources fail closed; unsupported workloads are never omitted from membership.

Alternatives rejected: per-domain projections duplicate platform state; scanning
all registry entries mounts unused credentials; alias-to-key guessing loses
source identity; patching release packs invalidates portable hashes; credential
env injection exposes values and violates strict ownership; reading Airflow
Connections at DAG parse time violates import safety. Existing environment
global defaults alone cannot fix the current native empty-projection path.

## Current official comparison

Sources checked 2026-09-23. This is a transport/ownership comparison, not a claim
of superior ETL capability or evidence of live dpone readiness.

| System/version | Relevant observation | Adopt / reject |
| --- | --- | --- |
| Airflow Kubernetes provider 10.22.0 | KPO supports typed Secret volumes and documents argument precedence. [Official reference](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html) | Adopt typed Pod construction; retain stricter deployment ownership instead of arbitrary author mount overrides |
| Astronomer Cosmos, rolling docs | Kubernetes dbt operators accept Kubernetes-specific `operator_args`, including Secrets. [Official guide](https://astronomer.github.io/astronomer-cosmos/getting_started/kubernetes.html) | Reuse KPO transport; avoid requiring every workflow author to repeat environment credentials |
| Kubernetes, rolling docs | Secret volume items map named keys to specific file paths. [Official guide](https://kubernetes.io/docs/tasks/inject-data-application/distribute-credentials-secure/) | Adopt exact key/path projection; reject mounting unrelated keys |
| dlt, Informatica, Airbyte, Fivetran, Pentaho, SSIS, gusty, Apache Beam | N/A for this bounded change: no integration with their deployment/credential delivery layers is proposed | No comparative product or runtime-performance claim |

Measurable target relative to this base: one native workload with target/control
aliases distinct from connection IDs gets exactly its declared files, no secret
values in serialized Pod env/artifacts, and a changed source key changes deployment
identity. All positive/negative matrix cases must pass; zero per-domain credential
edits. Evidence: offline contract results plus a separately approved disposable
Kubernetes execution record. This does not certify database routes or handover.

## Validation, documentation and rollout

| Layer | Required cases | Evidence |
| --- | --- | --- |
| Unit | Alias differs from ID; logical differs from registry; shared key; target/control; unused refs excluded; stable ordering; no source mutation | Focused pytest |
| Contract | Source ID/key/path/control/membership change affects deployment; missing/tampered artifact; mirrored digest/bytes; wrong environment; path and name collisions; limits | Schema/identity/provider tests |
| Provider | Native `{}` produces exact base mounts; no init/XCom mounts; hook and mapped paths; parse without Airflow/network; user overrides rejected | Generated Pod assertions and serialization matrix |
| Runtime | Read projected synthetic URI files; database/schema overlays preserved; files missing or escaped fail before connectors | Resolver/launcher tests |
| Integration | Real Secret key fan-out, removed key, changed mapping, namespace isolation and restart/retry | Approved disposable Kubernetes run |
| Compatibility | Old release hashes unchanged; old delivery lane unchanged; unsupported new wire rejected; no automatic native bypass | Version matrix |
| Performance | Bounded maximum closure; no per-ref external I/O at parse; unchanged plan byte limit | Deterministic limit and import checks |

The preparation tests intentionally fail on this base. The source-identity tests
measure the current snapshot producer, which exposes only three artifacts; when
the approved producer adds the projection, their subject must include that new
artifact. They do not require a gratuitous change to the three existing snapshot
formats. Implementation adds positive end-to-end coverage and turns the red
cases green; the negative missing closure case must remain fail-closed. No mocked
test is live certification.

Documentation delivery must include a first-run native tutorial, exact environment
and artifact schema reference, an architecture/ADR update, a watcher/runtime
provisioning how-to, and a runbook for missing Secret/key, stale mapping and
rollback. Update CLI reference only if the approved placement requires options;
update compatibility, changelog and generated schemas in the implementation PR.

Rollout: install compatible readers/provider/runtime first; provision reviewed
Secrets and watcher files; emit the new deployment authority; verify Pod dry-run
metadata and actual file reads; activate and manually run a synthetic workflow;
then enable normal production delivery only with environment-specific evidence.
Rollback selects an earlier compatible deployment and retains projection artifacts
and attempt evidence. It does not undo SQL or retire held workspace guards; those
remain the separate lifecycle contract. Disable new emission if parity or closure
checks fail. Do not publish an incomplete feature behind a false success status.

## Agent execution and approval

This preparation owns only this specification, its new focused test file, and
`docs/agent-task-contracts/deployment-credential-projection.yml`, plus ADR 0071.
Until the exact allocation below is acknowledged, all production
modules, shared schemas, CI, release metadata, polling and handover are read-only.
The root integrator owns cross-feature version allocation and shared edits.

Approved implementation sequence, subject to exact path allocation:

1. Approve existing-authority injection/parity, projection artifact, wire versions
   and closure limits; add the ADR and authoritative schemas without another
   authored control-ref field.
2. Implement the pure planner and exact source provenance, then immutable
   deployment publication and mirrored descriptor validation.
3. Implement provider-owned base mounts and runtime parity checks through normal
   composition; preserve native release bytes and strict author allowlists.
4. Add full matrix tests/docs, run required gates and fresh independent review;
   separately authorize live certification and release.

- [x] User problem, self-service journey and existing failure are concrete.
- [x] Mapping, ownership, trust and failure semantics are specified.
- [x] Relevant official transport references were checked.
- [x] Preparation path ownership is isolated and synthetic.
- [x] Maintainer approved authority injection/parity and version allocation.
- [x] Maintainer changed specification status to `APPROVED`.
- [ ] Implementation, migration and live acceptance completed.

### Exact implementation ownership request

All paths below are proposed, not permission to edit neighboring modules. New
files are marked `(new)`. If tracing finds another required semantic owner, stop
and request allocation. No polling, handover, reconciliation lifecycle, workflow,
version or release-publication changes belong to this feature writer.

Schemas and closed DTO/readers:

- `src/dpone/gitops/schema_runtime_credential_projection.py` (new)
- `src/dpone/gitops/schema_release_deployment_v6_contracts.py` (new)
- `src/dpone/gitops/schema_release_deployment_contracts.py`
- `src/dpone/gitops/schema_airflow_runtime_init_fetch_contracts.py`
- `src/dpone/contracts/airflow_deployment_projection.py`
- `packages/dpone-airflow-pack/src/dpone_airflow_pack/credential_projection_contract.py` (new dependency-light reader, reused by core)
- `packages/dpone-airflow-pack/src/dpone_airflow_pack/init_fetch_contract.py`
- `packages/dpone-airflow-pack/src/dpone_airflow_pack/init_fetch_wire.py`
- `packages/dpone-airflow-pack/src/dpone_airflow_pack/deployment_index_contract.py`
- `packages/dpone-airflow-pack/src/dpone_airflow_pack/runtime_artifact_delivery_contract.py`
- `packages/dpone-airflow-pack/src/dpone_airflow_pack/cache_status_exact.py`

Build, source provenance and immutable publication:

- `src/dpone/readiness/airflow_credential_projection.py` (new pure planner)
- `src/dpone/commands/airflow_deployment_build_cmd.py`
- `src/dpone/readiness/airflow_self_service_deployment.py`
- `src/dpone/readiness/airflow_deployment_projection.py`
- `src/dpone/readiness/airflow_deployment_projection_io.py` (bounded v6 filename allowlist)
- `src/dpone/readiness/airflow_mssql_deployment_index.py`
- `src/dpone/runtime/airflow_credential_projection_inventory.py` (new)
- `src/dpone/runtime/airflow_artifact_inventory.py`
- `src/dpone/runtime/airflow_artifact_materialization_headers.py`
- `src/dpone/runtime/airflow_artifact_publication_preparation.py`
- `src/dpone/runtime/airflow_runtime_connection_inventory.py`
- `src/dpone/runtime/deployment_cache_projection_validator.py`

Provider/base-runtime projection and verification:

- `packages/dpone-airflow-pack/src/dpone_airflow_pack/credential_projection_pod.py` (new)
- `packages/dpone-airflow-pack/src/dpone_airflow_pack/init_fetch_pod.py`
- `src/dpone/runtime/runtime_init_fetch_plan.py`
- `src/dpone/runtime/runtime_init_fetch_plan_codec.py`
- `src/dpone/runtime/runtime_init_fetch_schema.py`
- `src/dpone/runtime/runtime_init_fetch_receipts.py`
- `src/dpone/runtime/runtime_init_fetch_service.py`
- `src/dpone/runtime/runtime_credential_projection.py` (new)

Generated schema/reference outputs and focused tests:

- `docs/schemas/gitops/deployment-set-v6.schema.json` (new)
- `docs/schemas/gitops/airflow-deployment-index-v6.schema.json` (new)
- `docs/schemas/gitops/airflow-runtime-init-fetch-plan-v6.schema.json` (new)
- `docs/schemas/gitops/runtime-credential-projection.schema.json` (new)
- `docs/reference/gitops-schema-catalog.md`
- `tests/test_native_deployment_credential_projection_gap.py`
- `tests/test_runtime_credential_projection_contract.py` (new)
- `tests/test_airflow_credential_projection.py` (new)
- `tests/test_airflow_credential_projection_delivery.py` (new)
- `tests/test_airflow_credential_projection_compatibility.py` (new)
- `docs/airflow-native-credential-projection.md` (new user/operator guide)

Integrator-owned coordination: ADR index/MkDocs navigation/CHANGELOG; protected
authority parity at desired-state preparation/publication and activation; the
final base-launcher entrypoint calling the new verification helper. This writer
supplies the helper's typed contract and tests, but does not edit handover or
reconciliation lifecycle files. These integration points are required for feature
completion, not deferred optional work. The existing source snapshot format and
native pack producer remain unchanged.

### Compatibility and acceptance gate

1. Re-run current six synthetic cases on the updated base; preserve their red
   evidence until each real path is implemented. Change the identity subject to
   include the new projection only when its real producer exists.
2. Assert frozen v1-v5 schemas, existing release bytes/fingerprints, old
   non-workspace Pod output and old plan decoding remain unchanged. Older readers
   must explicitly reject v6; no automatic fallback may remove credentials.
3. Exercise v6 production, ordinary non-production and both existing protected
   development-authority forms. Test exact field sets, environment/authority
   mismatch, missing control binding, wrong source IDs, shared keys, alias
   collisions, path escape, limits, unused refs and deterministic reordering.
4. Verify full producer -> publication inventory -> sealed cache -> provider
   base mounts -> init-fetch -> base revalidation path with synthetic files.
   Corrupt descriptor bytes/hash/mirror/membership and assert fail-closed before
   connectors; assert init/XCom do not receive credential mounts or values.
5. Run changed-scope policy selection, lint/format/type checks, import/layer/size
   gates, generated-schema drift, docs checks/strict build and offline test suite.
   Obtain fresh independent review of the integrated diff. Live Kubernetes/SQL
   certification is separately authorized and cannot be inferred from unit tests.
