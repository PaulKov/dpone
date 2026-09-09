# Feature design: executable Airflow KPO init-fetch delivery

- Status: APPROVED
- Owner: dpone maintainers
- Issue: global Airflow self-service review for v0.73.1
- Target release: next patch after 0.73.1
- Last verified: 2026-07-30

## Executive summary

An indexed Airflow deployment can declare `runtime_artifact_delivery.mode:
init_fetch`, but the current provider reduces that declaration to a Boolean and
materializes the same scheduler-embedded bootstrap used by the legacy lane. The
generated KPO does not invoke the runtime `InitFetchExecutor`, does not verify a
production attestation, and does not force the base container to consume the
fetched pack.

This is a fail-open implementation bug against ADR 0009. The correction makes
`init_fetch` an executable, pinned and fail-closed KPO contract:

1. the deployment index carries a runnable OCI image reference and bounded
   non-secret fetch descriptors;
2. the lightweight provider composes a fixed init container and a fixed base
   launcher instead of trusting commands embedded by the scheduler;
3. the init process fetches exact artifacts, verifies size, checksum and the
   configured attestation policy, then publishes a ready manifest;
4. the base launcher re-verifies the ready manifest and executes only the
   command selected from the fetched workload pack;
5. unsupported delivery modes fail before any DAG is installed.

No production attestation is claimed until a concrete verifier and live
Kubernetes evidence exist. `required_for_prod` therefore fails closed when no
verifier is configured.

### Approved hardening amendment: derived workload-pack identity

The strict v2 lane must derive `pack_fingerprint`; copying an embedded claim
between pack, release, deployment, fetch plan and ready evidence is not
verification.

Every newly built compact pack carries:

```yaml
pack_identity:
  schema: dpone.airflow-pack-identity.v1
pack_fingerprint: sha256:...
```

The canonical identity payload is the complete JSON pack minus only:

- `pack_fingerprint`;
- advisory producer/report fields `producer`, `meta`, `artifact_dir`,
  `output_path`, `bundle_path`, `next_actions`, `warnings`, and `blockers`.

All other current and future fields are bound by default, including `kind`,
`schema_version`, `pack_identity`, `steps`, Airflow assets/partitions, mapping,
runtime selection/bootstrap/payload, XCom image, outcome gate,
`provider_execution`, process plans, legacy execution projections and
artifact indexes. Object key order and source JSON whitespace do not affect
identity. Array order and string bytes remain significant. Duplicate keys,
NaN/Infinity, non-JSON values and non-canonical claims fail closed. The
implementation never uses `default=str`.

One zero-Airflow helper in `dpone-airflow-pack` owns identity payload,
computation and verification. The root producer depends on that helper; the
deployment materializer, provider loader/preflight, runtime receipt validator
and base launcher independently derive the same value before their first
mutable or executable side effect. Ready/evidence records use only the derived
value.

Packs without `dpone.airflow-pack-identity.v1` remain readable only through the
explicit raw/local compatibility lane. They cannot enter a strict v2
deployment and must be rebuilt with a new immutable release/deployment.

### Approved hardening amendment: one provider-execution contract and Kubernetes identity projection

The 2026-07-19 fresh-context review found two additional public-boundary
defects:

1. the checked-in Airflow-pack JSON Schema accepted `provider_execution`
   structures that the provider rejected, including a missing `labels` map,
   unbounded node selectors, arbitrary tolerations and unconstrained resource
   sections;
2. a valid logical workload ID of 64–128 characters was copied verbatim into a
   Kubernetes label value, whose maximum length is 63 characters.

The dependency-light `dpone-airflow-pack` package is the authority for the
closed `dpone.airflow-provider-execution.v1` structural contract. It owns the
field sets, limits, enums and JSON Schema producer used by both the root GitOps
schema catalog and the runtime validator. The root package must not maintain an
independent hand-written copy. Contract-parity tests bind the generated schema,
the checked-in schema reference and `require_provider_execution`.

Logical IDs remain unchanged in manifests, DAG specs, packs, artifact refs and
evidence. Kubernetes-specific representations are deterministic projections:

```text
logical workload id
  -> Airflow/task identity: full logical id
  -> Kubernetes label: unchanged when already valid and <=63 bytes,
     otherwise bounded readable prefix + "-" + sha256 suffix
  -> Kubernetes annotation: full logical id
  -> pod name: existing short-name behavior, bounded deterministically
```

The projection is content-derived, collision-resistant for long common
prefixes, never silently truncates a label, and preserves existing short valid
IDs. Producer and provider use the same zero-Airflow helper. The validator
requires the exact projected label rather than the raw logical ID. Tests cover
63/64/128-character IDs, trailing separators, common long prefixes, schema
limits and the final pod metadata.

### Approved correctness amendment: explicit runtime hook ownership

The 2026-07-29 production freshness incident exposed an ownership gap between
the generated DAG node and the strict runtime launcher. A process configured
with `execution.cli: inline` and `execution.airflow: separate_task` may be
collapsed to one inline Airflow node. The provider correctly omits separate
hook tasks in that projection, but the previous runtime plan did not carry the
resolved node visibility and the runtime bootstrap always set
`DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS=1`. A run could therefore load a valid but
stale source snapshot and report success.

The approved correction introduces
`dpone.airflow-runtime-init-fetch-plan.v3`. Its execution selection carries the
explicit `workload|process` scope, exact `process_selector`, and the closed
hook ownership enum `inline|externalized`. A null selector in process scope is
the default process; whole-workload execution is a separate explicit scope and
always externalizes hooks. The provider must prove that its top-level Airflow
hook tasks cover every process-local separate hook; incomplete ownership is a
parse-time blocker. The provider is the projection authority; the
verified runtime pack is the integrity authority. Runtime checks that the
selected process exists, selects the same process-keyed bootstrap command,
then sets or removes the hook-skip environment variable from the child process
accordingly. It also removes any inherited copy of that reserved variable
before subprocess execution.

Generated separate hook steps carry
`dpone.airflow-pre-hook-command.v1`. The structured value binds canonical hook
ID, selector and argv; the human-readable command is not executed by the
strict launcher. This avoids coupling Airflow task IDs such as
`pre_hook_refresh_orders` to hook IDs such as `refresh_orders`.

Immutable v1/v2 runtime plans remain decodable for rollback. Compatibility is
fail-closed: because their wire selector identifies a workload rather than a
process, runtime accepts the bridge only when the pack has exactly one
selector-coherent process plan. It accepts a generated structured pre-hook only
when the canonical command is unique across the pack and agrees with the legacy
command tokens. Every multi-process or malformed legacy pack is rejected before
source I/O. There is no “first process wins” behavior. Pre-hook and dbt child
failures propagate their real exit code; only normal runtime commands retain
the XCom-gated wrapper exit behavior.

Whole-workload ownership is equality of the complete canonical hook descriptor:
selector, argv, dependencies, required and credential policies, produced
evidence and reason. Labels alone do not transfer execution authority. The
nested command schema is closed so provider and runtime reject unknown fields
consistently.

This amendment changes no manifest authoring fields. It closes a false-success
path and requires coordinated provider/runtime rollout for newly emitted v3
plans.

## Personas and journey

| Persona | Goal | Current failure | Required outcome |
|---|---|---|---|
| Data engineer | Run the reviewed workload represented by the deployment digest. | KPO executes scheduler-copied shell content. | The pod executes only a fetched and verified pack. |
| Airflow operator | See deterministic pod construction and actionable failure. | `init_fetch` silently behaves like another delivery mode. | Missing/migrating fields fail with stable codes before DAG installation. |
| Platform engineer | Use workload identity and immutable registry configuration. | The provider has no runnable image or registry configuration contract. | The pod receives only logical refs and content digests. |
| Security engineer | Bound workload-pack command authority inside the current trusted scheduler/provider/cache model. | Inline archive and command are trusted by the base container. | Reserved image, command, volumes and mounts cannot be overridden by the pack; compromised-scheduler resistance remains out of scope. |

Normal authoring remains unchanged:

```text
authoring -> release-set -> deployment-set -> airflow-index.json
-> provider KPO -> init-fetch -> verified ready manifest -> runtime-pack-exec
```

### Current trusted computing base

This increment trusts the producer, promoted local cache and deployment index,
Airflow scheduler/DAG processor, installed provider, Kubernetes control plane
and kubelet, digest-pinned runtime image, workload-identity/IAM enforcement,
registry adapter, and configured attestation verifier.

The fixed KPO contract removes pack-owned image, command, argument, mount, and
init-container authority from an honest provider composition. It does not claim
that a compromised scheduler, provider, or local cache cannot submit a
different pod or plan. Resistance to those compromises requires a separate
admission/signing design and is not part of this patch.

## Scope

### In scope

- Add the complete pinned `init_fetch` projection to the deployment index.
- Require an OCI `runtime_image_ref` of the form
  `registry/repository@sha256:<digest>`.
- Add immutable registry configuration and optional trust-policy references.
- Add bounded release, deployment and workload-pack descriptors.
- Add a compact, deterministic, secret-free runtime fetch plan.
- Thread one immutable delivery context through all indexed provider paths.
- Compose fixed KPO init/base commands and reserved volumes.
- Add runtime init-fetch and verified-pack execution services and CLI entry
  points.
- Fail unsupported known modes and old incomplete `init_fetch` indexes closed.
- Keep `local_preview` on its existing non-runnable/inline lane.
- Preserve raw `DponeTaskGroup.from_pack` only as an explicit legacy/local
  escape hatch that cannot claim `init_fetch`.
- Add Airflow 2/3 model, serialization and parse-side-effect tests.

### Non-goals

- A generic delivery plugin framework.
- Resolving a physical object-storage URI during DAG parse.
- Importing core `dpone`, cloud SDKs, Vault or Kubernetes configuration into
  `dpone-airflow-pack`.
- A separate fetch image.
- Implementing a public signed-attestation ecosystem in this patch.
- Resistance to a compromised scheduler, provider, local cache, Kubernetes
  control plane, or runtime image.
- Treating live Kubernetes, workload identity, or production attestation as
  verified without current evidence.
- Supporting `shared_pvc`, `embedded_bundle`, `csi_volume` or `inline` before
  their own approved implementation.

## Public contract

### Fresh-review trust-boundary amendment

The 2026-07-19 independent security review confirmed additional P1 failures in
the first runtime snapshot. These rules are part of the approved correction:

1. The plan is untrusted input and cannot weaken production attestation.
   Producer, provider, and runtime plan decoding reject
   `production + optional` before runtime composition. For a valid
   `production + required_for_prod` plan, a trusted composition-root policy
   determines the effective requirement; no configured verifier fails before
   registry construction or registry I/O.
2. The deployment descriptor is a verified receipt. Its bytes bind
   `deployment_id`, `release_id`, the exact selected workload inventory and the
   logical registry reference. Evidence must not call a deployment identity
   verified until this receipt passes.
3. Artifact publication verifies size and SHA-256 over the bytes actually
   copied into the private publication tree. Reopening a previously verified
   staging pathname is insufficient.
4. Workload entries are converted all-or-nothing. A malformed entry, malformed
   nested identity/verify object or zero selected artifacts is a failure, never
   a successful empty fetch.
5. Logical registry and ConfigMap references use bounded allowlist grammars.
   URI syntax, userinfo, query/fragment, assignments and secret-shaped values
   are rejected before serialization or I/O.
6. A generic per-artifact byte limit is enforced for the complete plan before
   the first registry `stat`, in addition to count and aggregate limits.
7. Unsafe/symlink/integrity failures have a non-retryable integrity code;
   they are not reported as a retryable registry outage.
8. Internal readable paths and topology-free evidence locators are separate
   fields. Public evidence never substitutes an unreadable placeholder for the
   runtime path required by the launcher.
9. Existing safe-sample v1 readers remain compatible. Mandatory `bytes` belongs
   to the strict indexed KPO contract; legacy v1 input is either migrated
   explicitly or rejected with a migration-required code, never silently
   reinterpreted.
10. Strict indexed KPO tasks use `retries=0` until the durable target-commit
    journal and a connector-certified target fence from ADR 0022 exist.
    Non-zero DAG defaults, pack values, or operator overrides fail before any
    DAG is installed with
    `DPONE_AIRFLOW_RETRIES_REQUIRE_TARGET_FENCE`. The provider also writes an
    explicit zero on every materialized strict task so Airflow DAG defaults
    cannot re-enable retries implicitly. This is an availability trade-off,
    not a claim that manual reruns are duplicate-safe.

### Deployment projection

`init_fetch` requires:

```yaml
trust_tier: production
runtime_image_ref: registry.example/dpone-runtime@sha256:...
runtime_image_digest: sha256:...

runtime_artifact_delivery:
  mode: init_fetch
  trust_tier: production
  artifact_registry_ref: dpone-prod-artifacts
  identity:
    method: kubernetes_workload_identity
    service_account: dpone-runtime
    namespace: data-platform
  registry_config_ref:
    kind: kubernetes_config_map
    name: dpone-artifact-registry-4f3a
    key: registry.json
    sha256: sha256:...
  trust_policy_ref:
    kind: kubernetes_config_map
    name: dpone-artifact-trust-71bd
    key: policy.json
    sha256: sha256:...
  source:
    artifact_registry_ref: dpone-prod-artifacts
  verify:
    checksums: required
    attestations: required_for_prod
```

The image digest must equal the digest in `runtime_image_ref`. Names and keys
are bounded Kubernetes-safe logical identifiers. The index contains no physical
registry URI, signed URL, token, credential, Vault path or secret value.
Top-level and delivery-level `trust_tier` are mandatory and must match; an
environment name never infers the trust tier.

The index also contains exact descriptors for:

- `release-set.json`;
- `deployment.json`;
- every selected workload pack.

Every descriptor contains `artifact_ref`, canonical `sha256` and positive
`bytes`. A workload descriptor also carries its pack fingerprint.

The fetched `dpone.deployment-set.v2` is the runtime deployment receipt. There
is no second compact receipt schema or independently generated receipt file:
duplicating the same authority would create drift and a second identity to
maintain. Runtime verifies the exact fetched bytes against the descriptor,
recomputes `deployment_id` with the canonical deployment fingerprint contract,
and then checks the execution-bearing projection:

```yaml
# Structural excerpt only; omitted identity and provenance fields make this
# intentionally non-copyable as a complete deployment-set.
schema: dpone.deployment-set.v2
deployment_id: sha256:...
release_ref: sha256:...
runtime_image_ref: registry.example/dpone-runtime@sha256:...
runtime_image_digest: sha256:...
runtime_artifact_delivery:
  mode: init_fetch
  trust_tier: production
  artifact_registry_ref: dpone-prod-artifacts
  identity: {...}
  registry_config_ref: {...}
  trust_policy_ref: {...}
  verify: {...}
workloads:
  - id: load_orders
    sha256: sha256:...
    pack_fingerprint: sha256:...
```

Missing or different inventory, registry ref, trust tier, runtime image,
release or deployment identity fails before ready-manifest publication.
`release-set.v1` is independently recomputed and checked against
`release_id`; the selected pack must be a member of that release.

### Runtime fetch plan

The provider emits canonical compact JSON bounded to 16 KiB:

```yaml
schema: dpone.airflow-runtime-init-fetch-plan.v1
environment: prod
trust_tier: production
release_id: sha256:...
deployment_id: sha256:...
runtime_image:
  ref: registry.example/dpone-runtime@sha256:...
  digest: sha256:...
registry:
  logical_ref: dpone-prod-artifacts
  configuration:
    kind: kubernetes_config_map
    name: dpone-artifact-registry-4f3a
    key: registry.json
    sha256: sha256:...
trust_policy:
  kind: kubernetes_config_map
  name: dpone-artifact-trust-71bd
  key: policy.json
  sha256: sha256:...
identity:
  method: kubernetes_workload_identity
  service_account: dpone-runtime
  namespace: data-platform
release: {artifact_ref: cache://..., sha256: sha256:..., bytes: 4096}
deployment: {artifact_ref: cache://..., sha256: sha256:..., bytes: 2048}
workload_pack:
  id: load_orders
  artifact_ref: cache://...
  sha256: sha256:...
  bytes: 8192
  pack_fingerprint: sha256:...
execution:
  kind: runtime
  selector: load_orders
  hook_name: null
verify:
  checksums: required
  attestations: required_for_prod
```

The plan is base64-encoded in `DPONE_INIT_FETCH_PLAN_B64`. Its canonical SHA-256
is placed in `DPONE_INIT_FETCH_PLAN_SHA256` and in a pod annotation. Both
containers receive the same hash. Values are validated before pod construction.
The plan's `verify.attestations` is a requested minimum only. Trusted runtime
policy may strengthen it and can never be weakened by the plan.

### Pod contract

Reserved objects:

| Name | Type | Init | Base |
|---|---|---|---|
| `dpone-fetched-artifacts` | `emptyDir` | RW `/var/lib/dpone/artifacts` | RO |
| `dpone-worktree` | `emptyDir` | RW `/workspace/repo` | RO |
| `dpone-run-output` | `emptyDir` | absent | RW `/var/lib/dpone/run` |
| `dpone-artifact-registry-config` | ConfigMap | RO | absent |
| `dpone-artifact-trust-policy` | ConfigMap | RO when required | absent |

Each ConfigMap volume projects exactly the selected key to the fixed
`registry.json` or `policy.json` filename. The init process acquires one bounded
snapshot through one file descriptor, verifies the SHA-256 of those exact
bytes, and parses those same bytes. A mismatch fails before registry
construction or registry I/O. Operators roll out changed bytes under a new
reviewed ConfigMap snapshot and deployment identity; an in-place ConfigMap
update is not a trusted rollout mechanism.

Init container:

```text
image: <exact runtime_image_ref>
command: dpone airflow runtime-init-fetch
```

Base container:

```text
image: <exact runtime_image_ref>
command: dpone airflow runtime-pack-exec
```

The provider rejects pack-owned collisions for image, commands, arguments,
service account, namespace, reserved volumes, mounts, init-container names and
security context. Only non-security scheduling/resource fields from the pack
may be retained.

The strict scheduler authority is an explicit, closed workload-pack
projection:

```yaml
provider_execution:
  schema: dpone.airflow-provider-execution.v1
  kpo_kwargs:
    task_id: orders__dpone_runtime
    name: dpone-orders
    labels:
      dpone.dev/workload-id: orders
    env_vars:
      DPONE_DAG_ID: "{{ dag.dag_id }}"
  pod_spec:
    spec:
      nodeSelector:
        workload: etl
      containers:
        - name: base
          resources:
            requests:
              cpu: "1"
```

The producer derives this projection from the legacy/local execution
artifacts and includes it in `pack_fingerprint`. The indexed strict provider
reads only `provider_execution`; top-level `kpo_kwargs`, `pod_spec` and
`runtime_command` remain the explicit legacy/local lane and are never
scheduler authority for v2 `init_fetch`. Missing projection fails before DAG
installation with `DPONE_INIT_FETCH_PACK_MIGRATION_REQUIRED`. Unknown fields,
executable fields, namespace, service account, security fields, containers
other than the single resource-only `base` entry, volumes, mounts and init
containers fail with `DPONE_INIT_FETCH_RESERVED_COLLISION`. This prevents
silent stripping while preserving old raw-pack APIs.

Trusted strict composition disables base-container log tailing while init
containers own the pod and asks the provider to delete successful pods. Failed
base containers are normally retained, but cancellation, late task failure and
Kubernetes cleanup remain provider-native best-effort behavior. These values
are provider policy and are not pack-owned extension fields. See
[`feature-design-airflow-init-fetch-pod-retention-v0732.md`](feature-design-airflow-init-fetch-pod-retention-v0732.md).

### Ready manifest

Init writes `dpone.runtime-fetch-ready.v1` last and atomically. It records only:

- fetch-plan hash;
- release, deployment, pack and image identities;
- logical registry ref, registry-config fingerprint, and the pinned trust-policy
  fingerprint when one is selected;
- checksum status, the effective attestation requirement selected by trusted
  init composition, and the resulting attestation status;
- confined `payload/...` artifact locators.

The base launcher refuses execution when the ready manifest or any fetched
artifact is absent, oversized, mutable, mismatched or no longer equal to the
plan. It validates the pinned trust-policy fingerprint and the effective
attestation decision from the ready manifest; it does not read the init-only
registry or trust-policy ConfigMap mounts. Runtime-only state keeps a confined
readable local path; serialized ready/evidence payloads expose only a relative
locator.

### Delivery mode policy

For indexed deployments:

| Mode | Behavior |
|---|---|
| `local_preview` | Existing non-runnable preview lane |
| `init_fetch` | Strict KPO contract defined here |
| other known modes | `DPONE_RUNTIME_ARTIFACT_DELIVERY_MODE_UNSUPPORTED` |
| unknown mode | field/schema invalid |
| incomplete old `init_fetch` | `DPONE_RUNTIME_ARTIFACT_DELIVERY_MIGRATION_REQUIRED` |

Failure occurs before any real or diagnostic DAG is installed.

## Detailed algorithm

### Build and parse

1. Build `release-set` and `deployment-set` once.
2. Validate `runtime_image_ref` and digest equality.
3. Emit exact artifact descriptors and platform configuration refs.
4. Materializer reads the deployment index once and creates an immutable
   delivery context.
5. For every workload, build a bounded canonical fetch plan.
6. Reject secrets, mutable aliases, unknown fields and reserved collisions.
7. Compose a KPO using fixed trusted commands.
8. Airflow parse performs no network, DB, secret, Variable, Connection,
   Kubernetes or cache-refresh call.

### Init container

1. Decode at most 16 KiB of plan bytes.
2. Recompute and compare the plan hash.
3. Read the mounted registry catalog with a byte limit and verify its digest.
4. Select exactly `registry.logical_ref`.
5. Reject static credentials, signed URLs and environment-variable secrets.
6. Compose an `ArtifactRegistryReader` using workload identity.
7. Validate count, per-artifact and aggregate byte limits for the whole plan.
8. Fetch deployment, release and selected pack by exact key; never list.
9. Validate sizes and checksums.
10. Recompute deployment and release identities and validate their reference.
11. Validate the fetched `deployment-set.v2` receipt, registry provenance, pack inventory
    membership and pack fingerprint.
12. Apply the trusted effective attestation policy before extracting any
    bootstrap.
13. Copy into a private publication tree and recompute size/checksum from the
    copied bytes.
14. Extract the structured bootstrap with path and size confinement.
15. Write the ready manifest atomically as the final operation.

### Base launcher

1. Decode and verify the same fetch plan.
2. Open the ready manifest and fetched pack through confined no-follow reads.
3. Recompute hashes and verify ready/plan identities, including the pinned
   trust-policy digest and a non-downgraded effective attestation decision.
4. Do not reread init-only registry or trust-policy configuration.
5. Select the exact scope- and selector-bound runtime or pre-hook command from
   the fetched pack. Process runtime and hook policy must resolve the same
   process plan.
6. Reject scheduler-copied commands, inline archive fallback and pack-local
   image overrides.
7. For runtime selections, run the verified argv without a shell, capture
   evidence under `/var/lib/dpone/run`, write `/airflow/xcom/return.json`, and
   exit `0` so the KPO xcom sidecar publishes the outcome summary. For
   pre-hook and dbt selections, publish bounded evidence/XCom and propagate the
   real child exit code.
8. Preserve bounded redacted diagnostics; runtime outcome status lives in
   XCom, while pre-hook preserves the child exit code.

## Identity, ordering and retries

Ordering is:

```text
download -> size/checksum -> attestation -> safe extract -> ready manifest
-> base re-verification -> data execution -> evidence -> checkpoint/state
```

Every retry gets a fresh `emptyDir`. The plan is deterministic and all reads
are pinned, so retries fetch the same bytes. Init failure occurs before base
startup, data I/O, XCom success, checkpoint mutation or source-state advance.
Mapped tasks are pod-isolated.

Until ADR 0022 is accepted and one route has a certified target fence, the
strict lane does not permit Airflow task retries. A manually triggered rerun
after target mutation still requires operator reconciliation; pinned bytes
alone do not make the target mutation idempotent.

## Failure semantics

| Failure | Result |
|---|---|
| Invalid/incomplete index | Fatal loader error; no DAG installed |
| Unsupported mode | Fatal loader error; no DAG installed |
| Plan too large or secret field | KPO construction fails |
| Registry config mismatch | Init fails before registry access |
| Missing/oversized/checksum-mismatched artifact | Init fails; no ready manifest |
| Required verifier absent/rejects | Init fails closed |
| Published copy differs from verified staging bytes | Integrity failure; no ready manifest |
| Deployment receipt or registry provenance mismatch | Integrity failure; no ready manifest |
| Empty or malformed workload inventory | Contract failure before registry I/O |
| Ready manifest or pack tampered after init | Base refuses execution |
| Runtime command missing | Base refuses execution |
| Runtime command exits non-zero | Runtime XCom/evidence is failed; the launcher exits `0` so the sidecar can publish it, and the downstream outcome gate fails the DAG |
| Pre-hook or dbt command exits non-zero | The prerequisite KPO fails with the child exit code |

Errors expose stable codes and logical IDs only. Physical URIs, config bodies,
signed URLs, secret names/values and absolute paths are not serialized into
Airflow DAGs, XCom or normal logs.

## Components and dependency direction

```text
contracts/schema/producer
  -> lightweight provider transport projection
  -> KPO static pod composition

runtime CLI composition root
  -> registry reader + attestation verifier
  -> runtime init-fetch service
  -> verified-pack launcher

runtime domain does not import commands/readiness/Airflow/cloud SDKs
provider does not import core dpone/cloud SDKs/Vault/Kubernetes config
```

`ArtifactRegistryReader` owns `stat` and bounded `download_file`.
`ArtifactRegistryWriter` owns conditional creation. Existing
`ArtifactRegistry` extends both for compatibility.

## Compatibility and migration

- Existing `local_preview` behavior remains compatible.
- Raw pack APIs remain legacy/local only.
- Existing incomplete indexed `init_fetch` deployments must be regenerated;
  no immutable deployment is rewritten.
- Existing packs without `dpone.airflow-provider-execution.v1` remain valid
  only for the raw legacy/local API. A strict v2 deployment must rebuild the
  pack, release and deployment so the new projection and fingerprint are
  immutable together.
- Existing safe-sample v1 payloads remain readable through their compatibility
  path. The strict indexed KPO plan is versioned independently and never
  retroactively makes a formerly optional v1 field mandatory.
- Legacy imports retain their existing deprecation policy.
- Airflow matrix remains 2.10.5/2.11/3.2/3.3 with the pinned Kubernetes
  provider cells.
- The provider remains dependency-light and parse-safe.

## Test and certification plan

Required unit/contract tests:

- canonical plan JSON/hash/size and secret rejection;
- image ref/digest and artifact descriptor validation;
- supported/unsupported/migration mode policy;
- runtime, pre-hook and mapped-task pod composition;
- reserved collision rejection;
- absence of inline archive and scheduler command in `init_fetch`;
- KPO model serialization and dry-run in supported Airflow cells;
- parse probes for network, DB, Variables, Connections, Vault and Kubernetes;
- plan -> fake registry -> ready manifest -> verified command;
- staged-byte swap before/during publication;
- producer/provider/runtime rejection of `production + optional`;
- valid `production + required_for_prod` with no trusted verifier;
- malformed nested values and zero workload inventory;
- deployment receipt/inventory/registry mismatch;
- per-artifact preflight rejection before registry I/O;
- checksum, size, fingerprint, path traversal and symlink failures;
- registry-config hash and unknown logical ref;
- absent/rejected attestation;
- missing/tampered ready manifest and post-init pack tampering;
- deterministic retry and concurrent pod isolation;
- no checkpoint/state advance after init failure;
- runtime image smoke for both command entry points.

Live Kubernetes, object storage, workload identity and attestation remain
`UNVERIFIED` unless an approved environment is actually exercised.

## Documentation and rollout

1. Record the v1/v2 executable boundary and current TCB in ADR 0024.
2. Update provider, cache migration and runtime-image guides.
3. Add migration guidance for incomplete `init_fetch` indexes.
4. Ship the strict provider/runtime contract behind the existing mode.
5. Keep production attestation fail-closed until the concrete verifier is
   configured.
6. Certify one real Kubernetes deployment before changing production readiness
   from `UNVERIFIED`.

The v3 rollout uses a bridge state because an old runtime cannot decode a plan
emitted by the new provider:

```text
verified new packs (inactive)
-> old provider + new packs + new v1/v2/v3 runtime
-> new provider + new runtime
-> serialized-DAG convergence and freshness smoke
```

Rollback reverses the compatibility edge: downgrade the provider first while
the new runtime still accepts v1/v2, wait for serialized-DAG convergence, then
activate the previous exact deployment and runtime. The detailed matrix lives
in [the provider guide](airflow-pack-provider.md#roll-out-and-roll-back-v3-without-a-mixed-version-window).

## Market comparison

N/A. This change closes an internal correctness and supply-chain boundary. It
does not introduce a market-facing capability claim.
