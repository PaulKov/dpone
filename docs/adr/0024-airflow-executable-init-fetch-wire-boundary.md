# ADR 0024: Airflow executable init-fetch uses a strict versioned wire boundary

- Status: Accepted
- Date: 2026-07-19
- Amends: ADR 0007, ADR 0008, ADR 0009

## Context

The Airflow deployment-index v1 contract was introduced for local, parse-safe
projection. It can describe `runtime_artifact_delivery.mode: init_fetch`, but it
does not carry the complete executable image, artifact, trust-policy, and
workload-pack contract required to run that mode safely. Treating an incomplete
v1 declaration as runnable would let the provider fall back to scheduler-copied
commands or inline bootstrap content.

The hardening increment needs one unambiguous executable wire while preserving
the existing v1 local-preview journey. It also needs to state the actual trust
boundary. Fixed pod commands prevent a workload pack from replacing provider
owned execution fields; they do not make a compromised scheduler, provider, or
local deployment cache harmless.

## Decision

### Freeze the wire and migration boundary

The provider applies this closed policy before installing any DAG:

| Index wire | Delivery mode | Result |
| --- | --- | --- |
| `dpone.airflow-deployment-index.v1` | `local_preview` | Supported non-runnable preview compatibility lane. |
| `dpone.airflow-deployment-index.v1` | `init_fetch` | Reject with `DPONE_RUNTIME_ARTIFACT_DELIVERY_MIGRATION_REQUIRED`; regenerate an immutable v2 deployment. |
| `dpone.airflow-deployment-index.v2` | `init_fetch` | The only strict executable indexed lane. |
| v2 | Any other known mode | Reject with `DPONE_RUNTIME_ARTIFACT_DELIVERY_MODE_UNSUPPORTED`. |
| Either wire | Unknown mode or malformed field | Reject as an index field/schema error. |

Strict fields are not retroactively required in v1. A v1 deployment is never
silently reinterpreted as v2, and operators never repair an immutable index in
place.

### Make the executable path explicit

The only indexed executable path is:

```text
producer
  -> immutable release/deployment
  -> airflow-deployment-index.v2
  -> trusted local cache/current snapshot
  -> lightweight provider
  -> KubernetesPodOperator
  -> dpone airflow runtime-init-fetch
  -> runtime-fetch-ready.json
  -> dpone airflow runtime-pack-exec
  -> verified workload argv
```

The v2 index carries an exact digest-pinned OCI runtime image, exact
release/deployment/workload descriptors with positive byte counts, a workload
identity, bounded ConfigMap references, and one immutable delivery context. The
provider derives a canonical plan no larger than 16 KiB, transports it as
`DPONE_INIT_FETCH_PLAN_B64`, and pins its digest in
`DPONE_INIT_FETCH_PLAN_SHA256` and the pod annotation.

### Bind hook ownership to the execution plan

The deployment index remains v2, while newly generated per-task runtime plans
use `dpone.airflow-runtime-init-fetch-plan.v3`. Plan v3 adds:

- `execution.scope`, either `process` for one exact process-plan node or
  `workload` for the explicitly unselected whole-workload path;
- `execution.process_selector`, the exact selected process plan or `null` for
  the default process when `scope=process`; it is always `null` for
  `scope=workload`;
- `execution.hook_execution`, either `inline` or `externalized`.

The provider derives both fields from the resolved DAG node that creates the
KPO. Runtime verifies that the selected process exists in the fetched,
fingerprint-verified workload pack before applying hook policy. `inline`
removes `DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS` from the child environment;
`externalized` requires the skip flag because a separate Airflow hook task owns
the action. A `pre_hook` execution is always `externalized`.
Whole-workload execution is also always `externalized`; it cannot infer one
process node's inline ownership. Before installing a whole-workload DAG, the
provider proves that every process-local separate hook has an equivalent
top-level Airflow task. Incomplete ownership fails parse-time with
`DPONE_INIT_FETCH_HOOK_OWNERSHIP_INCOMPLETE`; runtime is never allowed to skip
an unowned hook.

Process-scoped runtime execution selects the exact process-keyed bootstrap
entry (`<selector>` or the reserved `__default_process__`) and verifies that
its argv carries the same selector. Process-plan metadata continues to use
`__default__`; command keys use a separate namespace so a valid workload ID
named `__default__` cannot collide with default-process execution.
Workload-scoped execution selects the workload-keyed bootstrap entry
`__workload__`, whose argv has no process selector. The launcher never applies
hook policy from one process plan while running another process command.

Separately materialized hooks use
`dpone.airflow-pre-hook-command.v1` inside the selected static step. This
structured command binds the display step name, canonical hook ID, process
selector, manifest path and argv without reparsing the human-readable
`command` field. Any mismatch fails before source I/O.

Runtime plan v1 and v2 remain readable for immutable deployment rollback. Their
wire selector identifies the workload, not a process, so runtime accepts the
bridge only when the fetched pack contains exactly one selector-coherent process
plan. Every legacy multi-process pack fails with
`DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED` before source I/O. Missing or
malformed process-plan metadata also fails closed; runtime never picks the
first mapping entry. During the bounded provider/runtime bridge, a generated
structured pre-hook is accepted only when its canonical command is unique
across the pack and its human-readable legacy command tokenizes to the same
argv. Conflicting process-local hooks fail closed.

Whole-workload hook ownership compares the complete canonical descriptor:
process selector, executable argv, dependencies, required and credential
policies, produced evidence, and reason. Matching display labels alone never
transfer execution ownership. The nested structured command schema is closed;
packs with unknown fields must be rebuilt.

The runtime launcher removes the reserved skip variable from its inherited
process environment before applying the verified command environment. This
prevents a pod-level or image-level ambient variable from overriding the
verified plan decision.

The provider replaces pack-owned executable fields with these fixed commands:

```text
init: dpone airflow runtime-init-fetch
base: dpone airflow runtime-pack-exec
```

Both containers use the exact same digest-pinned image. The base launcher
revalidates the ready manifest and fetched pack, selects a structured argv from
that pack, and launches it without invoking a shell.
The ready manifest binds the pinned trust-policy digest, the effective
attestation requirement selected by init, and its attestation status. The base
launcher validates that decision against the same plan and does not reread the
init-only registry or trust-policy ConfigMap. Runtime commands retain their
XCom outcome gate, while pre-hook and dbt commands propagate the real child exit
code so Airflow cannot mark a failed prerequisite successful.

### Keep pod cleanup inside the trusted provider policy

Strict composition fixes `get_logs=false`: the operator must not poll the base
container while init containers still own a `PodInitializing` pod. It also sets
`on_finish_action=delete_succeeded_pod`. Pack fields cannot override either
value.

This is provider-native, best-effort cleanup rather than a durable retention
state machine. The pinned Kubernetes provider decides cleanup from pod/base
container state. A task may still fail later during XCom handling or callbacks,
and cancellation follows provider `on_kill` semantics. Kubernetes deletion
errors can leave a pod behind and do not fail an already committed data load.

Production therefore requires the separate dpone runtime-Pod retention control
or an explicitly approved delete-all policy. The control inventories only
metadata in one namespace, selects provider-owned managed/contract labels plus
the provider-projected workload label, queries only `Succeeded` and `Failed`
phases, and conditionally deletes with UID/resourceVersion preconditions. Run
correlation comes from Airflow `dag_id`, `task_id` and `run_id` labels;
malformed correlation preserves the Pod. Infrastructure owns CronJob rollout,
ServiceAccount, durable logs, alerts and acceptance evidence.

The metadata queries for `Failed` and `Succeeded` phases consume one shared
10,000-item/32-MiB budget. An accepted Kubernetes delete request is reported as
`delete_accepted`; it is not promoted to observed absence. A later bounded
inventory cycle supplies convergence evidence. Apply records the selected
credential mode and identifies Kubernetes API RBAC as authorization authority;
only rendered in-cluster execution is specifically ServiceAccount-backed.

The control must not treat Pod retention as data success evidence. Airflow task
logs contain orchestration output only when `get_logs=false`; structured XCom,
runtime evidence and load-step audit remain the durable execution truth.
Runtime stdout requires an independently certified cluster log collector. The
metadata-only v1 janitor reports creation-time fallback explicitly and does not
claim exact time-since-terminal semantics.

### Use deployment-set v2 as the runtime receipt

The exact fetched `dpone.deployment-set.v2` is the runtime deployment receipt.
The runtime does not generate or consume a second compact receipt schema. It
verifies the fetched bytes against the plan descriptor, recomputes
`deployment_id`, and requires the receipt to mirror the pinned release,
runtime image, trust tier, registry reference, ConfigMap references, workload
identity, verification policy, and selected workload inventory.

This keeps one environment-specific deployment authority and avoids two
nominally equivalent receipts drifting. The fetched release contract is
`release-set.v1` for legacy/non-dbt releases. The compact Airflow compatibility
transport may carry an optional bounded `runtime_payloads` inventory in v1 so
already-authored packs can reach the verified init-fetch boundary; that
inventory proves byte identity only and is not dbt selection or promotion
authority. The compact-v1 materializer accepts at most 64 distinct payloads,
at most 268,435,456 bytes per payload, and at most 536,870,912 actual bytes
across the distinct release-wide inventory. Equality with either byte boundary
passes. An aggregate breach produces no release directory, release identity,
or success evidence, and has no override flag. This producer budget does not
make v1 authoritative and is not a universal ceiling on release-set v2. This
v1 bridge is non-production only. Production build and runtime
receipt validation reject a v1 release whenever its release-wide
`artifacts.runtime_payloads` inventory is non-empty, even when the selected
workload plan does not reference those payloads. `release-set.v2` remains
mandatory for `dpone dbt compile`, protected dbt promotion,
route-certification authority, and production dbt evidence. The selected
release, payload descriptors, and workload pack are verified separately before
the ready manifest is published.

### Require explicit trust tier and one verified ConfigMap snapshot

`trust_tier` is mandatory in both the v2 index and its
`runtime_artifact_delivery` block, and the two values must match. Allowed values
are `production` and `non_production`; environment names do not infer trust.

Production requires a digest-pinned `trust_policy_ref` and
`verify.attestations: required_for_prod`. The plan expresses a requested
minimum. A trusted runtime policy may strengthen it and cannot be weakened by
the plan. The first stock production adapter is the offline
`github_artifact_attestation_v1` verifier defined by ADR 0033. Its closed v2
policy pins repository, signer workflow and commit, predicate, issuer,
self-hosted-runner denial, trusted root, and GitHub CLI security range. Runtime
derives one immutable bundle key from the pinned release/subject digests and
performs no listing, `current` resolution, GitHub API call, or token-based
lookup. Production without complete verifier material still fails before
ready-state publication and never downgrades to checksum-only success.

ConfigMap names are selectors, not integrity evidence. The init process acquires
one bounded snapshot of each selected file, verifies the SHA-256 of those exact
bytes, and parses those same bytes. A mismatch fails before registry I/O.
Changing configuration requires a new reviewed ConfigMap snapshot and v2
deployment identity; in-place mutation is not a rollout mechanism.

Reserved pod storage is fixed:

| Volume | Init mount | Base mount |
| --- | --- | --- |
| `dpone-fetched-artifacts` | RW `/var/lib/dpone/artifacts` | RO `/var/lib/dpone/artifacts` |
| `dpone-worktree` | RW `/workspace/repo` | RO `/workspace/repo` |
| `dpone-run-output` | absent | RW `/var/lib/dpone/run` |
| `dpone-artifact-registry-config` | RO `/etc/dpone/artifact-registry` | absent |
| `dpone-artifact-trust-policy` | RO `/etc/dpone/artifact-trust` when selected | absent |

The provider rejects collisions with image, command, arguments, namespace,
service account, security context, init containers, reserved volumes, and
reserved mounts. Scheduling and resource fields remain the bounded extension
surface.

Strict scheduler extensions are carried only by the closed
`dpone.airflow-provider-execution.v1` projection inside the workload pack.
The projection contains bounded task identity/labels/environment plus
scheduling fields and the single base-container resource request. It is part
of the canonically derived whole-pack fingerprint. Producer, materializer,
provider, runtime init and base launcher recompute that identity instead of
comparing copied claims. Legacy top-level `kpo_kwargs`, `pod_spec` and
`runtime_command` remain available to the explicit raw/local compatibility
lane, but the indexed v2 provider never treats them as scheduler authority.
A missing projection requires an immutable pack/release/deployment rebuild;
unknown or reserved projection fields fail closed. This explicit split
replaces the earlier ambiguous behavior where the provider could silently
discard legacy executable fields.

The dependency-light provider package is the single structural authority for
this projection. Its constants produce the JSON Schema consumed by the root
GitOps schema catalog and constrain the executable validator; independently
maintained schema copies are forbidden. Structural boundaries are covered by
schema/runtime parity tests.

Logical workload identity is not a Kubernetes label contract. The complete ID
remains authoritative in the pack, DAG spec, artifact identity and evidence.
The shared provider contract derives a Kubernetes-safe label value: an already
valid value of at most 63 characters is preserved, while every other value uses
a bounded readable prefix plus a SHA-256 suffix. The complete ID is placed in a
provider-owned pod annotation. Producer and provider must use this same
projection, preventing admission-time failures and prefix-truncation
collisions without breaking existing short IDs.

### Bounded resources and explicit service-file publication (2026-09-10)

Workload authoring exposes `airflow.resources` (`gitops.airflow.resources` in
manifests) for CPU, memory and ephemeral-storage requests/limits. The
dependency-light provider owns their shared quantity/structural validator;
canonical core authoring policy composes it without importing an Airflow or
Kubernetes SDK. The compiler validates and projects these values before pack
identity is computed. Resource-only changes also enter authoring semantic
identity so selective builds cannot omit them. Absent settings preserve legacy
identity and Pod defaults. Existing v1 extended resource names retain bounded
structural compatibility; the new authoring surface admits only three names.

Runtime and separate-hook base containers inherit the same resources. No
arbitrary Pod override is enabled; resource-capable overrides are rejected
before strict rewriting can discard them. Init-fetch and sidecar resource
policy remain unchanged. The resource guide specifies exact quantity bounds,
comparison, precedence and upgrade behavior.

Verified execution derives XCom publication independently from the child exit
policy. Runtime/dbt publish through the KPO-provided writable XCom volume;
pre-hooks use the existing writable run emptyDir and never require XCom path
access. Canonical runtime helpers prepare service files and report safe OS
failure stage/type/errno. Workload/artifact mounts remain read-only, hook child
failures still block runtime, and no security-context authority is added.
See [resources](../airflow-workload-resources.md) and
[startup diagnostics](../airflow-runtime-startup-diagnostics.md).

### Record the current trusted computing base

For this increment, the trusted computing base includes:

- the producer and immutable release/deployment identity process;
- the promoted local cache snapshot and `airflow-index.json`;
- the Airflow scheduler/DAG processor and installed dpone provider code;
- the Kubernetes control plane and kubelet enforcing the admitted pod;
- the digest-pinned runtime image and its dpone CLI/runtime code;
- workload-identity/IAM enforcement, the selected registry adapter, and the
  configured attestation verifier and policy snapshot.

The scheduler, provider, and local cache are explicitly trusted. This decision
does not claim resistance to their compromise. A malicious scheduler or
provider can submit a different pod or plan, and a malicious trusted cache can
change the input accepted by that provider. The fixed KPO contract narrows
pack-owned and ordinary override authority inside this TCB.

## Consequences

Positive consequences:

- v1 compatibility and v2 executable behavior cannot be confused;
- incomplete or unsupported delivery fails before partial DAG installation;
- the base container cannot use legacy scheduler-copied shell or inline archive
  fields on the strict path;
- configuration, artifacts, runtime image, and workload identity are bound to
  one reviewable deployment contract;
- init failure occurs before the base workload, data I/O, XCom success,
  checkpoint mutation, or source-state advance.
- successful pod cleanup is requested without reintroducing base-log polling;
  failed or orphaned pod retention remains an explicit infrastructure concern.

Costs and limitations:

- existing v1 `init_fetch` indexes must be regenerated, published,
  materialized, and promoted under a new immutable deployment identity;
- producer, provider, and runtime image versions must understand the same v2
  deployment-index wire; newly generated tasks additionally require provider
  and runtime support for plan v3;
- production remains fail-closed until a concrete verifier is configured;
- compromised-scheduler resistance would require a separate admission,
  signing, or policy-enforcement design outside the current TCB.
- `delete_succeeded_pod` does not guarantee retention for every task failure or
  deletion of every successful pod; cancellation, late failures and Kubernetes
  cleanup errors require external observation and stale-pod governance.

## Evidence and certification status

Current unit and contract tests exercise v1/v2 mode policy, canonical plan
transport, fixed pod composition, snapshot digest mismatch, ready-manifest
publication, deployment/release receipt verification, post-init checksum and
attestation-downgrade rejection, and shell-free argv preparation.

Live Kubernetes projected volumes, workload identity, production attestation,
Vault integration, MSSQL, and ClickHouse execution are `UNVERIFIED` for this
decision unless current evidence from the exact release commit and environment
is attached. Local or mocked tests are not live certification.

## Related

- [Approved executable init-fetch feature design](../feature-design-airflow-kpo-init-fetch-execution-v0731.md)
- [ADR 0008: Airflow parse-safe provider](0008-airflow-parse-safe-provider.md)
- [ADR 0009: Artifact delivery and cache materializer](0009-artifact-delivery-and-cache-materializer.md)
- [Provider and cache migration guide](../airflow-provider-cache-migration.md)
- [Airflow cache sync and recovery runbook](../airflow-cache-sync.md)
