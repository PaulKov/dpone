# Formal Airflow provider and lightweight pack reader

Purpose: install and operate the parse-safe dpone provider in Airflow without
pulling connector/runtime dependencies into the control plane.

Audience: Airflow platform engineers, DAG repository maintainers, and
operators diagnosing provider parse failures.

Install `apache-airflow-providers-dpone` in Airflow scheduler, DAG processor,
API server and worker environments. It owns provider discovery and the typed
`airflow.providers.dpone` namespace. Its dependency `dpone-airflow-pack` is the
Airflow-independent static pack reader and DAG construction library.

It is intentionally separate from the full dpone runtime:

| Layer | Package | Responsibility |
| --- | --- | --- |
| Airflow scheduler / DAG processor / API server | `apache-airflow-providers-dpone` + `dpone-airflow-pack` | Discover the provider, parse static packs, read bounded cache and build visible KPO/outcome tasks. |
| KPO runtime pod | `dpone[full,accel]` | Execute manifests, hooks, SQL transforms, transfer, lineage, DQ, audit and cleanup. |

## Install a supported scheduler image

Use a tested cell from the [Airflow compatibility matrix](compatibility.md#airflow-provider-compatibility).
This executable example selects the primary Python 3.12 / Airflow 3.2.0 /
Kubernetes provider 10.14.0 cell. Following
[Apache Airflow's installation guidance](https://airflow.apache.org/docs/apache-airflow/stable/start.html),
use the official constraints file only for the Airflow installation, then pin
Airflow again while adding the dpone provider:

```bash
AIRFLOW_VERSION=3.2.0
PYTHON_VERSION=3.12
DPONE_VERSION=X.Y.Z
AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

python3 -m pip install \
  "apache-airflow[cncf.kubernetes]==${AIRFLOW_VERSION}" \
  --constraint "${AIRFLOW_CONSTRAINTS_URL}"
python3 -m pip install \
  "apache-airflow==${AIRFLOW_VERSION}" \
  "apache-airflow-providers-cncf-kubernetes==10.14.0" \
  "apache-airflow-providers-dpone==${DPONE_VERSION}"
python3 -m pip check
python3 -c "from airflow.providers.dpone import load_dpone_dags; print(load_dpone_dags.__module__)"
```

For the supported Airflow 2 path, use a tested compatibility cell rather than
editing only the major version in the preceding commands. This example pins
Airflow 2.10.5 and Kubernetes provider 10.1.0:

```bash
AIRFLOW_VERSION=2.10.5
PYTHON_VERSION=3.12
KUBERNETES_PROVIDER_VERSION=10.1.0
DPONE_VERSION=X.Y.Z
AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

python3 -m pip install \
  "apache-airflow[cncf.kubernetes]==${AIRFLOW_VERSION}" \
  --constraint "${AIRFLOW_CONSTRAINTS_URL}"
python3 -m pip install \
  "apache-airflow==${AIRFLOW_VERSION}" \
  "apache-airflow-providers-cncf-kubernetes==${KUBERNETES_PROVIDER_VERSION}" \
  "apache-airflow-providers-dpone==${DPONE_VERSION}"
python3 -m pip check
python3 -c "from airflow.providers.dpone import load_dpone_dags; print(load_dpone_dags.__module__)"
```

Airflow 2 uses scheduler/DAG-processor terminology and stable API v1; Airflow 3
uses `dagProcessor`, `apiServer`, and API v2. The provider/cache contract is the
same, while component placement and REST authentication follow the installed
Airflow major version.

Set `DPONE_VERSION` to an already published release. For an unreleased
candidate, use the CI artifact or build all matching wheels from one frozen
checkout, then verify and install their exact paths:

```bash
test "$(cat dist-airflow/SOURCE_COMMIT)" = "$(git rev-parse HEAD)"
(cd dist-airflow && sha256sum --check SHA256SUMS)

python3 -m pip install \
  dist-airflow/dpone_airflow_pack-*.whl \
  dist-airflow/apache_airflow_providers_dpone-*.whl
python3 -m pip check
```

`dist-airflow/SOURCE_COMMIT` and `SHA256SUMS` are emitted by the Airflow
compatibility workflow. Source documentation never assumes a candidate version
already exists on PyPI.

The provider distribution declares the compatible `dpone-airflow-pack`
dependency. Resolve the image once, record the exact installed versions and
wheel digests, then use that immutable image in
every Airflow component that imports DAG files. Keep the authoring environment
and KPO runtime image separate: authoring needs base `dpone`, while connector
extras and native clients belong in the runtime pod.

## Recommended DAG import

```python
from airflow.providers.dpone import load_and_acknowledge_dpone_dags

index_path = "/opt/airflow/.dpone-cache/current/airflow-index.json"
loaded = load_and_acknowledge_dpone_dags(
    globals(),
    index_path=index_path,
    ack_path="/opt/airflow/.dpone-ack/loader-ack.json",
    ack_root="/opt/airflow/.dpone-ack",
)
if loaded.report.fatal:
    error_code = (
        loaded.report.errors[0].get("code", "DPONE_AIRFLOW_INDEX_INVALID")
        if loaded.report.errors
        else "DPONE_AIRFLOW_INDEX_INVALID"
    )
    raise RuntimeError(
        f"{error_code}: dpone Airflow deployment index could not be loaded"
    )
```

The loader holds one shared cache lease through parse and acknowledgement. The
acknowledgement write is local, bounded to 64 KiB, and atomic. It records
only release/deployment IDs, the index digest captured by the completed parse,
the UUIDv4 `activation_id` captured with `current`, loaded/skipped DAG IDs,
bounded error codes, and a timestamp. It never calls Airflow Variables,
Connections, the metadata database, object storage, Vault, or Kubernetes.
It does not reopen `current/airflow-index.json`, so a concurrent activation
cannot bind the acknowledgement to a different deployment snapshot.
Infrastructure may publish this local evidence after parse and require it
before declaring a deployment converged. A missing or stale acknowledgement
must not be replaced by DAG visibility alone because unchanged DAG IDs can
still refer to older SerializedDAG rows.
The machine-readable contract is
[`dpone.airflow_loader_ack.v2`](schemas/gitops/airflow-loader-ack-v2.schema.json).
Historical v1 acknowledgement remains readable for migration diagnostics, but
it cannot prove an exact activation occurrence and must not authorize
convergence or destructive retention.

On Kubernetes, mount the canonical cache read-only in the parser container and
mount `/opt/airflow/.dpone-ack` read-write. Passing `ack_root` confines the ACK
directly to that separate volume without granting mutation rights over verified
cache bytes. Omitting `ack_root` preserves the compatibility destination under
`<cache>/status`, but that topology is not recommended for a shared Airflow
deployment.

The cache controller may additionally pass a bounded desired-state file and
its expected digest to `dpone airflow cache-sync`. That precommit guard is
checked under the promotion lock immediately before `current` changes. It
prevents bytes from changing during one pod-local activation transaction;
deployment-ID CAS protects the active local cache independently.

The canonical remote source is one conditionally updated object-storage
desired-state document. A one-shot init container restores an empty pod-local
cache, and a watcher sidecar observes later desired-state revisions. Both stage
and verify a complete local snapshot before invoking the existing materialize
and promotion commands. The provider never runs this synchronization and never
performs remote I/O during DAG parsing. Cluster convergence is proved
separately through authoritative loader acknowledgement and Airflow REST
evidence. See [ADR 0033](adr/0033-airflow-s3-desired-state-pull.md).

For legacy provider imports, generated loader upgrades, or the mutable
`latest/pack-index.json` cache, follow the
[provider and cache migration guide](airflow-provider-cache-migration.md).

## Indexed delivery boundary after v0.73.1

The published v0.73.1 line is the compatibility baseline. The approved
hardening patch after v0.73.1 introduces a separate executable v2 index; do not
hand-edit a v0.73.1 v1 index and assume it has acquired v2 guarantees. Confirm
that the producer, scheduler provider, and runtime image all support the same
v2 contract before promoting it.

The provider evaluates the wire and delivery mode before adding any DAG to the
module:

| Index wire | Mode | Provider behavior |
| --- | --- | --- |
| `dpone.airflow-deployment-index.v1` | `local_preview` | Preserve the non-runnable local preview lane. |
| v1 | `init_fetch` | Fatal `DPONE_RUNTIME_ARTIFACT_DELIVERY_MIGRATION_REQUIRED`; regenerate and promote a v2 deployment. |
| `dpone.airflow-deployment-index.v2` | `init_fetch` | Build the strict executable KPO path. |
| v2 | Any other known mode | Fatal `DPONE_RUNTIME_ARTIFACT_DELIVERY_MODE_UNSUPPORTED`. |
| Either | Unknown or malformed mode | Fatal index field/schema error. |

The executable path is:

```text
producer -> immutable release/deployment -> airflow-index.json (schema v2)
-> trusted local cache -> lightweight provider -> KPO
-> runtime-init-fetch -> runtime-fetch-ready.json
-> runtime-pack-exec -> verified workload argv
```

V2 requires an exact OCI image reference whose digest matches
`runtime_image_digest`, positive byte counts and SHA-256 for the exact
release/deployment/workload artifacts, workload identity, and digest-pinned
registry/trust-policy ConfigMap references. `trust_tier` is explicit at both
index and delivery levels and must match. `production` requires
`trust_policy_ref` plus `attestations: required_for_prod`; missing, invalid,
expired, or digest-mismatched v2 policy and attestation inputs fail before
registry payload extraction.

Every strict-v2 workload pack also declares
`pack_identity.schema: dpone.airflow-pack-identity.v1`. Its
`pack_fingerprint` is derived from the complete canonical JSON pack, excluding
only the self-reference and the fixed top-level advisory metadata allowlist.
The producer, deployment materializer, provider and runtime independently
derive that fingerprint. Comparing the index claim with a copied pack claim is
not verification. A legacy pack without this identity remains readable only
through the explicit local/raw compatibility lane and cannot become an
executable strict-v2 deployment.

The stock runtime supports two explicit production authorities. The existing
GitHub Artifact Attestations adapter consumes
`dpone.runtime-artifact-trust-policy.v2` for the exact `release-set.json`.
The deployment-scoped Cosign adapter consumes
`dpone.airflow-deployment-trust-policy.v1` for the exact Airflow release,
deployment, index and runtime-image subject. Exactly one authority is allowed;
the runtime rejects a dual-authority configuration before registry I/O. Both
paths verify offline from pinned policy bytes and have no checksum-only bypass.
This implemented verifier keeps strict-v2 execution fail-closed; it does not
certify a particular installation. Live Kubernetes, workload identity, Vault,
MSSQL, ClickHouse, and Airflow execution remain `UNVERIFIED` without current
evidence from the exact commit, images, and environment.

The provider creates a canonical plan of at most 16 KiB, carries it in
`DPONE_INIT_FETCH_PLAN_B64`, and pins its digest in
`DPONE_INIT_FETCH_PLAN_SHA256` and the pod annotation. It replaces pack-owned
execution fields with:

```text
init container: dpone airflow runtime-init-fetch
base container: dpone airflow runtime-pack-exec
```

New strict tasks emit `dpone.airflow-runtime-init-fetch-plan.v3`. The plan
binds an explicit `workload|process` execution scope, the exact process
selector, and whether hooks execute inside the runtime task (`inline`) or in
separate Airflow tasks (`externalized`). `process_selector: null` means the
default process only when `scope: process`; it no longer doubles as the
whole-workload signal. Whole-workload execution is explicit and always
externalizes hooks; it selects `runtime_bootstrap.commands.__workload__`, whose
argv has no `--selector`. The provider installs that path only after proving
that top-level hook tasks cover every process-local separate hook; otherwise
DAG construction fails with
`DPONE_INIT_FETCH_HOOK_OWNERSHIP_INCOMPLETE`.

Runtime verifies the selected process against the fetched pack, selects the
same process-keyed bootstrap command, and removes any inherited
`DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS` value for inline execution. Separate hook
tasks use a structured `dpone.airflow-pre-hook-command.v1` entry that binds the
step name, canonical hook ID, process selector and exact argv. The display
`command` is not an execution authority, but during a v1/v2 bridge its tokens
must agree with the structured argv. Runtime plan v1/v2 rollback remains
supported only when the fetched pack has exactly one selector-coherent process
plan. Every legacy multi-process pack fails before source I/O. Whole-workload
hook ownership compares the exact argv, selector, dependencies and execution
policy; matching labels are insufficient. Pre-hook failures propagate their
real child exit code to Airflow.

### Roll out and roll back v3 without a mixed-version window

Provider `0.73.32` emits v3 plans. Runtime `0.73.32` reads v1, v2 and v3;
runtime `0.73.28` and earlier do not read v3. Treat these as one coordinated
package set:

| Provider | Runtime | Plan | Result |
| --- | --- | --- | --- |
| `<=0.73.28` | `>=0.73.29` | v1/v2 | Bridge only for exactly one valid process plan and unique structured hook commands; every multi-process pack fails closed |
| `>=0.73.29` | `>=0.73.29` | v3 | Supported target state |
| `>=0.73.29` | `<=0.73.28` | v3 | Forbidden; runtime cannot decode the plan |
| `<=0.73.28` | `<=0.73.28` | v1/v2 | Previous exact rollback state |

Forward rollout:

1. Build and verify the new packs, but do not activate them.
2. Pause affected generated schedules for the bounded compatibility window.
3. While the provider is still `<=0.73.28`, activate the exact new pack release
   with runtime image `0.73.32`. The old provider ignores the optional
   v3 execution fields and emits v1/v2 plans; the new runtime accepts a
   generated hook only when its structured command is unique and agrees with
   the legacy command. Every legacy multi-process workload remains paused.
4. Upgrade scheduler, DAG processor and API server to provider/pack `0.73.32`;
   wait for serialized-DAG convergence.
5. Run the hook freshness smoke against the already active exact deployment,
   then unpause schedules.

Rollback:

1. Downgrade the provider/pack first while runtime `0.73.32` still accepts the
   resulting v1/v2 plans.
2. Wait for serialized-DAG convergence.
3. Activate the previous exact deployment, including its previous runtime
   image and release bytes.

Never activate an old-runtime deployment while a v3-emitting provider is
active. Never use floating `latest` references for either direction.

Both containers use the exact digest-pinned image. The fixed storage contract is:

| Volume | Init | Base |
| --- | --- | --- |
| `dpone-fetched-artifacts` | RW `/var/lib/dpone/artifacts` | RO `/var/lib/dpone/artifacts` |
| `dpone-worktree` | RW `/workspace/repo` | RO `/workspace/repo` |
| `dpone-run-output` | absent | RW `/var/lib/dpone/run` |
| `dpone-artifact-registry-config` | RO `/etc/dpone/artifact-registry` | absent |
| `dpone-artifact-trust-policy` | RO `/etc/dpone/artifact-trust` when selected | absent |

Image, commands, arguments, namespace, service account, security context,
reserved volumes/mounts, and init containers are provider-owned. Pack
scheduling and resource fields remain the bounded extension surface. The pack
must carry the exact `dpone.airflow-provider-execution.v1` projection and a
digest-pinned XCom sidecar image; missing, unknown or execution-owning fields
fail before DAG installation. See the
[v2 wire ADR](adr/0024-airflow-executable-init-fetch-wire-boundary.md), the
[runtime operator runbook](airflow-cache-sync.md#operate-strict-v2-init-fetch-pods),
and the [migration procedure](airflow-provider-cache-migration.md#migrate-v1-init-fetch-to-v2).

Strict runtime pods also receive the provider-owned labels
`dpone.dev/managed-by=airflow-provider` and
`dpone.dev/runtime-contract=init-fetch-v2`. New pack producers must omit these
keys. A historical v1 pack that already contains either key remains readable,
but its value is ignored and replaced by the provider, so pack input can never
change the effective maintenance authority.

The current TCB includes the producer, promoted cache/index, scheduler/DAG
processor, provider, Kubernetes control plane and kubelet, runtime image,
workload-identity enforcement, registry adapter, and configured verifier. The
fixed composition prevents an ordinary pack override; it does not claim
resistance to a compromised scheduler, provider, or local cache. Live
Kubernetes, workload identity, attestation, Vault, MSSQL, and ClickHouse remain
`UNVERIFIED` without current evidence from the exact commit and environment.

`DponeDag.from_spec(...)` and `DponeTaskGroup.from_pack(...)` remain the hybrid
escape hatches. Direct `dpone_airflow_pack` loaders are legacy pack-generation
compatibility paths, not the release/deployment delivery API:

```python
from dpone_airflow_pack import load_dpone_airflow_pack_with_provenance

pack, provenance = load_dpone_airflow_pack_with_provenance("cached://inter_ch_example_customer_directory")
```

`provenance` includes the cache source, generation, pack checksum, index checksum and last sync status. This is the
right evidence to serialize into Airflow task metadata. KubernetesExecutor worker pods do not need to mount the same
cache volume as the DAG processor.

A direct file-path/raw-pack `DponeTaskGroup.from_pack(...)` call has no v2
delivery context and cannot claim strict `init_fetch`. A deployment-scoped
`cached://` workload resolved through a v2 `index_path` receives that index's
immutable delivery context and uses the strict path.

Environment deployment promotion uses the separate content-addressed
release/deployment cache contract. Platform operators should follow the
[Airflow cache sync and recovery runbook](airflow-cache-sync.md) for pinned
release verification, current-pointer promotion, structured checksum errors,
and actor/CAS-bound recovery after a partial local write. Promotion uses a local
inter-process lock and activates the relative `current` symlink only after
pointer/audit authorization. Provider parsing never performs that sync,
locking, or recovery work. The deployment-index reader requires canonical
lowercase `sha256:<64 hex>` release, deployment, binding, registry, credential
runtime, image, and cached-reference pin identities, matching the public JSON
Schema exactly.

## GitOps Domain Groups

DAG wrappers should not duplicate workload lists when the same order is already declared in GitOps catalogs. Keep
repeatable orchestration groups in the domain YAML:

```yaml
workflow_groups:
  daily_datamarts:
    workload_ids:
      - dim_customer
      - fact_order
      - fact_order_history
```

Then resolve the ordered list with the lightweight provider:

```python
from dpone_airflow_pack import workload_ids_from_gitops_domain

workload_ids = workload_ids_from_gitops_domain(
    repo_root,
    "sales",
    group="daily_datamarts",
)
```

The resolver is scheduler-safe: it reads only local YAML files, checks that referenced workloads exist in the same
domain, and has no dependency on full `dpone`, database drivers, Kubernetes clients or cloud SDKs. This keeps the
Airflow wrapper thin while making GitOps catalogs the single source of truth for workload membership.

`dpone.airflow.*` remains a compatibility shim when full `dpone` is installed.
Scheduler-internal library helpers may use `dpone_airflow_pack` directly so the
lightweight package stays independent from source/sink runtime dependencies.
DAG files and provider-facing code use only `airflow.providers.dpone`.

Provider-facade imports from `dpone_airflow_pack` are deprecated compatibility
exports. They remain available for at least two minor releases and 365 days
after the `0.72.0` announcement, whichever is later, and warn at most once per
process. New DAG code always imports `airflow.providers.dpone`.

## Declarative loader and exact ACK

For YAML-only DAG repos, prefer the canonical loader:

```python
from airflow.providers.dpone import load_and_acknowledge_dpone_dags

loaded = load_and_acknowledge_dpone_dags(
    globals(),
    index_path="/opt/airflow/.dpone-cache/current/airflow-index.json",
    ack_path="/opt/airflow/.dpone-ack/loader-ack.json",
    ack_root="/opt/airflow/.dpone-ack",
)
if loaded.report.fatal:
    error_code = (
        loaded.report.errors[0].get("code", "DPONE_AIRFLOW_INDEX_INVALID")
        if loaded.report.errors
        else "DPONE_AIRFLOW_INDEX_INVALID"
    )
    raise RuntimeError(
        f"{error_code}: dpone Airflow deployment index could not be loaded"
    )
```

The combined loader materializes DAG objects from fingerprinted dag-spec artifacts and
compact packs in the bounded cache. It skips `dag_id`s already present in
`globals()` for collision-safe migration. Invalid specs use `skip_and_report`
by default; paused placeholder DAGs tagged `dpone_spec_error` require the
explicit `create_diagnostic_dag` platform policy.

The index itself is a shared trust boundary. The provider reads at most 8 MiB
before JSON parsing; a missing, unreadable, oversized, malformed, or
contract-invalid index returns `LoadReport(fatal=True)` under the default
policy. The generated loader guard above raises so Airflow cannot present an
empty parse as success. `invalid_dag_policy="fail_all"` raises the original
`AirflowDeploymentIndexError` directly.

Listed artifacts are isolated after the index is accepted. Each DAG spec is
read once and checked against its declared byte size and SHA-256 before parsing;
each workload pack is checked again at its final task-building boundary. One
missing, oversized, size-mismatched, or checksum-mismatched artifact is reported
with its `dag_id` while unrelated valid DAGs continue under `skip_and_report`.
The default artifact read ceiling is 64 MiB. Restore or rematerialize immutable
content from the trusted release; never edit a listed cache file in place. See
[Provider API: fatal index errors and recovery](airflow-provider-api.md#fatal-index-errors-and-recovery).

Producer outlets are declared under `airflow.execution.outlets` in workload
catalogs. Consumer DAGs may use dag-spec `schedule.assets` or manifest
`airflow.execution.inlets`. Build plane resolves cross-workload lineage; review
with:

```bash
: "${DPONE_WORKLOAD_SET:?set the repository-relative workload-set path}"
dpone gitops airflow deps --workload-set "${DPONE_WORKLOAD_SET}" --format md
```

See [Airflow self-service](airflow-self-service.md).

## Bounded backfill mapping

Generated packs may contain a `dpone.airflow-mapping-plan.v1` for one chunked
backfill process. `internal` preserves the existing concrete runtime task.
`visible` and `summary` use a static list with
`KubernetesPodOperator.partial(...).expand(env_vars=...)`; no upstream discovery
task or scheduler-side planner is created.

The provider validates the complete mapping plan before constructing an
operator: canonical fingerprint, ordered coverage, item count, pool,
`max_active_tis_per_dag`, and the 200-item hard ceiling. Provider-owned pool and
concurrency values cannot be replaced by pack overrides. Each mapped task gets
one bounded `DPONE_AIRFLOW_MAPPING_ITEM` value plus the same pinned composite
run identity. The item contains only indexes and digests, never predicates,
credentials or row data.

Mapped pack bytes use `mapped_kpo_kwargs` as a compatibility tripwire. This
provider validates the plan and normalizes the field internally; providers
released before bounded mapping fail their existing missing-`kpo_kwargs` check
before creating any task. Do not hand-edit an immutable pack to rename the
field.

Mapped tasks validate their own outcome inline. A shared downstream outcome
gate is intentionally omitted because it cannot represent every map index
consistently across the supported Airflow lines. Hooks remain static. A
provider without compatible `partial`/`expand` support fails with
`DPONE_AIRFLOW_MAPPING_UNSUPPORTED`; it never silently falls back to an
unbounded or whole-campaign task.

This feature preserves the parse-side-effect contract: only the verified local
deployment index and listed artifacts are read. Exact construction tests run
against Airflow 2.10.5, 2.11.0, 3.2.0 and 3.3.0 in the provider compatibility
workflow. Runtime multi-pod certification remains a separate route/state
evidence requirement.

## Composite run identity

For every index-backed workload task, the provider derives one bounded
`dpone.airflow-run-identity.v1` object from the already verified local index,
DAG spec, and workload pack. It attaches the DAG-level context for diagnostics,
stores the workload-specific value in task params, and passes canonical JSON in
`DPONE_AIRFLOW_RUN_IDENTITY` to the runtime pod.

The identity pins release, deployment, DAG spec, workload pack, runtime image,
binding-set, connection registry, credential runtime, and Airflow DAG Bundle.
It remains immutable and unchanged across reactivation. Exact-cache executions
also carry a separate `dpone.airflow-deployment-identity.v1` value through
`DPONE_AIRFLOW_DEPLOYMENT_IDENTITY`. Its UUIDv4 `activation_id` distinguishes a
later reactivation or rollback of byte-identical release and deployment artifacts.
The read-only exact-cache status and pack provenance expose the same
`activation_id`, so diagnostics and acceptance consumers do not need a second
index parser.
Both readers hold the cache shared read lease across the complete observation:
pointer, `current` symlink, index, and pack checksum. Promotion holds the
exclusive lease. A diagnostic can therefore report the old committed
activation or the new committed activation, but never a pointer from one and a
pack from the other.
The writer initializes the metadata-only `.promotion.lock` with mode `0664`;
readers open that existing inode with `O_RDONLY` and never create or modify it.
This supports scheduler/dagProcessor consumers that mount the cache read-only or
run under a different UID. Roll out one writer materialization before upgrading
readers. An existing cache root without the lock fails visibly with
`DPONE_CACHE_READ_LEASE_FAILED`; it is not silently read without serialization.
The compatibility sync downloads remote bytes into a unique temporary
generation before taking the exclusive publish lease. Network latency therefore
does not hold the scheduler parse lease; only the bounded local activation and
pruning phase excludes readers.
If the cache root is absent when a reader enters the lease, the reader freezes
that absence for the whole operation and returns an explicit cache-missing
diagnostic without inspecting paths that may appear concurrently. The next
parse cycle can observe the newly committed cache; the first cycle cannot mix
an absent pre-state with a partially published first activation.

The activation occurrence is read from the atomically switched
`current-pointer.json`, never from a similarly named field inside
`airflow-index.json`. The pointer and index deployment IDs must match. A v2
cache without a valid pointer activation is blocked; a historical v1 cache
remains readable but cannot prove an exact activation occurrence.
For desired-state delivery, the pointer value is the signed promotion
`source.occurrence_id`, reused by every pod-local cache participating in that
rollout. This keeps dagProcessor, serialized DAG and runtime XCom evidence on
one global occurrence despite Kubernetes projection and parse convergence
being eventual. Standalone local cache promotion generates its own UUID.
It contains no secret values, Vault paths, signed URLs, or authorization
material. Operator overrides cannot replace the provider-owned value, and the
provider verifies the pack checksum again after reading it to close a local
index/file replacement race.

Runtime XCom preserves both objects under `run_identity` and
`deployment_identity`; `gitops.airflow_evidence_bundle` preserves immutable
run identity as before.
Conflicting observations or an observed pod image mismatch fail closed with
`DPONE_AIRFLOW_RUN_IDENTITY_MISMATCH`. Release-policy evidence without the
identity fails with `DPONE_AIRFLOW_RUN_IDENTITY_MISSING`; older advisory evidence
remains readable with an explicit warning.

The separate deployment identity is optional for backward compatibility with
historical execution. Deployment-bound CI acceptance must require it and compare
it with the convergence occurrence; serialized DAG tags alone prove what Airflow
currently exposes, not what a completed runtime pod executed.

When the provider has an exact deployment expectation, a passed outcome gate
requires the same canonical identity in runtime XCom. Provider-produced attempt
evidence binds the envelope and XCom identities byte-for-byte, and terminal dbt
workflow evidence validates that its release and deployment IDs agree with the
embedded deployment identity. Missing, malformed, or mismatched identity is a
blocker even if every Airflow task state is `success`. Legacy outcomes without an
exact provider expectation remain readable and are never promoted as exact
deployment proof.

On Airflow 2, the terminal workflow task reads the ORM task-instance states for
its own DAG run. Airflow 3 Task SDK deliberately does not expose that ORM
method, so an otherwise unobserved expected terminal task is resolved from the
exact `dpone_outcome_gate_result` XCom written by that task in the same DAG run.
The outer XCom verdict and its embedded result verdict must both be booleans and
must agree. Missing, malformed, contradictory, or failed receipts remain
fail-closed; the fallback never replaces an ORM-observed failure or mapped task
state. This distinction prevents a successful Airflow 3 outcome gate from being
reported as `missing` without weakening terminal failure handling.

### Launch pin for separate `outcome_gate` (Exact-cache tip flips)

Long KubernetesPodOperator attempts can outlive an Exact-cache promote. A
deferred separate `outcome_gate` must not re-bind expectations from the live tip
while the pod still executes the launch-time activation.

Guarantee scope (**B** — full launch envelope):

- `build_pod_request_obj` persists the full envelope on the pod request
  (annotation `dpone.airflow/runtime-launch-envelope.v1` plus base-container
  env for `run_identity`, `deployment_identity`, and
  `expected_runtime_evidence_sha256`).
- After `get_or_create_pod` selects the concrete pod, if `metadata.uid` is
  missing (common for newly created pods when KPO returns the local request),
  the operator re-reads the exact namespace+name via Kubernetes before
  committing the pin. Reattach paths that already carry a UID skip the read.
- **Authority C:** the immutable launch envelope lives on the Kubernetes pod
  (annotation `dpone.airflow/runtime-launch-envelope.v1` plus base-container
  env; both must agree when present). The pin records the pod ref
  (`pod_namespace` / `pod_name` / `pod_uid`) and `kubernetes_conn_id`.
  `outcome_gate` re-fetches that pod via the same Connection/cluster authority
  as KPO, verifies UID, rebuilds the envelope digest, and requires
  `rebuilt.pin_sha256 == pointer.pin_sha256` plus matching subject coordinates.
  Operator parse-time tip values are not used as per-field fallbacks for a
  `PRESENT` pin.
- **Backend selection:** `launch_pin_store.backend` or the platform environment
  `DPONE_LAUNCH_PIN_STORE_BACKEND` selects `kubernetes_configmap` (the
  compatibility default) or `airflow_task_state`. Selection is frozen once
  into runtime, gate, and cleanup arguments; pack-local configuration wins over
  the environment. There is no automatic fallback. Unknown values and
  backend/authority contradictions are `PIN_INVALID`.
  `airflow_task_state` requires Airflow 3.3+ and
  `apache-airflow-providers-cncf-kubernetes>=10.20`, forces KPO
  `durable=True`, and requires the public `task_state_store` context accessor.
  KPO owns the stable `pod_identifier` value. dpone reads it after
  `get_or_create_pod`, requires its namespace/name to equal the selected Pod,
  hydrates the server UID, and transfers only a bounded occurrence claim in
  XCom. The gate re-fetches namespace/name/UID and rebuilds the immutable
  launch envelope and pin digest before accepting the claim. This backend
  performs no Secret, ConfigMap, Lease, DaemonSet, Variable, or direct Airflow
  metadata-database writes. Missing provider capability, task state, Pod, UID,
  or any mismatch fails closed.
- **Pod lifetime:** pin lifecycle enables only when a runtime task owns a
  separate `outcome_gate` **and** `launch_pin_required(pack)` is true
  (`pin_enabled = separate_outcome_gate and launch_pin_required(pack)`).
  Legacy/non-exact repository packs with `required=false` keep separate
  `outcome_gate` evaluation but perform **zero** pin Kubernetes I/O (no
  `keep_pod` force, no barrier, no ConfigMap, no cleanup task). When pin
  lifecycle is on, the provider force-closes `on_finish_action=keep_pod`.
  `kubernetes_configmap` injects init `dpone-launch-pin-barrier` (python3
  required; waits until **both** subject head and per-try ConfigMaps are
  `ACTIVE` for matching try / uid / `pin_sha256`, plus per-try
  `envelope_sha256` / subject coordinates; wget fallback removed), and on
  **pre-ACTIVE** CAS failure abandons the exact pod with termination
  verification. Post-ACTIVE failures are `PIN_RECOVERY_REQUIRED` and must not
  abandon the winner. `airflow_task_state` adds no barrier init container and
  composes with KPO's native durable reconnect state machine.
  After the gate consumes the envelope, `launch_pin_cleanup`
  (`trigger_rule=all_done`, teardown when available) deletes **only** the
  occurrence-bound handle (try / uid / pin_sha256 / pointer `resourceVersion` /
  frozen store coords). Both backends delete the exact Pod with a UID
  precondition. Only `kubernetes_configmap` deletes the matching per-try
  ConfigMap occurrence and marks the matching subject head `CONSUMED` (or
  exact-delete) — never a higher-try head.
  Cleanup rematerializes a failed gate result (including early evaluator
  failures that publish result XCom). If the gate never starts, the platform
  [runtime-pod-retention](airflow-runtime-pod-retention.md) sweeper provides
  bounded creation-age cleanup. Annotation
  `dpone.airflow/retain-for-outcome-gate=true` marks retained pods.
- Create-once CAS for the pod-ref pointer: injectable `InMemoryLaunchPinStore`
  (tests; phased locks matching production interleaving) or production
  `KubernetesConfigMapLaunchPinStore`. **Pre-activation subject-head CAS** is
  mandatory: (1) CAS head absent/predecessor → `CANDIDATE(try,uid,digest)`;
  (2) only the winner creates immutable per-try `CANDIDATE`; (3) verify
  per-try + head reservation; (4) per-try → `ACTIVE`; (5) CAS head exact
  `CANDIDATE` → exact `ACTIVE`. Head `409` is never ignored (readback +
  winner compare). Head write timeout/403/lost → `PIN_RECOVERY_REQUIRED`
  (barrier holds). `create_once` succeeds only when both head and per-try
  match `ACTIVE(self)`. Concurrent empty-head try1/try2 → exactly one dual
  ACTIVE winner (fake K8s API interleave test). Successor fencing: prior
  `ACTIVE` always blocks higher try (`PIN_RECOVERY_REQUIRED`; terminal/404
  insufficient); prior `CANDIDATE` + terminal/absent pod may be replaced.
  Same-pod CAS conflicts are idempotent winners. Malformed existing
  ConfigMaps are `PIN_INVALID` and never auto-replaced. Pack materializes
  one frozen store locator shared by runtime/barrier/gate/cleanup. Store/KPO
  authority is closed including `None`: store inherits KPO conn when absent;
  store conn + KPO in-cluster (`None`) is rejected; in-cluster requires a
  concrete namespace. Gate couples closed `summary.launch_pin_ref`
  (try/pod_uid/pin_sha256/envelope_sha256/pointer_resource_version; empty
  `{}` invalid) ↔ locator XCom ↔ exact per-try CM ↔ live pod. Deferrable
  KPO persists `launch_pin_ref` across worker re-entry via
  `defer(..., kwargs=)` and durable locator XCom rehydrate on a new
  operator instance (providers 10.1 / 10.14 / 10.20). `trigger_reentry`
  re-pulls `return_value` XCom when `do_xcom_push=true` and the CNCF provider
  returns `None` after pushing the sidecar summary (10.14 behaviour). Pre-ACTIVE
  per-try create failure (proved absent) releases own head `CANDIDATE` so try-2
  can admit; release status must land in `{consumed, already_consumed, deleted,
  absent}` or the path raises `PIN_RECOVERY_REQUIRED` (409 + own CANDIDATE gets
  one bounded retry with fresh resourceVersion). Per-try ConfigMap delete runs
  only after head closure is proven; `skipped_conflict` / `skipped_mismatch` /
  `cleanup_failed` skip per-try with `head_not_proven_closed`. Pod cleanup treats
  `already_absent` (404) as terminal success for the pod step. Cleanup after
  pod delete is head `ACTIVE(self)→CONSUMED` (+ readback) then per-try delete,
  with structured `pod_delete`/`head_transition`/`per_try_delete` status.
  `required=false` never opens Kubernetes. Production does **not** use the
  Airflow metadata DB and does **not** auto-fall back to in-memory CAS.
  Store miss with `required=true` is `PIN_MISSING`. Live K3d multi-worker CAS
  + RBAC evidence is **verified** (2026-08-11, merge commit
  `18570c11788088744e9eab2d33ab4fcfadf32224`; harness
  `tools/launch_pin_k3d_cert.sh`, receipt
  `test_artifacts/launch-pin-k3d-cert/receipt.json`). OSS review may proceed;
  tenant production activation of `launch_pin_required=true` still requires
  DEV/canary evidence on the consuming Airflow cluster before flipping prod
  required flags.
- Pod-only RBAC migration for `airflow_task_state`: upgrade the CNCF provider
  first while retaining `kubernetes_configmap`; then upgrade dpone; then set
  `DPONE_LAUNCH_PIN_STORE_BACKEND=airflow_task_state` consistently on DAG
  parsing and task-execution components. Prove a required exact-cache DAG,
  retry/deferrable re-entry, tip flip, and exact cleanup in DEV before PROD.
  Rollback removes the backend setting only after in-flight runs using the
  frozen backend complete; returning to `kubernetes_configmap` also requires
  its ConfigMap RBAC. See [ADR 0050](adr/0050-airflow-native-launch-pin-backend.md)
  and the [feature design](feature-design-airflow-native-launch-pin-backend-v0740.md).
- For `required=true`, `run_identity` and `deployment_identity` must be
  non-null before ACTIVE. Evidence may still be an authoritative null. For
  `PRESENT` pins, null evidence is authoritative — the gate must not
  substitute parse-time tip values.
- Legacy deployment-only XCom
  (`dpone_run_pinned_deployment_identity`) is `PRESENT_LEGACY_PARTIAL` and may
  be used only when `deployment_identity_pin.required` is false. With
  `required=true` it is `PIN_MISSING`.
- Exact activation always forces `deployment_identity_pin.required=true`
  (packs that set `required=false` are rejected at enrich time).
- Malformed / missing pod envelope → `PIN_INVALID` / `PIN_MISSING`.
  Pod deleted / read failure / UID mismatch → `PIN_UNAVAILABLE`.
  Digest mismatch / annotation present without required identity or envelope
  digest env keys / annotation↔env disagreement → `PIN_INVALID`. Kubernetes
  reconciliation may omit the optional evidence env key only when the
  annotation authoritatively records `expected_runtime_evidence_sha256: null`;
  a non-null annotated evidence digest still requires an equal env value.
  Envelope injection preserves unrelated base-container `valueFrom` env
  entries.
- Pinning is enabled only for runtime tasks that own a separate `outcome_gate`
  **and** require a launch pin
  (`pin_deployment_identity_for_separate_outcome_gate=true`), not for
  pre-hooks with `do_xcom_push=False`, inline/mapped outcome paths, or legacy
  `required=false` packs.

Exact attempt and terminal workflow evidence use the closed v2 schemas
`dpone.dbt-airflow-attempt-evidence.v2` and
`dpone.dbt-workflow-evidence-outcome.v2`. Their v1 predecessors remain unchanged
for strict historical readers. Consumers dual-read v1/v2 and require
`deployment_identity` only from v2.

The identity does not change the parse-side-effect contract. Construction reads
only the bounded local deployment index and listed artifacts. It performs no
network, metadata DB, Variable, Connection, Vault, Kubernetes, or cache-refresh
operation. Use `dpone airflow rerun-plan` outside DAG parsing to select original
or latest Airflow delivery independently from original or latest dpone
artifacts.

Credential resolution remains a runtime concern after the provider has pinned
the deployment. `latest + workload_start` resolves once for each workload;
unsupported pinned/DAG-wide policies and ambiguous KV v2 version evidence fail
closed. Operator recovery and resolver certification status are documented in
[Airflow credential resolver lifecycle](airflow-credential-resolver-lifecycle.md).

After task execution, the evidence collector joins the immutable identity with
the concrete Airflow attempt, dpone run/evidence, and observed pod into
`dpone.airflow-correlation.v1`. The attempt digest is stable across late pod
observations but changes for a retry or mapped index. Release-policy evidence
requires a complete correlation; advisory/PR evidence may carry an explicit
`DPONE_AIRFLOW_CORRELATION_INCOMPLETE` warning. A digest, DAG, pack, or runtime
image contradiction always blocks evidence.

Use the final evidence bundle as the optional local input to
`dpone ops lineage-export --airflow-evidence-bundle ...` and
`dpone observability metrics-export --airflow-evidence-bundle ...`. Neither
exporter is imported by the provider or performs parse-time I/O. OpenLineage
gets a versioned custom run facet and deterministic UUIDv5 run ID; OTel gets
resource/data-point attributes; Prometheus labels remain unchanged.

## Interval-aware runs and data-aware scheduling

Compact packs are interval-aware out of the box:

- `kpo_kwargs.env_vars` carries Jinja-templated `DPONE_DAG_ID`, `DPONE_DAG_RUN_ID`, `DPONE_TRY_NUMBER`,
  `DPONE_LOGICAL_DATE`, `DPONE_INTERVAL_START`, `DPONE_INTERVAL_END`, and the optional
  `DPONE_PARTITION_KEY`. Airflow renders them per task instance, so every pod receives the concrete
  DAG-run interval while the pack stays a static artifact.
- Compose also injects scheduler-owned `DPONE_REPAIR_AUTHORITY_REF` from
  `dag_run.conf` after the pack-env allowlist (it is **not** an allowed pack
  key). Prefer map `DPONE_REPAIR_AUTHORITY_REFS` keyed by `ti.task_id` for a
  multi-process DAG; scalar `DPONE_REPAIR_AUTHORITY_REF` is the fallback.
  Empty render is a no-op. Do not bake an opaque authority ID into a pack or
  promotion-stable manifest. CLI `--repair-authority-ref` remains the
  non-Airflow admission path; see [Runtime State Backends](state.md).
- Inside the pod, `dpone run` consumes the same variables: they feed run-state identity
  (`dag_id` + `execution_date`) and resolve `{{ data_interval_start }}` / `{{ data_interval_end }}` /
  `{{ ds }}` tokens inside manifests. With `mode: backfill` + `inner_mode: partition_replace` this makes
  `catchup=True`, task clears and `airflow dags backfill` idempotent per interval — see the
  [Backfill guide](backfill.md#airflow-integration) and `examples/dags/`.
- Declare produced datasets under `airflow.execution.outlets` (list of asset URIs) in the workload
  catalog; the provider converts them into `Asset` (Airflow 3) / `Dataset` (Airflow 2.4+) outlets on the
  runtime task for data-aware downstream scheduling. Images without the Asset API skip outlets safely.
- The XCom summary includes the run `interval` and a bounded `backfill` progress section
  (see `docs/schemas/gitops/airflow-xcom-summary.schema.json`).

Partitioned dag-specs use public Airflow 3.2.x/3.3.x SDK timetables:

- a cron producer uses `CronPartitionTimetable`;
- an asset consumer uses `PartitionedAssetTimetable` with `IdentityMapper`;
- the attached DAG partition plan supplies runtime context even when the
  consumer inherited the partition and its compact pack has no duplicate
  declaration;
- Airflow 3.0/3.1 and 2.x retain the existing schedule and set
  `DPONE_PARTITION_MODE=degraded_unpartitioned`.

The provider fails closed when DAG and pack partition identities disagree or a
native task has no scheduler partition key. Capability negotiation imports only
`airflow.sdk`; it performs no parse-time network, metadata, Variable,
Connection, Vault, Kubernetes, or cache-refresh access.

Compatibility with Airflow 2.10, 2.11, 3.2 and 3.3 is exercised in CI
(`.github/workflows/airflow-pack-compat.yml`).

## Declarative DAG timezone (Airflow 2+)

Domain catalogs and `*.dag-spec.json` may declare `timezone` next to a cron
`schedule`. The loader never forwards that field as a raw `DAG()` kwarg
(Airflow 3 rejects it). Instead `dpone_airflow_pack.dag_schedule` applies the
timezone in a version-aware way:

| Airflow line | Cron + timezone behavior |
| --- | --- |
| 2.2+ and 3.x | `CronTriggerTimetable("0 7 * * *", timezone="Europe/Moscow")` |
| 2.0–2.1 (legacy) | `DAG(timezone=..., schedule_interval="0 7 * * *")` when timetables are unavailable |
| all supported | `start_date` is materialized in the catalog timezone via pendulum |

Manual/asset schedules ignore `timezone` on the schedule itself; only
`start_date` is localized. Invalid timezones quarantine the dag-spec with
`dpone_spec_error` instead of breaking the whole loader module.

## Scheduler guardrails

The provider performs only deterministic parse-time work:

- reads one local deployment index with an 8 MiB default ceiling;
- reads only listed DAG specs and workload packs, each with a 64 MiB default
  ceiling and declared size/SHA-256 verification;
- completes the v2 delivery and pod-contract preflight for every listed DAG
  before publishing any of them to `globals()`;
- validates pack kind, KPO kwargs, pod spec and outcome-gate metadata;
- builds visible Airflow tasks;
- reads Airflow connections only inside task execution when an unsafe development bridge is explicitly enabled.

It does not parse manifests, call databases, call Kubernetes APIs, generate GitOps artifacts, import ClickHouse/MSSQL
connectors, or execute business logic.

## Packaging

After `0.73.32` is published, install
`apache-airflow-providers-dpone==0.73.32` in the base Airflow image by using the
constrained, matrix-pinned sequence above. Before publication, install the
checksum-verified `dist-airflow/apache_airflow_providers_dpone-*.whl` and
matching `dpone_airflow_pack-*.whl` produced from the frozen candidate commit;
do not ask PyPI for an unreleased candidate. The provider pulls or installs the
matching lightweight reader without the full dpone runtime. Provider and
generated runtime packs must stay on the same dpone release line and commit.

Do not install `dpone`, `dpone[full]`, `dpone-native-accel`, `pyodbc`, pandas, polars, ConnectorX or sink/source
connectors into the scheduler image unless your platform intentionally runs pipeline runtime code in the scheduler,
which is not the recommended production topology.

Semantic-refresh V2 in 0.74 uses this heavier topology only in its disposable
local harness; it is not a production exception. Production activation raises
`DPONE_SEMANTIC_REFRESH_PRODUCTION_ACTIVATION_UNAVAILABLE` until a later release
ships the certified retention controller and allow-guard. Its local Airflow 3.3
Python operators execute the protected dbt/admission/publication application in
Kubernetes worker pods, so the harness scheduler/worker image must pin
`dpone[semantic-refresh-airflow]` to the exact same version as
`apache-airflow-providers-dpone` and `dpone-airflow-pack`. The image also needs
the approved Microsoft ODBC Driver for SQL Server. The DAG processor still does
only local index/sidecar verification and I/O-free dependency construction;
MSSQL, Vault, S3 and ClickHouse factories are invoked only inside workers. Do
not use this image profile for production or ordinary lightweight DAGs, and
never install the unbounded `dpone[full]` extra as a shortcut.

## Product pattern

This follows the same separation used by mature orchestration stacks: Airflow provider packages keep scheduler parsing
small, Cosmos consumes compiled dbt artifacts instead of parsing projects in the scheduler, and connector-heavy runtimes
execute in isolated worker images.

Cosmos can read a remote dbt manifest through Airflow Object Storage, and its
remote cache trades portability for a network read during DAG parsing. dpone
adopts the compiled-artifact, hash-invalidation, and cache-cleanup ideas but
keeps the scheduler parse path local. The watcher is therefore analogous to
`git-sync` for verified dpone deployments, not a Cosmos runtime component.

### Cosmos alignment

The comparison is capability-scoped. Cosmos solves dbt-to-Airflow rendering;
dpone uses the relevant artifact-delivery patterns but does not claim feature
parity with every Cosmos dbt execution feature.

| Pattern | Cosmos behavior | dpone decision |
| --- | --- | --- |
| Compiled artifact boundary | A prebuilt dbt `manifest.json` can drive DAG construction | Adopt: generated packs and DAG specs are immutable compiled inputs |
| Remote object storage | A manifest can be read through Airflow Object Storage | Adopt for publication, but synchronize outside DAG parsing |
| Cache invalidation | Project/config hashes invalidate affected caches | Adopt with SHA-256 identities for release, deployment, index, and files |
| Partial parsing and cache reuse | dbt/Cosmos caches avoid repeated project parsing | Adopt the general reuse principle; dpone never recompiles manifests in the parser |
| Stale-cache cleanup | Cosmos exposes cleanup for unused local and remote cache entries | Adopt as bounded generations plus retention and protected rollback references |
| Remote read during DAG construction | Remote manifest/cache access can occur while the DAG is constructed | Reject for scheduler packs because remote latency and availability enter the parse path |
| Deployment activation | Cosmos does not define dpone's exact release/deployment CAS protocol | Extend with conditional desired state, last-known-good, rollback occurrence, loader ACK, and REST convergence |

| Airflow line | Parse authority | dpone synchronization |
| --- | --- | --- |
| 2.10 | Scheduler-managed parser or standalone DAG processor | Init and watcher beside the configured parse authority |
| 2.11 | Scheduler-managed parser or standalone DAG processor | Same compatibility placement as 2.10; verify the actual deployment topology |
| 3.2 | Dedicated `dagProcessor` | Init and watcher beside `dagProcessor` only |
| 3.3.x | DAG Bundle capable processor | Keep watcher until a custom versioned bundle proves equivalent exact-ID semantics |

For Airflow 2.10 and 3.2, the watcher is the compatibility mechanism that gives
the parse authority a local, network-free view of the selected deployment.
Airflow 3.3 introduces `S3DagBundle`, but the official S3/GCS bundles currently
do not provide bundle versioning. They therefore do not yet replace dpone's
immutable release, desired-state CAS, checksum, last-known-good, activation
occurrence, loader acknowledgement, and convergence contracts. A future
versioned custom DAG Bundle may replace the watcher only if it preserves those
contracts and can retrieve the exact bundle version required by a DAG run.
This limitation is explicit in the
[Airflow 3.3 DAG Bundles documentation](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-bundles.html):
`S3DagBundle` tasks always run against the latest bucket content. The migration
and certification criteria for a future dpone custom versioned bundle are in
[Exact Airflow desired-state delivery](airflow-desired-state.md#airflow-33-dag-bundles).

### Production readiness

The architecture is production-target and fail-closed: its ownership
boundaries, exact identities, conditional writes, bounded cache,
last-known-good behavior, fail-open pod lifecycle, fail-visible loader,
rollback, and convergence evidence are specified and contract-tested. The
current rollout is a preview. A concrete environment becomes production-ready
only after its release has current evidence for:

1. conditional create and replace against the configured S3-compatible service;
2. checksum rejection and partial-download isolation;
3. an empty-cache parser restart followed by automatic recovery;
4. an object-storage outage while an existing `current` remains usable;
5. rollback and roll-forward between two exact immutable deployments;
6. loader acknowledgement and Airflow REST convergence for the complete expected
   DAG set.

Strict runtime-pod cleanup additionally requires the separately deployed dpone
label-scoped retention control,
durable-log prerequisite and acceptance evidence in
[Airflow runtime pod retention](airflow-runtime-pod-retention.md).

The relevant Cosmos ideas are covered by the alignment table above. This does
not by itself certify a deployment. Direct remote reads during every DAG parse
are deliberately not adopted because they make parser availability and latency
depend on object storage. The dpone watcher moves remote synchronization outside
the parse path and adds exact deployment CAS, last-known-good recovery, rollback
identity, and durable convergence evidence.

## Legacy pack cache CLI

This CLI consumes a mutable `latest/pack-index.json` for pre-release/deployment
installations. It remains supported for compatibility, but new installations
must use `dpone airflow publish`, `dpone airflow cache-materialize`,
`dpone airflow cache-sync`, and the local `airflow-index.json` provider loader.
The legacy sync is pack-only: it is not allowed to manufacture a `release-set`,
`deployment-set`, or current pointer.

For scheduler startup or a refresh sidecar:

```bash
dpone-airflow-pack-sync \
  --once \
  --index-uri s3://bucket/dpone-artifacts/prod/repo/latest/pack-index.json \
  --reader-connection-id s3_dpone_artifacts_reader \
  --cache-dir /opt/airflow/.dpone-legacy-pack-cache \
  --max-total-bytes 512MiB \
  --max-pack-bytes 10MiB \
  --max-index-bytes 25MiB \
  --partial-download-ttl-minutes 30 \
  --status-path /opt/airflow/.dpone-legacy-pack-cache/status/last-sync-status.json
```

The sync initializes `.dpone-cache-layout.json` with
`layout=legacy_pack_index_v1`. Never point it at the exact deployment cache
root: that root declares `exact_deployment_v1`, uses an atomic `current`
symlink, and is owned by `dpone airflow cache-materialize/cache-sync`.
Cross-layout access fails closed with `DPONE_CACHE_LAYOUT_MISMATCH`; use a
separate root and follow the
[provider cache migration runbook](airflow-provider-cache-migration.md).

Downloaded bytes stay in an owner-marked, fsGroup-managed stage with an active attempt
lease. Capacity is reserved before payload writes. A verified generation is
fsynced and published for the shared Airflow group (`02770` directories,
`0440` files), so a
dagProcessor/scheduler UID can read bytes produced by another init/sidecar UID.
Shared control directories/files use explicit `02775`/`0664` modes when the
process owns the inode; foreign-owned PVC roots (common CSI `uid 0` mounts)
are validated against the same minimum shared bits and are never chmod'd.
Prefer provisioning `02775`; `02777` is accepted only for CSI compatibility
and must stay confined to the Airflow `fsGroup` (multi-tenant world-writable
NFS/hostPath roots are unsupported). Attempt work paths use `02770`/`0660`
with an exact foreign-owned contract.
Writers and readers require one shared Kubernetes `fsGroup`. The legacy commit
point is
`status/current-commit.json`; it binds the actual generation, index digest, and
monotonic local sequence. A durable pending phase makes interrupted commits
recoverable. `last-sync-status.json` and an optional Airflow Variable are
diagnostics derived from that receipt, not independent pointers.

Defaults are bounded: 512 MiB total cache, 10 MiB per pack/DAG spec, 25 MiB
index, three generations, and a 30-minute abandoned-stage TTL. Retention
detaches old generations under a short CAS-protected lease and deletes them
outside the scheduler-blocking section. A cache that remains above its hard
budget reports `airflow_pack_cache_budget_exceeded` as a blocker. Increase a
budget only after inspecting generated pack sizes; do not disable it with an
unbounded value in shared Airflow.

For output/exit codes and recovery by blocker code, use the
[legacy pack cache operations runbook](airflow-legacy-pack-cache-operations.md).
For production cache placement and access modes, use
[Deploy the exact cache on Kubernetes](airflow-cache-kubernetes-deployment.md).

For diagnostics:

```bash
dpone-airflow-pack-cache-status \
  --cache-dir /opt/airflow/.dpone-legacy-pack-cache \
  --json

# Exact desired-state cache layout (current/airflow-index.json):
dpone-airflow-pack-cache-status \
  --cache-dir /opt/airflow/.dpone-cache \
  --json
```

The status CLI auto-detects exact (`current/airflow-index.json`) versus legacy
(`generations/<git_sha>/pack-index.json`) layouts so operators do not get a false
`airflow_pack_json_missing` when pointing at the exact-cache root.

The sync command writes an atomically replaced status object for every success
or failed watch cycle. Use the tested shell boundary and status semantics in
the [legacy cache operations runbook](airflow-legacy-pack-cache-operations.md);
do not add S3 access to DAG parsing.

The canonical release/deployment materializer is fail-closed and exact-ID. It
runs outside DAG parsing and does not share this command's `latest` semantics.
Use the
[provider and cache migration guide](airflow-provider-cache-migration.md) to
cut over without making the scheduler fetch remote state, then follow
[Airflow cache sync and recovery](airflow-cache-sync.md#materialize-the-prerequisite)
for ongoing operation.
