# dpone Airflow provider API

Purpose: define the typed scheduler-side API, bounded local-read contract,
failure isolation, and recovery boundary for dpone DAG loading.

Audience: DAG authors, Airflow platform engineers, and operators consuming
`LoadReport` diagnostics.

The canonical import namespace is:

```python
from airflow.providers.dpone import (
    AirflowDeploymentIndex,
    AirflowDeploymentIndexError,
    AirflowIndexArtifact,
    CacheResolver,
    DponeDag,
    DponeTaskGroup,
    load_airflow_deployment_index,
    load_and_acknowledge_dpone_dags,
    load_dpone_dags,
)
```

The `apache-airflow-providers-dpone` distribution owns this namespace and the
Airflow provider-discovery entry point. The dependency-light
`dpone-airflow-pack` distribution owns static artifact reading and DAG
construction without the full dpone runtime.

`dpone_airflow_pack` keeps compatibility exports during migration. Accessing
provider facade objects from that legacy root namespace emits one
`DeprecationWarning` per process; use `airflow.providers.dpone` in new DAG
files. The compatibility exports remain until at least `0.74.0` and
2027-07-13, whichever is later. Follow the
[provider and cache migration guide](airflow-provider-cache-migration.md) for
the import mapping, generated loader upgrade, immutable cache cutover, and
rollback procedure.

The formal provider distribution declares the
`apache_airflow_provider` entry point:

```toml
[project.entry-points."apache_airflow_provider"]
provider_info = "airflow.providers.dpone:get_provider_info"
```

`get_provider_info()` reports the installed distribution name and version, so
Airflow provider discovery and wheel smoke checks use the same metadata.

## Static typing

The distribution is PEP 561 typed. Both `airflow.providers.dpone` and the
legacy `dpone_airflow_pack` package contain `py.typed`; the canonical namespace
also ships an explicit `__init__.pyi` contract. IDEs and type checkers therefore
see concrete signatures for `load_dpone_dags`, `DponeDag.from_spec`,
`DponeTaskGroup.from_pack`, `CacheResolver`, deployment-index models, and
`LoadReport` instead of a variadic `Any` facade.

`DponeDag.from_spec()` returns the concrete Airflow DAG type and
`DponeTaskGroup.from_pack()` returns the concrete Airflow TaskGroup type. The
typing compatibility import uses the class locations shared by the supported
Airflow 2/3 matrix; runtime materialization still prefers the official Airflow
3 `airflow.sdk` public interface and falls back for Airflow 2. Type metadata
does not add import-time Airflow, network, database, Variable, Connection,
Vault, or cache-refresh calls.

## Canonical load and acknowledgement

Production scheduler and DAG-processor files use the combined loader so one
shared cache lease binds the parsed DAG set to its exact activation evidence:

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

Mount the cache read-only and the ACK root as a separate writable volume.
`load_dpone_dags()` remains the lower-level parse-only API for hybrid callers
that deliberately manage acknowledgement elsewhere.

## load_dpone_dags

```python
def load_dpone_dags(
    globals_dict: MutableMapping[str, Any],
    repo_root: str | Path | None = None,
    *,
    index_path: str | Path | None = None,
    domains: Sequence[str] | None = None,
    operator_overrides: Mapping[str, Any] | None = None,
    duplicate_policy: Literal[
        "skip_and_report", "fail_all", "replace_if_same_fingerprint"
    ] = "skip_and_report",
    invalid_dag_policy: Literal[
        "skip_and_report", "fail_all", "create_diagnostic_dag"
    ] = "skip_and_report",
) -> LoadReport:
    ...
```

`load_dpone_dags()` does not write a loader ACK. It is reserved for hybrid
callers that own an equivalent acknowledgement protocol outside this helper.
It is intentionally not shown as a scheduler copy/paste example: production
generated loaders must use the combined load-and-acknowledge API above so cache
retention cannot delete a parsed but unacknowledged activation.

When `index_path` is provided, the provider reads either the v1 compatibility
wire or `dpone.airflow-deployment-index.v2`. When `repo_root` is provided
without `index_path`, the legacy
`.dpone/gitops/airflow/_dags/*.dag-spec.json` loader is used for compatibility.
For deployment-index loading, node `pack_ref` values that use `cached://`
workload references are resolved through the same local `airflow-index.json`
before Airflow tasks are materialized. The task builder receives the verified
local pack path, not an unresolved runtime `current` reference.

The index reader requires `dag_specs`, `workload_packs`, and a supported
`runtime_artifact_delivery.mode`; malformed or partial projections produce one
structured fatal load error instead of an empty successful report. Logical
artifact ids must be unique within each index section; duplicates fail with
`DPONE_AIRFLOW_INDEX_ARTIFACT_DUPLICATE` rather than selecting the first entry.

Mode policy is wire-specific:

| Wire and mode | Result before DAG installation |
| --- | --- |
| v1 `local_preview` | Supported static, non-runnable preview; no KPO/runtime pack wiring. |
| v1 `init_fetch` | `DPONE_RUNTIME_ARTIFACT_DELIVERY_MIGRATION_REQUIRED`. |
| v2 `init_fetch` | Continue through the strict executable KPO preflight. |
| v2 known non-`init_fetch` mode | `DPONE_RUNTIME_ARTIFACT_DELIVERY_MODE_UNSUPPORTED`. |
| Unknown mode | `DPONE_AIRFLOW_INDEX_FIELD_INVALID`. |

The v2 loader preflights every listed DAG and its KPO contract into an isolated
namespace before mutating the caller's `globals()`. A late v2 contract failure
therefore returns a fatal report with no partially installed DAGs.

Every selected v2 workload pack must also contain:

```yaml
provider_execution:
  schema: dpone.airflow-provider-execution.v1
  kpo_kwargs:
    task_id: orders__dpone_runtime
    name: dpone-orders
    env_vars: {}
  pod_spec:
    spec:
      containers:
        - name: base
```

This closed object is the only pack-owned scheduler extension surface. It is
included in `pack_fingerprint`. A pack without it fails with
`DPONE_INIT_FETCH_PACK_MIGRATION_REQUIRED`; rebuild and republish the immutable
release/deployment rather than editing the pack. Top-level `kpo_kwargs`,
`pod_spec`, and `runtime_command` remain inputs only to the explicit raw
legacy/local API.

Optional `kpo_kwargs` extensions are `pool`, `execution_timeout_seconds`, and
`executor`. The pack builder projects `executor` from the workload-set
`airflow.execution.task_executor` policy (dbt execution packs use the dbt
publish profile policy with its `KubernetesExecutor` contract default) so
strict runtime and pre-hook tasks keep their declared placement (for example
`KubernetesExecutor` under Airflow hybrid executors) instead of falling back
to the deployment default executor. Unknown `kpo_kwargs` fields keep failing
closed with `DPONE_INIT_FETCH_RESERVED_COLLISION`.

Rollout order matters: a loader older than the `executor` field rejects packs
that carry it, fail-closed, with `DPONE_INIT_FETCH_RESERVED_COLLISION`.
Upgrade the Airflow image (`dpone-airflow-pack`) first, then rebuild and
republish artifacts that declare `task_executor`.

Strict `init_fetch` fixes `get_logs=true` so Airflow follows `base` container
logs after pod start (modern cncf-kubernetes awaits init completion first),
and uses `on_finish_action=delete_succeeded_pod`. The provider requests
deletion for successful pods; failed base containers are normally retained,
subject to provider-native cancellation, late task failure and best-effort
Kubernetes cleanup semantics. These trusted values are not pack-owned
`provider_execution` fields. Production environments must independently govern
stale retained pods through the
[runtime pod retention runbook](airflow-runtime-pod-retention.md).

## Strict v2 init-fetch delivery context

The v2 wire requires one complete immutable context:

- matching top-level and delivery-level `trust_tier` values:
  `production` or `non_production`;
- an exact `runtime_image_ref` ending in the declared
  `runtime_image_digest`;
- exact release, deployment, and non-empty workload-pack descriptors with
  positive `bytes`, SHA-256, and pack fingerprint;
- a bounded logical artifact registry reference and
  `kubernetes_workload_identity` service account/namespace;
- a registry ConfigMap `{kind, name, key, sha256}` and, for production, a trust
  policy ConfigMap with the same closed shape;
- checksum verification and a trust-tier-compatible attestation requirement.

The provider rejects unknown fields, mutable aliases, secret-shaped values,
image/digest mismatch, trust-tier mismatch, and an absent production trust
policy before KPO construction. Each runtime, pre-hook, and mapped task receives
the same frozen context.

For each task, the context produces canonical compact JSON no larger than
16 KiB:

| Transport | Value |
| --- | --- |
| `DPONE_INIT_FETCH_PLAN_B64` | Base64 of the canonical v3 runtime fetch plan. |
| `DPONE_INIT_FETCH_PLAN_SHA256` | SHA-256 of those exact canonical bytes. |
| `dpone.io/init-fetch-plan-sha256` | Matching pod annotation. |

The generated KPO uses the exact pinned image and fixed commands
`dpone airflow runtime-init-fetch` and
`dpone airflow runtime-pack-exec`. Arguments are empty. Namespace and service
account come from the validated workload identity. The provider preserves only
the closed projection's task identity, labels, allowed interval/partition
environment, scheduling fields, and base-container resources. Image, command,
arguments, namespace, service account, extra containers, init containers,
security context, volumes, mounts, and unknown fields inside that projection
fail with `DPONE_INIT_FETCH_RESERVED_COLLISION`; no active strict field is
silently replaced.

Plan v3 carries `execution.process_selector` and
`execution.hook_execution=inline|externalized`. The provider derives these
from the resolved DAG node; runtime verifies the selected process against the
fetched pack before setting the child hook environment. Plan v1/v2 remains a
read-only rollback contract, is accepted only for exactly one selector-coherent
process plan, and fails closed for every multi-process pack. Whole-workload
projections are accepted only when top-level Airflow tasks cover every
process-local separate hook with the same selector, argv, dependencies and
execution policy. Pre-hook child failures propagate a non-zero task exit after
diagnostics are captured under `/var/lib/dpone/run`; they do not publish XCom. See
[startup service files](airflow-runtime-startup-diagnostics.md).

Strict tasks default to `retries=0`. A bounded positive value is accepted only
when the authenticated workload pack contains compiler-issued
`provider_execution.retry_authority`. Version 1 authority is emitted solely for
PostgreSQL XMin `initial` to MSSQL backfill with `retry_policy: non_committed`,
distributed audit-schema state, target-atomic MSSQL receipts, and either fenced
`incremental_merge` or the exact retained-shadow route (`incremental_append`,
`only_new_rows: false`, `publication.mode: shadow_swap`). The maximum is three
retries. Every task is checked against its own pack, so one uncertified node
makes global DAG preflight fail before any DAG is installed.

The retry count may come from the DAG default or the closed scheduler-only
`operator_overrides.retries` surface. `operator_overrides.pool` is also allowed;
runtime image, command, namespace, secrets, pod security and all other static
authority remain provider-owned. Missing authority, an unsupported route, a
boolean/negative/over-limit retry value, or malformed authority fails closed
with `DPONE_AIRFLOW_RETRIES_REQUIRE_TARGET_FENCE` or
`DPONE_INIT_FETCH_PROVIDER_EXECUTION_INVALID`. On retry, a new KPO starts and
the runtime reconciles the target ledger: committed chunks are skipped and only
non-committed chunks can be claimed. The authority applies only to the runtime
KPO; externalized hooks and outcome tasks remain at zero retries because their
side effects are not covered by the chunk target fence.

The strict release compiler carries only `pool` and `retries` from a DAG
specification into the immutable release. Resource-capable Pod overrides are
rejected with migration guidance to [workload resources](airflow-workload-resources.md).
Other legacy hints such as `in_cluster` and executable or pod-security overrides
are removed before the DAG-spec
fingerprint is recomputed. Deploy this compiler behavior only with a
coordinated `dpone-airflow-pack` and `apache-airflow-providers-dpone` version of
at least `0.74.20`; older readers reject the retained closed override surface.
Verify the serialized Airflow task's `pool` after convergence instead of using
the authoring YAML or pre-release pack output as deployment evidence.

Inspect the same machine-readable wire contracts used by CI and runtime:

```bash
dpone gitops schema show dpone.airflow-runtime-init-fetch-plan.v3
dpone gitops schema show dpone.runtime-fetch-ready.v1
```

The init container alone mounts the selected registry configuration at
`/etc/dpone/artifact-registry/registry.json` and, when present, trust policy at
`/etc/dpone/artifact-trust/policy.json`. Runtime resolves kubelet ConfigMap
projection symlinks within each mount directory, opens the resolved regular
file as one bounded snapshot, verifies its pinned digest, and parses those same
bytes before registry construction. The base container sees fetched artifacts
and worktree read-only; its only reserved read-write volume mount is
`/var/lib/dpone/run`. Init commits the pinned trust-policy fingerprint,
effective attestation requirement, and attestation status to
`runtime-fetch-ready.json`; the base launcher validates that record against the
same plan and never rereads an init-only ConfigMap.

This contract trusts the producer, promoted cache/index, Airflow
scheduler/provider, Kubernetes enforcement, runtime image, workload identity,
registry adapter, and configured verifier. It prevents pack-owned overrides but
does not claim compromised-scheduler/provider/cache resistance. Live
Kubernetes, ConfigMap projection, workload identity, and production attestation
remain `UNVERIFIED` without current environment evidence.

## LoadReport

```python
@dataclass(frozen=True)
class LoadReport:
    loaded: tuple[str, ...]
    skipped: tuple[dict[str, str], ...]
    errors: tuple[dict[str, str], ...]
    duration_ms: int
    release_id: str | None
    deployment_id: str | None
    airflow_index_sha256: str | None
    cache_root: str | None
    activation_id: str | None
    fatal: bool = False
```

The report is local-only. Creating it must not write to Airflow metadata DB,
Airflow Variables, network services, Vault, Kubernetes, or object storage.
For indexed loads, `airflow_index_sha256` is captured by the same bounded,
confined read that produced the parsed index. Loader acknowledgement must use
this value and must not reopen the mutable `current` path after DAG
materialization. `cache_root` prevents cross-cache acknowledgement, while
`activation_id` distinguishes separate activation occurrences even when a
rollback selects the same release and deployment IDs.
`LoadReport.errors` uses the Phase 1A structured shape
`schema: dpone.error.v1`, `stage: airflow_parse`, and `severity: error` for
deployment-index reader failures, dag-spec validation issues, and DAG
materialization failures.

Interpret `fatal` before interpreting `loaded`:

| `fatal` | Scope | Default `skip_and_report` behavior |
| --- | --- | --- |
| `true` | The deployment index could not establish one trusted snapshot. | No DAG is loaded; `release_id` and `deployment_id` are `None`; the generated loader raises using `errors[0].code`. |
| `false` | The index is trusted; any errors belong to listed DAG/pack consumers. | Valid unrelated DAGs continue loading and every isolated error carries its `dag_id` and local path. |

With `invalid_dag_policy="fail_all"`, index and per-DAG failures raise
`AirflowDeploymentIndexError` instead of returning the default isolated report.
Do not treat `loaded == ()` as success without checking `fatal` and `errors`.

## CacheResolver

```python
class CacheResolver:
    @classmethod
    def from_index(
        cls,
        index_path: str | Path,
        *,
        cache_root: str | Path | None = None,
        max_index_bytes: int = 8388608,
        max_artifact_bytes: int = 67108864,
    ) -> CacheResolver:
        ...

    def resolve(self, cached_ref: str) -> CacheResolution:
        ...
```

The underlying public reader has the same limits:

```python
def load_airflow_deployment_index(
    index_path: str | Path,
    *,
    cache_root: str | Path | None = None,
    max_index_bytes: int = 8388608,
    max_artifact_bytes: int = 67108864,
) -> AirflowDeploymentIndex:
    ...
```

The reader performs `max_index_bytes + 1` bounded binary bytes of input before
UTF-8 decoding or JSON parsing. Both limits must be positive. Declared artifact
sizes above the artifact ceiling fail before a full artifact read. The public
reader always size- and SHA-256-verifies every listed artifact while loading the
index; deferred verification is an internal loader implementation detail, not a
public trust-bypass switch.

`load_dpone_dags()` uses one internal two-stage path: it first loads the bounded
index with artifact verification deferred, then verifies each DAG spec and
workload pack at its isolated final-consumer boundary. This is not a trust
downgrade: each consumer receives the expected digest and byte count from the
same pinned `AirflowDeploymentIndex`. It prevents one damaged artifact from
blocking unrelated DAGs while retaining fail-closed verification for the DAG
that consumes it. `DponeDag.from_spec()` also reads the index once and reuses
that snapshot for target resolution and materialization.

`CacheResolver` is the public, parse-safe resolver for deployment-index
references. It supports:

```text
cached://dags/<dag_id>
cached://workloads/<workload_id>
cached://deployments/<deployment_id>/dags/<dag_id>
cached://deployments/<deployment_id>/workloads/<workload_id>
cached://dags/<dag_id>?release=sha256:...&deployment=sha256:...
cached://workloads/<workload_id>?release=sha256:...&deployment=sha256:...
```

Resolution is local-only. The resolver reads the supplied
`airflow-index.json`, verifies listed `cache://` artifacts through the same
checksum and path-safety rules as the loader, and rejects mismatched
release/deployment pins with `DPONE_CACHE_REF_PIN_MISMATCH`. Deployment-scoped
refs are the strict internal pinned form for task/operator arguments; they
are also the recommended examples for hybrid DAG authors. They still resolve
only against the supplied deployment index and never consult `current` at
runtime. Query-form release/deployment pins are retained for compatibility
with earlier Phase 1A examples. The only accepted query pins are `release` and
`deployment`; typo-like keys such as
`deployment_id` fail fast with `DPONE_CACHE_REF_PIN_INVALID` instead of being
silently ignored. Query pins must also be unique, so ambiguous refs such as
`release=old&release=new` fail with the same error code.
If the supplied `airflow-index.json` path is missing, the reader raises
`DPONE_AIRFLOW_INDEX_NOT_FOUND`; `load_dpone_dags()` reports the same code in
`LoadReport.errors`, sets `fatal=True`, and loads no DAGs. The generated
`dags/dpone.py` guard turns that fatal report into an Airflow import error so a
missing index cannot look like a successful empty deployment.

```python
from airflow.providers.dpone import CacheResolver

resolver = CacheResolver.from_index(
    "/opt/airflow/.dpone-cache/current/airflow-index.json",
)
pack = resolver.resolve(
    "cached://deployments/sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/workloads/load_orders",
)
```

`CacheResolution.to_jsonable()` returns the artifact kind, logical id, release
id, deployment id, `cache://` artifact reference, resolved local path, checksum,
byte size, and provenance. It never resolves `current` during task execution and
does not read Airflow Variables, Connections, Vault, Kubernetes, network
services, or remote artifact storage.

`AirflowDeploymentIndexError` is exported from the same canonical namespace for
hybrid DAG files, tests, and platform diagnostics that need to branch on
machine-readable provider failures:

```python
from airflow.providers.dpone import AirflowDeploymentIndexError, DponeDag

try:
    orders_daily = DponeDag.from_spec(
        "cached://deployments/sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/dags/orders_daily",
        index_path="/opt/airflow/.dpone-cache/current/airflow-index.json",
    )
except AirflowDeploymentIndexError as exc:
    raise RuntimeError(f"{exc.code}: {exc.path or 'unknown path'}") from exc
```

`AirflowDeploymentIndexError.to_jsonable()` follows the Phase 1A structured
error shape with `schema: dpone.error.v1`, `stage: airflow_parse`, and
`severity: error`. `load_dpone_dags()` uses the same shape for deployment-index
reader failures reported through `LoadReport.errors`.

The `load_airflow_deployment_index()` return type, `AirflowDeploymentIndex`,
and its listed artifact entries, `AirflowIndexArtifact`, are also exported from
`airflow.providers.dpone` for typed platform smoke tests and cache diagnostics.

## Fatal index errors and recovery

Index-level errors are fatal because no trusted release/deployment snapshot
exists. This family includes `DPONE_AIRFLOW_INDEX_NOT_FOUND`,
`DPONE_AIRFLOW_INDEX_READ_FAILED`, `DPONE_AIRFLOW_INDEX_TOO_LARGE`,
`DPONE_AIRFLOW_INDEX_JSON_INVALID`, schema/required-field/identity errors, and
duplicate artifact ids. Do not switch to `skip_and_report` to hide them; that
is already the default policy and intentionally returns `fatal=True`.

Recover on the build/deployment plane, not in the scheduler process:

1. Preserve `LoadReport.errors[0].code`; use JSON output when the exact local
   path is needed.
2. For a local preview, rebuild the complete projection and then re-check it:

   ```bash
   dpone airflow preview orders_daily
   dpone airflow explain orders_daily
   ```

3. For a shared environment, inspect current/pointer/candidate state before any
   mutation:

   ```bash
   dpone airflow cache-recovery-plan \
     --cache-root /opt/airflow/.dpone-cache \
     --environment prod \
     --format json
   ```

4. Rematerialize the exact immutable release/deployment from trusted storage
   when the plan reports missing or corrupt content. Review and apply the
   generated recovery or `cache-sync` command with its fresh CAS guard. Never
   edit `airflow-index.json`, a DAG spec, or a workload pack in place.

An artifact-specific error such as `DPONE_CACHE_ARTIFACT_MISSING`,
`DPONE_CACHE_ARTIFACT_TOO_LARGE`, `DPONE_CACHE_ARTIFACT_SIZE_MISMATCH`,
`DPONE_CACHE_CHECKSUM_MISMATCH`, or
`DPONE_AIRFLOW_DAG_SPEC_LOAD_FAILED` leaves `fatal=False`; unrelated DAGs may
remain available, but the affected DAG is not trustworthy. Restore or rebuild
the immutable release and projection before retrying that DAG. Follow the
[cache sync and recovery runbook](airflow-cache-sync.md) for the reviewed
materialize/promote sequence.

## DponeDag

```python
class DponeDag:
    @staticmethod
    def from_spec(
        spec_ref: str | Path,
        *,
        index_path: str | Path | None = None,
        globals_dict: dict[str, Any] | None = None,
    ) -> airflow.sdk.DAG:
        ...
```

This is an escape hatch for hybrid DAG files.
`load_and_acknowledge_dpone_dags()` remains the recommended production
zero-boilerplate path. For `cached://dags/<dag_id>` refs, pass
`index_path` so the provider can validate optional release/deployment pins
through the same parse-safe `CacheResolver` contract before returning the
materialized DAG:

```python
orders_daily = DponeDag.from_spec(
    "cached://deployments/sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/dags/orders_daily",
    index_path="/opt/airflow/.dpone-cache/current/airflow-index.json",
)
```

`index_path` is required for the deployment-index API. Missing `index_path`
raises `DPONE_AIRFLOW_INDEX_REQUIRED` before any DAG loading starts.
`DponeDag.from_spec()` accepts `cached://dags/<dag_id>` and
`cached://deployments/<deployment_id>/dags/<dag_id>` references for the cached
form. Passing any workload reference fails early with `DPONE_CACHE_REF_INVALID`;
embed workloads through `DponeTaskGroup.from_pack()`.
The method follows the provider's default parse-isolation policy: unrelated
invalid DAG specs in the same deployment index are reported by the loader, but
they do not block returning the explicitly requested DAG when that DAG
materializes successfully.
When `globals_dict` is supplied, `DponeDag.from_spec()` still materializes the
DAG in an isolated namespace first, then writes the requested DAG into
`globals_dict[target_dag_id]`. A stale existing entry in the caller namespace is
replaced by the DAG from the pinned deployment index.
If the requested DAG spec is present but invalid, the method raises
`AirflowDeploymentIndexError` with `DPONE_AIRFLOW_DAG_SPEC_INVALID` and the
invalid spec path instead of returning an unstructured runtime error.
If the requested DAG spec is valid but cannot be materialized, the method
preserves the matching loader code, such as
`DPONE_AIRFLOW_DAG_SPEC_LOAD_FAILED`, and includes the source dag-spec path.
If no loaded DAG matches the requested logical id, it raises
`DPONE_AIRFLOW_DAG_SPEC_NOT_FOUND`.

## DponeTaskGroup

```python
class DponeTaskGroup:
    @staticmethod
    def from_pack(
        pack_ref: str | Path,
        *,
        dag: airflow.sdk.DAG | None = None,
        index_path: str | Path | None = None,
        deployment_id: str | None = None,
        operator_overrides: Mapping[str, Any] | None = None,
    ) -> airflow.sdk.TaskGroup:
        ...
```

This is an escape hatch for embedding one dpone workload in a custom DAG.
File paths continue to delegate to the existing pack task builder. For
`cached://workloads/<workload_id>` refs, pass `index_path` so the provider can
resolve the workload through the same parse-safe `CacheResolver` contract before
calling the task builder:

```python
from airflow.providers.dpone import DponeTaskGroup

load_orders = DponeTaskGroup.from_pack(
    "cached://deployments/sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/workloads/load_orders",
    index_path="/opt/airflow/.dpone-cache/current/airflow-index.json",
)
```

`DponeTaskGroup.from_pack()` accepts `cached://workloads/<workload_id>` and
`cached://deployments/<deployment_id>/workloads/<workload_id>` references for
the cached form. Passing any DAG reference fails early with
`DPONE_CACHE_REF_INVALID`; materialize whole DAGs through `DponeDag.from_spec()`
or the recommended `load_and_acknowledge_dpone_dags()` loader.
Calling the cached form without `index_path` raises
`DPONE_AIRFLOW_INDEX_REQUIRED` before any pack builder code runs.

If `deployment_id` is supplied, it must match the deployment index. Mismatches
fail early with `DPONE_DEPLOYMENT_ID_MISMATCH` before any pack builder code
runs. The runtime task builder receives the verified local pack path, not the
unresolved `cached://` URI.

For a direct file-path pack, no index delivery context exists, so the result is
a legacy/local escape hatch and cannot claim strict `init_fetch`. A
deployment-scoped `cached://` workload resolved through a v2 `index_path`
inherits the index's frozen delivery context and uses the strict KPO path.

When a compact pack declares an Airflow Connection operator bridge through
`connection_projection.mode: kubernetes_secret_volume` and
`payload_format: airflow_connection_uri`, the task builder selects
`AirflowConnectionSecretVolumeKubernetesPodOperator`. That closed bridge shape
is also valid on the strict v2 `init_fetch` lane: the provider validates the
projection, keeps the secret-volume operator under a delivery context, and the
deployment publisher rewrites published runtime registry snapshots from
`airflow_connection` to matching `kubernetes_secret_volume` mounts so
`connection_ref` can resolve inside the Airflow-free runtime image.
`unsafe_airflow_env` remains rejected under strict init-fetch and is only a
legacy/local escape hatch. The operator-side bridge is execution-time only:
DAG parsing still reads no Airflow Connections, Kubernetes resources, Vault
paths, or secret values. Deferrable KPO execution must use
`cleanup_policy: retain` plus platform-managed cleanup; the default
`after_execute` cleanup is rejected before any Airflow Connection is read.

At execution, `secret_name` is a base prefix rather than a shared physical
object name. The provider derives one DNS-safe Secret name from the Airflow task
instance key (`dag_id`, `task_id`, `run_id`, `try_number`, and `map_index`),
creates that Secret as immutable, mounts that exact name, and deletes only that
attempt's Secret after synchronous execution. Raw task identity is hashed rather
than embedded in the name. Retries, mapped tasks, and overlapping DAG runs
therefore cannot intentionally replace or delete one another's credentials.
The Kubernetes volume keeps the configured base as its stable logical slot while
`secret.secretName` changes per attempt, so reusing an operator object cannot
accumulate stale volumes or duplicate mounts.

`AirflowConnectionSecretProjector.upsert()` keeps its historical method name for
source compatibility, but its v0.72.8 contract is create-only. HTTP `409` raises
`AirflowConnectionSecretConflictError`; the default Kubernetes adapter never
calls replace. Other publish and cleanup failures raise
`AirflowConnectionSecretProjectionError` with only a stable code, operation,
integer status, and digest-only Secret reference. Missing task-attempt context
raises `AirflowTaskAttemptIdentityError` before Connection or Kubernetes I/O.
An unavailable Kubernetes client/configuration raises the redacted
`AirflowConnectionSecretDependencyError` rather than exposing vendor details.
The compatibility attribute `airflow_connection_projected_secret` now contains
only safe digest references; URI-bearing `stringData` is never retained on the
operator.

Starting with v0.72.9, the provider also stamps lifecycle metadata on the
immutable Secret and every supported dictionary/object Pod representation. The
metadata contains only fixed ownership labels plus `sha256:` `secret_ref` and
`attempt_ref` annotations. Existing user labels and annotations are preserved;
validated dpone-owned keys take precedence. A custom projector must publish the
`metadata` supplied by `build_airflow_connection_projected_secret()` unchanged.
Dropping those labels leaves the object intentionally invisible to automated
GC and is unsupported.

`AirflowConnectionSecretLifecycle` is the pure value object shared by Secret
and Pod publication:

```python
from dpone_airflow_pack.connection_secret_lifecycle import (
    AirflowConnectionSecretLifecycle,
)

lifecycle = AirflowConnectionSecretLifecycle(
    secret_ref="sha256:" + "a" * 64,
    attempt_ref="sha256:" + "b" * 64,
    cleanup_policy="retain",
)
secret_metadata = lifecycle.secret_metadata()
pod_metadata = lifecycle.pod_metadata()
```

The platform GC is not part of DAG parsing. It uses the canonical
`AirflowConnectionSecretGcService` behind an injected metadata inventory and
conditional-delete port. Its CLI reads only Kubernetes
`PartialObjectMetadataList`, protects every matching existing Pod and every
young Secret, and deletes with both UID and resourceVersion preconditions. See
the [operator runbook](gitops-airflow-runner-pack.md#attempt-secret-retention-gc)
for the plan/apply workflow. Neither `load_dpone_dags()` nor the provider import
loads Kubernetes configuration or starts cleanup.

## Duplicate and invalid DAG behavior

Default duplicate behavior is `skip_and_report`: DAG ids already present in
`globals()` are left untouched and reported in `LoadReport.skipped`.
`duplicate_policy="fail_all"` turns that into a fail-closed
`DPONE_AIRFLOW_DAG_DUPLICATE` error. `duplicate_policy="replace_if_same_fingerprint"`
is the idempotent reload escape hatch: the provider replaces an existing DAG
only when its `_dpone_spec_fingerprint` exactly matches the incoming
`spec_fingerprint`; otherwise the existing DAG remains untouched and the report
records `duplicate_dag_id_fingerprint_missing` or
`duplicate_dag_id_fingerprint_mismatch`.

Default invalid DAG behavior is `skip_and_report`: invalid or unmaterializable
dag-specs are reported in `LoadReport.errors`, while valid DAGs keep loading.
`invalid_dag_policy="fail_all"` raises a structured provider error and stops
loading. `invalid_dag_policy="create_diagnostic_dag"` is the explicit platform
opt-in for a paused diagnostic DAG. Publish/build remains fail-closed; parse
isolation is only an emergency guard for already-published local cache issues.
Unknown `duplicate_policy` or `invalid_dag_policy` values fail before index
loading with `DPONE_AIRFLOW_POLICY_INVALID`; the error `path` contains the
invalid policy field name.

## Parse side-effect contract

Provider parse code may read only local deployment index artifacts and listed
cache files. The default ceilings are 8 MiB for the index and 64 MiB per listed
artifact; bytes are checked against the index before a DAG or task group is
materialized. Provider parse code must not parse manifests, scan repositories
recursively, import the dpone runtime, import `vault-kv-client`, call Vault,
call Kubernetes, call databases, read Airflow Variables/Connections, or refresh
remote cache.

## Build-plane artifact delivery API

Remote publication and materialization belong to the full `dpone` build and
deployment package, not `airflow.providers.dpone`. They are intentionally absent
from the scheduler/provider namespace:

```python
from dpone.runtime.airflow_artifact_delivery import (
    AirflowArtifactMaterializer,
    AirflowArtifactPublisher,
    MaterializeRequest,
    PublishRequest,
)
```

Both services depend on `dpone.ports.artifact_registry.ArtifactRegistry`.
`AirflowArtifactPublisher.publish()` validates one local canonical projection
before registry I/O, freezes those exact bytes into a private verified snapshot,
and then conditionally creates or byte-compares every object.
`AirflowArtifactMaterializer.materialize()` accepts exact release and
deployment IDs, applies per-object and total byte limits, verifies downloaded
SHA-256 bytes, installs immutable local trees, and returns without activating
`current`.

The provider consumes only the resulting local
`current/airflow-index.json` after a separate reviewed `cache-sync`. It never
imports these services, the object-storage adapter, credentials code, or cloud
SDKs during DAG parse. Cache-root inference preserves the lexical `current`
path before resolving its activation symlink, so a configured cache root works
without requiring the directory itself to be named `.dpone-cache`.
