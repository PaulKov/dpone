# GitOps Airflow runner pack

> **Advanced compatibility path (not the default self-service plane).** New
> self-service deployments should use the
> [indexed-v2 self-service plane](airflow-self-service.md). Use this runner-pack
> workflow for existing custom-image GitOps integrations that still require its
> compatibility contracts.

`dpone gitops airflow` turns a verified `gitops.bundle` into a small handoff
pack for Airflow jobs that run a custom dpone image on Kubernetes. It is built
for Airflow `KubernetesExecutor`, `KubernetesPodOperator`, and
KubernetesPodExecutor-style runner patterns where the scheduler should fetch a
minimal sparse checkout, verify the bundle, and then run only the impacted
manifests.

The render, doctor, runtime-profile, pod-contract, pod-doctor, pack,
artifact-index, preflight, cluster-doctor, k8s-manifests, admission-check,
k8s-smoke, pod-watch, evidence-bundle, connection-bridge-plan, outcome-gate,
run-spec, image-contract, and evidence-verify commands stay control-plane only
by default. They do not
import Airflow, call a Kubernetes API, start pods, execute manifests, or open
source/sink connections. The generated `entrypoint.sh` calls
`dpone gitops airflow run-spec-exec` inside the custom dpone image; that
runtime adapter executes the already-rendered `run-spec.json`, writes
`runtime-evidence.json`, and writes the final XCom outcome to
`xcom-summary.json` for `/airflow/xcom/return.json`.

## What gets rendered

`dpone gitops airflow render` writes these artifacts:

| Artifact | Purpose |
| --- | --- |
| `pod_template.yaml` | Airflow `KubernetesExecutor` `pod_template_file` with `metadata.name`, `spec.containers[0].name: base`, and the custom dpone image. |
| `executor_config.json` | A reviewable Airflow executor-config example that points at the same `pod_template_file`. |
| `airflow_task.py` | A generated `KubernetesPodOperator` task snippet for DAG authors. |
| `entrypoint.sh` | Container entrypoint that calls `dpone gitops airflow run-spec-exec` with the rendered runtime contract. |
| `run-spec.json` | Stable runtime contract for the custom dpone image: bundle digest, image digest, worktree, evidence output, manifest entries, and ordered commands. |
| `runtime-profile.json` | Stable placement contract for Airflow/Kubernetes: image, digest, namespace, service account, resources, artifact sink, optional sparse `git_sync` contract, labels, annotations, and runner policy. |
| `pod-contract.json` | Stable pod contract that ties the bundle, run-spec, runtime profile, generated PodSpec, sparse git-sync initContainers, KPO kwargs, and final XCom outcome together. |
| `pod-spec.yaml` | Kubernetes PodSpec for a custom dpone image with `spec.containers[0].name: base`, runtime resources, env handoff, optional secrets, mounts, selectors, tolerations, and optional `dpone-sparse-checkout` / `dpone-git-sync` initContainers. |
| `kpo-kwargs.json` | JSON-safe KubernetesPodOperator kwargs, including `pod_template_file`, `do_xcom_push`, logging, labels, annotations, and finish behavior. |
| `connection-bridge-plan.json` | Deploy-time plan for runtime-only Airflow Connection bridge rollout. It records required connection ids, `AIRFLOW_CONN_*` env names, Secret refs, generated skeleton artifact paths, warnings, and blockers without serializing URI values. |
| `airflow-connections-secret.yaml` | Kubernetes Secret skeleton with placeholder `AIRFLOW_CONN_*` values for clusters that keep Airflow Connection URIs in a namespaced Secret. |
| `airflow-connections-externalsecret.yaml` | External Secrets Operator skeleton that maps external secret manager keys into the same Kubernetes Secret contract. |
| `airflow-connections.env.example` | Local/env-policy example file listing the required `AIRFLOW_CONN_*` variables with placeholder values. |
| `artifact-index.json` | Deterministic Airflow artifact inventory with expected kind, schema version, producer, byte size, and SHA-256 for each generated runtime artifact. |
| `airflow-runtime-pack.json` | Golden-path Airflow runtime pack report from `dpone gitops airflow pack`: expected artifacts, deterministic next commands, warnings, blockers, and opt-in live gates without launching pods. |
| `airflow-cluster-doctor.json` | Optional Airflow/Kubernetes cluster readiness evidence for namespace, service account, RBAC, imagePullSecrets, git-sync Secret keys, Airflow Connection Secret keys, ExternalSecret readiness, quota/LimitRange visibility, and pod security posture. |
| `airflow-k8s-manifests.json` | Optional manifest-pack report listing the generated Kubernetes objects for the target namespace without embedding Secret values. |
| `airflow-k8s-manifests.yaml` | Optional multi-document Kubernetes pack for GitOps tools such as ArgoCD or Flux: ServiceAccount, Role, RoleBinding, Secret skeletons, optional ExternalSecret, and optional NetworkPolicy. |
| `airflow-admission-check.json` | Optional server-side dry-run evidence proving the generated Kubernetes pack and `pod-spec.yaml` pass Kubernetes admission for the target cluster. |
| `xcom-summary.json` | Planned XCom handoff summary that `run-spec-exec` later replaces with the final XCom outcome and that the generated KubernetesPodOperator task copies to `/airflow/xcom/return.json`. The final summary contains `runtime_evidence_path` plus bounded inline `runtime_evidence` with step timings and any DQ/route/throughput/cleanup sections emitted by `dpone run --format json`. |
| `airflow_dag_factory.py` | Generated artifact-loading DAG factory for custom dpone image tasks. It loads `kpo-kwargs.json` plus `pod-spec.yaml`, wires `KubernetesPodOperator` with `do_xcom_push=True`, and marks direct KPO mode as not the sparse git-sync runtime contract. |
| `outcome_gate.py` | Self-contained downstream Airflow helper that evaluates a final XCom outcome after the pod task has pushed it. |
| `runtime-evidence.json` | Runtime evidence path written by `run-spec-exec` and verified by `dpone gitops airflow evidence-verify`. |
| `image-contract.json` | Stable image contract for the custom dpone image, including required tools such as `dpone`, `bcp`, and `clickhouse-client` when relevant. |
| `.dpone/gitops/airflow/_dags/<dag_id>.dag-spec.json` | Declarative DAG spec emitted by the dag-spec builder alongside compact packs (`gitops.airflow_dag_spec`, `spec_fingerprint`, resolved edges). |

## Declarative dag-spec and workload init

Domain catalogs may declare a `dags:` block (schedule, retries, wiring, workload
membership). `AirflowDagSpecBuilder` resolves those entries into dag-spec JSON
artifacts consumed by the scheduler loader.

`dpone workload init DOMAIN/WORKLOAD_ID` scaffolds GitOps artifacts with
plan-first idempotency:

- batch or catalog manifest layout;
- SQL/docs stubs;
- additive domain catalog patch (`workloads` + optional `dags:` entry);
- `ownership.yaml` update;
- post-apply manifest validation.

After `--apply`, prove compile readiness:

```bash
dpone gitops airflow pack --workload-set dpone_workloads/gitops/gitops.yaml --mode plan
dpone gitops airflow deps --workload-set dpone_workloads/gitops/gitops.yaml --format md
```

Dependency merge combines inferred lineage edges, manifest `depends_on`, and
curated `dags:` wiring. Inspect provenance with
`dpone dag explain-edge --dag-spec --dag-id <id>`.

See [Airflow self-service](airflow-self-service.md) for the analyst golden path.

The generated `run-spec.json` follows the same fail-closed order for every
bundle entry:

1. `dpone gitops bundle verify <bundle.json> --require-attestation`
2. `dpone gitops verify <gitops_plan.json> --worktree . --verify-lock`
3. `dpone run <manifest.yaml>`

The generated `entrypoint.sh` keeps Airflow simple:

```bash
dpone gitops airflow run-spec-exec .dpone/gitops/airflow/run-spec.json \
  --evidence-output .dpone/gitops/airflow/runtime-evidence.json \
  --xcom-output .dpone/gitops/airflow/xcom-summary.json
```

## Quickstart

Build an attested bundle from changed files:

```bash
dpone gitops bundle \
  --from-ref origin/master \
  --to-ref HEAD \
  --runner airflow \
  --policy-profile release \
  --attest \
  --output-dir .dpone/gitops/bundle
```

Describe the custom dpone image that Airflow will run:

```bash
dpone gitops airflow image-contract \
  --image ghcr.io/acme/dpone:2026.06.16 \
  --image-digest sha256:0000000000000000000000000000000000000000000000000000000000000000 \
  --dpone-version 0.15.0 \
  --python-version 3.12 \
  --airflow-provider-version 10.18.0 \
  --tool dpone \
  --tool bcp \
  --tool clickhouse-client \
  --output-path .dpone/gitops/airflow/image-contract.json
```

Render the Airflow handoff pack:

```bash
dpone gitops airflow render .dpone/gitops/bundle/bundle.json \
  --image ghcr.io/acme/dpone:2026.06.16 \
  --image-digest sha256:0000000000000000000000000000000000000000000000000000000000000000 \
  --image-contract .dpone/gitops/airflow/image-contract.json \
  --namespace dpone-runners \
  --service-account dpone-runner \
  --dag-id dpone_gitops \
  --task-id dpone_orders \
  --outcome-mode xcom_then_gate \
  --require-attestation \
  --output-dir .dpone/gitops/airflow
```

You can also build the runtime contract directly when another renderer owns the
pod template:

```bash
dpone gitops airflow run-spec .dpone/gitops/bundle/bundle.json \
  --image ghcr.io/acme/dpone:2026.06.16 \
  --image-digest sha256:0000000000000000000000000000000000000000000000000000000000000000 \
  --evidence-output .dpone/gitops/airflow/runtime-evidence.json \
  --require-attestation \
  --output-path .dpone/gitops/airflow/run-spec.json
```

Build only the runtime placement profile, XCom summary, and Airflow DAG factory
when a GitOps system or Airflow DAG factory already owns the pod template:

```bash
dpone gitops airflow runtime-profile .dpone/gitops/bundle/bundle.json \
  --run-spec-path .dpone/gitops/airflow/run-spec.json \
  --runtime-evidence-path .dpone/gitops/airflow/runtime-evidence.json \
  --output-path .dpone/gitops/airflow/runtime-profile.json \
  --xcom-summary-path .dpone/gitops/airflow/xcom-summary.json \
  --dag-factory-path .dpone/gitops/airflow/airflow_dag_factory.py \
  --outcome-gate-path .dpone/gitops/airflow/outcome_gate.py \
  --image ghcr.io/acme/dpone:2026.06.16 \
  --image-digest sha256:0000000000000000000000000000000000000000000000000000000000000000 \
  --namespace dpone-runners \
  --service-account dpone-runner \
  --cpu-request 250m \
  --memory-request 512Mi \
  --cpu-limit 2 \
  --memory-limit 2Gi \
  --artifact-sink-path .dpone/gitops/airflow \
  --label app.kubernetes.io/name=dpone \
  --annotation dpone.dev/runner=airflow \
  --env DPONE_PROFILE=release \
  --runner-policy release \
  --outcome-mode xcom_then_gate \
  --git-sync-repo ssh://git@git.example.test/platform/example-workloads.git \
  --git-sync-ref main \
  --git-sync-image registry.k8s.io/git-sync/git-sync:v4.7.0 \
  --git-sync-depth 1 \
  --git-sync-filter blob:none \
  --git-sync-auth-mode ssh_secret \
  --git-sync-ssh-secret example-workloads-git \
  --airflow-connection-bridge k8s_secret \
  --airflow-connection-secret dpone-airflow-connections \
  --airflow-runtime-mode runtime_only
```

When `--git-sync-repo` is set, `--git-sync-image` is required. dpone collects
the sparse allowlist from the already-rendered `gitops.bundle` entries and
`gitops.plan.sparse_paths`, then adds required Airflow control-plane artifacts
such as `bundle.json` and `run-spec.json`. This keeps manifest dependency
discovery inside dpone instead of duplicating manifest schema knowledge in an
Airflow DAG helper.

Sparse checkout and partial clone are separate controls. Sparse checkout uses
the generated `--sparse-checkout-file` to limit the files present in
`/workspace/repo`. Partial clone uses `--filter=blob:none` or `--filter=tree:0`
to reduce Git object transfer for large repositories. `git-sync:v4.4.0`
supports sparse checkout but does not expose `--filter`; use
`registry.k8s.io/git-sync/git-sync:v4.7.0` or newer before enabling
`--git-sync-filter` in release profiles. Unknown custom git-sync images are
allowed with a warning in advisory and PR profiles, but release profiles require
a verifiable `v4.7.0+` image when partial clone is requested.

### Airflow Connection bridge

The official dpone runtime image is runtime-only and does not install
`apache-airflow`. Manifests can still use `connection_type: airflow`: the
runtime first uses Airflow `BaseHook` when Airflow is present, then falls back
to standard `AIRFLOW_CONN_<CONN_ID>` URI environment variables. The runner pack
keeps that bridge first-class:

- `--airflow-connection-bridge k8s_secret` is the default and emits
  `connection_bridge.env` refs backed by a Kubernetes Secret.
- `--airflow-connection-secret dpone-airflow-connections` controls the Secret
  name; Secret keys are the exact env var names, for example
  `AIRFLOW_CONN_MSSQL_DWH`.
- `--airflow-connection-bridge env` documents required env names without
  adding Secret refs, for clusters that inject env through another policy.
- `--airflow-connection-bridge disabled` is allowed for advisory experiments,
  but `pod-doctor --runner-policy release` blocks runtime-only pods that use
  `connection_type: airflow` without a bridge.
- `--airflow-runtime-mode airflow_image` is for custom images that explicitly
  install Airflow and providers; the official GHCR runtime image remains
  runtime-only.

Generated artifacts never serialize Airflow URI values. They contain only
connection ids, env var names, Secret names, and Secret key names.

For the self-service provider path, compact packs that carry a
`kubernetes_secret_volume` projection with `payload_format:
airflow_connection_uri` use
`AirflowConnectionSecretVolumeKubernetesPodOperator`. The operator resolves
Airflow Connections only during task execution, writes URI values into a
short-lived Kubernetes Secret through an injectable projector, mounts that
Secret as files for the runtime pod, and keeps URI values out of DAG files, pod
specs, XCom, evidence, and normal logs. Synchronous KPO runs clean up the Secret
after `execute()` returns; deferrable KPO runs must use `cleanup_policy: retain`
with platform-managed cleanup so the Secret is not removed before the deferred
pod completes. The older env bridge remains a development or migration path
only.

`connection_projection.secret_name` is the base prefix for that temporary
object. At task execution the provider appends a deterministic digest of the
Airflow task-attempt key, creates an immutable Secret, and mounts and deletes
only the derived name. It never replaces a Secret on `409`. A retry, mapped task,
or overlapping DAG run therefore owns a different object without any new pack
field or user action.

If a task fails with `DPONE_AIRFLOW_CONNECTION_SECRET_CONFLICT`, do not retry by
blindly deleting the object. Confirm that the correlated task attempt is no
longer running, remove the stale attempt Secret through the platform's
credentialed Kubernetes workflow, and retry the Airflow task. For
`DPONE_AIRFLOW_CONNECTION_SECRET_CLEANUP_FAILED`, retain the task failure and
let the same restricted workflow or retained-Secret GC remove the object. Normal
logs expose only a SHA-256 reference, not the physical Secret name, namespace,
Connection URI, or Kubernetes response body. The operator ServiceAccount no
longer needs `replace` for this temporary-Secret path.

To map a digest-only diagnostic to the physical object, a platform operator
reads `dag_id`, `task_id`, `run_id`, `try_number`, and `map_index` from the
Airflow TaskInstance plus the projection's base `secret_name`, then constructs
`AirflowTaskAttemptIdentity` and calls
`derive_airflow_connection_secret_attempt()` from
`dpone_airflow_pack.connection_secret_identity` in a restricted admin shell.
Proceed only when the returned `secret_ref` equals the diagnostic reference;
use the returned `secret_name` and the KPO namespace for the reviewed delete.
This lookup is operator-only and must not be added to ordinary task logs or
pipeline-owner output.

### Attempt Secret retention GC

Use this platform workflow for deferrable `cleanup_policy: retain` objects and
for attempt Secrets left behind by an eager-cleanup failure. It is not part of
the pipeline-owner golden path. The GC is namespace-scoped, uses fixed dpone
selectors, and accepts only Kubernetes `PartialObjectMetadataList`; it never
reads Secret `data`/`stringData` or Pod `spec`/`status`.

Install the command in its restricted platform/CronJob image with the optional
SDK extra:

```bash
python -m pip install 'dpone[kubernetes]'
```

Do not add this dependency to DAG processor images solely for GC. Airflow
images that already install the CNCF Kubernetes provider may already contain a
compatible SDK, but the dedicated GC image and service account remain the
recommended production topology. `DPONE_AIRFLOW_SECRET_GC_SDK_UNAVAILABLE`
means this extra is absent or incomplete and fails before Kubernetes auth or
network access.

First inspect a fresh plan:

```bash
dpone airflow connection-secret-gc-plan \
  --namespace airflow-example \
  --minimum-age-seconds 86400 \
  --page-size 500
```

The text report contains only namespace, counts, SHA-256 references, reasons,
ages, and the next command. `protect` means an existing managed Pod references
the Secret or the object is younger than the age floor. `delete` means an
expired valid object has no matching Pod. `quarantine` means Secret metadata is
unsafe or incomplete; inspect it through a restricted platform workflow rather
than deleting it automatically. Any malformed managed Pod blocks the whole
plan because the credential it protects is ambiguous.

After review, run an explicitly authorized bounded apply:

```bash
export DPONE_SECRET_GC_ACTOR='ci://dpone/connection-secret-gc'
dpone airflow connection-secret-gc-apply \
  --namespace airflow-example \
  --minimum-age-seconds 86400 \
  --page-size 500 \
  --max-delete-count 100 \
  --kube-auth in-cluster \
  --actor "${DPONE_SECRET_GC_ACTOR}" \
  --allowed-actor "${DPONE_SECRET_GC_ACTOR}" \
  --confirm-delete
```

Apply validates confirmation and the exact actor allowlist before constructing
a Kubernetes client. It never consumes an old plan: it inventories again,
sorts by `secret_ref`, and deletes at most `max-delete-count` objects. Each
delete carries both observed UID and resourceVersion preconditions. HTTP `404`
is an already-absent success-compatible skip. HTTP `409` means the object
changed and returns `partial`; rerun the plan instead of deleting blindly. Any
other delete failure stops later mutations and reports already deleted, failed,
and unattempted digest refs truthfully.

Use a dedicated service account in the ephemeral-credential namespace. A
minimal Role is:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: dpone-airflow-connection-secret-gc
  namespace: airflow-example
rules:
  - apiGroups: [""]
    resources: ["secrets", "pods"]
    verbs: ["list"]
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["delete"]
```

Kubernetes RBAC cannot restrict `list secrets` to a metadata representation or
label selector. Keep this identity namespace-scoped, do not attach it to normal
task Pods, and do not provide a general shell/token export path. The provider
task identity needs create/delete for its own attempt objects but no longer
needs `replace`; the GC identity is separate.

Command outcomes:

| Exit | Meaning | Operator action |
| ---: | --- | --- |
| `0` | Plan succeeded, or apply completed/no-op | Record the JSON report when audit evidence is required |
| `1` | Quarantine, batch remainder, conflict, or partial apply | Rerun plan; review the digest refs and stable reasons |
| `2` | Invalid local policy/auth combination | Correct namespace, bounds, or auth options before retry |
| `3` | API unavailable or list continuation expired (`410`) | Restore dependency and rerun the whole plan |
| `4` | Confirmation/actor/RBAC/metadata-only safety failure | Fix policy/RBAC; HTTP `406` must not use a full-object fallback |
| `5` | Unexpected internal failure | Preserve redacted output and escalate with the trace/error code |

For automation, use `--format json`; payloads validate against
`dpone.airflow-connection-secret-gc-plan.v1` and
`dpone.airflow-connection-secret-gc-apply.v1`. Never scrape physical Secret
names from Kubernetes into logs. Pre-v0.72.9 unlabelled objects are outside the
selector and remain a reviewed manual migration task.

Recommended rollout is provider first, plan-only observation for at least one
retention window, non-production confirmed apply, approved live certification,
then a bounded CronJob. To roll back, suspend apply before rolling back provider
or CLI code. Existing labelled objects remain safe for a later fixed GC; do not
remove their lifecycle labels.

After `pod-contract` has written the final `connection_bridge` section, build
the deploy-time bridge skeletons:

```bash
dpone gitops airflow connection-bridge-plan \
  --artifact-dir .dpone/gitops/airflow \
  --external-secret-store airflow-connections \
  --external-secret-store-kind SecretStore \
  --external-secret-remote-prefix airflow/connections
```

The command reads `runtime-profile.json` and `pod-contract.json`; it does not
parse manifests or call Airflow. For the default `k8s_secret` bridge it writes
`connection-bridge-plan.json`, `airflow-connections-secret.yaml`,
`airflow-connections-externalsecret.yaml`, and
`airflow-connections.env.example`. The Secret manifest uses
`REPLACE_WITH_AIRFLOW_CONNECTION_URI` placeholders. Apply either the Kubernetes
Secret skeleton after filling values through your secret-management process or
the ExternalSecret skeleton after pointing `remoteRef.key` entries at your
external secret manager. Do not commit real Airflow Connection URI values.

Supported git-sync auth modes are:

| Mode | Contract behavior |
| --- | --- |
| `image` | Assumes the custom runtime image or cluster policy already provides repository access. No Kubernetes Secret reference is emitted. |
| `ssh_secret` | References a Kubernetes Secret by name and key names only. The Secret should provide the private key and `known_hosts`; secret values are never serialized to dpone artifacts. |
| `https_secret` | References a Kubernetes Secret by name and username/password key names only. Values are mounted as `GITSYNC_USERNAME` and `GITSYNC_PASSWORD` environment variables for the initContainer. |

Build the pod contract, PodSpec, and KubernetesPodOperator kwargs when Airflow
uses a custom dpone image with KubernetesPodExecutor-style pod creation:

```bash
dpone gitops airflow pod-contract .dpone/gitops/bundle/bundle.json \
  --run-spec-path .dpone/gitops/airflow/run-spec.json \
  --runtime-profile-path .dpone/gitops/airflow/runtime-profile.json \
  --output-path .dpone/gitops/airflow/pod-contract.json \
  --pod-spec-path .dpone/gitops/airflow/pod-spec.yaml \
  --kpo-kwargs-path .dpone/gitops/airflow/kpo-kwargs.json \
  --image-pull-secret regcred \
  --env-from-configmap dpone-runner-config \
  --env-secret DPONE_TOKEN=dpone-token:token \
  --volume dpone-artifacts=.dpone/gitops/airflow \
  --volume-mount dpone-artifacts=/workspace/.dpone/gitops/airflow:ro \
  --node-selector workload=dpone \
  --toleration dedicated=dpone:NoSchedule \
  --label app.kubernetes.io/component=dpone-runner \
  --annotation dpone.dev/pod-contract=enabled \
  --on-finish-action delete_pod \
  --outcome-mode xcom_then_gate
```

Validate the pod contract before a DAG factory or KubernetesPodExecutor helper
uses it:

```bash
dpone gitops airflow pod-doctor \
  --artifact-dir .dpone/gitops/airflow \
  --runner-policy release
```

`--artifact-dir` is the preferred self-service entrypoint for Airflow DAG
repositories. It loads `pod-contract.json`, `pod-spec.yaml`, and
`kpo-kwargs.json` from one generated directory, while the explicit
`--pod-contract-path`, `--pod-spec-path`, and `--kpo-kwargs-path` flags remain
available for drift debugging or non-standard artifact layouts.

### Cluster doctor

`dpone gitops airflow cluster-doctor` is the opt-in environment readiness gate
for the real Airflow/Kubernetes cluster. It consumes generated artifacts only:
`runtime-profile.json`, `pod-contract.json`, and optionally
`connection-bridge-plan.json`. It does not parse manifests, import Airflow,
call source/sink systems, or read secret values.

Plan mode is credential-free and prints the exact `kubectl` checks that a live
cluster gate should run:

```bash
dpone gitops airflow cluster-doctor \
  --artifact-dir .dpone/gitops/airflow \
  --runner-policy release \
  --mode plan \
  --output .dpone/gitops/airflow/airflow-cluster-doctor.json
```

Live mode is opt-in and should run only in a credentialed Airflow/Kubernetes
job:

```bash
dpone gitops airflow cluster-doctor \
  --artifact-dir .dpone/gitops/airflow \
  --runner-policy release \
  --mode live \
  --require-external-secret-ready \
  --output .dpone/gitops/airflow/airflow-cluster-doctor.json
```

The live checks validate namespace existence, service account existence, RBAC
for creating pods, reading pods, reading pod logs, and reading events, plus
imagePullSecrets, git-sync Secret keys, and Airflow Connection Secret keys.
When `--require-external-secret-ready` is set, generated ExternalSecret
resources must report `Ready=True`. Secret values are never requested: Secret
checks use key-listing jsonpath output and the report stores only expected key
names such as `AIRFLOW_CONN_MSSQL_DWH`, `ssh`, and `known_hosts`.

Write the artifact inventory and run the unified preflight gate before the DAG
repo consumes the pack:

```bash
dpone gitops airflow pack \
  --artifact-dir .dpone/gitops/airflow \
  --bundle-path .dpone/gitops/bundle/bundle.json \
  --image ghcr.io/acme/dpone:2026.06.16 \
  --image-digest sha256:0000000000000000000000000000000000000000000000000000000000000000 \
  --mode plan \
  --runner-policy release

dpone gitops airflow connection-bridge-plan \
  --artifact-dir .dpone/gitops/airflow

dpone gitops airflow artifact-index \
  --artifact-dir .dpone/gitops/airflow

dpone gitops airflow preflight \
  --artifact-dir .dpone/gitops/airflow \
  --runner-policy release

dpone gitops airflow cluster-doctor \
  --artifact-dir .dpone/gitops/airflow \
  --runner-policy release \
  --mode plan
```

`airflow-runtime-pack.json` is the top-level self-service checklist for
Airflow DAG repositories such as `example-workloads`. In `--mode plan`, missing
artifacts are warnings and the report shows the deterministic command order for
rendering, pod-contract generation, connection bridge planning, artifact
indexing, preflight, cluster-doctor, admission-check, k8s-smoke, pod-watch, and
evidence-bundle collection. In `--mode verify`, missing required artifacts
become blockers. Passing `--include-live-gates` adds credentialed
`cluster-doctor --mode live`, `k8s-smoke --mode live`, and
`pod-watch --mode live` commands, but `pack` still does not start Minikube,
call Airflow, launch Kubernetes pods, or read secret values.

For catalog-based DAGs prefer the compact pack command:

```bash
dpone gitops airflow pack \
  --workload wide_mart_crm \
  --workload-set dpone_workloads/gitops.yaml \
  --env dev \
  --output-path .dpone/gitops/airflow/wide_mart_crm/airflow-pack.json
```

The compact `airflow-pack.json` is scheduler-static. It embeds manifest-local
file dependencies such as `source.options.query.sql_file` and hook-level
`source.options.hooks.*.sql_file`, records their SHA-256 hashes in
`workload_dependencies`, and renders `pre_hook` actions with `execution.airflow:
separate_task` as visible preparation tasks before the runtime KPO. Airflow DAG
code should import only
`dpone_airflow_pack.build_dpone_gitops_task_group_from_pack`; local
loader/governance operators are not needed.

`artifact-index.json` records current SHA-256 values for the generated Airflow
artifacts. `preflight` recomputes the current index, validates registered
GitOps schemas, runs pod-doctor, compares the saved index for stale artifacts,
checks that `connection-bridge-plan.json` exists when `connection_type:
airflow` ids require runtime-only `AIRFLOW_CONN_*` bridging, and emits
`next_actions` such as regenerating `pod-contract`, running
`connection-bridge-plan`, or rerunning `artifact-index`. In release policy, a
missing `artifact-index.json` or required bridge plan is a blocker; in advisory
and PR policy it is a warning so local iteration stays lightweight.
`cluster-doctor` complements preflight: preflight proves the artifact pack is
internally consistent, while cluster-doctor proves the target namespace,
service account, RBAC, Secret keys, and optional ExternalSecret controller are
ready to run that pack.

### Kubernetes manifests and admission check

`dpone gitops airflow k8s-manifests` turns the generated runtime contract into
a deployable Kubernetes pack for GitOps repositories. It consumes
`runtime-profile.json`, `pod-contract.json`, and optionally
`connection-bridge-plan.json`; it does not inspect manifests or rebuild the
Airflow task. The YAML pack is intentionally name/key-only for Secrets:
generated Secret objects contain metadata, required key annotations, and empty
`stringData`, while real values stay in Kubernetes Secrets or an external
secret manager.

```bash
dpone gitops airflow k8s-manifests \
  --artifact-dir .dpone/gitops/airflow \
  --include-network-policy \
  --gitops-controller argocd \
  --manifest-output .dpone/gitops/airflow/airflow-k8s-manifests.yaml \
  --output .dpone/gitops/airflow/airflow-k8s-manifests.json
```

The default pack includes `ServiceAccount`, `Role`, `RoleBinding`, git-sync
Secret skeletons, Airflow Connection Secret skeletons, image pull Secret
skeletons, and an ExternalSecret skeleton when the connection bridge plan
declares one. `--include-network-policy` adds an explicit allow-all-egress
NetworkPolicy skeleton so platform teams can tighten it in the deployment
repo instead of editing Airflow DAG code.

Use `--gitops-controller argocd` when the file is consumed by an Argo CD
`Application`. dpone adds stable ownership labels plus
`argocd.argoproj.io/sync-wave` annotations so ServiceAccount, Secret,
ExternalSecret, Role, RoleBinding, and NetworkPolicy objects have deterministic
infrastructure ordering. Use `--gitops-controller flux` when the file is
committed under a Flux `Kustomization` path. dpone adds stable ownership labels
and leaves Flux prune/wait policy to the Flux `Kustomization` CR, where those
controls belong. Both modes keep Secret values out of the generated YAML.

`dpone gitops airflow admission-check` is the opt-in Kubernetes API gate for
that pack. Plan mode is credential-free and prints the exact server-side
dry-run commands:

```bash
dpone gitops airflow admission-check \
  --artifact-dir .dpone/gitops/airflow \
  --runner-policy release \
  --mode plan \
  --output .dpone/gitops/airflow/airflow-admission-check.json
```

Live mode should run only in a credentialed CI/Airflow/Kubernetes job:

```bash
dpone gitops airflow admission-check \
  --artifact-dir .dpone/gitops/airflow \
  --runner-policy release \
  --mode live \
  --output .dpone/gitops/airflow/airflow-admission-check.json
```

The live gate runs `kubectl apply --dry-run=server` against
`airflow-k8s-manifests.yaml` and `pod-spec.yaml`. This catches Kubernetes
admission, PodSecurityAdmission, Kyverno, Gatekeeper, quota, and API-version
rejections before Airflow schedules a real task.

With `runtime-profile.json.git_sync.enabled=true`, `pod-contract` adds a
first-class sparse git-sync runtime patch:

- `dpone-worktree` `emptyDir` volume mounted at `/workspace`;
- `dpone-sparse-checkout` initContainer that writes the deterministic sparse
  checkout file to `/workspace/.dpone/git-sync/sparse-checkout`;
- `dpone-git-sync` initContainer that runs git-sync one time with
  `--root=/workspace/.git-sync`, `--link=/workspace/repo`, and the generated
  sparse checkout file;
- optional partial clone hardening through `git_sync.clone.filter`, rendered as
  `--filter=blob:none` or `--filter=tree:0` only for git-sync images that dpone
  can safely allow;
- base container `workingDir=/workspace/repo`, so `run-spec-exec` executes
  from the synced worktree;
- reserved volume and mount names guarded by `pod-contract` blockers before
  a pod can be launched.

Validate the runner pack before Airflow consumes it:

```bash
dpone gitops airflow doctor .dpone/gitops/bundle/bundle.json \
  --pod-template .dpone/gitops/airflow/pod_template.yaml \
  --image-contract .dpone/gitops/airflow/image-contract.json \
  --image ghcr.io/acme/dpone:2026.06.16 \
  --require-attestation \
  --runner-policy release
```

After the Airflow pod finishes, verify the runtime evidence:

```bash
dpone gitops airflow evidence-verify .dpone/gitops/airflow/runtime-evidence.json \
  --run-spec-path .dpone/gitops/airflow/run-spec.json \
  --require-all-steps
```

Evaluate the final XCom outcome in CI, a release collector, or a downstream
Airflow task that materializes the XCom payload to JSON:

```bash
dpone gitops airflow outcome-gate .dpone/gitops/airflow/xcom-summary.json \
  --required-status passed
```

Plan the target cluster readiness checks without touching the cluster:

```bash
dpone gitops airflow cluster-doctor \
  --artifact-dir .dpone/gitops/airflow \
  --runner-policy release \
  --mode plan \
  --external-secret dpone-airflow-connections
```

Run the same checks only inside a credentialed Airflow/Kubernetes environment:

```bash
dpone gitops airflow cluster-doctor \
  --artifact-dir .dpone/gitops/airflow \
  --runner-policy release \
  --mode live \
  --require-external-secret-ready \
  --output .dpone/gitops/airflow/airflow-cluster-doctor.json
```

`cluster-doctor` checks namespace presence, service account presence, RBAC
verbs needed by the runner, `imagePullSecret` existence, git-sync Secret keys,
Airflow Connection Secret keys, optional ExternalSecret `Ready=True`, and
quota/LimitRange visibility. It lists Secret key names only; it never asks
Kubernetes for decoded Secret values and the JSON/Markdown report must not
contain Airflow Connection URI values, SSH private keys, HTTPS tokens, or
passwords.

Plan the Kubernetes smoke commands without touching the cluster:

```bash
dpone gitops airflow k8s-smoke \
  --run-spec-path .dpone/gitops/airflow/run-spec.json \
  --runtime-profile-path .dpone/gitops/airflow/runtime-profile.json \
  --pod-contract-path .dpone/gitops/airflow/pod-contract.json \
  --image-contract-path .dpone/gitops/airflow/image-contract.json \
  --xcom-summary-path .dpone/gitops/airflow/xcom-summary.json \
  --runner-kind kubernetes_pod_operator \
  --runner-policy release \
  --mode plan \
  --smoke-name dpone-orders \
  --format json
```

Use opt-in live mode only inside a credentialed Airflow/Kubernetes environment:

```bash
dpone gitops airflow k8s-smoke \
  --run-spec-path .dpone/gitops/airflow/run-spec.json \
  --runtime-profile-path .dpone/gitops/airflow/runtime-profile.json \
  --pod-contract-path .dpone/gitops/airflow/pod-contract.json \
  --image-contract-path .dpone/gitops/airflow/image-contract.json \
  --xcom-summary-path .dpone/gitops/airflow/xcom-summary.json \
  --runner-kind kubernetes_executor \
  --runner-policy release \
  --mode live \
  --smoke-name dpone-orders \
  --timeout-seconds 600 \
  --output .dpone/gitops/airflow/airflow-k8s-smoke.json
```

`--mode plan` is deterministic and credential-free. `--mode live` executes the
generated `kubectl` and outcome-gate commands, records command exit codes, and
turns failed required commands into `airflow_k8s_command_failed` blockers.

Plan the pod launch evidence commands without touching the cluster:

```bash
dpone gitops airflow pod-watch \
  --runtime-profile-path .dpone/gitops/airflow/runtime-profile.json \
  --pod-contract-path .dpone/gitops/airflow/pod-contract.json \
  --image-contract-path .dpone/gitops/airflow/image-contract.json \
  --runtime-evidence-path .dpone/gitops/airflow/runtime-evidence.json \
  --xcom-summary-path .dpone/gitops/airflow/xcom-summary.json \
  --runner-policy release \
  --expected-phase Succeeded \
  --mode plan \
  --format json
```

Use opt-in live mode after the KubernetesPodOperator or
KubernetesPodExecutor-style wrapper has created the pod and the custom dpone
image has written `runtime-evidence.json` and `xcom-summary.json`:

```bash
dpone gitops airflow pod-watch \
  --runtime-profile-path .dpone/gitops/airflow/runtime-profile.json \
  --pod-contract-path .dpone/gitops/airflow/pod-contract.json \
  --image-contract-path .dpone/gitops/airflow/image-contract.json \
  --runtime-evidence-path .dpone/gitops/airflow/runtime-evidence.json \
  --xcom-summary-path .dpone/gitops/airflow/xcom-summary.json \
  --pod-name dpone-gitops-runtime \
  --runner-policy release \
  --expected-phase Succeeded \
  --mode live \
  --timeout-seconds 600 \
  --log-tail-lines 500 \
  --output .dpone/gitops/airflow/airflow-pod-launch-evidence.json
```

`pod-watch` records the observed pod phase, namespace, service account, node,
base container image and imageID, restart count, exit code, Kubernetes events,
and base container log tail. Release mode fails closed when the pod phase is
not the expected phase, the image digest drifts, the base container exits
non-zero, the container restarts, or Kubernetes reports warning events such as
`FailedScheduling`, `ImagePullBackOff`, `CrashLoopBackOff`, or `OOMKilled`.
This complements `k8s-smoke`: smoke proves the custom image and runner contract
can be launched; pod-watch proves the actual Airflow pod launched and completed
with replication-grade evidence.

Collect one correlated Airflow attempt evidence bundle after the runtime,
outcome, smoke, and pod-watch artifacts are available:

```bash
dpone gitops airflow evidence-bundle \
  --bundle-path .dpone/gitops/bundle/bundle.json \
  --run-spec-path .dpone/gitops/airflow/run-spec.json \
  --runtime-profile-path .dpone/gitops/airflow/runtime-profile.json \
  --pod-contract-path .dpone/gitops/airflow/pod-contract.json \
  --runtime-evidence-path .dpone/gitops/airflow/runtime-evidence.json \
  --xcom-summary-path .dpone/gitops/airflow/xcom-summary.json \
  --k8s-smoke-path .dpone/gitops/airflow/airflow-k8s-smoke.json \
  --pod-launch-evidence-path .dpone/gitops/airflow/airflow-pod-launch-evidence.json \
  --dag-id dpone_gitops \
  --task-id dpone_orders \
  --run-id manual__2026-06-16T10:00:00+00:00 \
  --try-number 1 \
  --map-index -1 \
  --pod-name dpone-gitops-runtime \
  --runner-policy release \
  --require-k8s-smoke \
  --require-pod-launch-evidence \
  --output .dpone/gitops/airflow/airflow-evidence-bundle.json
```

`gitops.airflow_evidence_bundle` records the Airflow attempt correlation
(`dag_id`, `task_id`, `run_id`, `try_number`, and `map_index`), Kubernetes pod
correlation (`pod_name`, optional `pod_uid`, namespace, service account, image,
and digest), SHA-256 digests for every collected artifact, warnings for missing
optional evidence, and blockers for missing required artifacts, kind drift, or
failed child evidence. This is the artifact a release reviewer or downstream
GitOps system can keep next to Airflow logs to prove which attempt produced
which custom dpone pod and which evidence files were trusted.

Use `--format markdown` when the output should be pasted into a pull request
or release review. Use `--format json` for CI gates and task metadata.

## Runner policy profiles

`dpone gitops airflow doctor` supports `--runner-policy advisory`, `pr`, and
`release`. The selected profile is returned in the `runner_policy` field of the
`gitops.airflow_doctor` report.

| Profile | Behavior |
| --- | --- |
| `advisory` | Runs the base bundle, pod template, and image contract checks only. |
| `pr` | Adds runner hardening checks as warnings so pull requests can review image digest, service account, resource, and non-root posture. |
| `release` | Turns the same hardening checks into blockers for release gates. |

Release policy requires:

- bundle attestation;
- image contract `image_digest`;
- `spec.serviceAccountName`;
- base container `resources.requests` and `resources.limits`;
- pod-level non-root `securityContext`.

## Airflow handoff

For `KubernetesExecutor`, point Airflow at the rendered `pod_template_file` and
use the same custom dpone image in the scheduler environment. Airflow requires
the first container in the pod template to be named `base`; `dpone gitops
airflow doctor` checks that field and blocks if it drifts.

For `KubernetesPodOperator`, use the generated `airflow_dag_factory.py`
artifact-loading helper when sparse git-sync or `pod-contract` output is part
of the runtime. Its `build_dpone_gitops_task_from_artifacts` helper reads
`kpo-kwargs.json`, rewrites `pod_template_file` to the local `pod-spec.yaml`,
keeps `do_xcom_push=True`, and lets DAG owners add task-specific metadata
without rebuilding PodSpec internals. The older direct KPO snippet in
`airflow_task.py` is useful for simple examples, but direct KPO mode is not
the sparse git-sync runtime contract.

If the Airflow scheduler image installs only the lightweight provider, DAG code
can import the same artifact-loading behavior from `dpone_airflow_pack` instead
of vendoring a helper:

```python
from dpone_airflow_pack import (
    build_dpone_gitops_task_from_artifacts,
    load_dpone_kpo_kwargs,
    validate_dpone_artifacts,
)
from dpone_airflow_pack.step_tasks import build_dpone_gitops_step_tasks_from_artifacts

validate_dpone_artifacts(".dpone/gitops/airflow")
runtime_task = build_dpone_gitops_task_from_artifacts(
    dag=dag,
    artifact_dir=".dpone/gitops/airflow",
    task_id="orders_to_clickhouse",
)

# Use this when run-spec contains source_refresh hooks that should be visible
# as separate Airflow tasks.
step_tasks = build_dpone_gitops_step_tasks_from_artifacts(
    dag=dag,
    artifact_dir=".dpone/gitops/airflow",
)
```

`dpone.airflow.runtime_adapter` and the rest of `dpone.airflow.*` remain
available as compatibility shims when full `dpone` is installed, but production
scheduler images should prefer `dpone-airflow-pack` so the heavy runtime stays
inside KPO pods.

`validate_dpone_artifacts` is a DAG-parse-time guard. It checks that the
generated artifact directory still contains a final-XCom-enabled
`kpo-kwargs.json`, a `pod-spec.yaml` with `spec.containers[0].name: base`, and
the matching `pod-contract.json`. It does not parse manifests, discover sparse
paths, choose git-sync auth, or mutate Kubernetes PodSpecs.
Use `load_dpone_kpo_kwargs` directly only when the DAG needs to pass the loaded
kwargs into a local wrapper around `KubernetesPodOperator`.
`build_dpone_gitops_step_tasks_from_artifacts` reads `run-spec.json` and
creates one KPO per step, preserving `depends_on` edges. Hook steps such as
`source_refresh` do not push final XCom; the `dpone_run` step keeps XCom enabled
for the outcome gate.

For custom dpone images under KubernetesPodExecutor-style wrappers, use
`runtime-profile.json` and `pod-contract.json` as the scheduler-to-runner
contract. Airflow code should call
`build_dpone_gitops_task_from_artifacts` from the generated
`airflow_dag_factory.py`, or implement the same thin artifact-loading pattern
by reading `kpo-kwargs.json` and pointing `pod_template_file` at
`pod-spec.yaml`. The pod receives the same `run-spec.json`,
`runtime-evidence.json`, and `xcom-summary.json` paths. The generated
KubernetesPodOperator kwargs set `do_xcom_push=True`; `run-spec-exec` writes
the final XCom outcome even when a required runtime step fails, then the
wrapper copies `xcom-summary.json` to `/airflow/xcom/return.json`. The final
XCom is intentionally small: it does not include raw stdout/stderr, credentials
or full artifact payloads. It embeds only a bounded, redacted runtime summary so
Airflow CI can show route, DQ, throughput, cleanup and step-timeline evidence
after the successful runtime pod has been deleted.

### Interval-aware execution contract

Rendered `kpo_kwargs.json` and compact packs carry Jinja-templated
`env_vars` that Airflow resolves per task instance:

| Variable | Template | Consumed by |
|---|---|---|
| `DPONE_DAG_ID` | `{{ dag.dag_id }}` | run-state identity |
| `DPONE_DAG_RUN_ID` | `{{ run_id }}` | evidence correlation |
| `DPONE_TRY_NUMBER` | `{{ ti.try_number }}` | evidence correlation |
| `DPONE_LOGICAL_DATE` | `{{ logical_date }}` | `execution_date` default, `{{ ds }}` token |
| `DPONE_INTERVAL_START` | `{{ data_interval_start }}` | `{{ data_interval_start }}` token |
| `DPONE_INTERVAL_END` | `{{ data_interval_end }}` | `{{ data_interval_end }}` token |

Inside the pod, `dpone run` reads these variables (CLI flags
`--interval-start/--interval-end/--execution-date` take precedence), persists
run-state keyed by `(dag_id, execution_date)`, and resolves interval tokens in
manifest options and predicates. Manual DAG runs without a data interval
render empty values and behave exactly like plain CLI runs.

The canonical idempotent pattern (`mode: backfill` + `inner_mode:
partition_replace` + interval-templated chunk window) makes `catchup=True`,
task clears and `airflow dags backfill` replace exactly their own interval —
see the [Backfill guide](backfill.md#airflow-integration) and
`examples/dags/dpone_interval_catchup_dag.py`.

The XCom summary additionally carries the run `interval` and a bounded
`backfill` campaign progress section (schema:
`docs/schemas/gitops/airflow-xcom-summary.schema.json`).

For the `example-workloads` repository, keep the helper thin:

```python
import json
from pathlib import Path

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


def build_dpone_kubernetes_pod_operator(*, task_id: str, artifact_dir: str) -> KubernetesPodOperator:
    root = Path(artifact_dir)
    kwargs = json.loads((root / "kpo-kwargs.json").read_text(encoding="utf-8"))
    kwargs["task_id"] = task_id
    kwargs["pod_template_file"] = str(root / "pod-spec.yaml")
    kwargs["do_xcom_push"] = True
    return KubernetesPodOperator(**kwargs)
```

The helper consumes generated artifacts and may set DAG-specific fields such as
`task_id`; production DAGs can also import
`build_dpone_gitops_task_from_artifacts` from the generated
`airflow_dag_factory.py` instead of carrying this snippet. The helper should
not discover manifests, inspect `depends_on`, compute sparse paths, choose
git-sync auth, or mutate initContainers. Those concerns belong to dpone so CI,
local development, and Airflow all share the same runtime contract. In other
words, manifest discovery is not an Airflow helper responsibility.

`--outcome-mode strict_fail` preserves classic pod semantics: a failed required
step makes the pod exit non-zero. Use it when Airflow task state is the primary
failure signal and XCom on failed pods is only best-effort.

`--outcome-mode xcom_then_gate` is the Airflow-safe replication-grade handoff:
the pod writes `xcom-summary.json`, copies it to `/airflow/xcom/return.json`,
and exits `0` so KubernetesPodOperator can push XCom. The generated
`airflow_dag_factory.py` and `outcome_gate.py` then provide a downstream gate
that fails the DAG when the XCom `status` is not `passed` or blockers are
present. Do not use `xcom_then_gate` without wiring the generated outcome gate,
because the pod task itself intentionally succeeds to preserve the XCom
payload.

The Airflow DAG or KubernetesPodExecutor-style wrapper should not parse dpone
manifest internals. Its job is:

1. load the generated `kpo-kwargs.json` and point `pod_template_file` at the
   generated `pod-spec.yaml`;
2. mount or copy the rendered Airflow artifacts into the worker image or task
   workspace;
3. let the generated git-sync initContainers prepare `/workspace/repo` when
   `git_sync.enabled=true`;
4. run `entrypoint.sh`;
5. persist `bundle.json`, `run-spec.json`, `runtime-evidence.json`, task logs,
   `runtime-profile.json`, `pod-contract.json`, `pod-spec.yaml`,
   `kpo-kwargs.json`, `xcom-summary.json`, `outcome_gate.py`, and the Airflow
   report as release evidence;
6. run `dpone gitops airflow outcome-gate` over the final XCom summary when
   the scheduler or release collector has a materialized JSON copy;
7. run `dpone gitops airflow evidence-verify --require-all-steps` in the
   downstream CI or release collector.

`dpone gitops airflow run-spec-exec` is the only command in this family that
executes runtime work. It is designed for the custom dpone image, not for the
scheduler process. It executes the ordered `steps` from `run-spec.json`
sequentially, stops on the first required failed step, records exit codes and
timestamps, writes `runtime-evidence.json`, and writes a final XCom outcome
with `status`, `failed_step`, `runtime_evidence_sha256`, `step_counts`, and
`artifact_paths`.

## Custom dpone image contract

The image contract is a small JSON file that records the runtime assumptions
the scheduler cannot infer from a tag:

- image reference and optional immutable digest;
- dpone, Python, and Airflow Kubernetes provider versions;
- expected command-line tools such as `dpone`, `bcp`, and
  `clickhouse-client`;
- optional container user, workdir, and entrypoint path.

`dpone gitops airflow doctor` checks that the image contract image matches the
runner image and that the `tools` list includes `dpone`. Route-specific native
tools stay explicit in the contract so PostgreSQL -> MSSQL and MSSQL ->
ClickHouse runners can prove that the image contains the expected fast-path
executables before live execution.

## Public schemas

The Airflow runner pack ships public JSON Schema contracts next to the other
GitOps artifacts:

- `docs/schemas/gitops/airflow-render.schema.json`
- `docs/schemas/gitops/airflow-doctor.schema.json`
- `docs/schemas/gitops/airflow-image-contract.schema.json`
- `docs/schemas/gitops/airflow-run-spec.schema.json`
- `docs/schemas/gitops/airflow-runtime-evidence.schema.json`
- `docs/schemas/gitops/airflow-runtime-profile.schema.json`
- `docs/schemas/gitops/airflow-xcom-summary.schema.json`
- `docs/schemas/gitops/airflow-cluster-doctor.schema.json`
- `docs/schemas/gitops/airflow-k8s-manifests.schema.json`
- `docs/schemas/gitops/airflow-admission-check.schema.json`
- `docs/schemas/gitops/airflow-pack.schema.json`
- `docs/schemas/gitops/airflow-outcome-gate.schema.json`
- `docs/schemas/gitops/airflow-pod-contract.schema.json`
- `docs/schemas/gitops/airflow-pod-doctor.schema.json`
- `docs/schemas/gitops/airflow-connection-bridge-plan.schema.json`
- `docs/schemas/gitops/airflow-artifact-index.schema.json`
- `docs/schemas/gitops/airflow-preflight.schema.json`
- `docs/schemas/gitops/airflow-k8s-smoke.schema.json`
- `docs/schemas/gitops/airflow-pod-launch-evidence.schema.json`
- `docs/schemas/gitops/airflow-evidence-bundle.schema.json`

Use these schemas to validate `gitops.airflow_render`,
`gitops.airflow_doctor`, `gitops.airflow_image_contract`,
`gitops.airflow_run_spec`, `gitops.airflow_runtime_evidence`,
`gitops.airflow_runtime_profile`, `gitops.airflow_xcom_summary`,
`gitops.airflow_cluster_doctor`, `gitops.airflow_k8s_manifests`,
`gitops.airflow_admission_check`, `gitops.airflow_outcome_gate`, `gitops.airflow_pod_contract`,
`gitops.airflow_pod_doctor`,
`gitops.airflow_connection_bridge_plan`, `gitops.airflow_artifact_index`,
`gitops.airflow_preflight`, `gitops.airflow_k8s_smoke`,
`gitops.airflow_pod_launch_evidence`, `gitops.airflow_evidence_bundle`, and
`gitops.airflow_pack`
reports in CI, Airflow deployment checks, or release evidence collectors.

## Runbook

1. Build or pull the custom dpone image and publish it with an immutable digest.
2. Generate a release-profile `gitops.bundle` with `--attest`.
3. Generate or update the image contract with
   `dpone gitops airflow image-contract`.
4. Run `dpone gitops airflow render` and commit or publish the generated
   artifacts as CI outputs. If another system owns the pod template, run
   `dpone gitops airflow runtime-profile` after `dpone gitops airflow run-spec`
   to produce `runtime-profile.json`, `xcom-summary.json`, and
   `airflow_dag_factory.py`.
5. Run `dpone gitops airflow pod-contract` to produce `pod-contract.json`,
   `pod-spec.yaml`, and `kpo-kwargs.json` for a KubernetesPodExecutor helper or
   a custom DAG factory.
6. If the pod should fetch its own sparse worktree, pass `--git-sync-repo`,
   `--git-sync-image`, `--git-sync-ref`, and the selected auth mode to
   `runtime-profile`; then review the generated `git_sync` object and confirm
   that no secret values appear in artifacts. Add `--git-sync-filter blob:none`
   only with `git-sync:v4.7.0+` or a custom image that is intentionally accepted
   outside release mode.
7. If manifests use `connection_type: airflow` in a runtime-only pod, run
   `dpone gitops airflow connection-bridge-plan --artifact-dir
   .dpone/gitops/airflow`, review `connection-bridge-plan.json`, and wire
   either `airflow-connections-secret.yaml`,
   `airflow-connections-externalsecret.yaml`, or
   `airflow-connections.env.example` through your cluster secret process.
8. Run `dpone gitops airflow artifact-index --artifact-dir
   .dpone/gitops/airflow` to write `artifact-index.json`.
9. Run `dpone gitops airflow pack --artifact-dir .dpone/gitops/airflow
   --mode verify --runner-policy release` to write
   `airflow-runtime-pack.json` and review the top-level next actions before the
   DAG repository consumes the artifact directory.
10. Run
   `dpone gitops airflow pod-doctor --artifact-dir .dpone/gitops/airflow --runner-policy release`
   plus
   `dpone gitops airflow preflight --artifact-dir .dpone/gitops/airflow --runner-policy release`
   and
   `dpone gitops airflow doctor --require-attestation --runner-policy release`
   in CI before the Airflow deployment step.
11. Run `dpone gitops airflow cluster-doctor --mode plan --runner-policy
    release` in ordinary OSS CI, then run the same command with `--mode live`
    and `--require-external-secret-ready` only in the credentialed
    Airflow/Kubernetes environment.
12. Run `dpone gitops airflow k8s-smoke --mode plan --runner-policy release` in
   ordinary OSS CI, then run the same command with `--mode live` only in the
   credentialed Airflow/Kubernetes smoke environment.
13. Run `dpone gitops airflow pod-watch --mode plan --runner-policy release` in
   ordinary OSS CI, then run the same command with `--mode live` after the
   credentialed Airflow/KubernetesPodExecutor job creates the real pod.
14. Configure Airflow `KubernetesExecutor` or `KubernetesPodOperator` to use the
   rendered `pod_template_file` and the same custom dpone image.
15. Ensure the custom dpone image runs `entrypoint.sh`, which calls
   `dpone gitops airflow run-spec-exec`.
16. If using `xcom_then_gate`, wire `build_dpone_gitops_outcome_gate` from
   `airflow_dag_factory.py` or `airflow_python_operator_callable` from
   `outcome_gate.py` as the downstream Airflow task.
17. Verify `xcom-summary.json` with `dpone gitops airflow outcome-gate` when a
   CI or release collector has the final XCom JSON artifact.
18. Verify `runtime-evidence.json` with
   `dpone gitops airflow evidence-verify --require-all-steps`.
19. Collect `airflow-evidence-bundle.json` with
   `dpone gitops airflow evidence-bundle --require-k8s-smoke
   --require-pod-launch-evidence` so the final release evidence has one
   Airflow attempt and pod correlation object.
20. Store rendered artifacts, `airflow-cluster-doctor.json`, `airflow-k8s-smoke.json`,
   `airflow-pod-launch-evidence.json`, `airflow-evidence-bundle.json`, and
   runtime logs beside route readiness, bundle attestation, and release
   evidence.

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| `pod_template_base_container_missing` | Ensure `spec.containers[0].name` is `base` in `pod_template.yaml`. |
| `pod_template_image_missing` | Set `--image` when rendering and do not remove the image from the pod template. |
| `bundle_attestation_required` | Build the bundle with `dpone gitops bundle --attest` and rerun doctor with `--require-attestation`. |
| `image_contract_image_mismatch` | Regenerate the image contract with the exact image used by Airflow. |
| `image_contract_dpone_tool_missing` | Add `--tool dpone` to `dpone gitops airflow image-contract`. |
| `runtime_profile_image_digest_required` | Pass `--image-digest` to `dpone gitops airflow runtime-profile` before a release gate. |
| `runtime_profile_service_account_required` | Use a non-default `--service-account` for release runner pods. |
| `runtime_profile_resources_required` | Set CPU and memory requests and limits on the runtime profile. |
| `runtime_profile_artifact_sink_required` | Set `--artifact-sink-path` so evidence handoff paths are explicit. |
| `git_sync_repo_required` | Pass `--git-sync-repo` whenever git-sync is enabled through runtime-profile flags. |
| `git_sync_image_required` | Pass a pinned or approved `--git-sync-image`; git-sync is never inferred from the dpone image. |
| `git_sync_auth_invalid` | Use `--git-sync-auth-mode image`, `ssh_secret`, or `https_secret`; for secret modes pass only Kubernetes Secret names and key names. |
| `git_sync_depth_invalid` | Set `--git-sync-depth` to `0` or a positive integer. |
| `git_sync_filter_invalid` | Use `--git-sync-filter blob:none` or `--git-sync-filter tree:0`. |
| `git_sync_filter_unsupported` | Use `git-sync:v4.7.0+` before enabling partial clone; `git-sync:v4.4.0` is sparse-checkout only. |
| `git_sync_filter_support_unverified` | Pin a verifiable `git-sync:v4.7.0+` image for release mode, or remove `--git-sync-filter`. |
| `git_sync_reserved_name` | Rename user-provided pod volumes or mounts that collide with `dpone-worktree`, `dpone-git-sync-ssh`, or `dpone-git-sync-https`. |
| `pod_contract_base_container_missing` | Regenerate `pod-spec.yaml` or restore `spec.containers[0].name: base`. |
| `pod_contract_xcom_push_required` | Ensure `kpo-kwargs.json` sets `do_xcom_push: true`. |
| `pod_contract_pod_template_mismatch` | Regenerate `kpo-kwargs.json` so `pod_template_file` points at `pod-spec.yaml`. |
| `pod_contract_git_sync_init_containers_required` | Regenerate `pod-spec.yaml`; the sparse checkout initContainer must run before the git-sync initContainer. |
| `pod_contract_git_sync_working_dir_mismatch` | Regenerate `pod-spec.yaml` so the base container starts in the synced worktree path. |
| `pod_contract_git_sync_worktree_mount_required` | Regenerate `pod-spec.yaml` so the base container mounts the `dpone-worktree` volume. |
| `pod_contract_git_sync_sparse_checkout_required` | Regenerate `pod-spec.yaml` so git-sync receives the generated sparse checkout file. |
| `pod_contract_git_sync_depth_required` | Regenerate `pod-spec.yaml` so git-sync receives the configured clone depth. |
| `pod_contract_git_sync_filter_missing` | Regenerate `pod-spec.yaml`; the pod contract requested partial clone but the git-sync initContainer does not include `--filter`. |
| `airflow_outcome_failed` | Inspect `xcom-summary.json`, then inspect the referenced `runtime-evidence.json` and failed step. |
| `airflow_k8s_image_digest_required` | Add an immutable digest to `run-spec.json`, `runtime-profile.json`, or `image-contract.json` before release smoke. |
| `airflow_k8s_xcom_summary_required` | Pass the final `--xcom-summary-path` to `dpone gitops airflow k8s-smoke` for release mode. |
| `airflow_k8s_command_failed` | Inspect `airflow-k8s-smoke.json` results, then rerun the failed generated command in the Airflow/Kubernetes environment. |
| `airflow_cluster_command_failed` | Inspect `airflow-cluster-doctor.json` results, then fix namespace, service account, RBAC, imagePullSecret, Secret, or ExternalSecret setup before running smoke pods. |
| `airflow_cluster_secret_key_missing` | Create the missing Kubernetes Secret key without writing the value to dpone artifacts. |
| `airflow_cluster_external_secret_not_ready` | Check the External Secrets Operator status and remote secret mapping, then rerun `cluster-doctor --mode live --require-external-secret-ready`. |
| `airflow_k8s_manifests_runtime_profile_kind` | Regenerate `runtime-profile.json`; `k8s-manifests` only accepts the public Airflow runtime profile contract. |
| `airflow_k8s_manifests_service_account` | Align `runtime-profile.json.service_account` and `pod-contract.json.service_account` before rendering Kubernetes manifests. |
| `airflow_admission_manifest_missing` | Run `dpone gitops airflow k8s-manifests` before `admission-check`, or pass `--manifest-path` to the generated YAML file. |
| `airflow_admission_command_failed` | Inspect `airflow-admission-check.json`; the Kubernetes API rejected a server-side dry-run for the manifest pack or `pod-spec.yaml`. |
| `airflow_pod_runtime_evidence_required` | Run `dpone gitops airflow run-spec-exec` inside the custom dpone image or publish the final `runtime-evidence.json` before release pod-watch. |
| `airflow_pod_xcom_summary_required` | Publish the final `xcom-summary.json` or switch pod-watch to advisory mode while debugging a pre-run plan. |
| `airflow_pod_phase_mismatch` | Inspect `airflow-pod-launch-evidence.json`, Kubernetes events, and the base container log tail; the pod did not reach the expected phase. |
| `airflow_pod_image_digest_mismatch` | Ensure Airflow launched the digest-pinned custom dpone image from `runtime-profile.json` or `image-contract.json`. |
| `airflow_pod_restart_detected` | Inspect node pressure, image startup scripts, resource limits, and retry policy before trusting the runtime evidence. |
| `airflow_pod_event_blocker` | Review the Kubernetes event reason in `airflow-pod-launch-evidence.json`, then fix scheduling, pull secret, quota, or image issues. |
| `airflow_evidence_artifact_missing` | Publish the required artifact path passed to `dpone gitops airflow evidence-bundle`; release mode should not proceed with an incomplete evidence set. |
| `airflow_evidence_kind_mismatch` | Check that the path passed to `evidence-bundle` points at the expected `gitops.*` report type. |
| `airflow_evidence_child_blocked` | Open the referenced child artifact; the bundle is correctly failing because one collected report already has blockers. |
| `runtime_step_failed` | Open `runtime-evidence.json`, inspect the failed step exit code, and rerun the underlying command in the custom dpone image. |
| `runtime_step_missing` | Re-run `dpone gitops airflow run-spec-exec` with the same `run-spec.json` or check whether the pod terminated before writing evidence. |

For the underlying sparse checkout and bundle semantics, see
[GitOps control plane](gitops-control-plane.md).
