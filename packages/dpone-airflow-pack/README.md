# dpone-airflow-pack

`dpone-airflow-pack` is the lightweight, Airflow-independent reader and DAG
construction library for dpone GitOps packs. The formal
`apache-airflow-providers-dpone` distribution owns provider discovery and the
canonical `airflow.providers.dpone` namespace.

It only reads a static `airflow-pack.json` and builds visible Airflow/Kubernetes tasks. It does not import the full
`dpone` runtime and intentionally contains no source/sink/native transfer dependencies such as ClickHouse, MSSQL,
`pyodbc`, pandas, polars, or ConnectorX.

Recommended zero-boilerplate Airflow DAG import:

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

The formal provider's canonical namespace is PEP 561 typed.
`DponeDag.from_spec(...)` and
`DponeTaskGroup.from_pack(...)` remain typed escape hatches for hybrid DAGs.
The old `dpone_airflow_pack` imports remain compatibility shims and emit one
deprecation warning per process for provider-facade objects.

For the complete import, generated-loader, and cache-layout cutover, follow the
[provider and cache migration guide](https://paulkov.github.io/dpone/airflow-provider-cache-migration/).
Legacy watcher output, exit codes, retention, and recovery are documented in
[legacy pack cache operations](https://paulkov.github.io/dpone/airflow-legacy-pack-cache-operations/).

Legacy installations that still consume a mutable remote
`latest/pack-index.json` may keep the compatibility sync temporarily:

```bash
set -euo pipefail

dpone-airflow-pack-sync \
  --once \
  --index-uri s3://bucket/dpone-artifacts/prod/repo/latest/pack-index.json \
  --reader-connection-id s3_dpone_artifacts_reader \
  --cache-dir /opt/airflow/.dpone-legacy-pack-cache

dpone-airflow-pack-cache-status \
  --cache-dir /opt/airflow/.dpone-legacy-pack-cache \
  --json

# Exact desired-state cache (airflow-index.json under current/):
dpone-airflow-pack-cache-status \
  --cache-dir /opt/airflow/.dpone-cache \
  --ack-path /opt/airflow/.dpone-ack/loader-ack.json \
  --ack-root /opt/airflow/.dpone-ack \
  --json
```

An infrastructure-owned projector may additionally pass
`--airflow-variable-key dpone_airflow_pack_cache_status`. The publication is
diagnostic, bounded to 1 MiB and fail-open; local cache/ACK bytes remain the
authority.

The loader reads only the local composite deployment index and its listed
checksum-verified DAG specs/packs. It does not scan authoring sources, refresh a
remote cache, or read Airflow Variables, Connections, Vault, Kubernetes, or a
metadata database during parse.

## Official Helm chart ACK isolation

The official Airflow Helm chart propagates component-level volume mounts to
auxiliary containers. Use Helm `3.19.0` or newer in the Helm 3 release line and
the package post-renderer for exact-cache profiles:

For a published release, replace `X.Y.Z` with a version that exists on PyPI.
Before release, install the exact candidate wheel produced from the reviewed
commit and verify the values through the same immutable CI evidence. Never
combine a wheel and values from different commits.

```bash
set -euo pipefail

DPONE_VERSION=X.Y.Z
: "${DPONE_RELEASE_SOURCE_COMMIT:?set the attested 40-hex source commit for this release}"
case "${DPONE_RELEASE_SOURCE_COMMIT}" in
  *[!0-9a-f]*|'') printf 'DPONE_RELEASE_SOURCE_COMMIT must be lowercase hex\n' >&2; exit 2 ;;
esac
[ "${#DPONE_RELEASE_SOURCE_COMMIT}" -eq 40 ] || exit 2
: "${AIRFLOW_VERSION:?set the certified Airflow version}"
case "${AIRFLOW_VERSION}" in
  2.10.5) PROFILE=airflow-cache-values-2.10.yaml; CHART_VERSION=1.19.0 ;;
  3.2.0) PROFILE=airflow-cache-values-3.2.yaml; CHART_VERSION=1.22.0 ;;
  *) printf 'AIRFLOW_VERSION is not certified by this profile set\n' >&2; exit 2 ;;
esac
release_dir="$(mktemp -d dpone-airflow-release.XXXXXX)"
trap 'rm -rf "${release_dir}"' EXIT
python3 -m pip download --no-deps --only-binary=:all: \
  --dest "${release_dir}" "dpone-airflow-pack==${DPONE_VERSION}"
release_wheel="$(find "${release_dir}" -maxdepth 1 -type f -name 'dpone_airflow_pack-*.whl' -print -quit)"
[ -n "${release_wheel}" ]
gh attestation verify "${release_wheel}" \
  --repo PaulKov/dpone \
  --source-digest "${DPONE_RELEASE_SOURCE_COMMIT}" \
  --format json >"${release_dir}/attestation-verification.json"
python3 -m pip install "${release_wheel}"
curl --disable --proto '=https' --tlsv1.2 --fail --location \
  --output airflow-cache-values.yaml -- \
  "https://raw.githubusercontent.com/PaulKov/dpone/${DPONE_RELEASE_SOURCE_COMMIT}/docs/examples/${PROFILE}"
sha256sum -- airflow-cache-values.yaml >airflow-cache-values.SHA256SUMS
sha256sum -c airflow-cache-values.SHA256SUMS
helm repo add apache-airflow https://airflow.apache.org --force-update
helm repo update apache-airflow

helm template airflow apache-airflow/airflow \
  --version "${CHART_VERSION}" \
  -f airflow-cache-values.yaml \
  --post-renderer "$(command -v dpone-airflow-pack-helm-post-renderer)" \
  > airflow.rendered.yaml

dpone-airflow-pack-helm-post-renderer --verify-only \
  < airflow.rendered.yaml > /dev/null
```

### Candidate checkout

For a candidate checkout, download the immutable `dist-airflow` artifact
produced by one successful `airflow-pack-compat.yml` run for the exact reviewed
commit. Its `SHA256SUMS` is the single binding set for the exact wheels and both
bundled Helm-values files. Install and copy the selected values only after every
entry verifies and the checksum set explicitly names each consumed file; no
byte is copied from the possibly dirty checkout:

```bash
set -euo pipefail

: "${DPONE_REVIEWED_CANDIDATE_SHA:?set the exact externally reviewed 40-hex commit}"
case "${DPONE_REVIEWED_CANDIDATE_SHA}" in
  *[!0-9a-f]*|'') printf 'DPONE_REVIEWED_CANDIDATE_SHA must be lowercase hex\n' >&2; exit 2 ;;
esac
[ "${#DPONE_REVIEWED_CANDIDATE_SHA}" -eq 40 ] || exit 2
CANDIDATE_CHECKOUT="$(mktemp -d dpone-reviewed-source.XXXXXX)"
git -C "${CANDIDATE_CHECKOUT}" init -q
git -C "${CANDIDATE_CHECKOUT}" remote add origin https://github.com/PaulKov/dpone.git
git -C "${CANDIDATE_CHECKOUT}" fetch --quiet --depth 1 origin "${DPONE_REVIEWED_CANDIDATE_SHA}"
[ "$(git -C "${CANDIDATE_CHECKOUT}" rev-parse FETCH_HEAD)" = "${DPONE_REVIEWED_CANDIDATE_SHA}" ]
git -C "${CANDIDATE_CHECKOUT}" checkout --quiet --detach "${DPONE_REVIEWED_CANDIDATE_SHA}"
[ "$(git -C "${CANDIDATE_CHECKOUT}" rev-parse HEAD)" = "${DPONE_REVIEWED_CANDIDATE_SHA}" ]
DPONE_CANDIDATE_SHA="${DPONE_REVIEWED_CANDIDATE_SHA}"
: "${AIRFLOW_VERSION:?set the certified Airflow version}"
case "${AIRFLOW_VERSION}" in
  2.10.5) PROFILE=airflow-cache-values-2.10.yaml; CHART_VERSION=1.19.0 ;;
  3.2.0) PROFILE=airflow-cache-values-3.2.yaml; CHART_VERSION=1.22.0 ;;
  *) printf 'AIRFLOW_VERSION is not certified by this profile set\n' >&2; exit 2 ;;
esac
RUN_ID="$(
  gh run list \
    --repo PaulKov/dpone \
    --workflow airflow-pack-compat.yml \
    --commit "${DPONE_CANDIDATE_SHA}" \
    --status success \
    --limit 1 \
    --json databaseId \
    --jq '.[0].databaseId'
)"
test -n "${RUN_ID}"
DIST_AIRFLOW="$(mktemp -d dpone-airflow-candidate.XXXXXX)"
HELM_EVIDENCE="$(mktemp -d "${TMPDIR:-/tmp}/dpone-airflow-helm-evidence.XXXXXX")"
CHART_DIR="$(mktemp -d "${TMPDIR:-/tmp}/dpone-airflow-chart.XXXXXX")"
trap 'rm -rf "${DIST_AIRFLOW}" "${HELM_EVIDENCE}" "${CHART_DIR}" "${CANDIDATE_CHECKOUT}"' EXIT
gh run download "${RUN_ID}" \
  --repo PaulKov/dpone \
  --name "airflow-distributions-${DPONE_CANDIDATE_SHA}" \
  --dir "${DIST_AIRFLOW}"
gh run download "${RUN_ID}" \
  --repo PaulKov/dpone \
  --name "airflow-helm-ack-mounts-${DPONE_CANDIDATE_SHA}" \
  --dir "${HELM_EVIDENCE}"
test "$(cat "${DIST_AIRFLOW}/SOURCE_COMMIT")" = "${DPONE_REVIEWED_CANDIDATE_SHA}"
test "$(cat "${HELM_EVIDENCE}/SOURCE_COMMIT")" = "${DPONE_REVIEWED_CANDIDATE_SHA}"
(cd "${DIST_AIRFLOW}" && sha256sum --check SHA256SUMS)
helm repo add apache-airflow https://airflow.apache.org --force-update
helm repo update apache-airflow
helm pull apache-airflow/airflow --version "${CHART_VERSION}" --destination "${CHART_DIR}"
awk -v chart="airflow-${CHART_VERSION}.tgz" '$2 == chart {print}' \
  "${HELM_EVIDENCE}/CHART_SHA256SUMS" >"${CHART_DIR}/CHART_SHA256SUMS"
test "$(wc -l <"${CHART_DIR}/CHART_SHA256SUMS" | tr -d ' ')" -eq 1
(cd "${CHART_DIR}" && sha256sum -c "${HELM_EVIDENCE}/CHART_SHA256SUMS" \
  --ignore-missing && test "$(sha256sum -c CHART_SHA256SUMS | grep -c ': OK$')" -eq 1)
checksum_covers() {
  awk -v target="$1" '
    {
      name = $2
      sub(/^\*/, "", name)
      sub(/^\.\//, "", name)
      if (name == target) found = 1
    }
    END { exit(found ? 0 : 1) }
  ' "${DIST_AIRFLOW}/SHA256SUMS"
}
core_wheels=("${DIST_AIRFLOW}"/dpone-*.whl)
pack_wheels=("${DIST_AIRFLOW}"/dpone_airflow_pack-*.whl)
provider_wheels=("${DIST_AIRFLOW}"/apache_airflow_providers_dpone-*.whl)
test "${#core_wheels[@]}" -eq 1
test "${#pack_wheels[@]}" -eq 1
test "${#provider_wheels[@]}" -eq 1
for required in "$(basename "${core_wheels[0]}")" \
  "$(basename "${pack_wheels[0]}")" \
  "$(basename "${provider_wheels[0]}")" \
  airflow-cache-values-2.10.yaml airflow-cache-values-3.2.yaml; do
  test -f "${DIST_AIRFLOW}/${required}"
  checksum_covers "${required}"
done
values="${DIST_AIRFLOW}/${PROFILE}"
python3 -m pip install "${core_wheels[0]}[kubernetes]" \
  "${pack_wheels[0]}" "${provider_wheels[0]}"
[ ! -e airflow-cache-values.yaml ] || { printf 'airflow-cache-values.yaml already exists\n' >&2; exit 3; }
[ ! -e "airflow-${CHART_VERSION}.tgz" ] || { printf 'reviewed chart artifact already exists\n' >&2; exit 3; }
[ ! -e airflow-chart.SHA256SUMS ] || { printf 'reviewed chart checksum already exists\n' >&2; exit 3; }
install -m 0444 "${values}" airflow-cache-values.yaml
install -m 0444 "${CHART_DIR}/airflow-${CHART_VERSION}.tgz" "airflow-${CHART_VERSION}.tgz"
install -m 0444 "${CHART_DIR}/CHART_SHA256SUMS" airflow-chart.SHA256SUMS
sha256sum -c airflow-chart.SHA256SUMS
helm template airflow "airflow-${CHART_VERSION}.tgz" \
  -f airflow-cache-values.yaml \
  --post-renderer "$(command -v dpone-airflow-pack-helm-post-renderer)" \
  > airflow.rendered.yaml
dpone-airflow-pack-helm-post-renderer --verify-only \
  < airflow.rendered.yaml > /dev/null
```

`dist-airflow` and `airflow-helm-ack-mounts` are the two immutable candidate
artifacts produced by the same exact-SHA compatibility run. Their source
bindings, wheel/profile checksums, selected official chart checksum and rendered
policy verification must all pass before the candidate is deployable. Preserve
the copied `airflow-<chart>.tgz` and `airflow-chart.SHA256SUMS`; the deployment
must use those exact bytes rather than resolving the repository again. This
candidate path is intentionally separate from the published-release command
above.

This is a **render-only policy example**, not a self-contained Airflow
installation. Its image references, exact desired-state authority ConfigMap,
wrapper ConfigMap and reader/projector Secrets are deliberately platform-owned
placeholders. Do not run `helm upgrade` until those resources and digest-pinned
images have been prepared and reviewed. The complete preparation, preflight,
deploy, observe and rollback procedure is in
[Deploy the exact Airflow cache on Kubernetes](https://paulkov.github.io/dpone/airflow-cache-kubernetes-deployment/).

It grants loader-ACK write access to exactly one parser container, grants the
dpone watcher and status projector read-only access, and removes ACK mounts
from migration, cache init, log-groomer and unrelated containers. A render
without exactly one parser authority fails closed. See the full
[ACK mount policy](https://paulkov.github.io/dpone/airflow-loader-ack-mount-policy/).

Verification success reproduces the input manifest unchanged on stdout and
exits `0`, as required by the Helm post-renderer contract. Redirect stdout to
`/dev/null` only for a standalone diagnostic invocation. Invalid or oversized
input writes `DPONE_AIRFLOW_LOADER_ACK_MOUNT_INVALID` to stderr and exits `2`.
`dpone-airflow-pack-cache-status --json` writes bounded JSON to stdout and exits
`1` when the cache is blocked.

## Runtime pod lifecycle

Strict runtime tasks use `get_logs=true` and
`on_finish_action=delete_succeeded_pod`. Modern KubernetesPodOperator awaits
pod start before following `base` logs, so live runtime stdout appears in the
Airflow task log without the old PodInitializing race. Dpone KPO subclasses also
re-assert `get_logs=true` at `execute` time so a platform-only pack image roll
cannot leave Airflow 3 stuck on stale serialized `get_logs=false` when the DAG
file hash did not change. If the Kubernetes provider exhausts a retryable
`containerLogs` HTTP `429`/`500`/`502`/`503`/`504` while `base` is still
running, the same task attempt emits `DPONE_KPO_LOG_STREAM_DEGRADED` and falls
back to the provider's ordinary base-container status poll. It does not restart
the workload or treat missing live logs as a runtime failure. The fallback does
not accept authentication, authorization, not-found or malformed failures, and
a failed status poll still fails the task. No raw Kubernetes exception body is
logged by the fallback. Pod-status, container-discovery, callback and cleanup
API failures are outside the exact `read_pod_logs` boundary and remain
fail-closed even when their HTTP status is otherwise retryable. If provider
10.19 refreshes expired credentials and replaces its cached pod manager, dpone
rebinds the same narrow log-reader boundary to the replacement before the
provider retries; it does not widen the fallback to authentication failures.

Structured XCom/runtime evidence remains the outcome authority, and the
platform must still certify a cluster log collector plus the separately
deployed stale-pod retention control described in
[Retain and sweep dpone runtime pods](https://paulkov.github.io/dpone/airflow-runtime-pod-retention/).
This lightweight package never starts a Kubernetes janitor. The full `dpone`
distribution ships the plan/apply/render commands, while infrastructure must
review and deploy the rendered namespace-scoped CronJob explicitly.

Required separate outcome gates retain the exact runtime Pod until its launch
envelope is verified. The launch-pin backend is explicit:
`kubernetes_configmap` remains the compatibility default, while
`airflow_task_state` composes with Airflow 3.3+ and CNCF Kubernetes provider
10.20+ `KubernetesPodOperator(durable=True)` for Pod-only RBAC environments.
Select it platform-wide with
`DPONE_LAUNCH_PIN_STORE_BACKEND=airflow_task_state`; do not mix backends across
parser and task-execution components. The new backend verifies KPO's persisted
Pod name/namespace, the server UID, and the immutable live-Pod envelope, and
performs no ConfigMap/Secret/Lease/DaemonSet writes. There is no automatic
fallback. See the
[provider contract](https://paulkov.github.io/dpone/airflow-pack-provider/#launch-pin-for-separate-outcome_gate-exact-cache-tip-flips)
for rollout and rollback evidence.

For DAGs that should run a whole GitOps domain or a named workflow group, keep the grouping in the domain catalog and
resolve it through the lightweight provider:

```yaml
workflow_groups:
  daily:
    workload_ids:
      - dim_customer
      - fact_order
```

```python
from dpone_airflow_pack import workload_ids_from_gitops_domain

workload_ids = workload_ids_from_gitops_domain(
    repo_root,
    "sales",
    group="daily",
)
```

The resolver only reads YAML files and validates that every group member exists in the same domain catalog.

The full `dpone[full,accel]` package belongs in the KPO runtime image, not in the scheduler image.
