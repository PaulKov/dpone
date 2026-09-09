# Prepare an Airflow cache Kubernetes deployment

**Purpose.** Prepare pinned images, chart values, authority inputs, and fail-closed preflight checks before deploying the exact parser cache.

**Audience.** Platform engineers preparing an official Apache Airflow Helm deployment.

[Back to Kubernetes cache deployment overview](airflow-cache-kubernetes-deployment.md) · **Next likely task:** [configure the runtime wrappers](airflow-cache-kubernetes-runtime-wrappers.md).

## Prerequisites

Before rendering values, provide all of the following:

1. Kubernetes with `emptyDir.sizeLimit`, POSIX rename/fsync/flock and an
   `fsGroup`-compatible storage driver certified for the target cluster.
2. Helm 3 and the official Apache Airflow chart pinned to `1.19.0` for Airflow
   2.10 or `1.22.0` for Airflow 3.2.
3. An Airflow image pinned by tag and digest with the same released or exact
   candidate `apache-airflow-providers-dpone` wheel as the reviewed change.
4. A dpone sync image pinned by digest with the matching `dpone`, `timeout`, and
   the certified object-storage adapter.
5. The authority ConfigMap from
   [Airflow desired state](airflow-desired-state.md#first-successful-reconcile) and a
   Secret exposing only `AIRFLOW_CONN_S3_DPONE_ARTIFACTS_READER`.
6. The matching `dpone-airflow-pack-helm-post-renderer` executable from
   matching `dpone-airflow-pack` wheel.
7. The reviewed wrapper ConfigMap below.
8. For API-only diagnostics, a `dpone-airflow-status-projector` Secret with the
   installation's least-privilege Airflow Variable/API configuration. Remove
   only the optional projector container when that diagnostic path is not
   deployed, and record API-only freshness as `UNVERIFIED`.
9. A rollback workstation with Bash 5, Git, Python 3.11+ with PyYAML, Helm,
   `kubectl`, `jq`, GNU `sha256sum` from coreutils, and the matching
   `dpone[kubernetes]` extra. Verify these tools before capture: macOS does not
   provide GNU `sha256sum` by default. Update rollback uses `kubectl`
   compare-and-replace; first-install withdrawal uses the lazy Kubernetes SDK
   for an exact UID/resourceVersion-preconditioned delete.

The complete render-tested profiles are:

- [Airflow 2.10 / chart 1.19 values](examples/airflow-cache-values-2.10.yaml);
- [Airflow 3.2 / chart 1.22 values](examples/airflow-cache-values-3.2.yaml).

These files are policy templates. They intentionally contain placeholder image
references and refer to platform-owned ConfigMaps/Secrets. Copy the matching
file into the environment repository, replace every placeholder with a
digest-pinned value, and review the resulting file. The checked-in example is
never itself a deployable environment values file.

For an unreleased candidate, do not copy either profile from a working tree.
Read the candidate procedure only from the exact detached checkout created for
`DPONE_REVIEWED_CANDIDATE_SHA`:

```bash
test "$(git -C "${CANDIDATE_CHECKOUT}" rev-parse HEAD)" = "${DPONE_REVIEWED_CANDIDATE_SHA}"
sed -n '/^### Candidate checkout$/,/^`dist-airflow` is/p' \
  "${CANDIDATE_CHECKOUT}/packages/dpone-airflow-pack/README.md"
```

That procedure downloads the `dist-airflow` artifact from one successful workflow run. The
procedure requires an installed GitHub CLI `gh` authenticated for read access
to Actions artifacts; release consumers do not require `gh`. Its checked
`SHA256SUMS` must include the exact wheels and both bundled Helm profiles before
the selected profile can be consumed.

Use Helm `3.19.0` or newer in the Helm 3 release line for both render and
deployment. Chart `1.22.0` declares Helm `3.19.0` as its minimum.

## Render and preflight

Render before deployment:

```bash
set -euo pipefail

: "${PLATFORM_VALUES:?set the checksum-verified release or candidate values file}"
: "${AIRFLOW_VERSION:?set the certified Airflow version}"
: "${DPONE_AIRFLOW_CHART_TGZ:?set the checksum-verified local chart artifact}"
: "${DPONE_AIRFLOW_CHART_SHA256:?set the reviewed 64-hex chart digest}"
case "${AIRFLOW_VERSION}" in
  2.10.5) CHART_VERSION=1.19.0 ;;
  3.2.0) CHART_VERSION=1.22.0 ;;
  *) printf 'AIRFLOW_VERSION is not certified by this profile set\n' >&2; exit 2 ;;
esac
case "${DPONE_AIRFLOW_CHART_SHA256}" in *[!0-9a-f]*|'') exit 2 ;; esac
[ "${#DPONE_AIRFLOW_CHART_SHA256}" -eq 64 ] || exit 2
test "$(basename "${DPONE_AIRFLOW_CHART_TGZ}")" = "airflow-${CHART_VERSION}.tgz"
test "$(sha256sum "${DPONE_AIRFLOW_CHART_TGZ}" | awk '{print $1}')" = "${DPONE_AIRFLOW_CHART_SHA256}"

helm template airflow "${DPONE_AIRFLOW_CHART_TGZ}" \
  -f "${PLATFORM_VALUES}" \
  --post-renderer "$(command -v dpone-airflow-pack-helm-post-renderer)" \
  > /tmp/airflow-cache.rendered.yaml

dpone-airflow-pack-helm-post-renderer --verify-only \
  < /tmp/airflow-cache.rendered.yaml > /dev/null
```

The Airflow 2 render must place `dpone-cache-init` and `dpone-cache-watch` only
in the scheduler pod. The Airflow 3 render must place them only in the
dagProcessor pod. Both renders must show the main parser cache mount read-only,
writer mounts read-write, and the parser ACK mount separate from cache. The
[loader ACK mount policy](airflow-loader-ack-mount-policy.md) explains why the
post-renderer is mandatory with the official chart.

Before deployment, reject placeholders and mutable image values, then prove
that every referenced external object exists. This values check is structural;
the rendered-PodSpec check below is the image source of truth.

```bash
set -euo pipefail

: "${KUBE_CONTEXT:?set the reviewed kubeconfig context}"
: "${AIRFLOW_NAMESPACE:?set the namespace used by the reviewed Airflow Helm release}"
: "${PLATFORM_VALUES:?set the reviewed platform values file}"
: "${DPONE_CACHE_STATUS_PROJECTOR_ENABLED:?set true or false from reviewed values}"
if grep -niE 'registry\.example|example\.invalid|replace([_-]?with)?|placeholder|todo' "${PLATFORM_VALUES}"; then
  echo "platform values still contain placeholders" >&2
  exit 1
fi
python3 - "${PLATFORM_VALUES}" <<'PY'
import re
import sys

import yaml

DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
IMAGE = re.compile(r"^[^\s@]+@sha256:[0-9a-f]{64}$")

with open(sys.argv[1], encoding="utf-8") as stream:
    values = yaml.safe_load(stream)
if not isinstance(values, dict):
    raise SystemExit("platform values must be one YAML mapping")
airflow_image = values.get("images", {}).get("airflow", {})
if not isinstance(airflow_image, dict) or not DIGEST.fullmatch(str(airflow_image.get("digest", ""))):
    raise SystemExit("images.airflow.digest must be sha256:<64 lowercase hex>")

def explicit_images(value, path="$"):
    if isinstance(value, dict):
        for key, item in value.items():
            child = f"{path}.{key}"
            if key == "image" and isinstance(item, str) and not IMAGE.fullmatch(item):
                yield child
            yield from explicit_images(item, child)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from explicit_images(item, f"{path}[{index}]")

mutable = list(explicit_images(values))
if mutable:
    raise SystemExit("explicit image references must use @sha256: " + ", ".join(mutable))
PY
kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" get configmap dpone-airflow-desired-state-authority
kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" get configmap dpone-airflow-cache-scripts
kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" get secret dpone-airflow-artifacts-reader
case "${DPONE_CACHE_STATUS_PROJECTOR_ENABLED}" in
  true)
    kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
      get secret dpone-airflow-status-projector
    ;;
  false) ;;
  *) printf 'DPONE_CACHE_STATUS_PROJECTOR_ENABLED must be true or false\n' >&2; exit 2 ;;
esac
```

`DPONE_CACHE_STATUS_PROJECTOR_ENABLED` is an explicit reviewed topology input.
When it is `false`, remove the projector container and Secret reference from the
same values and record API-only status diagnostics as `UNVERIFIED`; the
preflight then deliberately skips only that Secret. Do not infer optionality
from a failed API lookup or leave a dangling reference.

## Deploy exact reviewed bytes

Deploy the same reviewed inputs, using the post-renderer on the actual Helm
operation rather than only on preview:

```bash
set -euo pipefail

: "${KUBE_CONTEXT:?set the reviewed kubeconfig context}"
: "${AIRFLOW_NAMESPACE:?set the namespace used by the reviewed Airflow Helm release}"
: "${PLATFORM_VALUES:?set the reviewed Airflow values file}"
: "${AIRFLOW_VERSION:?set the certified Airflow version}"
: "${DPONE_AIRFLOW_CHART_TGZ:?set the checksum-verified local chart artifact}"
: "${DPONE_AIRFLOW_CHART_SHA256:?set the reviewed 64-hex chart digest}"
: "${DPONE_HELM_DEPLOY_EVIDENCE_DIR:?set one absolute deployment evidence directory}"
: "${AIRFLOW_RELEASE:?set the reviewed Helm release name}"
POST_RENDERER="$(command -v dpone-airflow-pack-helm-post-renderer)"
case "${AIRFLOW_VERSION}" in
  2.10.5) CHART_VERSION=1.19.0 ;;
  3.2.0) CHART_VERSION=1.22.0 ;;
  *) printf 'AIRFLOW_VERSION is not certified by this profile set\n' >&2; exit 2 ;;
esac
case "${DPONE_AIRFLOW_CHART_SHA256}" in *[!0-9a-f]*|'') exit 2 ;; esac
[ "${#DPONE_AIRFLOW_CHART_SHA256}" -eq 64 ] || exit 2
test "$(basename "${DPONE_AIRFLOW_CHART_TGZ}")" = "airflow-${CHART_VERSION}.tgz"
test "$(sha256sum "${DPONE_AIRFLOW_CHART_TGZ}" | awk '{print $1}')" = "${DPONE_AIRFLOW_CHART_SHA256}"
deploy_evidence="${DPONE_HELM_DEPLOY_EVIDENCE_DIR%/}"
case "${deploy_evidence}" in /*) ;; *) printf 'deployment evidence directory must be absolute\n' >&2; exit 2 ;; esac
[ ! -L "${deploy_evidence}" ] || exit 2
mkdir -p -m 0700 "${deploy_evidence}"

rendered_manifest="$(mktemp airflow-cache-rendered.XXXXXX.yaml)"
installed_manifest="$(mktemp airflow-cache-installed.XXXXXX.yaml)"
trap 'rm -f "${rendered_manifest}" "${installed_manifest}"' EXIT
helm template "${AIRFLOW_RELEASE}" "${DPONE_AIRFLOW_CHART_TGZ}" \
  --namespace "${AIRFLOW_NAMESPACE}" \
  -f "${PLATFORM_VALUES}" \
  --post-renderer "${POST_RENDERER}" \
  >"${rendered_manifest}"
python3 - "${rendered_manifest}" "${DPONE_CACHE_STATUS_PROJECTOR_ENABLED}" <<'PY'
import re
import sys

import yaml

IMAGE = re.compile(r"^[^\s@]+@sha256:[0-9a-f]{64}$")
required = {"dpone-cache-init", "dpone-cache-watch"}
if sys.argv[2] == "true":
    required.add("dpone-cache-status-projector")
elif sys.argv[2] != "false":
    raise SystemExit("DPONE_CACHE_STATUS_PROJECTOR_ENABLED must be true or false")
observed = set()
invalid = []
with open(sys.argv[1], encoding="utf-8") as stream:
    for document in yaml.safe_load_all(stream):
        if not isinstance(document, dict):
            continue
        spec = document.get("spec", {})
        if document.get("kind") == "CronJob":
            pod_spec = spec.get("jobTemplate", {}).get("spec", {}).get("template", {}).get("spec", {})
        else:
            pod_spec = spec.get("template", {}).get("spec", {})
        if not isinstance(pod_spec, dict):
            continue
        for section in ("initContainers", "containers"):
            for container in pod_spec.get(section, []) or []:
                name = str(container.get("name", ""))
                image = str(container.get("image", ""))
                observed.add(name)
                if not IMAGE.fullmatch(image):
                    invalid.append(f"{document.get('kind')}/{document.get('metadata', {}).get('name')}:{name}={image}")
missing = sorted(required - observed)
if missing:
    raise SystemExit("rendered cache containers are missing: " + ", ".join(missing))
if invalid:
    raise SystemExit("rendered PodSpecs contain mutable images: " + ", ".join(invalid))
PY
"${POST_RENDERER}" --verify-only <"${rendered_manifest}" >/dev/null

helm upgrade --install "${AIRFLOW_RELEASE}" "${DPONE_AIRFLOW_CHART_TGZ}" \
  --kube-context "${KUBE_CONTEXT}" \
  --namespace "${AIRFLOW_NAMESPACE}" \
  --create-namespace \
  -f "${PLATFORM_VALUES}" \
  --post-renderer "${POST_RENDERER}" \
  --atomic --wait --timeout 10m

helm_status="$(mktemp airflow-cache-helm-status.XXXXXX.json)"
namespace_json="$(mktemp airflow-cache-namespace.XXXXXX.json)"
trap 'rm -f "${rendered_manifest}" "${installed_manifest}" "${helm_status}" "${namespace_json}"' EXIT
helm status "${AIRFLOW_RELEASE}" --kube-context "${KUBE_CONTEXT}" \
  --namespace "${AIRFLOW_NAMESPACE}" -o json >"${helm_status}"
kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
  --request-timeout=30s get namespace "${AIRFLOW_NAMESPACE}" -o json >"${namespace_json}"
helm get manifest "${AIRFLOW_RELEASE}" --kube-context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
  >"${installed_manifest}"
"${POST_RENDERER}" --verify-only <"${installed_manifest}" >/dev/null
jq -cn --arg context "${KUBE_CONTEXT}" --arg namespace "${AIRFLOW_NAMESPACE}" \
  --arg namespace_uid "$(jq -er '.metadata.uid' "${namespace_json}")" \
  --arg release "${AIRFLOW_RELEASE}" --arg airflow_version "${AIRFLOW_VERSION}" \
  --argjson release_revision "$(jq -er '.version | select(type == "number" and . > 0)' "${helm_status}")" \
  --arg chart_version "${CHART_VERSION}" \
  --arg chart_sha256 "sha256:${DPONE_AIRFLOW_CHART_SHA256}" \
  --arg rendered_sha256 "sha256:$(sha256sum "${rendered_manifest}" | awk '{print $1}')" \
  --arg installed_sha256 "sha256:$(sha256sum "${installed_manifest}" | awk '{print $1}')" \
  --arg helm_status_sha256 "sha256:$(sha256sum "${helm_status}" | awk '{print $1}')" \
  '{schema:"dpone.airflow-cache-helm-deployment.v1",passed:true,
    kube_context:$context,namespace:$namespace,namespace_uid:$namespace_uid,
    release:$release,release_revision:$release_revision,
    airflow_version:$airflow_version,chart_version:$chart_version,
    chart_sha256:$chart_sha256,rendered_manifest_sha256:$rendered_sha256,
    installed_manifest_sha256:$installed_sha256,
    helm_status_sha256:$helm_status_sha256}' \
  >"${deploy_evidence}/deployment.json"
(cd "${deploy_evidence}" && sha256sum -- deployment.json >SHA256SUMS && sha256sum -c SHA256SUMS)
chmod 0400 "${deploy_evidence}/deployment.json" "${deploy_evidence}/SHA256SUMS"
trap - EXIT
rm -f "${rendered_manifest}" "${installed_manifest}" "${helm_status}" "${namespace_json}"
```

Record the rendered manifest hash and the CI image-provenance receipt that
proves the pinned images contain matching `dpone`, `dpone-airflow-pack` and
provider wheels. PodSpec image validation alone does not prove package parity.

If that Helm revision also used different authority bytes, restore the exact
reviewed previous JSON through the validate/diff/apply/byte-compare procedure
in [Airflow desired state](airflow-desired-state.md#first-successful-reconcile).
Set its real namespace explicitly; do not assume `airflow`. Then restart only
the parse-authority workload so every init/watcher process opens the restored
projection, and wait for rollout convergence:

```bash
set -euo pipefail

: "${KUBE_CONTEXT:?set the reviewed kubeconfig context}"
: "${AIRFLOW_NAMESPACE:?set the namespace used by the restored Helm revision}"
: "${PARSE_AUTHORITY_WORKLOAD:?set deployment/name or statefulset/name}"
kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
  rollout restart "${PARSE_AUTHORITY_WORKLOAD}"
kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
  rollout status "${PARSE_AUTHORITY_WORKLOAD}" --timeout=10m
```

Finally run one complete desired-state reconcile and compare the restored
authority digest, desired/current exact IDs, loader ACK and Airflow REST DAG
inventory. A Helm rollback alone is not authority or cache rollback evidence.

Set `AIRFLOW_VERSION=2.10.5` with the complete 2.10 values profile, or
`AIRFLOW_VERSION=3.2.0` with the complete 3.2 profile. Any other version is
uncertified and fails closed. Then verify desired/current
identity and ACK convergence through the REST procedure in
[cache diagnostics without Kubernetes access](airflow-cache-diagnostics-without-kubectl.md).

## Filesystem authority

- Writer init/sidecar mounts `/opt/airflow/.dpone-cache` read-write.
- The scheduler or `dagProcessor` main container mounts the same volume
  read-only.
- The parser main container mounts a separate `/opt/airflow/.dpone-ack`
  `emptyDir` read-write. It is the only writable path used by the formal loader.
- One pod-level `fsGroup` gives writers shared ownership; parser processes need
  read and traverse permission only.
- `--max-total-bytes` limits one candidate deployment, not aggregate cache
  occupancy. Size the physical volume as `current + candidate + protected
  rollback generations + control/temp/filesystem allowance`.
- The example keeps one rollback generation: `512 MiB + 512 MiB + 512 MiB +
  512 MiB allowance = 2 GiB`. If operations protect more generations, increase
  the volume before increasing the per-deployment budget.
- The volume must support atomic rename, directory fsync and POSIX `flock`.

Do not grant the parser container write access to the cache volume. A parser
that can replace a DAG spec after index verification defeats immutable
deployment identity. The separate ACK volume is the parser's only writable
dpone path.

## Required image and configuration

The init/watch image contains the exact pinned `dpone` release and its S3
adapter. The Airflow image contains the matching
`apache-airflow-providers-dpone` package. Mount the protected
`dpone.airflow-desired-state-authority.v2` JSON as read-only. Its
`workspace_authority_connection_ref` is a logical, non-secret binding to the
shared SQL Server control database; it must be present in the sealed runtime
binding/registry snapshots and must not reuse a workload target binding. Inject the
read-only Airflow/S3 connection through the platform secret mechanism.

The examples below use these environment variables:

```text
DPONE_AIRFLOW_DESIRED_STATE_AUTHORITY_FILE=/etc/dpone/airflow-authority/airflow-desired-state-authority.json
DPONE_AIRFLOW_PACK_CACHE_DIR=/opt/airflow/.dpone-cache
DPONE_AIRFLOW_PACK_READER_CONNECTION_ID=s3_dpone_artifacts_reader
DPONE_PACK_SYNC_TIMEOUT_SECONDS=20
DPONE_PACK_SYNC_INTERVAL_SECONDS=60
DPONE_PACK_CACHE_MAX_TOTAL_BYTES=536870912
DPONE_PACK_RETENTION_INTERVAL_CYCLES=60
```

The authority file, not CLI flags, binds environment, desired-state URI,
registry scope, endpoint and protected source ref.

The referenced Kubernetes Secret must expose exactly
`AIRFLOW_CONN_S3_DPONE_ARTIFACTS_READER`. The repository, values and ConfigMap
must not contain the connection payload. The sync image needs full `dpone` plus
the certified object-storage dependency; the parser image needs only the
matching `apache-airflow-providers-dpone` distribution.
