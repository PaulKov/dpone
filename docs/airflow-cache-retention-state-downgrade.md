# Downgrade cache-retention state safely

**Purpose.** Recover Airflow after a runtime wrote retention WAL/ACK v2 and an
older runtime that understands only v1 must be restored.

**Audience.** Platform operators performing an approved downgrade or disaster
recovery.

[Back to cache sync and recovery](airflow-cache-sync.md) · **Next likely task:**
[verify the recovered cache](airflow-cache-sync-recovery.md#verification-order).

## Choose one recovery authority

Never translate v2 JSON to v1 and never copy a live v2 cache into the older
runtime.

| Storage | Recovery authority | Supported boundary |
| --- | --- | --- |
| `emptyDir` | Older exact image/chart plus immutable release and deployment artifacts | New parser Pod with a new empty volume |
| Persistent volume | **Not certified by this release** | Keep the newer runtime and stop; a separate snapshot/PVC evidence contract is required |

If the applicable authority does not exist, keep the newer runtime, remove
destructive-retention approval, and escalate. A tar file captured while the
cache writer is running is forensic material, not a recovery snapshot.

## Capture the live occurrence

First complete the
[approval-withdrawal proof](airflow-cache-retention-approval.md#withdrawal-proof)
and suspend runtime Pod retention when installed. Capture exact namespace,
workload and Pod identities before changing GitOps:

```bash
set -euo pipefail

: "${KUBE_CONTEXT:?set the reviewed Kubernetes context}"
: "${AIRFLOW_NAMESPACE:?set the reviewed Airflow namespace}"
: "${PARSE_AUTHORITY_WORKLOAD:?set deployment/name or statefulset/name}"
: "${CACHE_STATUS_CONTAINER:?set the cache-watch container name}"
: "${AIRFLOW_RUNTIME_CONTAINER:?set the Airflow runtime container name}"
: "${CACHE_ROOT:=/opt/airflow/.dpone-cache}"
: "${DOWNGRADE_EVIDENCE_DIR:?set one new absolute evidence directory}"
: "${CACHE_STORAGE_KIND:?set emptyDir or persistent}"
case "${CACHE_STORAGE_KIND}" in emptyDir|persistent) ;; *) exit 2 ;; esac
case "${DOWNGRADE_EVIDENCE_DIR}" in /*) ;; *) exit 2 ;; esac
[ ! -e "${DOWNGRADE_EVIDENCE_DIR}" ] || exit 3
mkdir -m 0700 "${DOWNGRADE_EVIDENCE_DIR}"

kube() {
  kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
    --request-timeout=30s "$@"
}
kube get namespace "${AIRFLOW_NAMESPACE}" -o json \
  >"${DOWNGRADE_EVIDENCE_DIR}/namespace.json"
kube get "${PARSE_AUTHORITY_WORKLOAD}" -o json \
  >"${DOWNGRADE_EVIDENCE_DIR}/workload.json"
cache_volume_name="$(jq -er --arg container "${CACHE_STATUS_CONTAINER}" --arg root "${CACHE_ROOT}" '
  [.spec.template.spec.containers[] | select(.name == $container)]
  | if length == 1 then .[0] else error("expected one cache-status container") end
  | [.volumeMounts[]? | select(.mountPath == $root)]
  | if length == 1 then .[0].name else error("expected one cache-root volume mount") end
' "${DOWNGRADE_EVIDENCE_DIR}/workload.json")"
actual_storage_kind="$(jq -er --arg volume "${cache_volume_name}" '
  [.spec.template.spec.volumes[] | select(.name == $volume)]
  | if length != 1 then error("expected one cache volume")
    elif .[0] | has("emptyDir") then "emptyDir"
    elif .[0] | has("persistentVolumeClaim") then "persistent"
    else error("cache volume is neither emptyDir nor PVC") end
' "${DOWNGRADE_EVIDENCE_DIR}/workload.json")"
[ "${actual_storage_kind}" = "${CACHE_STORAGE_KIND}" ] || {
  printf 'declared cache storage kind does not match the workload\n' >&2
  exit 4
}
selector="$(jq -er '.spec.selector.matchLabels | to_entries | sort_by(.key)
  | map("\(.key)=\(.value)") | join(",")' "${DOWNGRADE_EVIDENCE_DIR}/workload.json")"
kube get pods -l "${selector}" -o json >"${DOWNGRADE_EVIDENCE_DIR}/pods.json"
jq -e --arg container "${CACHE_STATUS_CONTAINER}" '
  (.items | length > 0)
  and all(.items[];
    any(.status.conditions[]?; .type == "Ready" and .status == "True")
    and ([.spec.containers[]? | select(.name == $container)] | length == 1))
' "${DOWNGRADE_EVIDENCE_DIR}/pods.json" >/dev/null
source_runtime_image="$(jq -er --arg container "${AIRFLOW_RUNTIME_CONTAINER}" '
  [.spec.template.spec.containers[] | select(.name == $container)]
  | if length == 1 then .[0].image else error("expected one Airflow runtime container") end
  | select(test("@sha256:[0-9a-f]{64}$"))
' "${DOWNGRADE_EVIDENCE_DIR}/workload.json")"
source_cache_image="$(jq -er --arg container "${CACHE_STATUS_CONTAINER}" '
  [.spec.template.spec.containers[] | select(.name == $container)]
  | if length == 1 then .[0].image else error("expected one cache-status container") end
  | select(test("@sha256:[0-9a-f]{64}$"))
' "${DOWNGRADE_EVIDENCE_DIR}/workload.json")"
source_runtime_digest="sha256:${source_runtime_image##*@sha256:}"
source_cache_digest="sha256:${source_cache_image##*@sha256:}"
jq -e --arg runtime "${AIRFLOW_RUNTIME_CONTAINER}" --arg runtime_digest "${source_runtime_digest}" \
  --arg cache "${CACHE_STATUS_CONTAINER}" --arg cache_digest "${source_cache_digest}" '
  all(.items[];
    any(.status.containerStatuses[]?;
      .name == $runtime and (.imageID | endswith("@" + $runtime_digest)))
    and any(.status.containerStatuses[]?;
      .name == $cache and (.imageID | endswith("@" + $cache_digest))))
' "${DOWNGRADE_EVIDENCE_DIR}/pods.json" >/dev/null
jq -r '.items[].metadata.uid' "${DOWNGRADE_EVIDENCE_DIR}/pods.json" \
  | LC_ALL=C sort -u >"${DOWNGRADE_EVIDENCE_DIR}/old-pod-uids.txt"

while IFS= read -r pod; do
  kube exec "pod/${pod}" -c "${AIRFLOW_RUNTIME_CONTAINER}" -i -- python3 - \
    >"${DOWNGRADE_EVIDENCE_DIR}/source-runtime-versions-${pod}.json" <<'PY'
import importlib.metadata
import json

dpone_versions = {}
for distribution in ("dpone", "dpone-airflow-pack", "apache-airflow-providers-dpone"):
    try:
        dpone_versions[distribution] = importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError:
        pass
print(json.dumps({
    "airflow": importlib.metadata.version("apache-airflow"),
    "dpone_distributions": dpone_versions,
}, sort_keys=True))
PY
  kube exec "pod/${pod}" -c "${CACHE_STATUS_CONTAINER}" -- \
    dpone-airflow-pack-cache-status --cache-dir "${CACHE_ROOT}" --json \
    >"${DOWNGRADE_EVIDENCE_DIR}/source-cache-status-${pod}.json"
done < <(jq -r '.items[].metadata.name' "${DOWNGRADE_EVIDENCE_DIR}/pods.json")
jq -s -e '
  length > 0 and .[0] as $first
  | all(.[]; .airflow == $first.airflow and .dpone_distributions == $first.dpone_distributions)
' "${DOWNGRADE_EVIDENCE_DIR}"/source-runtime-versions-*.json >/dev/null
jq -s -e '
  length > 0 and .[0] as $first
  | ($first.release_id | test("^sha256:[0-9a-f]{64}$"))
  and ($first.current_deployment_id | test("^sha256:[0-9a-f]{64}$"))
  and all(.[];
    .release_id == $first.release_id
    and .current_deployment_id == $first.current_deployment_id
    and (.blockers | length) == 0)
' "${DOWNGRADE_EVIDENCE_DIR}"/source-cache-status-*.json >/dev/null
source_release_id="$(jq -er '.release_id' "$(find "${DOWNGRADE_EVIDENCE_DIR}" -name 'source-cache-status-*.json' -print | LC_ALL=C sort | head -n 1)")"
source_deployment_id="$(jq -er '.current_deployment_id' "$(find "${DOWNGRADE_EVIDENCE_DIR}" -name 'source-cache-status-*.json' -print | LC_ALL=C sort | head -n 1)")"

jq -cn --arg context "${KUBE_CONTEXT}" --arg namespace "${AIRFLOW_NAMESPACE}" \
  --arg namespace_uid "$(jq -er '.metadata.uid' "${DOWNGRADE_EVIDENCE_DIR}/namespace.json")" \
  --arg workload "${PARSE_AUTHORITY_WORKLOAD}" \
  --arg workload_uid "$(jq -er '.metadata.uid' "${DOWNGRADE_EVIDENCE_DIR}/workload.json")" \
  --arg container "${CACHE_STATUS_CONTAINER}" --arg runtime_container "${AIRFLOW_RUNTIME_CONTAINER}" \
  --arg root "${CACHE_ROOT}" --arg runtime_image "${source_runtime_image}" \
  --arg cache_image "${source_cache_image}" --arg release_id "${source_release_id}" \
  --arg deployment_id "${source_deployment_id}" \
  --arg storage_kind "${actual_storage_kind}" --arg cache_volume_name "${cache_volume_name}" \
  '{schema:"dpone.airflow-cache-retention-downgrade-occurrence.v1",
    kube_context:$context,namespace:$namespace,namespace_uid:$namespace_uid,
    workload:$workload,workload_uid:$workload_uid,container:$container,
    runtime_container:$runtime_container,cache_root:$root,
    runtime_image:$runtime_image,cache_image:$cache_image,release_id:$release_id,
    deployment_id:$deployment_id,storage_kind:$storage_kind,
    cache_volume_name:$cache_volume_name}' \
  >"${DOWNGRADE_EVIDENCE_DIR}/occurrence.json"
(cd "${DOWNGRADE_EVIDENCE_DIR}" && \
  sha256sum -- namespace.json workload.json pods.json old-pod-uids.txt occurrence.json \
    source-runtime-versions-*.json source-cache-status-*.json \
    >SHA256SUMS && sha256sum -c SHA256SUMS)
chmod 0400 "${DOWNGRADE_EVIDENCE_DIR}"/*
```

The captured files identify the v2 occurrence for incident review. They are
not injected into a v1 runtime.

## Restore certified `emptyDir`

Promote the exact older image, chart bytes, desired release and deployment via
GitOps. The rollout must replace every old Pod and create fresh `emptyDir`
volumes. Verify cache status inside every new Ready parser Pod:

```bash
set -euo pipefail

: "${KUBE_CONTEXT:?set the reviewed Kubernetes context}"
: "${AIRFLOW_NAMESPACE:?set the reviewed Airflow namespace}"
: "${PARSE_AUTHORITY_WORKLOAD:?set deployment/name or statefulset/name}"
: "${CACHE_STATUS_CONTAINER:?set the cache-watch container name}"
: "${AIRFLOW_RUNTIME_CONTAINER:?set the Airflow runtime container name}"
: "${CACHE_ROOT:=/opt/airflow/.dpone-cache}"
: "${DOWNGRADE_EVIDENCE_DIR:?reuse the sealed occurrence directory}"
: "${EXPECTED_DEPLOYMENT_ID:?set the older immutable deployment sha256}"
: "${EXPECTED_RUNTIME_IMAGE:?set the exact older image@sha256:<digest>}"
: "${EXPECTED_CACHE_STATUS_IMAGE:=${EXPECTED_RUNTIME_IMAGE}}"
: "${EXPECTED_DPONE_VERSION:?set the older dpone package-set version}"
: "${EXPECTED_AIRFLOW_VERSION:?set the older Airflow version}"
expected_image_digest="${EXPECTED_RUNTIME_IMAGE##*@sha256:}"
[ "${expected_image_digest}" != "${EXPECTED_RUNTIME_IMAGE}" ] || exit 2
case "${expected_image_digest}" in *[!0-9a-f]*|'') exit 2 ;; esac
[ "${#expected_image_digest}" -eq 64 ] || exit 2
expected_cache_image_digest="${EXPECTED_CACHE_STATUS_IMAGE##*@sha256:}"
[ "${expected_cache_image_digest}" != "${EXPECTED_CACHE_STATUS_IMAGE}" ] || exit 2
case "${expected_cache_image_digest}" in *[!0-9a-f]*|'') exit 2 ;; esac
[ "${#expected_cache_image_digest}" -eq 64 ] || exit 2

(cd "${DOWNGRADE_EVIDENCE_DIR}" && sha256sum -c SHA256SUMS)
kube() {
  kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
    --request-timeout=30s "$@"
}
kube rollout status "${PARSE_AUTHORITY_WORKLOAD}" --timeout=10m
kube get "${PARSE_AUTHORITY_WORKLOAD}" -o json >"${DOWNGRADE_EVIDENCE_DIR}/workload-restored.json"
jq -e \
  --arg context "${KUBE_CONTEXT}" --arg namespace "${AIRFLOW_NAMESPACE}" \
  --arg workload "${PARSE_AUTHORITY_WORKLOAD}" '
  .kube_context == $context and .namespace == $namespace
  and .workload == $workload and .storage_kind == "emptyDir"
' "${DOWNGRADE_EVIDENCE_DIR}/occurrence.json" >/dev/null
jq -e \
  --slurpfile occurrence "${DOWNGRADE_EVIDENCE_DIR}/occurrence.json" \
  --slurpfile namespace "${DOWNGRADE_EVIDENCE_DIR}/namespace.json" '
  .metadata.uid == $occurrence[0].workload_uid
  and $namespace[0].metadata.uid == $occurrence[0].namespace_uid
' "${DOWNGRADE_EVIDENCE_DIR}/workload-restored.json" >/dev/null
cache_volume_name="$(jq -er --arg container "${CACHE_STATUS_CONTAINER}" --arg root "${CACHE_ROOT}" '
  [.spec.template.spec.containers[] | select(.name == $container)]
  | if length == 1 then .[0] else error("expected one cache-status container") end
  | [.volumeMounts[]? | select(.mountPath == $root)]
  | if length == 1 then .[0].name else error("expected one cache-root volume mount") end
' "${DOWNGRADE_EVIDENCE_DIR}/workload-restored.json")"
jq -e --arg volume "${cache_volume_name}" --arg runtime "${AIRFLOW_RUNTIME_CONTAINER}" \
  --arg runtime_image "${EXPECTED_RUNTIME_IMAGE}" --arg cache "${CACHE_STATUS_CONTAINER}" \
  --arg cache_image "${EXPECTED_CACHE_STATUS_IMAGE}" '
  ([.spec.template.spec.volumes[] | select(.name == $volume and has("emptyDir"))] | length) == 1
  and ([.spec.template.spec.volumes[] | select(.name == $volume and has("persistentVolumeClaim"))] | length) == 0
  and ([.spec.template.spec.containers[] | select(.name == $runtime and .image == $runtime_image)] | length) == 1
  and ([.spec.template.spec.containers[] | select(.name == $cache and .image == $cache_image)] | length) == 1
' "${DOWNGRADE_EVIDENCE_DIR}/workload-restored.json" >/dev/null
selector="$(jq -er '.spec.selector.matchLabels | to_entries | sort_by(.key)
  | map("\(.key)=\(.value)") | join(",")' "${DOWNGRADE_EVIDENCE_DIR}/workload-restored.json")"
kube get pods -l "${selector}" -o json >"${DOWNGRADE_EVIDENCE_DIR}/pods-restored.json"
jq -e --slurpfile old <(jq -Rsc 'split("\n") | map(select(length > 0))' \
    "${DOWNGRADE_EVIDENCE_DIR}/old-pod-uids.txt") \
  --arg cache "${CACHE_STATUS_CONTAINER}" --arg cache_digest "sha256:${expected_cache_image_digest}" \
  --arg runtime "${AIRFLOW_RUNTIME_CONTAINER}" --arg runtime_digest "sha256:${expected_image_digest}" '
  (.items | length > 0)
  and all(.items[]; . as $pod
    | ($old[0] | index($pod.metadata.uid)) == null
    and any($pod.status.conditions[]?; .type == "Ready" and .status == "True")
    and any($pod.status.containerStatuses[]?;
      .name == $runtime and (.imageID | endswith("@" + $runtime_digest)))
    and any($pod.status.containerStatuses[]?;
      .name == $cache and (.imageID | endswith("@" + $cache_digest))))
' "${DOWNGRADE_EVIDENCE_DIR}/pods-restored.json" >/dev/null
while IFS= read -r pod; do
  kube exec "pod/${pod}" -c "${AIRFLOW_RUNTIME_CONTAINER}" -i -- python3 - \
    >"${DOWNGRADE_EVIDENCE_DIR}/runtime-versions-${pod}.json" <<'PY'
import importlib.metadata
import json

dpone_versions = {}
for distribution in ("dpone", "dpone-airflow-pack", "apache-airflow-providers-dpone"):
    try:
        dpone_versions[distribution] = importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError:
        pass
print(json.dumps({
    "airflow": importlib.metadata.version("apache-airflow"),
    "dpone_distributions": dpone_versions,
}, sort_keys=True))
PY
  jq -e --arg airflow "${EXPECTED_AIRFLOW_VERSION}" --arg dpone "${EXPECTED_DPONE_VERSION}" '
    .airflow == $airflow
    and (.dpone_distributions | length) > 0
    and all(.dpone_distributions[]; . == $dpone)
  ' "${DOWNGRADE_EVIDENCE_DIR}/runtime-versions-${pod}.json" >/dev/null
  kube exec "pod/${pod}" -c "${CACHE_STATUS_CONTAINER}" -- \
    dpone-airflow-pack-cache-status --cache-dir "${CACHE_ROOT}" --json \
    >"${DOWNGRADE_EVIDENCE_DIR}/cache-status-${pod}.json"
  jq -e --arg deployment "${EXPECTED_DEPLOYMENT_ID}" '
    .current_deployment_id == $deployment and (.blockers | length) == 0
  ' "${DOWNGRADE_EVIDENCE_DIR}/cache-status-${pod}.json" >/dev/null
done < <(jq -r '.items[].metadata.name' "${DOWNGRADE_EVIDENCE_DIR}/pods-restored.json")
(cd "${DOWNGRADE_EVIDENCE_DIR}" && \
  sha256sum -- workload-restored.json pods-restored.json runtime-versions-*.json cache-status-*.json \
    >RESTORE_SHA256SUMS && sha256sum -c RESTORE_SHA256SUMS)
chmod 0400 "${DOWNGRADE_EVIDENCE_DIR}"/workload-restored.json \
  "${DOWNGRADE_EVIDENCE_DIR}"/pods-restored.json \
  "${DOWNGRADE_EVIDENCE_DIR}"/runtime-versions-*.json \
  "${DOWNGRADE_EVIDENCE_DIR}"/cache-status-*.json \
  "${DOWNGRADE_EVIDENCE_DIR}"/RESTORE_SHA256SUMS
```

Then run the cache-status diagnostic DAG and compare Airflow REST DAG inventory
before unpausing schedules.

## Persistent cache downgrade is not certified

This release does not provide a public evidence contract that binds a
`VolumeSnapshot`, `VolumeSnapshotContent`, restored PVC, storage driver, exact
older images, fresh Pod UIDs and per-Pod cache status into one verified
occurrence. Therefore the `emptyDir` verifier above must never be reused for a
persistent cache.

Stop explicitly when the sealed occurrence is persistent:

```bash
set -euo pipefail

: "${DOWNGRADE_EVIDENCE_DIR:?reuse the sealed occurrence directory}"
(cd "${DOWNGRADE_EVIDENCE_DIR}" && sha256sum -c SHA256SUMS)
storage_kind="$(jq -er '.storage_kind' "${DOWNGRADE_EVIDENCE_DIR}/occurrence.json")"
if [ "${storage_kind}" = persistent ]; then
  printf 'persistent cache downgrade is UNVERIFIED in this release; keep the newer runtime\n' >&2
  exit 4
fi
printf 'this guard applies only to persistent cache occurrences\n' >&2
exit 2
```

Keep the newer runtime, remove destructive-retention approval and escalate to
the storage/platform owners. A storage-native snapshot may be retained as
forensic material, but it is not production recovery evidence. Never restore
over a mounted live volume and never hand-edit v2 state into a v1 shape.

## Acceptance

Accept the downgrade only when:

- approval withdrawal and exact old namespace/workload/Pod evidence are sealed;
- the cache storage is `emptyDir`; persistent-volume downgrade is `UNVERIFIED`;
- no v2 state was copied into a v1-only runtime;
- every activated parser Pod has a new UID and a verified cache status;
- desired/current deployment, loader ACK and Airflow REST DAG inventory agree;
- the incident evidence and previous snapshot/PVC remain retained.

Any mismatch keeps schedules paused and restores the newer exact runtime. Never
retry with hand-edited evidence.
