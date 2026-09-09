# Deploy and operate the Airflow cache Helm release

**Purpose.** Place the exact cache in the official chart, optionally project API diagnostics, verify the deployment, and roll it back.

**Audience.** Airflow platform engineers and on-call operators responsible for the Helm release.

[Back to Kubernetes cache deployment overview](airflow-cache-kubernetes-deployment.md) · **Next likely task:** [materialize and activate an exact cache](airflow-cache-sync.md).

## Official chart values pattern

The chart exposes `extraInitContainers`, `extraContainers`, `extraVolumes` and
`extraVolumeMounts` on scheduler and `dagProcessor`. Component-level mounts are
also propagated to auxiliary chart containers. The values therefore omit the
ACK mount from `extraVolumeMounts`; the mandatory post-renderer grants it only
to the parser after Helm has produced the complete manifest. Adapt image and
secret names, but preserve this access policy:

The following Airflow 3.2/chart 1.22 fragment is complete for the dpone-owned
volumes and containers. The authority ConfigMap and connection Secret are
platform inputs created separately.

```yaml
airflowVersion: 3.2.0
images:
  airflow:
    repository: registry.example/airflow
    tag: "3.2.0-dpone-0.73.32"
    digest: sha256:REPLACE_WITH_AIRFLOW_IMAGE_DIGEST

dagProcessor:
  enabled: true
  securityContexts:
    pod:
      fsGroup: 50000
      fsGroupChangePolicy: OnRootMismatch
  extraVolumes:
    - name: dpone-airflow-cache
      emptyDir:
        sizeLimit: 2Gi
    - name: dpone-airflow-loader-ack
      emptyDir:
        sizeLimit: 16Mi
    - name: dpone-desired-state-authority
      configMap:
        name: dpone-airflow-desired-state-authority
    - name: dpone-airflow-cache-scripts
      configMap:
        name: dpone-airflow-cache-scripts
  extraVolumeMounts:
    - name: dpone-airflow-cache
      mountPath: /opt/airflow/.dpone-cache
      readOnly: true
  extraInitContainers:
    - name: dpone-cache-init
      image: registry.example/dpone-sync@sha256:REPLACE
      resources:
        requests: {cpu: 50m, memory: 128Mi, ephemeral-storage: 64Mi}
        limits: {cpu: 500m, memory: 512Mi, ephemeral-storage: 256Mi}
      command: [/bin/sh]
      args: ["/opt/dpone/bin/cache-init-fail-open.sh"]
      env:
        - name: DPONE_AIRFLOW_DESIRED_STATE_AUTHORITY_FILE
          value: /etc/dpone/airflow-authority/airflow-desired-state-authority.json
        - name: DPONE_AIRFLOW_PACK_CACHE_DIR
          value: /opt/airflow/.dpone-cache
        - name: DPONE_AIRFLOW_PACK_READER_CONNECTION_ID
          value: s3_dpone_artifacts_reader
        - name: DPONE_PACK_SYNC_TIMEOUT_SECONDS
          value: "20"
        - name: DPONE_PACK_CACHE_MAX_TOTAL_BYTES
          value: "536870912"
      envFrom:
        - secretRef:
            name: dpone-airflow-artifacts-reader
      volumeMounts:
        - name: dpone-airflow-cache
          mountPath: /opt/airflow/.dpone-cache
        - name: dpone-desired-state-authority
          mountPath: /etc/dpone/airflow-authority
          readOnly: true
        - name: dpone-airflow-cache-scripts
          mountPath: /opt/dpone/bin
          readOnly: true
  extraContainers:
    - name: dpone-cache-watch
      image: registry.example/dpone-sync@sha256:REPLACE
      resources:
        requests: {cpu: 25m, memory: 128Mi, ephemeral-storage: 64Mi}
        limits: {cpu: 500m, memory: 512Mi, ephemeral-storage: 256Mi}
      command: [/bin/sh]
      args: ["/opt/dpone/bin/cache-watch.sh"]
      env:
        - name: DPONE_AIRFLOW_DESIRED_STATE_AUTHORITY_FILE
          value: /etc/dpone/airflow-authority/airflow-desired-state-authority.json
        - name: DPONE_AIRFLOW_PACK_CACHE_DIR
          value: /opt/airflow/.dpone-cache
        - name: DPONE_AIRFLOW_PACK_READER_CONNECTION_ID
          value: s3_dpone_artifacts_reader
        - name: DPONE_PACK_SYNC_TIMEOUT_SECONDS
          value: "20"
        - name: DPONE_PACK_SYNC_INTERVAL_SECONDS
          value: "60"
        - name: DPONE_PACK_CACHE_MAX_TOTAL_BYTES
          value: "536870912"
        - name: DPONE_PACK_RETENTION_INTERVAL_CYCLES
          value: "60"
        - name: DPONE_CACHE_RETENTION_IDENTITY
          value: airflow-cache-controller/dev
        - name: DPONE_CACHE_RETENTION_APPROVED_PLAN_SHA256
          value: ""
        - name: DPONE_CACHE_RETENTION_REVIEW_ID
          value: ""
        - name: DPONE_ENVIRONMENT
          value: dev
      envFrom:
        - secretRef:
            name: dpone-airflow-artifacts-reader
      volumeMounts:
        - name: dpone-airflow-cache
          mountPath: /opt/airflow/.dpone-cache
        - name: dpone-airflow-loader-ack
          mountPath: /opt/airflow/.dpone-ack
          readOnly: true
        - name: dpone-desired-state-authority
          mountPath: /etc/dpone/airflow-authority
          readOnly: true
        - name: dpone-airflow-cache-scripts
          mountPath: /opt/dpone/bin
          readOnly: true
```

The DAG repository loader must use
`/opt/airflow/.dpone-cache/current/airflow-index.json` and write through
`ack_root=/opt/airflow/.dpone-ack`. Do not mechanically move the Airflow 3
fragment for an Airflow 2 deployment; use the complete versioned profiles
linked above. Airflow 3 `apiServer` consumes serialized DAG metadata and must
not own this cache.

The raw values are intentionally fail-closed: without the post-renderer the
parser has no ACK mount, so destructive retention cannot be authorized. Never
restore the ACK path through component-level `extraVolumeMounts`; that would
also expose it to migration and log-groomer containers.

The official chart supports these additional containers through component
`extraInitContainers` and `extraContainers`; see
[using additional containers](https://airflow.apache.org/docs/helm-chart/stable/using-additional-containers.html).
For the older supported chart, use the versioned
[chart 1.19 documentation](https://airflow.apache.org/docs/helm-chart/1.19.0/index.html)
instead of assuming the stable parameter set is identical.

## Optional API diagnostics projector

API-only operations require one projector beside the same parse authority. It
uses the matching pinned Airflow/provider image, mounts cache and ACK read-only,
and receives a least-privilege Airflow configuration from the
`dpone-airflow-status-projector` Secret. That identity may update only
`dpone_airflow_pack_cache_status`.

Add this item to the parse authority's `extraContainers` list:

```yaml
- name: dpone-cache-status-projector
  image: registry.example/airflow@sha256:REPLACE_WITH_AIRFLOW_IMAGE_DIGEST
  resources:
    requests: {cpu: 10m, memory: 64Mi, ephemeral-storage: 32Mi}
    limits: {cpu: 100m, memory: 192Mi, ephemeral-storage: 128Mi}
  command: [/bin/sh, -c]
  args:
    - |
      set +e
      trap 'exit 0' TERM INT
      while :; do
        dpone-airflow-pack-cache-status \
          --cache-dir /opt/airflow/.dpone-cache \
          --ack-path /opt/airflow/.dpone-ack/loader-ack.json \
          --ack-root /opt/airflow/.dpone-ack \
          --airflow-variable-key dpone_airflow_pack_cache_status \
          --json
        printf 'dpone cache-status projector exit=%s\n' "$?"
        sleep "${DPONE_CACHE_STATUS_INTERVAL_SECONDS:-60}" & wait $!
      done
  env:
    - name: DPONE_CACHE_STATUS_INTERVAL_SECONDS
      value: "60"
  envFrom:
    - secretRef: {name: dpone-airflow-status-projector}
  volumeMounts:
    - {name: dpone-airflow-cache, mountPath: /opt/airflow/.dpone-cache, readOnly: true}
    - {name: dpone-airflow-loader-ack, mountPath: /opt/airflow/.dpone-ack, readOnly: true}
```

The projector is diagnostic only: it cannot activate, delete or acknowledge a
deployment. Its failure does not stop the main container. Verify its bounded
Variable through the Airflow 2/v1 or Airflow 3/v2 REST procedure in
[cache diagnostics without Kubernetes access](airflow-cache-diagnostics-without-kubectl.md).

## Verification and rollback

1. Render Helm values through the pinned post-renderer and prove exactly one
   parse authority has init/watch.
2. Prove the parser mount is read-only and writer mounts are read-write.
3. Run the post-renderer in `--verify-only` mode. Prove the parser ACK mount is
   separate and read-write, both dpone readers are read-only, and migration,
   init, log-groomer and unrelated containers have no ACK mount.
4. Run one valid cycle; record exact release/deployment/activation IDs,
   `last-reconcile-status.json` and loader ACK.
5. Break S3 access in a controlled rollout; the pod must start, the previous
   current must remain readable, and status must be non-green.
6. Restart with an empty `emptyDir`; ordinary DAGs must remain available. dpone
   DAGs recover after S3 returns and must never disappear silently.
7. Compare expected DAG IDs, paginated REST inventory and import errors.
8. Roll back through the protected desired-state `fetch -> prepare -> publish`
   sequence in [Airflow desired state](airflow-desired-state.md#rollback). A
   local `cache-sync` is allowed only for a standalone cache whose watcher is
   disabled; otherwise the next cycle will undo it.

When the chart or post-renderer deployment itself must be rolled back, restore
the external wrapper ConfigMap and Helm revision as one operation. Use the
evidence directory produced by the
[wrapper ConfigMap lifecycle](airflow-cache-kubernetes-wrapper-delivery.md#deliver-the-wrappers).
An update
rollback restores only captured `.data` before Helm rollback. A first-install
rollback restores Helm first, proves the old workload no longer references the
ConfigMap, and only then conditionally deletes the exact ConfigMap occurrence:

```bash
set -euo pipefail

: "${KUBE_CONTEXT:?set the reviewed kubeconfig context}"
: "${AIRFLOW_NAMESPACE:?set the namespace used by the reviewed Airflow Helm release}"
: "${AIRFLOW_RELEASE:?set the reviewed Helm release name}"
: "${PREVIOUS_REVISION:?set the exact reviewed previous Helm revision}"
: "${DPONE_CACHE_SCRIPTS_EVIDENCE_DIR:?set the sealed ConfigMap activation evidence directory}"
: "${DPONE_CACHE_SCRIPTS_ROLLBACK_ACK:?set to restore-cache-scripts-and-helm}"
: "${DPONE_CACHE_PARSER_WORKLOAD:?set exact deployment/name or statefulset/name}"
: "${DPONE_CACHE_WATCH_CONTAINER:?set the exact cache-watch container name}"
[ "${DPONE_CACHE_SCRIPTS_ROLLBACK_ACK}" = restore-cache-scripts-and-helm ] || exit 2
case "${DPONE_CACHE_PARSER_WORKLOAD}" in deployment/*|statefulset/*) ;; *) exit 2 ;; esac
[ -d "${DPONE_CACHE_SCRIPTS_EVIDENCE_DIR}" ]
(cd "${DPONE_CACHE_SCRIPTS_EVIDENCE_DIR}" && sha256sum -c SHA256SUMS)
rollback_evidence="$(mktemp -d dpone-airflow-cache-scripts-rollback.XXXXXX)"
POST_RENDERER="$(command -v dpone-airflow-pack-helm-post-renderer)"

restore_previous_configmap() {
  live_before="${rollback_evidence}/live-before-restore.json"
  kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
    get configmap dpone-airflow-cache-scripts -o json >"${live_before}"
  jq -e --slurpfile activated "${DPONE_CACHE_SCRIPTS_EVIDENCE_DIR}/observed.json" '
    (.metadata.uid | type == "string" and length > 0) and
    (.metadata.resourceVersion | type == "string" and length > 0) and
    .metadata.uid == $activated[0].metadata.uid and
    .metadata.resourceVersion == $activated[0].metadata.resourceVersion
  ' "${live_before}" >/dev/null
  jq --slurpfile previous "${DPONE_CACHE_SCRIPTS_EVIDENCE_DIR}/previous-data.json" '
    .data = $previous[0]
    | del(.metadata.managedFields, .metadata.creationTimestamp, .metadata.generation)
  ' "${live_before}" >"${rollback_evidence}/restore-configmap.json"
  kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
    replace --dry-run=server -f "${rollback_evidence}/restore-configmap.json" >/dev/null
  kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
    replace -f "${rollback_evidence}/restore-configmap.json"
  kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
    get configmap dpone-airflow-cache-scripts -o json \
    >"${rollback_evidence}/restored-configmap.json"
  jq -e --slurpfile activated "${DPONE_CACHE_SCRIPTS_EVIDENCE_DIR}/observed.json" '
    .metadata.uid == $activated[0].metadata.uid
  ' "${rollback_evidence}/restored-configmap.json" >/dev/null
  jq -S '.data' "${rollback_evidence}/restored-configmap.json" \
    >"${rollback_evidence}/restored-data.json"
  cmp -- "${DPONE_CACHE_SCRIPTS_EVIDENCE_DIR}/previous-data.json" \
    "${rollback_evidence}/restored-data.json"
}

validate_activated_occurrence() {
  jq -e --slurpfile activated "${DPONE_CACHE_SCRIPTS_EVIDENCE_DIR}/observed.json" '
    (.metadata.uid | type == "string" and length > 0) and
    (.metadata.resourceVersion | type == "string" and length > 0) and
    .metadata.uid == $activated[0].metadata.uid and
    .metadata.resourceVersion == $activated[0].metadata.resourceVersion
  ' "$1" >/dev/null
}

if [ -f "${DPONE_CACHE_SCRIPTS_EVIDENCE_DIR}/previous.json" ]; then
  restore_previous_configmap
  rollback_mode=restore_previous
elif [ -f "${DPONE_CACHE_SCRIPTS_EVIDENCE_DIR}/previous.absent" ]; then
  rollback_mode=withdraw_first_install
else
  printf 'ConfigMap evidence has neither previous object nor absence marker\n' >&2
  exit 3
fi

helm history "${AIRFLOW_RELEASE}" --kube-context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}"
helm rollback "${AIRFLOW_RELEASE}" "${PREVIOUS_REVISION}" \
  --kube-context "${KUBE_CONTEXT}" \
  --namespace "${AIRFLOW_NAMESPACE}" \
  --wait --timeout 10m
helm get manifest "${AIRFLOW_RELEASE}" --kube-context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
  | "${POST_RENDERER}" --verify-only \
  > /dev/null
kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
  rollout status "${DPONE_CACHE_PARSER_WORKLOAD}" --timeout=10m
kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
  get "${DPONE_CACHE_PARSER_WORKLOAD}" -o json >"${rollback_evidence}/workload.json"

if [ "${rollback_mode}" = restore_previous ]; then
  selector="$(jq -er '.spec.selector.matchLabels | to_entries | sort_by(.key)
    | map("\(.key)=\(.value)") | join(",")' "${rollback_evidence}/workload.json")"
  kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
    get pods -l "${selector}" -o json >"${rollback_evidence}/pods.json"
  jq -e '(.items | length > 0)
    and all(.items[]; any(.status.conditions[]?; .type == "Ready" and .status == "True"))' \
    "${rollback_evidence}/pods.json" >/dev/null
  python3 - "${DPONE_CACHE_SCRIPTS_EVIDENCE_DIR}/previous-data.json" \
    >"${rollback_evidence}/expected-script-sha256.txt" <<'PY'
import hashlib
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    scripts = json.load(stream)
for name, content in sorted(scripts.items()):
    print(f"{hashlib.sha256(content.encode()).hexdigest()}  /opt/dpone/bin/{name}")
PY
  while IFS= read -r pod; do
    kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
      exec "pod/${pod}" -c "${DPONE_CACHE_WATCH_CONTAINER}" -- \
      sha256sum -- /opt/dpone/bin/cache-init-fail-open.sh /opt/dpone/bin/cache-watch.sh \
      | sort >"${rollback_evidence}/mounted-${pod}.txt"
    cmp -- "${rollback_evidence}/expected-script-sha256.txt" \
      "${rollback_evidence}/mounted-${pod}.txt"
  done < <(jq -r '.items[].metadata.name' "${rollback_evidence}/pods.json")
else
  jq -e '[.. | objects
    | select(.configMap?.name == "dpone-airflow-cache-scripts")]
    | length == 0' "${rollback_evidence}/workload.json" >/dev/null
  kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
    get configmap dpone-airflow-cache-scripts -o json \
    >"${rollback_evidence}/delete-candidate.json"
  validate_activated_occurrence "${rollback_evidence}/delete-candidate.json"
  jq -S '.data' "${rollback_evidence}/delete-candidate.json" \
    >"${rollback_evidence}/delete-candidate-data.json"
  cmp -- "${DPONE_CACHE_SCRIPTS_EVIDENCE_DIR}/candidate-data.json" \
    "${rollback_evidence}/delete-candidate-data.json"
  python3 - "${rollback_evidence}/delete-candidate.json" "${KUBE_CONTEXT}" \
    "${AIRFLOW_NAMESPACE}" <<'PY'
import json
import sys
import time

from kubernetes import client, config
from kubernetes.client.exceptions import ApiException

path, context, namespace = sys.argv[1:]
with open(path, encoding="utf-8") as stream:
    payload = json.load(stream)
metadata = payload["metadata"]
uid = metadata["uid"]
resource_version = metadata["resourceVersion"]
config.load_kube_config(context=context)
api = client.CoreV1Api()
body = client.V1DeleteOptions(
    preconditions=client.V1Preconditions(uid=uid, resource_version=resource_version)
)
deadline = time.monotonic() + 60
def request_timeout():
    remaining = max(1, int(deadline - time.monotonic()))
    return min(10, remaining), min(30, remaining)

api.delete_namespaced_config_map(
    "dpone-airflow-cache-scripts",
    namespace,
    body=body,
    _request_timeout=request_timeout(),
)
while time.monotonic() < deadline:
    try:
        observed = api.read_namespaced_config_map(
            "dpone-airflow-cache-scripts",
            namespace,
            _request_timeout=request_timeout(),
        )
    except ApiException as exc:
        if exc.status == 404:
            break
        raise
    if observed.metadata.uid != uid:
        raise SystemExit("replacement ConfigMap appeared during rollback")
    time.sleep(min(1, max(0, deadline - time.monotonic())))
else:
    raise SystemExit("timed out waiting for ConfigMap deletion")
PY
  observed="$(kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
    get configmap dpone-airflow-cache-scripts --ignore-not-found -o name)"
  [ -z "${observed}" ]
fi
(cd "${rollback_evidence}" && find . -type f ! -name SHA256SUMS -print0 \
  | sort -z | xargs -0 sha256sum >SHA256SUMS && sha256sum -c SHA256SUMS)
chmod 0400 "${rollback_evidence}"/*
printf 'cache script rollback evidence: %s\n' "${rollback_evidence}"
```

The update branch compares both live UID and `resourceVersion` with the sealed
post-activation `observed.json` before mutation. `kubectl replace` carries that
same `resourceVersion`, so a concurrent update or replacement is a conflict,
not an overwrite. The first-install withdrawal makes the same identity check
before its conditional delete. That delete helper uses the Kubernetes Python
SDK; install the rollback environment with `dpone[kubernetes]` before relying
on this branch.

Live cross-UID/`fsGroup`, storage-driver durability and restart tests are
`UNVERIFIED` until evidence from the exact image, chart render and cluster is
attached. Continue with
[cache diagnostics without Kubernetes access](airflow-cache-diagnostics-without-kubectl.md)
and [cache sync and recovery](airflow-cache-sync.md).
