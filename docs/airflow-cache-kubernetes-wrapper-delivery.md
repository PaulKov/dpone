# Deliver Airflow cache wrappers safely

**Purpose.** Install, verify, update, and roll back the reviewed wrapper ConfigMap without losing predecessor evidence.

**Audience.** Platform engineers managing Kubernetes ConfigMaps and parser workload rollouts.

[Back to Kubernetes cache deployment overview](airflow-cache-kubernetes-deployment.md) · **Next likely task:** [deploy and verify the Helm release](airflow-cache-kubernetes-chart-operations.md).

## Deliver the wrappers

The paths referenced by Helm values must exist. Store the two reviewed scripts
in a ConfigMap; do not rely on files that happen to be present in one image:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: dpone-airflow-cache-scripts
data:
  cache-init-fail-open.sh: |
    set +e
    timeout "${DPONE_PACK_SYNC_TIMEOUT_SECONDS:-20}" \
      dpone airflow desired-state reconcile \
      --connection-type airflow \
      --connection-id "${DPONE_AIRFLOW_PACK_READER_CONNECTION_ID}" \
      --artifact-connection-type airflow \
      --artifact-connection-id "${DPONE_AIRFLOW_PACK_READER_CONNECTION_ID}" \
      --cache-root "${DPONE_AIRFLOW_PACK_CACHE_DIR}" \
      --max-total-bytes "${DPONE_PACK_CACHE_MAX_TOTAL_BYTES:-536870912}"
    printf 'dpone exact-cache init exit=%s; startup remains fail-open\n' "$?"
    exit 0
  cache-watch.sh: |
    set +e
    trap 'exit 0' TERM INT
    publish_retention_status() {
      publication_tmp="$(mktemp "$(dirname "$2")/.retention-publication.XXXXXX" 2>/dev/null)" || return 4
      dpone airflow cache-status-publish \
        --status-root "$(dirname "$2")" \
        --source "$(basename "$1")" \
        --target "$(basename "$2")" \
        --failure-marker "$4" \
        --expected-schema "$3" \
        --format json >"${publication_tmp}"
      publication_rc=$?
      if [ -s "${publication_tmp}" ]; then
        chmod 0600 "${publication_tmp}" && mv -f "${publication_tmp}" "$5" || publication_rc=4
      else
        rm -f "${publication_tmp}"
      fi
      return "${publication_rc}"
    }
    retention_error_code() {
      python3 -c 'import json,sys; p=json.load(open(sys.argv[1])); errors=p.get("errors") or []; print((errors[0].get("code") if errors else None) or p.get("error_code") or ("ok" if p.get("passed") is True or p.get("status") in {"ok", "needs_cleanup", "committed"} else "unavailable"))' "$1" 2>/dev/null || printf 'unavailable\n'
    }
    cycle=0
    while :; do
      timeout "${DPONE_PACK_SYNC_TIMEOUT_SECONDS:-20}" \
        dpone airflow desired-state reconcile \
        --connection-type airflow \
        --connection-id "${DPONE_AIRFLOW_PACK_READER_CONNECTION_ID}" \
        --artifact-connection-type airflow \
        --artifact-connection-id "${DPONE_AIRFLOW_PACK_READER_CONNECTION_ID}" \
        --cache-root "${DPONE_AIRFLOW_PACK_CACHE_DIR}" \
        --max-total-bytes "${DPONE_PACK_CACHE_MAX_TOTAL_BYTES:-536870912}"
      rc=$?
      printf 'dpone exact-cache watch exit=%s\n' "${rc}"
      cycle=$((cycle + 1))
      if [ "${rc}" -eq 0 ] \
        && [ $((cycle % ${DPONE_PACK_RETENTION_INTERVAL_CYCLES:-60})) -eq 0 ] \
        && [ -f /opt/airflow/.dpone-ack/loader-ack.json ]; then
        retention_status_dir="${DPONE_AIRFLOW_PACK_CACHE_DIR}/status"
        umask 077
        mkdir -p "${retention_status_dir}"
        chmod 0700 "${retention_status_dir}"
        status_dir_rc=$?
        if [ "${status_dir_rc}" -ne 0 ]; then
          printf 'dpone cache retention status directory unavailable exit=%s\n' "${status_dir_rc}"
          sleep "${DPONE_PACK_SYNC_INTERVAL_SECONDS:-60}" & wait $!
          continue
        fi
        plan_tmp="$(mktemp "${retention_status_dir}/.retention-plan.XXXXXX" 2>/dev/null)"
        if [ -z "${plan_tmp}" ]; then
          printf 'dpone cache retention plan temp unavailable\n'
          sleep "${DPONE_PACK_SYNC_INTERVAL_SECONDS:-60}" & wait $!
          continue
        fi
        plan_status="${retention_status_dir}/last-retention-plan.json"
        dpone airflow cache-retention-plan \
          --cache-root "${DPONE_AIRFLOW_PACK_CACHE_DIR}" \
          --environment "${DPONE_ENVIRONMENT:-dev}" \
          --evidence-file /opt/airflow/.dpone-ack/loader-ack.json \
          --format json >"${plan_tmp}"
        plan_rc=$?
        plan_code="$(retention_error_code "${plan_tmp}")"
        plan_publication_status="${retention_status_dir}/last-retention-plan-publication.json"
        publish_retention_status \
          "${plan_tmp}" "${plan_status}" \
          dpone.deployment-cache-retention-plan.v1 \
          last-retention-plan-publication-failure.json \
          "${plan_publication_status}"
        publish_rc=$?
        publish_code="$(retention_error_code "${plan_publication_status}")"
        rm -f "${plan_tmp}"
        printf 'dpone cache retention plan exit=%s code=%s publish=%s publish_code=%s evidence=%s publication=%s\n' \
          "${plan_rc}" "${plan_code}" "${publish_rc}" "${publish_code}" \
          "${plan_status}" "${plan_publication_status}"
        if [ "${plan_rc}" -eq 0 ] && [ "${publish_rc}" -eq 0 ]; then
          plan_sha256="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["plan_sha256"])' "${plan_status}" 2>/dev/null)"
          approved_sha256="${DPONE_CACHE_RETENTION_APPROVED_PLAN_SHA256:-}"
          approved_review_id="${DPONE_CACHE_RETENTION_REVIEW_ID:-}"
          if [ -z "${approved_sha256}" ]; then
            printf 'dpone cache retention plan-only reason=approval_missing plan_sha256=%s\n' "${plan_sha256}"
          elif [ -z "${approved_review_id}" ]; then
            printf 'dpone cache retention plan-only reason=review_id_missing plan_sha256=%s\n' "${plan_sha256}"
          elif [ "${approved_sha256}" != "${plan_sha256}" ]; then
            printf 'dpone cache retention plan-only reason=approval_mismatch approved=%s actual=%s\n' \
              "${approved_sha256}" "${plan_sha256}"
          else
            apply_tmp="$(mktemp "${retention_status_dir}/.retention-apply.XXXXXX" 2>/dev/null)"
            apply_status="${retention_status_dir}/last-retention-apply.json"
            if [ -z "${apply_tmp}" ]; then
              printf 'dpone cache retention apply temp unavailable\n'
            else
              dpone airflow cache-retention-apply \
              --cache-root "${DPONE_AIRFLOW_PACK_CACHE_DIR}" \
              --environment "${DPONE_ENVIRONMENT:-dev}" \
              --evidence-file /opt/airflow/.dpone-ack/loader-ack.json \
              --expected-plan-sha256 "${approved_sha256}" \
              --review-id "${approved_review_id}" \
              --loader-ack-file /opt/airflow/.dpone-ack/loader-ack.json \
              --promoted-by "${DPONE_CACHE_RETENTION_IDENTITY}" \
              --allowed-promoter "${DPONE_CACHE_RETENTION_IDENTITY}" \
              --confirm-delete \
              --evidence-version v3 \
              --format json >"${apply_tmp}"
              apply_rc=$?
              apply_code="$(retention_error_code "${apply_tmp}")"
              apply_publication_status="${retention_status_dir}/last-retention-apply-publication.json"
              publish_retention_status \
                "${apply_tmp}" "${apply_status}" \
                dpone.deployment-cache-retention-apply.v3 \
                last-retention-apply-publication-failure.json \
                "${apply_publication_status}"
              apply_publish_rc=$?
              apply_publish_code="$(retention_error_code "${apply_publication_status}")"
              rm -f "${apply_tmp}"
              printf 'dpone cache retention apply exit=%s code=%s publish=%s publish_code=%s evidence=%s publication=%s\n' \
                "${apply_rc}" "${apply_code}" "${apply_publish_rc}" "${apply_publish_code}" \
                "${apply_status}" "${apply_publication_status}"
            fi
          fi
        else
          printf 'dpone cache retention skipped apply code=%s\n' \
            "${plan_code}"
        fi
      fi
      sleep "${DPONE_PACK_SYNC_INTERVAL_SECONDS:-60}" & wait $!
    done
```

Treat this ConfigMap as reviewed infrastructure, not an ad-hoc prerequisite.
Save the exact block above as `dpone-airflow-cache-scripts.yaml`, then use one
reproducible lifecycle for first install, update and rollback:

```bash
set -euo pipefail

: "${KUBE_CONTEXT:?set the reviewed kubeconfig context}"
: "${AIRFLOW_NAMESPACE:?set the namespace used by the reviewed Airflow Helm release}"
: "${DPONE_CACHE_SCRIPTS_MANIFEST:?set the reviewed wrapper ConfigMap manifest}"
: "${DPONE_CACHE_SCRIPTS_APPLY_ACK:?set to review-and-apply-cache-scripts}"
: "${DPONE_CACHE_PARSER_WORKLOAD:?set exact deployment/name or statefulset/name}"
: "${DPONE_CACHE_WATCH_CONTAINER:?set the exact cache-watch container name}"
[ "${DPONE_CACHE_SCRIPTS_APPLY_ACK}" = review-and-apply-cache-scripts ] || exit 2
[ -f "${DPONE_CACHE_SCRIPTS_MANIFEST}" ]
case "${DPONE_CACHE_PARSER_WORKLOAD}" in deployment/*|statefulset/*) ;; *) exit 2 ;; esac

evidence_dir="$(mktemp -d dpone-airflow-cache-scripts.XXXXXX)"
candidate_list="${evidence_dir}/candidate-list.json"
candidate_data="${evidence_dir}/candidate-data.json"
observed="${evidence_dir}/observed.json"
observed_data="${evidence_dir}/observed-data.json"
workload_before="${evidence_dir}/workload-before.json"
workload_after="${evidence_dir}/workload-after.json"
kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
  create --dry-run=client -f "${DPONE_CACHE_SCRIPTS_MANIFEST}" -o json \
  | jq -s '.' >"${candidate_list}"
jq -e --arg namespace "${AIRFLOW_NAMESPACE}" '
  length == 1 and
  .[0].apiVersion == "v1" and
  .[0].kind == "ConfigMap" and
  .[0].metadata.name == "dpone-airflow-cache-scripts" and
  ((.[0].metadata.namespace // $namespace) == $namespace) and
  ((.[0].data | keys | sort) == ["cache-init-fail-open.sh", "cache-watch.sh"])
' "${candidate_list}" >/dev/null
jq -S '.[0].data' "${candidate_list}" >"${candidate_data}"
kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
  get "${DPONE_CACHE_PARSER_WORKLOAD}" -o json >"${workload_before}"
jq -e '.metadata.uid != null and (.spec.selector.matchLabels | type == "object" and length > 0)' \
  "${workload_before}" >/dev/null
pod_selector="$(jq -er '.spec.selector.matchLabels | to_entries | sort_by(.key) | map("\(.key)=\(.value)") | join(",")' \
  "${workload_before}")"

previous_tmp="${evidence_dir}/previous.tmp"
kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
  get configmap dpone-airflow-cache-scripts --ignore-not-found -o json \
  >"${previous_tmp}"
if [ -s "${previous_tmp}" ]; then
  mv "${previous_tmp}" "${evidence_dir}/previous.json"
  jq -S '.data' "${evidence_dir}/previous.json" >"${evidence_dir}/previous-data.json"
else
  rm -f "${previous_tmp}"
  printf 'first_install=true\n' >"${evidence_dir}/previous.absent"
fi

kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
  apply --dry-run=server -f "${DPONE_CACHE_SCRIPTS_MANIFEST}" >/dev/null
diff_rc=0
kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
  diff -f "${DPONE_CACHE_SCRIPTS_MANIFEST}" \
  >"${evidence_dir}/kubectl.diff" || diff_rc=$?
[ "${diff_rc}" -le 1 ] || exit "${diff_rc}"
cat "${evidence_dir}/kubectl.diff"
kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
  apply --server-side --field-manager=dpone-airflow-cache-scripts \
  -f "${DPONE_CACHE_SCRIPTS_MANIFEST}"
kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
  get configmap dpone-airflow-cache-scripts -o json \
  >"${observed}"
jq -e --arg namespace "${AIRFLOW_NAMESPACE}" '
  .apiVersion == "v1" and
  .kind == "ConfigMap" and
  .metadata.name == "dpone-airflow-cache-scripts" and
  ((.metadata.namespace // $namespace) == $namespace) and
  ((.data | keys | sort) == ["cache-init-fail-open.sh", "cache-watch.sh"])
' "${observed}" >/dev/null
jq -S '.data' "${observed}" >"${observed_data}"
cmp -- "${candidate_data}" "${observed_data}"
kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
  rollout restart "${DPONE_CACHE_PARSER_WORKLOAD}"
kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
  rollout status "${DPONE_CACHE_PARSER_WORKLOAD}" --timeout=10m
kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
  get "${DPONE_CACHE_PARSER_WORKLOAD}" -o json >"${workload_after}"
jq -e --arg uid "$(jq -r '.metadata.uid' "${workload_before}")" \
  '.metadata.uid == $uid' "${workload_after}" >/dev/null
kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
  get pods -l "${pod_selector}" -o json >"${evidence_dir}/replacement-pods.json"
jq -e '
  (.items | length > 0) and
  all(.items[]; any(.status.conditions[]?; .type == "Ready" and .status == "True"))
' "${evidence_dir}/replacement-pods.json" >/dev/null
jq -r '.items[] | [.metadata.name, .metadata.uid] | @tsv' \
  "${evidence_dir}/replacement-pods.json" >"${evidence_dir}/replacement-pods.tsv"
python3 - "${candidate_data}" >"${evidence_dir}/expected-script-sha256.txt" <<'PY'
import hashlib
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    scripts = json.load(stream)
for name, content in sorted(scripts.items()):
    digest = hashlib.sha256(content.encode()).hexdigest()
    print(f"{digest}  /opt/dpone/bin/{name}")
PY
while IFS=$'\t' read -r pod pod_uid; do
  [ -n "${pod}" ] && [ -n "${pod_uid}" ]
  mounted="${evidence_dir}/mounted-script-sha256-${pod}.txt"
  kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
    exec "pod/${pod}" -c "${DPONE_CACHE_WATCH_CONTAINER}" -- \
    sha256sum -- /opt/dpone/bin/cache-init-fail-open.sh /opt/dpone/bin/cache-watch.sh \
    | sort >"${mounted}"
  cmp -- "${evidence_dir}/expected-script-sha256.txt" "${mounted}"
done <"${evidence_dir}/replacement-pods.tsv"
(cd "${evidence_dir}" && find . -type f ! -name SHA256SUMS -print0 \
  | sort -z | xargs -0 sha256sum >SHA256SUMS && sha256sum -c SHA256SUMS)
chmod 0400 "${evidence_dir}"/*
printf 'cache script ConfigMap evidence: %s\n' "${evidence_dir}"
```

The lifecycle records the exact previous live object, or an explicit
`previous.absent` marker, before mutation. Applying a ConfigMap alone is not a
runtime activation: the exact parser workload is rolled out, its stable UID and
selector are checked, and every replacement Pod proves the mounted script
hashes in the named watch container. Rollback reconstructs the fixed-name
ConfigMap from the captured previous `.data`, repeats candidate validation,
server dry-run, diff and apply, then performs the same workload rollout and
mounted-byte verification. A first-install rollback deletes only
`configmap/dpone-airflow-cache-scripts` after the same explicit acknowledgement
and rolls the workload so stale projected bytes cannot remain in a running
process. Never apply the captured multi-resource input or edit the live
ConfigMap. Retain predecessor, candidate, diff, workload/Pod identities,
mounted hashes and `SHA256SUMS` with the corresponding Helm revision evidence.
