# Approve and withdraw one cache-retention plan

**Purpose.** Define the GitOps approval lifecycle for one exact deployment-cache retention plan.

**Audience.** Platform change approvers and operators responsible for proving that destructive cache retention is enabled and later withdrawn safely.

[Back to cache sync and recovery overview](airflow-cache-sync.md) · **Next likely task:** execute the approval or withdrawal procedure below.

## Approval identity

Approval is the pair of exact `plan_sha256` and one canonical UUIDv4
`review_id`, not a permanent boolean. The reviewed environment configuration
sets `DPONE_CACHE_RETENTION_APPROVED_PLAN_SHA256` and
`DPONE_CACHE_RETENTION_REVIEW_ID` only after the plan, protection set, desired
deployment, candidates, and evidence freshness are accepted. Generate the UUID
once for the approved attempt; do not generate it inside the watcher loop.

```text
schema-valid plan
  -> human review
  -> exact digest in GitOps
  -> converged parser workload
  -> watcher applies only matching digest
  -> receipt-backed committed evidence
  -> approval removal
  -> converged post-removal Pods prove plan-only mode
```

This page owns the lifecycle, acceptance criteria, and the single contract-tested executable procedure below.

## Approval acceptance

- The projected plan is current, schema-valid, fresh, and has no publication
  warning.
- The exact environment, protected set, candidate set, and `plan_sha256` were
  reviewed.
- The canonical reviewed plan and its publication report are sealed in one
  operator-retained directory and reused by the post-apply verifier.
- The approved attempt has a persisted UUIDv4 `review_id` that is reused only
  for retries of that attempt.
- The active Deployment or StatefulSet has converged to the approved template.
- Apply evidence names the reviewed digest and a committed v3 receipt.
- A changed plan or aborted receipt requires a new review and UUIDv4. An old
  digest/review pair never authorizes another attempt.

## Withdrawal proof

Removing the GitOps value is not enough. Acceptance requires all of the
following:

1. The active parser workload template has no non-empty approval value and no
   ambiguous `envFrom` source for the watcher container.
2. Workload generation, updated/ready/available replicas, and revision status
   prove rollout convergence.
3. Every selected parser Pod is Ready and was created at or after the recorded
   approval-removal time.
4. Every selected Pod has exactly one watcher container with no non-empty
   approval value or ambiguous `envFrom` source.
5. Every selected watcher emits `plan-only reason=approval_missing` after the
   removal timestamp and within the configured bounded wait.
6. The result is retained in one operator-supplied evidence directory as
   `approval-withdrawal.json`, `workload.json`, `pods.json`, bounded watcher
   logs, Pod evidence and verified `SHA256SUMS`.

The caller-provided timestamp is only a lower bound. Pod creation time,
workload rollout status, template env, Pod env, and post-removal logs jointly
prove withdrawal. Any API error, stale Pod, active value, missing log, or timeout
is a failed proof, not a warning.

The evidence directory is an idempotency boundary. A retry validates checksums
and returns the already proved result only when the removal commit, Kubernetes
context, namespace, parser workload, watcher container and removal timestamp
all match. A different environment or control identity cannot reuse that
result. Kubernetes JSON, log tail length, log bytes and request duration are
independently bounded and configurable.

## Evidence to retain

- reviewed plan and its SHA-256;
- environment change that added approval;
- rendered manifest digest and rollout evidence;
- receipt-backed v3 apply evidence;
- environment change that removed approval;
- sealed `approval-withdrawal.json` bound to that removal commit;
- workload generation, Pod UID/resourceVersion/timestamp/env, and bounded
  post-removal plan-only logs with `SHA256SUMS`.

Return to [bounded cache retention](airflow-cache-retention.md) for replay and
migration semantics.

## Executable approval and withdrawal procedure


Retention approval is a one-plan GitOps change, not a permanent switch:

1. Retrieve `status/last-retention-plan.json` through the read-only cache-status
   projector/API diagnostic path and validate it as
   `dpone.deployment-cache-retention-plan.v1`.
2. Confirm its environment, current deployment, protected generations,
   candidates and `plan_sha256` against immutable desired-deployment evidence.
   A warning or missing projector is not approval.
3. Put that exact `plan_sha256` and one generated UUIDv4 in the
   environment-owned values `DPONE_CACHE_RETENTION_APPROVED_PLAN_SHA256` and
   `DPONE_CACHE_RETENTION_REVIEW_ID`; never edit the checked-in example values.
4. Render the complete Helm release, review the diff, and deploy through the
   explicit `KUBE_CONTEXT`, `AIRFLOW_NAMESPACE`, `AIRFLOW_RELEASE` and
   `PLATFORM_VALUES`
   [exact-byte deployment procedure](airflow-cache-kubernetes-prerequisites.md#deploy-exact-reviewed-bytes).
5. Retrieve the next `last-retention-apply.json`. Reopen the sealed reviewed
   plan, validate its checksum and canonical digest, require the apply item and
   candidate projections to match it exactly, and require the publication
   `target_sha256` to identify the exact canonical apply bytes. A destructive
   result also requires the approved review ID and a committed v3 receipt.
6. Clear both approval values in the next GitOps change and wait through the
   configured retention cycle for the active watcher to
   emit `plan-only reason=approval_missing`. Reusing an approval for a changed
   plan is forbidden and fails closed.

Keep the plan, environment diff, rendered manifest hash, apply evidence and
approval-removal commit together. If the actual plan digest changes before
apply, review the new plan from step 1 instead of copying it blindly.

Set `DPONE_CACHE_RETENTION_REVIEW_EVIDENCE_DIR` to an externally retained,
absolute directory. Extract and seal the exact canonical plan there from the
API snapshot produced by
[cache diagnostics](airflow-cache-diagnostics-without-kubectl.md). Publication
time, schema, environment, canonical plan digest and publication target bytes
are all gates. The publisher canonicalizes a target with sorted keys, two-space
indentation, UTF-8 and one trailing newline; `jq -S` reproduces those bytes for
verification:

```bash
set -euo pipefail

: "${CACHE_STATUS_SNAPSHOT:?set the reviewed cache-status-from-variable.json path}"
: "${DPONE_ENVIRONMENT:?set the reviewed environment}"
: "${DPONE_CACHE_RETENTION_REVIEW_EVIDENCE_DIR:?set one retained absolute review evidence directory}"
: "${RETENTION_EVIDENCE_MAX_AGE_SECONDS:=600}"
review_evidence_dir="${DPONE_CACHE_RETENTION_REVIEW_EVIDENCE_DIR%/}"
case "${review_evidence_dir}" in
  /*) ;;
  *) printf 'review evidence directory must be absolute\n' >&2; exit 2 ;;
esac
[ ! -L "${review_evidence_dir}" ] || exit 4
mkdir -p -m 0700 "${review_evidence_dir}"
if [ -e "${review_evidence_dir}/plan.json" ] \
  || [ -e "${review_evidence_dir}/publication.json" ] \
  || [ -e "${review_evidence_dir}/SHA256SUMS" ]; then
  printf 'review evidence directory already contains retention evidence\n' >&2
  exit 4
fi
jq -e '
  (.warnings | map(select(.code == "airflow_cache_status_publication_attention")) | length) == 0
  and (.operational_status.retention_plan_publication_failure == null)
' "${CACHE_STATUS_SNAPSHOT}" >/dev/null
jq -S -e '.operational_status.retention_plan' "${CACHE_STATUS_SNAPSHOT}" \
  >"${review_evidence_dir}/plan.json"
jq -S -e '.operational_status.retention_plan_publication' "${CACHE_STATUS_SNAPSHOT}" \
  >"${review_evidence_dir}/publication.json"
dpone gitops schema validate --kind dpone.deployment-cache-retention-plan.v1 \
  --payload "${review_evidence_dir}/plan.json"
dpone gitops schema validate --kind dpone.airflow-cache-status-publication.v1 \
  --payload "${review_evidence_dir}/publication.json"
python3 - "${review_evidence_dir}/publication.json" "${RETENTION_EVIDENCE_MAX_AGE_SECONDS}" <<'PY'
import datetime
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    attempted = json.load(stream)["attempted_at"]
observed = datetime.datetime.fromisoformat(attempted.replace("Z", "+00:00"))
age = (datetime.datetime.now(datetime.timezone.utc) - observed).total_seconds()
if age < 0 or age > int(sys.argv[2]):
    raise SystemExit("retention evidence is stale or from the future")
PY
plan_target_sha256="sha256:$(sha256sum "${review_evidence_dir}/plan.json" | awk '{print $1}')"
jq -e --arg target_sha256 "${plan_target_sha256}" '
  .passed == true and .status == "published"
  and .expected_schema == "dpone.deployment-cache-retention-plan.v1"
  and (.source_sha256 | test("^sha256:[0-9a-f]{64}$"))
  and .target_sha256 == $target_sha256
' "${review_evidence_dir}/publication.json" >/dev/null
approved_plan_sha256="$(jq -er --arg environment "${DPONE_ENVIRONMENT}" '
  select(.environment == $environment) | .plan_sha256
  | select(test("^sha256:[0-9a-f]{64}$"))' "${review_evidence_dir}/plan.json")"
approved_review_id="$(python3 -c 'import uuid; print(uuid.uuid4())')"
printf 'DPONE_CACHE_RETENTION_APPROVED_PLAN_SHA256=%s\n' "${approved_plan_sha256}"
printf 'DPONE_CACHE_RETENTION_REVIEW_ID=%s\n' "${approved_review_id}"
(cd "${review_evidence_dir}" && sha256sum -- plan.json publication.json >SHA256SUMS \
  && sha256sum -c SHA256SUMS)
chmod 0400 "${review_evidence_dir}"/*
```

After the one-time approval is deployed, fetch a new API snapshot and prove a
fresh v3 result for exactly the sealed plan. Keep
`DPONE_CACHE_RETENTION_REVIEW_EVIDENCE_DIR` pointed at the directory created in
the review step:

A schema-valid v3 no-op has no deleted deployment IDs and intentionally omits
receipt fields because no destructive authority was exercised. The verifier
accepts that shape only when the sealed plan has no candidates and the complete
apply item projection matches the plan. A destructive result must delete every
and only reviewed candidate, must not delete current or protected deployments,
and must carry the approved review ID and a committed receipt.

```bash
set -euo pipefail

: "${CACHE_STATUS_SNAPSHOT:?set the post-apply cache-status snapshot}"
: "${DPONE_ENVIRONMENT:?set the reviewed deployment environment}"
: "${DPONE_CACHE_RETENTION_APPROVED_PLAN_SHA256:?set the reviewed plan digest}"
: "${DPONE_CACHE_RETENTION_REVIEW_ID:?set the approved UUIDv4 attempt id}"
: "${DPONE_CACHE_RETENTION_REVIEW_EVIDENCE_DIR:?set the retained review evidence directory}"
: "${RETENTION_EVIDENCE_MAX_AGE_SECONDS:=600}"
review_evidence_dir="${DPONE_CACHE_RETENTION_REVIEW_EVIDENCE_DIR%/}"
case "${review_evidence_dir}" in
  /*) ;;
  *) printf 'review evidence directory must be absolute\n' >&2; exit 2 ;;
esac
[ -d "${review_evidence_dir}" ] && [ ! -L "${review_evidence_dir}" ] || exit 4
(cd "${review_evidence_dir}" && sha256sum -c SHA256SUMS)
dpone gitops schema validate --kind dpone.deployment-cache-retention-plan.v1 \
  --payload "${review_evidence_dir}/plan.json"
dpone gitops schema validate --kind dpone.airflow-cache-status-publication.v1 \
  --payload "${review_evidence_dir}/publication.json"
plan_target_sha256="sha256:$(sha256sum "${review_evidence_dir}/plan.json" | awk '{print $1}')"
jq -e --arg target_sha256 "${plan_target_sha256}" '
  .passed == true and .status == "published"
  and .expected_schema == "dpone.deployment-cache-retention-plan.v1"
  and (.source_sha256 | test("^sha256:[0-9a-f]{64}$"))
  and .target_sha256 == $target_sha256
' "${review_evidence_dir}/publication.json" >/dev/null
reviewed_plan_sha256="$(jq -er --arg environment "${DPONE_ENVIRONMENT}" '
  select(.environment == $environment) | .plan_sha256
  | select(test("^sha256:[0-9a-f]{64}$"))' "${review_evidence_dir}/plan.json")"
if [ "${reviewed_plan_sha256}" != "${DPONE_CACHE_RETENTION_APPROVED_PLAN_SHA256}" ]; then
  printf 'sealed retention plan digest does not match the deployed approval\n' >&2
  exit 4
fi
apply_evidence="$(mktemp -d cache-retention-apply.XXXXXX)"
jq -e '
  (.warnings | map(select(.code == "airflow_cache_status_publication_attention")) | length) == 0
  and (.operational_status.retention_apply_publication_failure == null)
' "${CACHE_STATUS_SNAPSHOT}" >/dev/null
jq -S -e '.operational_status.retention_apply' "${CACHE_STATUS_SNAPSHOT}" \
  >"${apply_evidence}/apply.json"
jq -S -e '.operational_status.retention_apply_publication' "${CACHE_STATUS_SNAPSHOT}" \
  >"${apply_evidence}/publication.json"
dpone gitops schema validate --kind dpone.deployment-cache-retention-apply.v3 \
  --payload "${apply_evidence}/apply.json"
dpone gitops schema validate --kind dpone.airflow-cache-status-publication.v1 \
  --payload "${apply_evidence}/publication.json"
apply_environment="$(jq -er '.environment | select(type == "string" and length > 0)' \
  "${apply_evidence}/apply.json")"
if [ "${apply_environment}" != "${DPONE_ENVIRONMENT}" ]; then
  printf 'retention apply environment mismatch: expected=%s observed=%s\n' \
    "${DPONE_ENVIRONMENT}" "${apply_environment}" >&2
  exit 4
fi
python3 - "${apply_evidence}/publication.json" "${RETENTION_EVIDENCE_MAX_AGE_SECONDS}" <<'PY'
import datetime
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    attempted = json.load(stream)["attempted_at"]
observed = datetime.datetime.fromisoformat(attempted.replace("Z", "+00:00"))
age = (datetime.datetime.now(datetime.timezone.utc) - observed).total_seconds()
if age < 0 or age > int(sys.argv[2]):
    raise SystemExit("retention apply evidence is stale or from the future")
PY
apply_target_sha256="sha256:$(sha256sum "${apply_evidence}/apply.json" | awk '{print $1}')"
jq -e --arg target_sha256 "${apply_target_sha256}" '
  .passed == true and .status == "published"
  and .expected_schema == "dpone.deployment-cache-retention-apply.v3"
  and (.source_sha256 | test("^sha256:[0-9a-f]{64}$"))
  and .target_sha256 == $target_sha256
' "${apply_evidence}/publication.json" >/dev/null
python3 - "${review_evidence_dir}/plan.json" "${apply_evidence}/apply.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    plan = json.load(stream)
with open(sys.argv[2], encoding="utf-8") as stream:
    apply = json.load(stream)

def projected_item(item):
    result = {
        "deployment_id": item["deployment_id"],
        "action": "deleted" if item["action"] == "delete" else "skipped",
        "reason": item["reason"],
        "path": item["path"],
    }
    if "error_code" in item:
        result["error_code"] = item["error_code"]
    return result

def item_key(item):
    return json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

expected_items = sorted((projected_item(item) for item in plan["items"]), key=item_key)
observed_items = sorted(apply["items"], key=item_key)
candidates = sorted(plan["delete_candidates"])
deleted = sorted(apply["deleted_deployment_ids"])
protected = set(plan["protected_deployment_ids"])
forbidden = protected | ({plan["current_deployment_id"]} if plan["current_deployment_id"] else set())

if apply["environment"] != plan["environment"]:
    raise SystemExit("retention apply environment does not match the sealed plan")
if apply["current_deployment_id"] != plan["current_deployment_id"]:
    raise SystemExit("retention apply current deployment does not match the sealed plan")
if apply.get("reviewed_plan_sha256") != plan["plan_sha256"]:
    raise SystemExit("retention apply digest does not match the sealed plan")
if observed_items != expected_items:
    raise SystemExit("retention apply items do not match the sealed plan projection")
if deleted != candidates:
    raise SystemExit("retention apply deleted set does not match every reviewed candidate")
if forbidden.intersection(deleted):
    raise SystemExit("retention apply reports deletion of a current or protected deployment")
PY
if jq -e '
  (.deleted_deployment_ids | length) == 0
  and all(.items[]?; .action != "deleted")
  and (has("review_id") | not)
  and (has("operation_id") | not)
  and (has("receipt_revision") | not)
  and (has("transaction_status") | not)
' "${apply_evidence}/apply.json" >/dev/null; then
  printf 'verified plan-bound schema-valid v3 no-op; no deletion receipt was created\n'
else
  jq -e \
    --arg approved "${DPONE_CACHE_RETENTION_APPROVED_PLAN_SHA256}" \
    --arg review_id "${DPONE_CACHE_RETENTION_REVIEW_ID}" '
    (.deleted_deployment_ids | length) > 0
    and .reviewed_plan_sha256 == $approved
    and .review_id == $review_id
    and .transaction_status == "committed"
    and (.operation_id | test("^sha256:[0-9a-f]{64}$"))
    and (.receipt_revision | test("^sha256:[0-9a-f]{64}$"))
  ' "${apply_evidence}/apply.json" >/dev/null
  printf 'verified plan-bound destructive v3 apply evidence\n'
fi
(cd "${apply_evidence}" && sha256sum -- apply.json publication.json >SHA256SUMS \
  && sha256sum -c SHA256SUMS)
chmod 0400 "${apply_evidence}"/*
```

After removing the approval in GitOps, use the rollout timestamp and exact
parser workload to prove that every active watcher reached its configured
retention cycle and emitted the expected plan-only decision. The bound includes
one reconcile timeout and sleep interval for every configured cycle, plus one
poll interval. If that bound is
larger than the reviewed operator maximum, or the event is absent at timeout,
the proof fails closed with workload, Pod and recent-log diagnostics. This log
check is diagnostic evidence; the sealed plan/apply files above remain the
deletion authority. Set `DPONE_APPROVAL_WITHDRAWAL_EVIDENCE_DIR` to one
externally retained absolute directory and bind it to the exact
`APPROVAL_REMOVAL_COMMIT`. A successful retry verifies the sealed result instead
of polling again:

```bash
set -euo pipefail

: "${KUBE_CONTEXT:?set the reviewed kubeconfig context}"
: "${AIRFLOW_NAMESPACE:?set the reviewed Airflow namespace}"
: "${AIRFLOW_RELEASE:?set the reviewed Helm release name}"
: "${DPONE_CACHE_PARSER_WORKLOAD:?set deployment/name or statefulset/name}"
: "${DPONE_CACHE_WATCH_CONTAINER:?set the exact cache-watch container name}"
: "${APPROVAL_REMOVED_AT:?set the RFC3339 rollout time of the removal commit}"
: "${APPROVAL_REMOVAL_COMMIT:?set the reviewed 40-hex approval-removal commit}"
: "${DPONE_APPROVAL_WITHDRAWAL_EVIDENCE_DIR:?set one externally retained evidence directory}"
: "${DPONE_APPROVAL_WITHDRAWAL_MAX_WAIT_SECONDS:=7200}"
: "${DPONE_APPROVAL_WITHDRAWAL_POLL_SECONDS:=10}"
: "${DPONE_APPROVAL_WITHDRAWAL_REQUEST_TIMEOUT_SECONDS:=30}"
: "${DPONE_APPROVAL_WITHDRAWAL_MAX_JSON_BYTES:=8388608}"
: "${DPONE_APPROVAL_WITHDRAWAL_LOG_TAIL_LINES:=1000}"
: "${DPONE_APPROVAL_WITHDRAWAL_LOG_LIMIT_BYTES:=1048576}"
case "${DPONE_CACHE_PARSER_WORKLOAD}" in deployment/*|statefulset/*) ;; *) exit 2 ;; esac
case "${APPROVAL_REMOVAL_COMMIT}" in *[!0-9a-f]*|'') exit 2 ;; esac
[ "${#APPROVAL_REMOVAL_COMMIT}" -eq 40 ] || exit 2
for value in "${DPONE_APPROVAL_WITHDRAWAL_MAX_WAIT_SECONDS}" \
  "${DPONE_APPROVAL_WITHDRAWAL_POLL_SECONDS}" \
  "${DPONE_APPROVAL_WITHDRAWAL_REQUEST_TIMEOUT_SECONDS}" \
  "${DPONE_APPROVAL_WITHDRAWAL_MAX_JSON_BYTES}" \
  "${DPONE_APPROVAL_WITHDRAWAL_LOG_TAIL_LINES}" \
  "${DPONE_APPROVAL_WITHDRAWAL_LOG_LIMIT_BYTES}"; do
  case "${value}" in ''|*[!0-9]*) printf 'withdrawal bounds must be positive integers\n' >&2; exit 2 ;; esac
  [ "${value}" -gt 0 ] || exit 2
done
withdrawal_evidence_dir="${DPONE_APPROVAL_WITHDRAWAL_EVIDENCE_DIR%/}"
case "${withdrawal_evidence_dir}" in /*) ;; *) printf 'evidence directory must be absolute\n' >&2; exit 2 ;; esac
[ ! -L "${withdrawal_evidence_dir}" ] || exit 2
mkdir -p -m 0700 "${withdrawal_evidence_dir}"
approval_kube() {
  kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
    --request-timeout="${DPONE_APPROVAL_WITHDRAWAL_REQUEST_TIMEOUT_SECONDS}s" "$@"
}
capture_approval_json() {
  output="$1"
  shift
  tmp="${output}.tmp.$$"
  approval_kube "$@" | python3 -c '
import sys
limit = int(sys.argv[1])
payload = sys.stdin.buffer.read(limit + 1)
if len(payload) > limit:
    raise SystemExit("Kubernetes JSON capture exceeded configured byte limit")
sys.stdout.buffer.write(payload)
' "${DPONE_APPROVAL_WITHDRAWAL_MAX_JSON_BYTES}" >"${tmp}"
  [ -s "${tmp}" ]
  mv "${tmp}" "${output}"
}
identity_dir="$(mktemp -d "${TMPDIR:-/tmp}/dpone-approval-identity.XXXXXX")"
trap 'rm -rf "${identity_dir}"' EXIT
capture_approval_json "${identity_dir}/namespace.json" \
  get namespace "${AIRFLOW_NAMESPACE}" -o json
capture_approval_json "${identity_dir}/workload.json" \
  get "${DPONE_CACHE_PARSER_WORKLOAD}" -o json
helm status "${AIRFLOW_RELEASE}" --kube-context "${KUBE_CONTEXT}" \
  --namespace "${AIRFLOW_NAMESPACE}" -o json >"${identity_dir}/helm-status.json"
namespace_uid="$(jq -er '.metadata.uid' "${identity_dir}/namespace.json")"
workload_uid="$(jq -er '.metadata.uid' "${identity_dir}/workload.json")"
helm_revision="$(jq -er '.version | select(type == "number" and . > 0)' \
  "${identity_dir}/helm-status.json")"
if [ -f "${withdrawal_evidence_dir}/approval-withdrawal.json" ]; then
  (cd "${withdrawal_evidence_dir}" && sha256sum -c SHA256SUMS)
  jq -e --arg removal_commit "${APPROVAL_REMOVAL_COMMIT}" \
    --arg context "${KUBE_CONTEXT}" --arg namespace "${AIRFLOW_NAMESPACE}" \
    --arg namespace_uid "${namespace_uid}" --arg workload_uid "${workload_uid}" \
    --arg workload "${DPONE_CACHE_PARSER_WORKLOAD}" \
    --arg release "${AIRFLOW_RELEASE}" --argjson helm_revision "${helm_revision}" \
    --arg container "${DPONE_CACHE_WATCH_CONTAINER}" \
    --arg removed_at "${APPROVAL_REMOVED_AT}" '
      .passed == true
      and .removal_commit == $removal_commit
      and .kube_context == $context
      and .namespace == $namespace
      and .namespace_uid == $namespace_uid
      and .workload == $workload
      and .workload_uid == $workload_uid
      and .helm_release == $release
      and .helm_revision == $helm_revision
      and .watcher_container == $container
      and .approval_removed_at == $removed_at
    ' "${withdrawal_evidence_dir}/approval-withdrawal.json" >/dev/null || {
      printf 'sealed approval-withdrawal identity does not match this live occurrence\n' >&2
      exit 5
    }
  exit 0
fi
started_at=${SECONDS}
install -m 0600 "${identity_dir}/namespace.json" "${withdrawal_evidence_dir}/namespace.json"
install -m 0600 "${identity_dir}/workload.json" "${withdrawal_evidence_dir}/workload.json"
install -m 0600 "${identity_dir}/helm-status.json" "${withdrawal_evidence_dir}/helm-status.json"
workload="$(cat "${withdrawal_evidence_dir}/workload.json")"
selector="$(jq -er '.spec.selector.matchLabels | to_entries | sort_by(.key)
  | map("\(.key)=\(.value)") | join(",")' <<<"${workload}")"
watcher="$(jq -ce --arg container "${DPONE_CACHE_WATCH_CONTAINER}" '
  [.spec.template.spec.containers[] | select(.name == $container)]
  | if length == 1 then .[0] else error("expected exactly one cache-watch container") end
' <<<"${workload}")"
approval_names='["DPONE_CACHE_RETENTION_APPROVED_PLAN_SHA256","DPONE_CACHE_RETENTION_REVIEW_ID"]'
jq -e --argjson approvals "${approval_names}" '
  . as $watcher
  | all($approvals[]; . as $approval |
    ([$watcher.env[]? | select(.name == $approval)]
      | length == 0 or
        (length == 1 and (.[0].value // null) == "" and (.[0] | has("valueFrom") | not))))
' <<<"${watcher}" >/dev/null || {
  printf 'active workload still has an approval value or ambiguous env source\n' >&2
  exit 4
}
approval_sources_clear() {
  container_json="$1"
  owner="$2"
  index=0
  while IFS= read -r source; do
    [ -n "${source}" ] || continue
    index=$((index + 1))
    kind="$(jq -er '.kind' <<<"${source}")"
    name="$(jq -er '.name' <<<"${source}")"
    prefix="$(jq -r '.prefix' <<<"${source}")"
    optional="$(jq -r '.optional' <<<"${source}")"
    source_file="${withdrawal_evidence_dir}/env-source-${owner}-${index}.json"
    if ! approval_kube get "${kind}/${name}" --ignore-not-found -o json >"${source_file}.tmp"; then
      rm -f "${source_file}.tmp"
      printf 'envFrom source lookup failed: %s/%s\n' "${kind}" "${name}" >&2
      return 4
    fi
    if [ ! -s "${source_file}.tmp" ]; then
      rm -f "${source_file}.tmp"
      [ "${optional}" = true ] && continue
      printf 'required envFrom source does not exist: %s/%s\n' "${kind}" "${name}" >&2
      return 4
    fi
    mv "${source_file}.tmp" "${source_file}"
    jq -e --arg prefix "${prefix}" --argjson approvals "${approval_names}" '
      ((.data // {}) + (.binaryData // {})) | keys as $keys
      | all($approvals[]; . as $approval
          | (($approval | startswith($prefix)) | not)
            or (($approval | ltrimstr($prefix)) as $key | ($keys | index($key)) == null))
    ' "${source_file}" >/dev/null || {
      printf 'envFrom source can still inject a retention approval: %s/%s\n' "${kind}" "${name}" >&2
      return 4
    }
  done < <(jq -r '
    (.envFrom // [])[]
    | if has("secretRef") then
        {kind:"secret",name:.secretRef.name,prefix:(.prefix // ""),optional:(.secretRef.optional // false)}
      elif has("configMapRef") then
        {kind:"configmap",name:.configMapRef.name,prefix:(.prefix // ""),optional:(.configMapRef.optional // false)}
      else error("unsupported envFrom source") end
    | @json
  ' <<<"${container_json}")
}
approval_sources_clear "${watcher}" workload
jq -e '
  (.metadata.generation | type == "number")
  and .status.observedGeneration == .metadata.generation
  and if .kind == "Deployment" then
    (.status.updatedReplicas // 0) == (.spec.replicas // 1)
    and (.status.readyReplicas // 0) == (.spec.replicas // 1)
    and (.status.availableReplicas // 0) == (.spec.replicas // 1)
  elif .kind == "StatefulSet" then
    (.status.currentRevision | type == "string")
    and .status.currentRevision == .status.updateRevision
    and (.status.updatedReplicas // 0) == (.spec.replicas // 1)
    and (.status.readyReplicas // 0) == (.spec.replicas // 1)
  else false end
' <<<"${workload}" >/dev/null || {
  printf 'active workload has not completed the approval-removal rollout\n' >&2
  exit 4
}
configured_value() {
  jq -er --arg name "$1" --arg default "$2" '
    [.env[]? | select(.name == $name)]
    | if length == 0 then $default
      elif length == 1 and (.[0].value | type) == "string" then .[0].value
      else error("watch timing must be one literal env value") end
  ' <<<"${watcher}"
}
sync_interval="$(configured_value DPONE_PACK_SYNC_INTERVAL_SECONDS 60)"
sync_timeout="$(configured_value DPONE_PACK_SYNC_TIMEOUT_SECONDS 20)"
retention_cycles="$(configured_value DPONE_PACK_RETENTION_INTERVAL_CYCLES 60)"
for value in "${sync_interval}" "${sync_timeout}" "${retention_cycles}"; do
  case "${value}" in ''|*[!0-9]*) printf 'watch timing must use positive integer literals\n' >&2; exit 2 ;; esac
  [ "${value}" -gt 0 ] && [ "${#value}" -le 6 ] || exit 2
done
configured_cycle_seconds=$(((sync_interval + sync_timeout) * retention_cycles))
required_wait_seconds=$((configured_cycle_seconds + DPONE_APPROVAL_WITHDRAWAL_POLL_SECONDS))
if [ "${required_wait_seconds}" -gt "${DPONE_APPROVAL_WITHDRAWAL_MAX_WAIT_SECONDS}" ]; then
  printf 'configured retention cycle exceeds withdrawal bound configured_cycle_seconds=%s required_wait_seconds=%s max_wait_seconds=%s\n' \
    "${configured_cycle_seconds}" "${required_wait_seconds}" \
    "${DPONE_APPROVAL_WITHDRAWAL_MAX_WAIT_SECONDS}" >&2
  exit 2
fi

deadline=$((started_at + required_wait_seconds))
while :; do
  capture_approval_json "${withdrawal_evidence_dir}/pods.json" \
    get pods -l "${selector}" -o json
  pods="$(cat "${withdrawal_evidence_dir}/pods.json")"
  all_observed=true
  if ! jq -e --arg removed "${APPROVAL_REMOVED_AT}" \
    --arg container "${DPONE_CACHE_WATCH_CONTAINER}" --argjson approvals "${approval_names}" '
    ($removed | fromdateiso8601) as $removed_at
    | (.items | length > 0)
    and all(.items[];
      ((.metadata.creationTimestamp | fromdateiso8601) >= $removed_at)
      and any(.status.conditions[]?; .type == "Ready" and .status == "True")
      and ([.spec.containers[]? | select(.name == $container)]
        | length == 1
        and .[0] as $watcher
        | all($approvals[]; . as $approval |
          ([$watcher.env[]? | select(.name == $approval)]
            | length == 0 or
              (length == 1 and (.[0].value // null) == ""
                and (.[0] | has("valueFrom") | not))))))' \
    <<<"${pods}" >/dev/null; then
    all_observed=false
  else
    while IFS= read -r pod; do
      pod_watcher="$(jq -ce --arg pod "${pod}" --arg container "${DPONE_CACHE_WATCH_CONTAINER}" '
        .items[] | select(.metadata.name == $pod)
        | [.spec.containers[] | select(.name == $container)]
        | if length == 1 then .[0] else error("expected one watcher") end
      ' <<<"${pods}")"
      approval_sources_clear "${pod_watcher}" "${pod}"
      log_file="${withdrawal_evidence_dir}/pod-${pod}.log"
      if ! approval_kube logs "pod/${pod}" -c "${DPONE_CACHE_WATCH_CONTAINER}" \
        --since-time "${APPROVAL_REMOVED_AT}" \
        --tail="${DPONE_APPROVAL_WITHDRAWAL_LOG_TAIL_LINES}" \
        --limit-bytes="${DPONE_APPROVAL_WITHDRAWAL_LOG_LIMIT_BYTES}" \
        >"${log_file}" 2>"${log_file}.stderr"; then
        all_observed=false
      elif ! grep -F 'dpone cache retention plan-only reason=approval_missing ' \
        "${log_file}" >/dev/null; then
        all_observed=false
      fi
    done < <(jq -r '.items[].metadata.name' <<<"${pods}")
  fi
  [ "${all_observed}" = false ] || break
  if [ "${SECONDS}" -ge "${deadline}" ]; then
    printf 'approval withdrawal timed out configured_cycle_seconds=%s required_wait_seconds=%s\n' \
      "${configured_cycle_seconds}" "${required_wait_seconds}" >&2
    approval_kube \
      get "${DPONE_CACHE_PARSER_WORKLOAD}" -o yaml >&2 || true
    approval_kube \
      get pods -l "${selector}" -o wide >&2 || true
    while IFS= read -r pod; do
      approval_kube \
        logs "pod/${pod}" -c "${DPONE_CACHE_WATCH_CONTAINER}" \
        --since-time "${APPROVAL_REMOVED_AT}" \
        --tail="${DPONE_APPROVAL_WITHDRAWAL_LOG_TAIL_LINES}" \
        --limit-bytes="${DPONE_APPROVAL_WITHDRAWAL_LOG_LIMIT_BYTES}" >&2 || true
    done < <(jq -r '.items[]?.metadata.name' <<<"${pods}")
    exit 4
  fi
  remaining_seconds=$((deadline - SECONDS))
  sleep_seconds=${DPONE_APPROVAL_WITHDRAWAL_POLL_SECONDS}
  [ "${sleep_seconds}" -le "${remaining_seconds}" ] || sleep_seconds=${remaining_seconds}
  [ "${sleep_seconds}" -gt 0 ] || continue
  sleep "${sleep_seconds}"
done
: >"${withdrawal_evidence_dir}/pod-log-evidence.jsonl"
while IFS= read -r pod; do
  log_file="pod-${pod}.log"
  test -f "${withdrawal_evidence_dir}/${log_file}"
  jq -cn --arg pod "${pod}" --arg file "${log_file}" \
    --arg sha256 "$(sha256sum "${withdrawal_evidence_dir}/${log_file}" | awk '{print $1}')" \
    '{pod:$pod,file:$file,sha256:("sha256:" + $sha256)}' \
    >>"${withdrawal_evidence_dir}/pod-log-evidence.jsonl"
done < <(jq -r '.items[].metadata.name' "${withdrawal_evidence_dir}/pods.json")
jq -s '.' "${withdrawal_evidence_dir}/pod-log-evidence.jsonl" \
  >"${withdrawal_evidence_dir}/pod-log-evidence.json"
jq -e --arg container "${DPONE_CACHE_WATCH_CONTAINER}" '
  [.items[] | {
    name:.metadata.name,
    uid:.metadata.uid,
    resource_version:.metadata.resourceVersion,
    created_at:.metadata.creationTimestamp,
    watcher_container:$container,
    ready:any(.status.conditions[]?; .type == "Ready" and .status == "True")
  }]
' "${withdrawal_evidence_dir}/pods.json" >"${withdrawal_evidence_dir}/pod-evidence.json"
result_tmp="${withdrawal_evidence_dir}/approval-withdrawal.json.tmp.$$"
jq -cn --arg removal_commit "${APPROVAL_REMOVAL_COMMIT}" \
  --arg context "${KUBE_CONTEXT}" \
  --arg namespace "${AIRFLOW_NAMESPACE}" \
  --arg namespace_uid "${namespace_uid}" \
  --arg removed_at "${APPROVAL_REMOVED_AT}" \
  --arg workload "${DPONE_CACHE_PARSER_WORKLOAD}" \
  --arg workload_uid "${workload_uid}" \
  --arg release "${AIRFLOW_RELEASE}" --argjson helm_revision "${helm_revision}" \
  --arg container "${DPONE_CACHE_WATCH_CONTAINER}" \
  --argjson workload_generation "$(jq -er '.metadata.generation' "${withdrawal_evidence_dir}/workload.json")" \
  --slurpfile pod_evidence "${withdrawal_evidence_dir}/pod-evidence.json" \
  --slurpfile log_evidence "${withdrawal_evidence_dir}/pod-log-evidence.json" \
  '{schema:"dpone.airflow-cache-retention-approval-withdrawal.v1",passed:true,
    status:"withdrawn",removal_commit:$removal_commit,approval_removed_at:$removed_at,
    kube_context:$context,namespace:$namespace,namespace_uid:$namespace_uid,
    workload:$workload,workload_uid:$workload_uid,
    helm_release:$release,helm_revision:$helm_revision,
    watcher_container:$container,workload_generation:$workload_generation,
    pod_evidence:$pod_evidence[0],log_evidence:$log_evidence[0]}' >"${result_tmp}"
mv "${result_tmp}" "${withdrawal_evidence_dir}/approval-withdrawal.json"
(cd "${withdrawal_evidence_dir}" && \
  find . -maxdepth 1 -type f ! -name SHA256SUMS -print0 | sort -z | \
  xargs -0 sha256sum -- > SHA256SUMS && sha256sum -c SHA256SUMS)
chmod 0400 "${withdrawal_evidence_dir}"/*
```
