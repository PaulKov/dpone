# Deploy runtime Pod retention on Kubernetes

**Purpose.** Render, review, deploy, activate, and validate the namespace-scoped retention CronJob and its evidence.

**Audience.** Airflow platform engineers and SREs introducing scheduled retention in a reviewed Kubernetes namespace.

[Back to runtime Pod retention overview](airflow-runtime-pod-retention.md) · **Next likely task:** [operate alerts and incident recovery](airflow-runtime-pod-retention-operations.md).

## Render the Kubernetes control

Render plan mode first. Use the exact candidate/runtime image digest, never a
tag:

```bash
set -euo pipefail

: "${AIRFLOW_NAMESPACE:?set the reviewed Airflow namespace}"
tmp="$(mktemp runtime-pod-retention-render.XXXXXX)"
trap 'rm -f "${tmp}"' EXIT
dpone airflow runtime-pod-retention-render \
  --namespace "${AIRFLOW_NAMESPACE}" \
  --image 'registry.example/dpone@sha256:<64-hex-digest>' \
  --schedule '17 * * * *' \
  --mode plan \
  --alerts prometheus \
  --stale-after-seconds 7200 \
  --format yaml >"${tmp}"
mv -f "${tmp}" runtime-pod-retention.yaml
trap - EXIT
```

The artifact contains:

- a dedicated ServiceAccount;
- a plan-mode Role with only `list` on namespace Pods; apply mode adds only
  `delete`;
- a RoleBinding;
- a `concurrencyPolicy: Forbid` CronJob with argv execution, no shell, bounded
  histories/resources and restricted container security context;
- inherited mode and template-SHA annotations that bind every Job to the exact
  immutable image and argv of the current CronJob rollout;
- when requested, Prometheus alerts for failed jobs and stale last success.

For a bounded non-production certification, render apply mode with explicit
operator acknowledgement. Do not promote this stock CLI profile to production:

```bash
set -euo pipefail

: "${AIRFLOW_NAMESPACE:?set the reviewed Airflow namespace}"
actor="serviceaccount://${AIRFLOW_NAMESPACE}/dpone-runtime-pod-retention"
tmp="$(mktemp runtime-pod-retention-apply-render.XXXXXX)"
trap 'rm -f "${tmp}"' EXIT
dpone airflow runtime-pod-retention-render \
  --namespace "${AIRFLOW_NAMESPACE}" \
  --image 'registry.example/dpone@sha256:<64-hex-digest>' \
  --schedule '17 * * * *' \
  --mode apply \
  --actor "${actor}" \
  --allowed-actor "${actor}" \
  --confirm-delete \
  --alerts prometheus \
  --stale-after-seconds 7200 \
  --format yaml >"${tmp}"
mv -f "${tmp}" runtime-pod-retention-apply.yaml
trap - EXIT
```

Use `--format json` to obtain
`dpone.airflow-runtime-pod-retention-render.v1`, including policy, resource
inventory and `manifest_sha256` for review/promotion evidence.

Validate the rendered API objects server-side and deploy plan mode first:

```bash
set -euo pipefail

: "${KUBE_CONTEXT:?set the reviewed kubeconfig context}"
: "${AIRFLOW_NAMESPACE:?set the reviewed Airflow namespace}"
: "${KUBECTL_REQUEST_TIMEOUT:=30s}"
: "${JOB_LOG_TAIL_LINES:=10000}"
: "${JOB_LOG_LIMIT_BYTES:=10485760}"
kubectl --context "${KUBE_CONTEXT}" --request-timeout="${KUBECTL_REQUEST_TIMEOUT}" apply --dry-run=server \
  -f runtime-pod-retention.yaml || { rc=$?; exit "${rc}"; }
plan_rollout_not_before="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
kubectl --context "${KUBE_CONTEXT}" --request-timeout="${KUBECTL_REQUEST_TIMEOUT}" apply \
  -f runtime-pod-retention.yaml || { rc=$?; exit "${rc}"; }
job="dpone-runtime-pod-retention-manual-$(date +%s)"
kubectl --context "${KUBE_CONTEXT}" --request-timeout="${KUBECTL_REQUEST_TIMEOUT}" create job \
  --from=cronjob/dpone-runtime-pod-retention "${job}" \
  --namespace "${AIRFLOW_NAMESPACE}" || { rc=$?; exit "${rc}"; }

deadline=$((SECONDS + 600))
job_state=timeout
while [ "${SECONDS}" -lt "${deadline}" ]; do
  conditions="$(kubectl --context "${KUBE_CONTEXT}" --request-timeout="${KUBECTL_REQUEST_TIMEOUT}" get "job/${job}" \
    --namespace "${AIRFLOW_NAMESPACE}" \
    -o 'jsonpath={range .status.conditions[*]}{.type}={.status}{"\n"}{end}')" \
    || { job_state=api_error; break; }
  case "${conditions}" in
    *Complete=True*) job_state=complete; break ;;
    *Failed=True*) job_state=failed; break ;;
  esac
  sleep 5
done

logs_rc=0
kubectl --context "${KUBE_CONTEXT}" --request-timeout="${KUBECTL_REQUEST_TIMEOUT}" \
  logs "job/${job}" --namespace "${AIRFLOW_NAMESPACE}" \
  --tail="${JOB_LOG_TAIL_LINES}" --limit-bytes="${JOB_LOG_LIMIT_BYTES}" \
  >"${job}.log" || logs_rc=$?
if [ "${logs_rc}" -ne 0 ]; then
  exit "${logs_rc}"
fi
if [ "${job_state}" != complete ]; then
  printf 'retention plan Job ended with state=%s; inspect %s.log\n' \
    "${job_state}" "${job}" >&2
  exit 1
fi
plan_cronjob_tmp="$(mktemp runtime-pod-retention-plan-cronjob.XXXXXX)"
plan_marker_tmp="$(mktemp runtime-pod-retention-plan-rollout.XXXXXX)"
trap 'rm -f "${plan_cronjob_tmp}" "${plan_marker_tmp}"' EXIT
kubectl --context "${KUBE_CONTEXT}" --request-timeout="${KUBECTL_REQUEST_TIMEOUT}" get cronjob \
  dpone-runtime-pod-retention --namespace "${AIRFLOW_NAMESPACE}" -o json >"${plan_cronjob_tmp}"
jq -e --arg rollout_not_before "${plan_rollout_not_before}" '
  .metadata.uid as $uid
  | .metadata.resourceVersion as $resource_version
  | .metadata.generation as $generation
  | .spec.jobTemplate.metadata.annotations["dpone.dev/runtime-pod-retention-mode"] as $mode
  | .spec.jobTemplate.metadata.annotations["dpone.dev/runtime-pod-retention-template-sha256"] as $template
  | select(($uid | type == "string" and length > 0)
      and ($resource_version | type == "string" and length > 0)
      and ($generation | type == "number" and floor == . and . > 0)
      and $mode == "plan"
      and ($template | type == "string" and test("^sha256:[0-9a-f]{64}$")))
  | {schema:"dpone.airflow-runtime-pod-retention-rollout-marker.v1",
      rollout_not_before:$rollout_not_before,cronjob_uid:$uid,
      cronjob_resource_version:$resource_version,cronjob_generation:$generation,
      mode:$mode,template_sha256:$template}
' "${plan_cronjob_tmp}" >"${plan_marker_tmp}"
mv -f "${plan_marker_tmp}" runtime-pod-retention-plan-rollout.json
rm -f "${plan_cronjob_tmp}"
trap - EXIT
```

The manual Job is only a smoke check. Leave the plan CronJob scheduled, then run
the scheduled validator below with
`RETENTION_ROLLOUT_MARKER=runtime-pod-retention-plan-rollout.json` for two
distinct completed plan cycles. Preserve both evidence directories. Activation
requires both bundles and rejects duplicate Job identities or evidence from a
different CronJob occurrence.

Review both plan reports and alert queries before replacing the plan-only
control. Set `PLAN_CYCLE_EVIDENCE_1` and `PLAN_CYCLE_EVIDENCE_2` to those two
directories.

Then validate and activate the already reviewed apply manifest:

#### Activate apply after two plan cycles

```bash
set -euo pipefail

: "${KUBE_CONTEXT:?set the reviewed kubeconfig context}"
: "${AIRFLOW_NAMESPACE:?set the reviewed Airflow namespace}"
: "${KUBECTL_REQUEST_TIMEOUT:=30s}"
kubectl --context "${KUBE_CONTEXT}" apply --dry-run=server --request-timeout="${KUBECTL_REQUEST_TIMEOUT}" \
  -f runtime-pod-retention-apply.yaml || { rc=$?; exit "${rc}"; }
: "${PLAN_CYCLE_EVIDENCE_1:?set the first scheduled plan evidence directory}"
: "${PLAN_CYCLE_EVIDENCE_2:?set the second scheduled plan evidence directory}"
plan_cronjob_tmp="$(mktemp runtime-pod-retention-plan-current.XXXXXX)"
apply_cronjob_tmp="$(mktemp runtime-pod-retention-apply-current.XXXXXX)"
apply_marker_tmp="$(mktemp runtime-pod-retention-apply-rollout.XXXXXX)"
trap 'rm -f "${plan_cronjob_tmp}" "${apply_cronjob_tmp}" "${apply_marker_tmp}"' EXIT
for cycle_dir in "${PLAN_CYCLE_EVIDENCE_1}" "${PLAN_CYCLE_EVIDENCE_2}"; do
  [ -d "${cycle_dir}" ] || { printf 'plan evidence directory is missing: %s\n' "${cycle_dir}" >&2; exit 1; }
  (cd "${cycle_dir}" && sha256sum -c SHA256SUMS)
done
kubectl --context "${KUBE_CONTEXT}" --request-timeout="${KUBECTL_REQUEST_TIMEOUT}" get cronjob \
  dpone-runtime-pod-retention --namespace "${AIRFLOW_NAMESPACE}" -o json >"${plan_cronjob_tmp}"
python3 - "${PLAN_CYCLE_EVIDENCE_1}" "${PLAN_CYCLE_EVIDENCE_2}" "${plan_cronjob_tmp}" <<'PY'
import json
import pathlib
import re
import sys

first_path, second_path, cronjob_path = map(pathlib.Path, sys.argv[1:])
cronjob = json.loads(cronjob_path.read_text(encoding="utf-8"))
metadata = cronjob.get("metadata", {})
annotations = cronjob.get("spec", {}).get("jobTemplate", {}).get("metadata", {}).get("annotations", {})
identity = {
    "cronjob_uid": metadata.get("uid"),
    "cronjob_generation": metadata.get("generation"),
    "mode": annotations.get("dpone.dev/runtime-pod-retention-mode"),
    "template_sha256": annotations.get("dpone.dev/runtime-pod-retention-template-sha256"),
}
if (
    not isinstance(identity["cronjob_uid"], str)
    or not identity["cronjob_uid"]
    or not isinstance(identity["cronjob_generation"], int)
    or isinstance(identity["cronjob_generation"], bool)
    or identity["cronjob_generation"] < 1
    or identity["mode"] != "plan"
    or not isinstance(identity["template_sha256"], str)
    or not re.fullmatch(r"sha256:[0-9a-f]{64}", identity["template_sha256"])
):
    raise SystemExit("current CronJob is not the reviewed plan rollout")

job_uids = []
for evidence_path in (first_path, second_path):
    occurrence = json.loads((evidence_path / "rollout-occurrence.json").read_text(encoding="utf-8"))
    report = json.loads((evidence_path / "report.json").read_text(encoding="utf-8"))
    if occurrence.get("schema") != "dpone.airflow-runtime-pod-retention-rollout-occurrence.v1" or any(
        occurrence.get(key) != value for key, value in identity.items()
    ):
        raise SystemExit("plan cycle does not belong to the current plan rollout")
    if report.get("schema") != "dpone.airflow-runtime-pod-retention-plan.v1" or report.get("status") not in {
        "ok",
        "needs_cleanup",
    }:
        raise SystemExit("plan cycle has no acceptable completed plan report")
    job_uid = occurrence.get("job_uid")
    if not isinstance(job_uid, str) or not job_uid:
        raise SystemExit("plan cycle has no Job UID")
    job_uids.append(job_uid)
if len(set(job_uids)) != 2:
    raise SystemExit("duplicate plan Job UID; activation requires two distinct completed plan cycles")
PY
diff_rc=0
kubectl --context "${KUBE_CONTEXT}" --request-timeout="${KUBECTL_REQUEST_TIMEOUT}" diff \
  -f runtime-pod-retention-apply.yaml || diff_rc=$?
if [ "${diff_rc:-0}" -gt 1 ]; then
  exit "${diff_rc}"
fi
rollout_not_before="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
kubectl --context "${KUBE_CONTEXT}" apply \
  --request-timeout="${KUBECTL_REQUEST_TIMEOUT}" \
  -f runtime-pod-retention-apply.yaml || { rc=$?; exit "${rc}"; }
kubectl --context "${KUBE_CONTEXT}" --request-timeout="${KUBECTL_REQUEST_TIMEOUT}" get cronjob \
  dpone-runtime-pod-retention --namespace "${AIRFLOW_NAMESPACE}" -o json >"${apply_cronjob_tmp}"
jq -e --arg rollout_not_before "${rollout_not_before}" '
  .metadata.uid as $uid
  | .metadata.resourceVersion as $resource_version
  | .metadata.generation as $generation
  | .spec.jobTemplate.metadata.annotations["dpone.dev/runtime-pod-retention-mode"] as $mode
  | .spec.jobTemplate.metadata.annotations["dpone.dev/runtime-pod-retention-template-sha256"] as $template
  | select(($uid | type == "string" and length > 0)
      and ($resource_version | type == "string" and length > 0)
      and ($generation | type == "number" and floor == . and . > 0)
      and $mode == "apply"
      and ($template | type == "string" and test("^sha256:[0-9a-f]{64}$")))
  | {schema:"dpone.airflow-runtime-pod-retention-rollout-marker.v1",
      rollout_not_before:$rollout_not_before,cronjob_uid:$uid,
      cronjob_resource_version:$resource_version,cronjob_generation:$generation,
      mode:$mode,template_sha256:$template}
' "${apply_cronjob_tmp}" >"${apply_marker_tmp}"

kubectl --context "${KUBE_CONTEXT}" --request-timeout="${KUBECTL_REQUEST_TIMEOUT}" auth can-i list pods \
  --namespace "${AIRFLOW_NAMESPACE}" \
  --as "system:serviceaccount:${AIRFLOW_NAMESPACE}:dpone-runtime-pod-retention" \
  || { rc=$?; exit "${rc}"; }
kubectl --context "${KUBE_CONTEXT}" --request-timeout="${KUBECTL_REQUEST_TIMEOUT}" auth can-i delete pods \
  --namespace "${AIRFLOW_NAMESPACE}" \
  --as "system:serviceaccount:${AIRFLOW_NAMESPACE}:dpone-runtime-pod-retention" \
  || { rc=$?; exit "${rc}"; }
mv -f "${apply_marker_tmp}" runtime-pod-retention-apply-rollout.json
rm -f "${plan_cronjob_tmp}" "${apply_cronjob_tmp}"
trap - EXIT
```

This replaces the same named Role and CronJob with apply-mode RBAC and command
arguments; it does not install a second cleanup controller. Confirm the next
scheduled execution and its JSON evidence before declaring apply mode healthy.

### Validate one retained scheduled Job

Kubernetes merges container stdout and stderr in the Pod log API. dpone keeps
the streams machine-separable through their stable schemas: the aggregate
plan/apply report has the plan/apply schema, while pre-mutation and outcome
events have the event schema. Preserve the raw merged log, split it by schema,
and validate both projections:

```bash
set -euo pipefail

: "${KUBE_CONTEXT:?set the reviewed kubeconfig context}"
: "${AIRFLOW_NAMESPACE:?set the namespace used by the reviewed Airflow Helm release}"
: "${REVIEWED_RETENTION_MANIFEST:?set the exact reviewed retention manifest path}"
: "${RETENTION_ROLLOUT_MARKER:?set the exact plan/apply rollout marker JSON path}"
: "${KUBECTL_REQUEST_TIMEOUT:=30s}"
: "${JOB_LOG_TAIL_LINES:=10000}"
: "${JOB_LOG_LIMIT_BYTES:=10485760}"
: "${JOB_INVENTORY_LIMIT:=100}"
: "${JOB_INVENTORY_MAX_BYTES:=8388608}"
: "${SCHEDULED_JOB_MAX_AGE_SECONDS:=7200}"
certification_observed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
evidence_dir="$(mktemp -d runtime-pod-retention-scheduled.XXXXXX)"
capture_kube_json_bounded() {
  output="$1"
  shift
  tmp="${output}.tmp.$$"
  trap 'rm -f "${tmp}"' RETURN
  kubectl --context "${KUBE_CONTEXT}" --request-timeout="${KUBECTL_REQUEST_TIMEOUT}" \
    "$@" | python3 -c '
import sys
limit = int(sys.argv[1])
payload = sys.stdin.buffer.read(limit + 1)
if len(payload) > limit:
    raise SystemExit("Kubernetes JSON capture exceeded JOB_INVENTORY_MAX_BYTES")
sys.stdout.buffer.write(payload)
' "${JOB_INVENTORY_MAX_BYTES}" >"${tmp}"
  [ -s "${tmp}" ] || { rm -f "${tmp}"; printf 'empty Kubernetes JSON capture\n' >&2; exit 1; }
  mv "${tmp}" "${output}"
  trap - RETURN
}
cp "${REVIEWED_RETENTION_MANIFEST}" "${evidence_dir}/reviewed-manifest.yaml"
cp "${RETENTION_ROLLOUT_MARKER}" "${evidence_dir}/rollout-marker.json"
reviewed_manifest_sha256="sha256:$(sha256sum "${evidence_dir}/reviewed-manifest.yaml" | awk '{print $1}')"
diff_rc=0
kubectl --context "${KUBE_CONTEXT}" --request-timeout="${KUBECTL_REQUEST_TIMEOUT}" diff \
  -f "${evidence_dir}/reviewed-manifest.yaml" >"${evidence_dir}/manifest-diff.txt" || diff_rc=$?
if [ "${diff_rc}" -ne 0 ]; then
  if [ "${diff_rc}" -eq 1 ]; then
    printf 'live retention resources differ from the reviewed manifest\n' >&2
  fi
    exit "${diff_rc}"
fi
capture_kube_json_bounded "${evidence_dir}/namespace.json" \
  get namespace "${AIRFLOW_NAMESPACE}" -o json
capture_kube_json_bounded "${evidence_dir}/cronjob.json" \
  get cronjob dpone-runtime-pod-retention --namespace "${AIRFLOW_NAMESPACE}" -o json
python3 - "${evidence_dir}/rollout-marker.json" "${evidence_dir}/cronjob.json" \
  "${evidence_dir}/verified-rollout.json" <<'PY'
from datetime import datetime
import json
import pathlib
import re
import sys

marker_path, cronjob_path, verified_path = map(pathlib.Path, sys.argv[1:])
marker = json.loads(marker_path.read_text(encoding="utf-8"))
cronjob = json.loads(cronjob_path.read_text(encoding="utf-8"))
mode_key = "dpone.dev/runtime-pod-retention-mode"
sha_key = "dpone.dev/runtime-pod-retention-template-sha256"

try:
    rollout_not_before = datetime.fromisoformat(str(marker.get("rollout_not_before")).replace("Z", "+00:00"))
except ValueError as exc:
    raise SystemExit("rollout marker has no valid ISO-8601 rollout_not_before") from exc
if rollout_not_before.tzinfo is None:
    raise SystemExit("rollout marker rollout_not_before must include a timezone")
template = cronjob.get("spec", {}).get("jobTemplate", {})
annotations = template.get("metadata", {}).get("annotations", {})
pod_annotations = template.get("spec", {}).get("template", {}).get("metadata", {}).get("annotations", {})
identity = {
    "cronjob_uid": cronjob.get("metadata", {}).get("uid"),
    "cronjob_generation": cronjob.get("metadata", {}).get("generation"),
    "mode": annotations.get(mode_key),
    "template_sha256": annotations.get(sha_key),
}
if (
    marker.get("schema") != "dpone.airflow-runtime-pod-retention-rollout-marker.v1"
    or not isinstance(identity["cronjob_uid"], str)
    or not identity["cronjob_uid"]
    or not isinstance(identity["cronjob_generation"], int)
    or isinstance(identity["cronjob_generation"], bool)
    or identity["cronjob_generation"] < 1
    or identity["mode"] not in {"plan", "apply"}
    or not isinstance(identity["template_sha256"], str)
    or not re.fullmatch(r"sha256:[0-9a-f]{64}", identity["template_sha256"])
    or pod_annotations.get(mode_key) != identity["mode"]
    or pod_annotations.get(sha_key) != identity["template_sha256"]
    or any(marker.get(key) != value for key, value in identity.items())
):
    raise SystemExit("current CronJob UID/generation/template does not match the rollout marker")
verified_path.write_text(json.dumps(marker, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
PY
capture_kube_json_bounded "${evidence_dir}/jobs.json" get jobs \
  --namespace "${AIRFLOW_NAMESPACE}" \
  --selector 'app.kubernetes.io/name=dpone-runtime-pod-retention,app.kubernetes.io/managed-by=dpone' \
  --limit="${JOB_INVENTORY_LIMIT}" -o json
jq -e '(.metadata.continue // "") == ""' "${evidence_dir}/jobs.json" >/dev/null || {
  printf 'scheduled Job inventory exceeded JOB_INVENTORY_LIMIT\n' >&2
  exit 1
}
rollout_not_before="$(jq -er '.rollout_not_before' "${evidence_dir}/verified-rollout.json")"
python3 - "${evidence_dir}/cronjob.json" "${evidence_dir}/jobs.json" \
  "${evidence_dir}/candidate-job.json" "${rollout_not_before}" <<'PY'
from datetime import datetime
import json
import pathlib
import re
import sys

cronjob_path, jobs_path, selected_path = map(pathlib.Path, sys.argv[1:4])
try:
    rollout_not_before = datetime.fromisoformat(sys.argv[4].replace("Z", "+00:00"))
except ValueError as exc:
    raise SystemExit("RETENTION_ROLLOUT_NOT_BEFORE must be an ISO-8601 timestamp") from exc
if rollout_not_before.tzinfo is None:
    raise SystemExit("RETENTION_ROLLOUT_NOT_BEFORE must include a timezone")
cronjob = json.loads(cronjob_path.read_text(encoding="utf-8"))
jobs = json.loads(jobs_path.read_text(encoding="utf-8"))
mode_key = "dpone.dev/runtime-pod-retention-mode"
sha_key = "dpone.dev/runtime-pod-retention-template-sha256"
annotations = cronjob["spec"]["jobTemplate"]["metadata"]["annotations"]
mode = annotations.get(mode_key)
template_sha = annotations.get(sha_key)
if mode not in {"plan", "apply"} or not isinstance(template_sha, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", template_sha):
    raise SystemExit("current CronJob has no valid retention mode/template identity")

expected_job_spec = cronjob["spec"]["jobTemplate"]["spec"]
expected_pod_spec = expected_job_spec["template"]["spec"]

def exact_execution_template(job):
    job_spec = job.get("spec", {})
    if job_spec.get("template", {}).get("spec") != expected_pod_spec:
        return False
    return all(
        job_spec.get(key) == expected_job_spec.get(key)
        for key in ("activeDeadlineSeconds", "ttlSecondsAfterFinished", "backoffLimit")
    )
cronjob_uid = cronjob["metadata"]["uid"]
candidates = []
for job in jobs.get("items", []):
    owners = job.get("metadata", {}).get("ownerReferences", [])
    job_annotations = job.get("metadata", {}).get("annotations", {})
    pod_annotations = job.get("spec", {}).get("template", {}).get("metadata", {}).get("annotations", {})
    owned = any(
        owner.get("kind") == "CronJob"
        and owner.get("uid") == cronjob_uid
        and owner.get("controller") is True
        for owner in owners
    )
    conditions = job.get("status", {}).get("conditions", [])
    created_text = job.get("metadata", {}).get("creationTimestamp")
    try:
        created_at = datetime.fromisoformat(str(created_text).replace("Z", "+00:00"))
    except ValueError:
        continue
    if created_at.tzinfo is None:
        continue
    complete = any(item.get("type") == "Complete" and item.get("status") == "True" for item in conditions)
    failed = any(item.get("type") == "Failed" and item.get("status") == "True" for item in conditions)
    if (
        owned
        and created_at >= rollout_not_before
        and complete
        and not failed
        and job_annotations.get(mode_key) == mode
        and job_annotations.get(sha_key) == template_sha
        and pod_annotations.get(mode_key) == mode
        and pod_annotations.get(sha_key) == template_sha
        and exact_execution_template(job)
    ):
        candidates.append(job)
if not candidates:
    raise SystemExit("no completed Job belongs to this reviewed rollout occurrence")
selected = max(candidates, key=lambda item: item["metadata"].get("creationTimestamp", ""))
selected_path.write_text(json.dumps(selected, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
PY
python3 - "${evidence_dir}/cronjob.json" "${evidence_dir}/verified-rollout.json" \
  "${evidence_dir}/candidate-job.json" "${evidence_dir}/selected-job.json" \
  "${certification_observed_at}" "${SCHEDULED_JOB_MAX_AGE_SECONDS}" <<'PY'
from datetime import datetime, timedelta
import json
import pathlib
import sys

cronjob_path, marker_path, candidate_path, selected_path = map(pathlib.Path, sys.argv[1:5])

def timestamp(value, *, field):
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise SystemExit(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise SystemExit(f"{field} must include a timezone")
    return parsed

try:
    max_age_seconds = int(sys.argv[6])
except ValueError as exc:
    raise SystemExit("SCHEDULED_JOB_MAX_AGE_SECONDS must be an integer") from exc
if not 1 <= max_age_seconds <= 86_400:
    raise SystemExit("SCHEDULED_JOB_MAX_AGE_SECONDS must be between 1 and 86400")
observed_at = timestamp(sys.argv[5], field="certification_observed_at")
cronjob = json.loads(cronjob_path.read_text(encoding="utf-8"))
marker = json.loads(marker_path.read_text(encoding="utf-8"))
job = json.loads(candidate_path.read_text(encoding="utf-8"))
rollout_not_before = timestamp(marker.get("rollout_not_before"), field="rollout_not_before")
created_at = timestamp(job.get("metadata", {}).get("creationTimestamp"), field="Job creationTimestamp")
status = job.get("status", {})
completion_at = timestamp(status.get("completionTime"), field="Job completionTime")
fresh_not_before = max(rollout_not_before, observed_at - timedelta(seconds=max_age_seconds))

def nonnegative_int(value):
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0

conditions = status.get("conditions", [])
complete = any(item.get("type") == "Complete" and item.get("status") == "True" for item in conditions)
failed = any(item.get("type") == "Failed" and item.get("status") == "True" for item in conditions)
if (
    not complete
    or failed
    or not nonnegative_int(status.get("active", 0))
    or status.get("active", 0) != 0
    or not nonnegative_int(status.get("failed", 0))
    or status.get("failed", 0) != 0
    or not nonnegative_int(status.get("succeeded"))
    or status.get("succeeded") != 1
    or created_at < rollout_not_before
    or not fresh_not_before <= completion_at <= observed_at
    or created_at > completion_at
):
    raise SystemExit("selected Job is not recent, completed, and inactive")

metadata = job.get("metadata", {})
owners = metadata.get("ownerReferences", [])
owned = any(
    owner.get("kind") == "CronJob"
    and owner.get("uid") == marker.get("cronjob_uid")
    and owner.get("controller") is True
    for owner in owners
)
expected_template = cronjob.get("spec", {}).get("jobTemplate", {})
expected_metadata = expected_template.get("metadata", {})
expected_job_spec = expected_template.get("spec", {})
job_spec = job.get("spec", {})
generated_label_keys = {
    "batch.kubernetes.io/controller-uid",
    "batch.kubernetes.io/job-name",
    "controller-uid",
    "job-name",
}

def exact_metadata(actual, expected, *, allowed_annotations=()):
    actual_labels = actual.get("labels", {})
    expected_labels = expected.get("labels", {})
    actual_annotations = actual.get("annotations", {})
    expected_annotations = expected.get("annotations", {})
    return (
        all(actual_labels.get(key) == value for key, value in expected_labels.items())
        and set(actual_labels) <= set(expected_labels) | generated_label_keys
        and all(actual_annotations.get(key) == value for key, value in expected_annotations.items())
        and set(actual_annotations) <= set(expected_annotations) | set(allowed_annotations)
    )

expected_pod_template = expected_job_spec.get("template", {})
pod_template = job_spec.get("template", {})
metadata_matches = exact_metadata(
    metadata,
    expected_metadata,
    allowed_annotations={"batch.kubernetes.io/cronjob-scheduled-timestamp"},
)
pod_template_matches = exact_metadata(
    pod_template.get("metadata", {}),
    expected_pod_template.get("metadata", {}),
) and pod_template.get("spec") == expected_pod_template.get("spec")
spec_matches = all(
    job_spec.get(key) == value
    for key, value in expected_job_spec.items()
    if key != "template"
)
bounded_defaults = {
    "parallelism": 1,
    "completions": 1,
    "completionMode": "NonIndexed",
    "suspend": False,
    "manualSelector": False,
}
bounds_match = all(job_spec.get(key, value) == value for key, value in bounded_defaults.items())
integer_bounds_are_exact = all(
    isinstance(job_spec.get(key, value), int)
    and not isinstance(job_spec.get(key, value), bool)
    and job_spec.get(key, value) == value
    for key, value in (("parallelism", 1), ("completions", 1))
)
extra_policy_absent = all(
    job_spec.get(key) is None
    for key in ("backoffLimitPerIndex", "maxFailedIndexes", "podFailurePolicy", "successPolicy", "managedBy")
)
if (
    not owned
    or not metadata_matches
    or not pod_template_matches
    or not spec_matches
    or not bounds_match
    or not integer_bounds_are_exact
    or not extra_policy_absent
):
    raise SystemExit("selected Job does not match the full bounded Job execution policy")
selected_path.write_text(json.dumps(job, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
PY
job="$(jq -er '.metadata.name' "${evidence_dir}/selected-job.json")"
job_uid="$(jq -er '.metadata.uid' "${evidence_dir}/selected-job.json")"
jq -e '
  any(.status.conditions[]?; .type == "Complete" and .status == "True")
  and (any(.status.conditions[]?; .type == "Failed" and .status == "True") | not)
' "${evidence_dir}/selected-job.json" >/dev/null
jq -cn --arg context "${KUBE_CONTEXT}" \
  --arg namespace "${AIRFLOW_NAMESPACE}" \
  --arg namespace_uid "$(jq -er '.metadata.uid' "${evidence_dir}/namespace.json")" \
  --arg reviewed_manifest_sha256 "${reviewed_manifest_sha256}" \
  --arg rollout_not_before "$(jq -er '.rollout_not_before' "${evidence_dir}/verified-rollout.json")" \
  --arg cronjob_uid "$(jq -er '.metadata.uid' "${evidence_dir}/cronjob.json")" \
  --arg cronjob_resource_version "$(jq -er '.metadata.resourceVersion' "${evidence_dir}/cronjob.json")" \
  --argjson cronjob_generation "$(jq -er '.metadata.generation' "${evidence_dir}/cronjob.json")" \
  --arg mode "$(jq -er '.mode' "${evidence_dir}/verified-rollout.json")" \
  --arg template_sha256 "$(jq -er '.template_sha256' "${evidence_dir}/verified-rollout.json")" \
  --arg certification_observed_at "${certification_observed_at}" \
  --arg job "${job}" --arg job_uid "${job_uid}" \
  '{schema:"dpone.airflow-runtime-pod-retention-rollout-occurrence.v1",
    kube_context:$context,namespace:$namespace,namespace_uid:$namespace_uid,
    reviewed_manifest_sha256:$reviewed_manifest_sha256,
    rollout_not_before:$rollout_not_before,cronjob_uid:$cronjob_uid,
    cronjob_resource_version:$cronjob_resource_version,
    cronjob_generation:$cronjob_generation,mode:$mode,template_sha256:$template_sha256,
    certification_observed_at:$certification_observed_at,job:$job,job_uid:$job_uid}' \
  >"${evidence_dir}/rollout-occurrence.json"
kubectl --context "${KUBE_CONTEXT}" --request-timeout="${KUBECTL_REQUEST_TIMEOUT}" \
  logs "job/${job}" --namespace "${AIRFLOW_NAMESPACE}" --all-containers \
  --tail="${JOB_LOG_TAIL_LINES}" --limit-bytes="${JOB_LOG_LIMIT_BYTES}" \
  >"${evidence_dir}/combined.log"
python3 - "${evidence_dir}/combined.log" "${evidence_dir}/reports.jsonl" \
  "${evidence_dir}/events.jsonl" <<'PY'
import json
import pathlib
import sys

source, reports_path, events_path = map(pathlib.Path, sys.argv[1:])
content = source.read_text(encoding="utf-8")
decoder = json.JSONDecoder()
cursor = 0
reports = []
events = []
while cursor < len(content):
    while cursor < len(content) and content[cursor].isspace():
        cursor += 1
    if cursor == len(content):
        break
    try:
        payload, cursor = decoder.raw_decode(content, cursor)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"combined log is not a sequence of JSON values: {exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit("combined log contains a non-object JSON value")
    schema = payload.get("schema")
    if schema in {
        "dpone.airflow-runtime-pod-retention-plan.v1",
        "dpone.airflow-runtime-pod-retention-apply.v1",
    }:
        reports.append(payload)
    elif schema == "dpone.airflow-runtime-pod-retention-event.v1":
        events.append(payload)
    else:
        raise SystemExit(f"combined log contains an unsupported schema: {schema!r}")

def write_jsonl(path, values):
    text = "".join(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n" for value in values)
    path.write_text(text, encoding="utf-8")

write_jsonl(reports_path, reports)
write_jsonl(events_path, events)
PY
[ "$(wc -l <"${evidence_dir}/reports.jsonl" | tr -d ' ')" -eq 1 ]
cp "${evidence_dir}/reports.jsonl" "${evidence_dir}/report.json"
report_kind="$(jq -er '.schema' "${evidence_dir}/report.json")"
current_mode="$(jq -er '.spec.jobTemplate.metadata.annotations["dpone.dev/runtime-pod-retention-mode"]' \
  "${evidence_dir}/cronjob.json")"
expected_report_kind="dpone.airflow-runtime-pod-retention-${current_mode}.v1"
[ "${report_kind}" = "${expected_report_kind}" ] || {
  printf 'report schema does not match current CronJob mode\n' >&2
  exit 1
}
dpone gitops schema validate --payload "${evidence_dir}/report.json" --kind "${report_kind}"
if [ "${report_kind}" = dpone.airflow-runtime-pod-retention-plan.v1 ]; then
  jq -e '.status == "ok" or .status == "needs_cleanup"' "${evidence_dir}/report.json" >/dev/null
  [ ! -s "${evidence_dir}/events.jsonl" ]
else
  jq -e '.status == "ok" and .evidence_status == "complete"
    and .evidence_durability == "process_ordered"' \
    "${evidence_dir}/report.json" >/dev/null
  [ -s "${evidence_dir}/events.jsonl" ]
  while IFS= read -r event; do
    printf '%s\n' "${event}" >"${evidence_dir}/event.json"
    dpone gitops schema validate --payload "${evidence_dir}/event.json" \
      --kind dpone.airflow-runtime-pod-retention-event.v1
  done <"${evidence_dir}/events.jsonl"
  rm -f "${evidence_dir}/event.json"
  operation_id="$(jq -er '.operation_id' "${evidence_dir}/report.json")"
  jq -se --arg operation_id "${operation_id}" '
    (map(.operation_id) | all(. == $operation_id))
    and ([.[].sequence] == [range(1; length + 1)])
    and ([.[] | select(.event == "operation_started")] | length == 1)
    and ([.[] | select(.event == "operation_completed" and .outcome == "ok")] | length == 1)
    and (first.event == "operation_started")
    and (last.event == "operation_completed")
    and ([.[] | select(.event == "delete_intent") | .precondition_ref] | sort
      == [.[] | select(.event == "delete_outcome") | .precondition_ref] | sort)
  ' "${evidence_dir}/events.jsonl" >/dev/null
fi
(cd "${evidence_dir}" && sha256sum -- candidate-job.json combined.log cronjob.json events.jsonl jobs.json \
  manifest-diff.txt namespace.json report.json reports.jsonl reviewed-manifest.yaml rollout-marker.json \
  rollout-occurrence.json selected-job.json verified-rollout.json \
  >SHA256SUMS && sha256sum -c SHA256SUMS)
chmod 0400 "${evidence_dir}"/*
printf 'scheduled retention evidence retained at %s\n' "${evidence_dir}"
```

After the reviewed-manifest diff passes, the validator binds the current
CronJob UID, generation, mode and mirrored template digest to the rollout
marker before selecting a Job. It accepts only a Job completed within
`SCHEDULED_JOB_MAX_AGE_SECONDS` (two hours by
default), inactive with one success, owned by that CronJob occurrence, and
matching the complete rendered Job template plus single-run execution bounds.

An apply report without a valid non-empty event stream is incomplete evidence,
even when the Kubernetes Job is `Complete`. Retain the Job long enough to run
this check; cleanup of the Job is a separate reviewed action.

The CronJob has a five-minute `activeDeadlineSeconds`; metadata calls have
bounded connect/read timeouts, response bytes, total inventory, and evidence
arrays. A limit breach fails closed instead of occupying every later cycle.
