# Run runtime Pod retention manually

**Purpose.** Plan or apply bounded runtime Pod retention from a reviewed operator environment and preserve schema-valid evidence.

**Audience.** Airflow and Kubernetes platform operators performing a manual dry run or controlled cleanup.

[Back to runtime Pod retention overview](airflow-runtime-pod-retention.md) · **Next likely task:** [deploy the Kubernetes control](airflow-runtime-pod-retention-kubernetes.md).

## Run manually

Install the Kubernetes adapter in the maintenance image or local diagnostic
environment from one exact published release:

```bash
: "${DPONE_VERSION:?set the reviewed published dpone version}"
python3 -m pip install "dpone[kubernetes]==${DPONE_VERSION}"
```

For an unreleased candidate, use the checksum- and `SOURCE_COMMIT`-verified
`dist-airflow/dpone-*.whl[kubernetes]` installation in the
[exact candidate procedure](airflow-cache-kubernetes-prerequisites.md#prerequisites).
Never mix an index package, working-tree source and candidate manifests.

Use `--kube-auth kubeconfig --kube-context <name>` locally. The rendered
CronJob uses `--kube-auth in-cluster`. Keep manual and CronJob authorization
checks separate:

```bash
: "${KUBE_CONTEXT:?set the reviewed kubeconfig context}"
: "${AIRFLOW_NAMESPACE:?set the reviewed Airflow namespace}"
```

```bash
set -euo pipefail

kubectl --context "${KUBE_CONTEXT}" auth can-i list pods \
  --namespace "${AIRFLOW_NAMESPACE}"
```

The plan-mode ServiceAccount must be able to list Pods and must not be able to
delete them:

```bash
set -euo pipefail

: "${AIRFLOW_NAMESPACE:?set the reviewed Airflow namespace}"
kubectl --context "${KUBE_CONTEXT}" auth can-i list pods \
  --namespace "${AIRFLOW_NAMESPACE}" \
  --as "system:serviceaccount:${AIRFLOW_NAMESPACE}:dpone-runtime-pod-retention"
delete_answer="$(kubectl --context "${KUBE_CONTEXT}" auth can-i delete pods \
  --namespace "${AIRFLOW_NAMESPACE}" \
  --as "system:serviceaccount:${AIRFLOW_NAMESPACE}:dpone-runtime-pod-retention" \
  2>/dev/null || true)"
if [ "${delete_answer}" != no ]; then
  printf 'plan ServiceAccount must report delete=no, got %s\n' \
    "${delete_answer:-unavailable}" >&2
  exit 4
fi
```

Plan is read-only:

```bash
set -uo pipefail

: "${AIRFLOW_NAMESPACE:?set the reviewed Airflow namespace}"
evidence_dir="$(mktemp -d runtime-pod-retention-plan.XXXXXX)"
tmp="${evidence_dir}/output.json"
rc=0
dpone airflow runtime-pod-retention-plan \
  --namespace "${AIRFLOW_NAMESPACE}" \
  --minimum-age-seconds 86400 \
  --page-size 500 \
  --kube-auth kubeconfig \
  --kube-context "${KUBE_CONTEXT}" \
  --format json >"${tmp}" || rc=$?
if [ ! -s "${tmp}" ]; then
  [ "${rc}" -ne 0 ] || rc=1
  printf 'runtime Pod plan produced no JSON; temporary output is %s\n' \
    "${tmp}" >&2
  exit "${rc}"
fi

schema_rc=0
dpone gitops schema validate \
  --kind dpone.airflow-runtime-pod-retention-plan.v1 \
  --payload "${tmp}" \
  --format json >/dev/null || schema_rc=$?
if [ "${schema_rc}" -ne 0 ]; then
  [ "${rc}" -ne 0 ] || rc="${schema_rc}"
  printf 'runtime Pod plan is not valid plan evidence; temporary output is %s\n' \
    "${tmp}" >&2
  exit "${rc}"
fi

destination=runtime-pod-retention-plan.json
if [ "${rc}" -ne 0 ]; then
  destination="${evidence_dir}/attention.json"
fi
publish_rc=0
if [ "${rc}" -eq 0 ]; then
  mv -f "${tmp}" "${destination}" || publish_rc=$?
  [ "${publish_rc}" -ne 0 ] || rmdir "${evidence_dir}" || publish_rc=$?
else
  mv "${tmp}" "${destination}" || publish_rc=$?
  if [ "${publish_rc}" -eq 0 ]; then
    (cd "${evidence_dir}" && sha256sum attention.json > SHA256SUMS && sha256sum -c SHA256SUMS) \
      || publish_rc=$?
    [ "${publish_rc}" -ne 0 ] || chmod 0400 "${destination}" "${evidence_dir}/SHA256SUMS" \
      || publish_rc=$?
  fi
  printf 'runtime Pod attention evidence: %s\n' "${destination}" >&2
fi
[ "${publish_rc}" -eq 0 ] || exit "${publish_rc}"
exit "${rc}"
```

Inspect all `delete_candidates`, `quarantine` items, `warnings`,
`age_basis` and `terminal_age_exact`. Then apply with an exact identity.
`--actor` and `--allowed-actor` are operator acknowledgement recorded in
evidence, not authentication. The selected Kubernetes API credentials and RBAC
are the authorization authority; the rendered CronJob uses its dedicated
ServiceAccount:

```bash
set -uo pipefail

: "${DPONE_RETENTION_ACTOR:?set the reviewed operator identity, for example user://name}"
: "${AIRFLOW_NAMESPACE:?set the reviewed Airflow namespace}"
kubectl --context "${KUBE_CONTEXT}" auth can-i delete pods \
  --namespace "${AIRFLOW_NAMESPACE}" || exit $?

actor="${DPONE_RETENTION_ACTOR}"
evidence_dir="$(mktemp -d runtime-pod-retention-apply.XXXXXX)"
report_tmp="${evidence_dir}/output.json"
events="${evidence_dir}/events.jsonl"
rc=0
dpone airflow runtime-pod-retention-apply \
  --namespace "${AIRFLOW_NAMESPACE}" \
  --minimum-age-seconds 86400 \
  --page-size 500 \
  --max-delete-count 100 \
  --actor "${actor}" \
  --allowed-actor "${actor}" \
  --confirm-delete \
  --kube-auth kubeconfig \
  --kube-context "${KUBE_CONTEXT}" \
  --format json >"${report_tmp}" 2>"${events}" || rc=$?
cat "${events}" >&2
if [ ! -s "${report_tmp}" ]; then
  [ "${rc}" -ne 0 ] || rc=1
  printf 'runtime Pod apply produced no JSON; temporary output is %s\n' \
    "${report_tmp}" >&2
  exit "${rc}"
fi

schema_rc=0
dpone gitops schema validate \
  --kind dpone.airflow-runtime-pod-retention-apply.v1 \
  --payload "${report_tmp}" \
  --format json >/dev/null || schema_rc=$?
event_tmp="${evidence_dir}/event.json"
event_count=0
if [ -s "${events}" ]; then
  while IFS= read -r event; do
    event_count=$((event_count + 1))
    printf '%s\n' "${event}" >"${event_tmp}"
    dpone gitops schema validate \
      --kind dpone.airflow-runtime-pod-retention-event.v1 \
      --payload "${event_tmp}" \
      --format json >/dev/null || schema_rc=$?
    [ "${schema_rc}" -eq 0 ] || break
  done <"${events}"
fi
rm -f "${event_tmp}"
if [ "${event_count}" -lt 2 ]; then
  schema_rc=1
fi
if [ "${schema_rc}" -eq 0 ]; then
  operation_id="$(jq -er '.operation_id' "${report_tmp}")" || schema_rc=$?
  report_status="$(jq -er '.status' "${report_tmp}")" || schema_rc=$?
fi
if [ "${schema_rc}" -eq 0 ]; then
  jq -e -s \
    --arg operation_id "${operation_id}" \
    --arg report_status "${report_status}" '
      length >= 2 and
      (all(.[]; .operation_id == $operation_id)) and
      ([.[].sequence] == [range(1; length + 1)]) and
      ([.[] | select(.event == "operation_started")] | length == 1) and
      ([.[] | select(.event == "operation_completed")] | length == 1) and
      (first.event == "operation_started") and
      (last.event == "operation_completed") and
      (last.outcome == $report_status)
    ' "${events}" >/dev/null || schema_rc=$?
fi
if [ "${schema_rc}" -ne 0 ]; then
  [ "${rc}" -ne 0 ] || rc="${schema_rc}"
  printf 'runtime Pod apply report/events are not complete valid evidence; unsealed directory is %s\n' \
    "${evidence_dir}" >&2
  exit "${rc}"
fi

report_name=report.json
if [ "${rc}" -ne 0 ]; then
  report_name=failed.json
fi
mv "${report_tmp}" "${evidence_dir}/${report_name}"
publish_rc=0
(cd "${evidence_dir}" && sha256sum "${report_name}" events.jsonl > SHA256SUMS \
  && sha256sum -c SHA256SUMS) || publish_rc=$?
[ "${publish_rc}" -ne 0 ] || chmod 0400 \
  "${evidence_dir}/${report_name}" "${events}" "${evidence_dir}/SHA256SUMS" || publish_rc=$?
[ "${publish_rc}" -eq 0 ] || exit "${publish_rc}"
if [ "${rc}" -eq 0 ]; then
  canonical_tmp="$(mktemp runtime-pod-retention-apply.XXXXXX.json)"
  cp "${evidence_dir}/report.json" "${canonical_tmp}"
  mv -f "${canonical_tmp}" runtime-pod-retention-apply.json
fi
printf 'runtime Pod apply evidence bundle: %s\n' "${evidence_dir}" >&2
exit "${rc}"
```

Only schema-valid exit-`0` reports replace the canonical plan/apply artifacts.
Every apply run remains in its own unique directory as `report.json` or
`failed.json` plus `events.jsonl`, with a verified `SHA256SUMS` binding and
read-only local files. Plan attention evidence uses the same unique-directory
rule. A later incident cannot overwrite these bundles through this procedure.
Both successful and failed apply reports are sealed only after their complete
event chain is validated against the report. Invalid, truncated or unrelated
event streams remain in the printed unsealed directory and are never presented
as valid evidence.
Publish the complete directory to the platform's versioned/WORM
incident evidence store before leaving the host. Local permissions alone are
not durable immutability.
Empty output and generic `dpone.error.v1` envelopes remain at the printed
temporary path and cannot overwrite reviewed evidence.

Apply always performs a fresh inventory. A previous plan is review evidence,
not mutation authority. HTTP 404 is `already_absent`; 409 is
`changed_since_plan`; another delete failure stops the remaining batch.
Exit `0` means a complete clean cycle, `1` means bounded partial/attention
evidence, `2` means invalid input, `3` means a dependency failure, `4` means a
security/RBAC blocker, and `5` is an unexpected redacted failure. Preserve the
JSON artifact even when the exit code is non-zero.

`delete_accepted` means that the Kubernetes API accepted a conditional delete
request for the observed UID/resourceVersion. It does not prove immediate Pod
absence. The next bounded inventory cycle provides convergence evidence;
terminating Pods remain protected meanwhile. `status: ok` means every selected
request was accepted or already absent, not that asynchronous deletion was
observed to completion. `credential_mode` records the resolved source,
`in-cluster` or `kubeconfig`; the requested `auto` mode is never persisted as
mutation evidence. `evidence_durability: process_ordered` means events were
ordered and flushed in the current process; it is not a durable sink
acknowledgement. Only a publisher that returns after its durable sink confirms
the write may report `durable_acknowledged`. Kubernetes API credentials and
RBAC remain the authorization authority.

## Expected machine-readable evidence

A green plan with no terminal runtime Pods is a complete schema-valid report:

<!-- runtime-pod-retention-plan-example -->
```json
{
  "schema": "dpone.airflow-runtime-pod-retention-plan.v1",
  "status": "ok",
  "namespace": "airflow-example",
  "observed_at": "2026-08-03T08:00:00Z",
  "minimum_age_seconds": 86400,
  "page_size": 500,
  "age_basis": "creation_timestamp_fallback",
  "terminal_age_exact": false,
  "inventory": {
    "terminal_pods": 0,
    "succeeded_pods": 0,
    "failed_pods": 0,
    "quarantined": 0
  },
  "warnings": [],
  "items": [],
  "delete_candidates": []
}
```

A green apply cycle with no selected Pods is also explicit; it is not an empty
or generic success envelope:

<!-- runtime-pod-retention-apply-example -->
```json
{
  "schema": "dpone.airflow-runtime-pod-retention-apply.v1",
  "status": "ok",
  "operation_id": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
  "evidence_status": "complete",
  "evidence_durability": "process_ordered",
  "namespace": "airflow-example",
  "actor": "service-account://dpone-runtime-pod-retention",
  "actor_source": "operator_acknowledgement",
  "authorization_authority": "kubernetes_api_rbac",
  "credential_mode": "in-cluster",
  "credential_context": null,
  "deletion_evidence": "api_request_accepted_not_observed",
  "observed_at": "2026-08-03T08:00:00Z",
  "minimum_age_seconds": 86400,
  "max_delete_count": 100,
  "age_basis": "creation_timestamp_fallback",
  "terminal_age_exact": false,
  "items": [],
  "delete_accepted_pod_names": [],
  "skipped_pod_names": [],
  "failed_pod_names": []
}
```

Validate saved reports against the installed release, rather than relying on a
copied example:

```bash
dpone gitops schema validate \
  --kind dpone.airflow-runtime-pod-retention-plan.v1 \
  --payload runtime-pod-retention-plan.json
dpone gitops schema validate \
  --kind dpone.airflow-runtime-pod-retention-apply.v1 \
  --payload runtime-pod-retention-apply.json
```
