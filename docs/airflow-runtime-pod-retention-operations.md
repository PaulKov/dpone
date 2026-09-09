# Operate and recover runtime Pod retention

**Purpose.** Classify retention decisions, respond to errors and alerts, roll back safely, and verify public evidence schemas.

**Audience.** On-call operators, incident responders, security reviewers, and auditors.

[Back to runtime Pod retention overview](airflow-runtime-pod-retention.md) · **Next likely task:** [diagnose Airflow cache identity without Kubernetes access](airflow-cache-diagnostics-without-kubectl.md).

## Classification reference

| Reason | Action | Meaning |
| --- | --- | --- |
| `stale_terminal` | delete candidate | Terminal, owned, correlated and older than floor |
| `minimum_age` | protect | Not old enough |
| `terminating` | protect | Kubernetes deletion already in progress |
| `missing_correlation` | quarantine | Missing `dag_id`, `task_id` or `run_id` |
| `invalid_ownership` | quarantine | Provider-owned labels do not match |
| `inventory_phase_conflict` | quarantine | UID appeared in both terminal queries |
| `timestamp_missing` / `future_timestamp` | quarantine | Age cannot be trusted |
| `invalid_identity` / `invalid_phase` | quarantine | Conditional delete cannot be formed safely |

Any quarantine makes plan status `needs_attention`; apply never deletes those
objects. Inventory responses containing `spec`, `status`, data fields, invalid
pagination or unsupported metadata media types fail closed.

## Error recovery table

| Stable code | Meaning | Safe recovery |
| --- | --- | --- |
| `DPONE_AIRFLOW_RUNTIME_POD_RETENTION_ACTOR_MISSING`, `DPONE_AIRFLOW_RUNTIME_POD_RETENTION_ACTOR_UNAUTHORIZED`, `DPONE_AIRFLOW_RUNTIME_POD_RETENTION_CONFIRMATION_REQUIRED` | Apply authority is absent or not allowlisted | Keep plan mode. Correct the reviewed actor/acknowledgement; never bypass the check. |
| `DPONE_AIRFLOW_RUNTIME_POD_RETENTION_CREDENTIAL_SOURCE_MISSING`, `DPONE_AIRFLOW_RUNTIME_POD_RETENTION_CREDENTIAL_SOURCE_INVALID`, `DPONE_AIRFLOW_RUNTIME_POD_RETENTION_CREDENTIAL_MODE_MISMATCH` | The runtime cannot prove which Kubernetes identity it uses | Fix ServiceAccount/kubeconfig wiring and rerun `auth can-i`; do not expand RBAC. |
| `DPONE_AIRFLOW_RUNTIME_POD_RETENTION_ACCESS_DENIED`, `DPONE_AIRFLOW_RUNTIME_POD_RETENTION_DEPENDENCY_UNAVAILABLE`, `DPONE_AIRFLOW_RUNTIME_POD_RETENTION_SDK_UNAVAILABLE` | Kubernetes API or required client is unavailable | Preserve evidence and retry after platform recovery. A skipped inventory is not success. |
| `DPONE_AIRFLOW_RUNTIME_POD_RETENTION_INVENTORY_EXPIRED`, `DPONE_AIRFLOW_RUNTIME_POD_RETENTION_INVENTORY_INVALID`, `DPONE_AIRFLOW_RUNTIME_POD_RETENTION_METADATA_ONLY_UNSUPPORTED` | The bounded metadata snapshot cannot be trusted | Rerun plan from the beginning; do not reuse candidates from the failed occurrence. |
| `DPONE_AIRFLOW_RUNTIME_POD_RETENTION_DELETE_CAPABILITY_MISSING`, `DPONE_AIRFLOW_RUNTIME_POD_RETENTION_DELETE_FAILED` | Conditional deletion was unavailable or rejected | Inspect the exact Pod UID/resourceVersion and RBAC. Replan before retrying. |
| `DPONE_AIRFLOW_RUNTIME_POD_RETENTION_EVIDENCE_CAPABILITY_MISSING`, `DPONE_AIRFLOW_RUNTIME_POD_RETENTION_EVIDENCE_CAPABILITY_INVALID`, `DPONE_AIRFLOW_RUNTIME_POD_RETENTION_EVIDENCE_UNAVAILABLE` | Pre-mutation audit cannot be durably emitted | Treat the operation as blocked/incomplete. Restore the collector before any new apply. |
| `DPONE_AIRFLOW_RUNTIME_POD_RETENTION_INTERRUPTED` | The process ended after operation start | Reconcile `delete_intent` and `delete_outcome` by operation ID, then produce a fresh plan. |
| `DPONE_AIRFLOW_RUNTIME_POD_RETENTION_INPUT_INVALID`, `DPONE_AIRFLOW_RUNTIME_POD_RETENTION_CLOCK_INVALID` | Policy or time basis is unsafe | Correct the reviewed policy/clock source; never lower the age floor to hide the blocker. |

## Alerts and acceptance evidence

Before enabling apply:

1. Confirm structured dpone runtime/load-step evidence for successful and
   failed tasks.
2. Confirm the chosen runtime stdout/log collector retention exceeds the Pod
   age floor. With `get_logs=false`, Airflow remote task logging does not
   automatically capture runtime-container stdout.
3. Review two plan-mode CronJob reports.
4. Create one owned terminal test Pod beyond the threshold and one unlabelled
   control Pod. Only the owned Pod may be a candidate.
5. Run one bounded non-production apply and verify UID/resourceVersion evidence.
6. Rehearse denied delete and verify alert delivery.
7. Store image digest, manifest SHA, policy, reports, alert result and rollback
   owner in the platform deployment evidence set.
8. For production, require `.evidence_durability == "durable_acknowledged"`
   from an injected publisher whose external sink acknowledgement has live
   certification. The stock rendered/CLI apply profile reports
   `process_ordered` and must remain disabled in production.

Use the [Python API durable publisher composition](airflow-runtime-pod-retention-python-api.md#durable-publisher-composition)
for the connector-neutral exact-byte ACK adapter. Infrastructure still supplies
and live-certifies the concrete durable sink.

Compare the plan/apply CAS identities for every Pod present in both artifacts:

```bash
jq -S '[.items[] | {pod_name, pod_ref, precondition_ref}]' \
  runtime-pod-retention-plan.json >plan-refs.json
jq -S '[.items[] | {pod_name, pod_ref, precondition_ref}]' \
  runtime-pod-retention-apply.json >apply-refs.json
jq -n --slurpfile plan plan-refs.json --slurpfile apply apply-refs.json \
  '($plan[0] | INDEX(.pod_name)) as $p
   | ($apply[0] | INDEX(.pod_name)) as $a
   | [$p | keys[] | select($a[.] != null) | select($p[.] != $a[.])]'
```

The final array must be empty. A changed `precondition_ref` means UID or
resourceVersion changed and the reviewed occurrence was not the one applied.

The rendered PrometheusRule assumes kube-state-metrics naming. Validate its
queries against the target monitoring stack; use `--alerts off` and provide an
equivalent reviewed rule when the CRD or metrics differ.

## Rollback and incident response

Use the task-focused [runtime Pod retention withdrawal runbook](airflow-runtime-pod-retention-withdrawal.md#executable-withdrawal-procedure). It owns the exact forensic capture, preconditioned deletion, retry identity and post-delete proof.

## Public evidence schemas

For apply evidence, `items` is authoritative. The three Pod-name summary arrays
are deterministic projections and the canonical `dpone gitops schema validate`
command rejects any mismatch that portable JSON Schema alone cannot express.
Direct artifacts: [plan schema](schemas/gitops/airflow-runtime-pod-retention-plan.schema.json),
[apply schema](schemas/gitops/airflow-runtime-pod-retention-apply.schema.json),
[event schema](schemas/gitops/airflow-runtime-pod-retention-event.schema.json),
and [render schema](schemas/gitops/airflow-runtime-pod-retention-render.schema.json).

```bash
set -euo pipefail

dpone gitops schema show dpone.airflow-runtime-pod-retention-plan.v1
dpone gitops schema show dpone.airflow-runtime-pod-retention-apply.v1
dpone gitops schema show dpone.airflow-runtime-pod-retention-event.v1
dpone gitops schema show dpone.airflow-runtime-pod-retention-render.v1
```

Return to the [documentation home](index.md) or continue with
[Airflow cache diagnostics without Kubernetes access](airflow-cache-diagnostics-without-kubectl.md)
to compare desired/current deployment identity through the Airflow API.

Official references: [Kubernetes field selectors](https://kubernetes.io/docs/concepts/overview/working-with-objects/field-selectors/),
[metadata-only API responses](https://kubernetes.io/docs/reference/using-api/api-concepts/#metadata-only-fetches),
[delete preconditions](https://kubernetes.io/docs/reference/kubernetes-api/definitions/preconditions-v1-meta/),
and [Airflow KubernetesPodOperator](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html).
