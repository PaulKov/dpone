# Feature design: strict init-fetch pod retention

- Status: IMPLEMENTED
- Owner: dpone maintainers
- Target release: 0.73.32
- Last verified: 2026-08-01
- Approval source: approved Airflow executor policy and canonical deployment rollout
- Evidence: `tests/test_airflow_provider_execution_authority.py`,
  `tests/test_airflow_provider_init_fetch_execution.py`

## Problem and user journey

The strict `init_fetch` lane correctly disables KubernetesPodOperator base-log
tailing while init containers run. It currently also uses `keep_pod` for every
outcome. Successful workload pods therefore accumulate even though operators
usually need pod-level forensics only for failed or interrupted runs.

After this change an Airflow operator sees deterministic behavior:

1. The task runs with `get_logs=false`, avoiding the known `PodInitializing`
   base-log race.
2. KubernetesPodOperator requests deletion when the provider classifies the pod
   or base container as successful.
3. A base-container failure is normally retained, but task failure after base
   success, task cancellation and Kubernetes cleanup errors follow the pinned
   provider's native semantics and are not a retention guarantee.
4. Durable post-run truth is structured XCom/runtime/load-step evidence. Airflow
   remote logging preserves orchestration logs, not runtime stdout when
   `get_logs=false`.

## Public behavior

Strict v2 `init_fetch` composition uses:

```yaml
get_logs: false
on_finish_action: delete_succeeded_pod
```

These are trusted provider defaults, not pack-owned fields. The closed
`provider_execution.v1` projection remains unchanged and continues to reject
`get_logs`, `logging_interval`, `deferrable` and `on_finish_action` collisions.
The legacy/local pack lane remains backward compatible and may still apply its
declared `airflow.execution` policy.

## Algorithm and failure semantics

```text
verified deployment context
-> strict provider execution projection
-> trusted init_fetch KPO composition
-> get_logs=false
-> provider sees successful pod/base: request pod deletion
-> base fails: normally retain pod
-> later task failure, cancellation or delete error: provider-native outcome
```

No retry, XCom, command, image, connection or workload identity behavior
changes. The policy is fixed before task execution, so replay and concurrent
runs remain isolated by their existing pod identity.

Deletion is best-effort. Production rollout requires the dpone label-scoped
retention control, remote-log prerequisite, dry-run and alert evidence defined in the
[runtime pod retention runbook](airflow-runtime-pod-retention.md), or an
explicitly approved delete-all policy when failed pod forensics are not
required. The provider owns labels and normal-completion policy. The full dpone
runtime owns the executable plan/apply policy and deterministic manifests;
infrastructure owns deployment, schedule, credentials and alert routing.

Rollback is a provider-package rollback to the prior exact release. Reverting
to `keep_pod` is behaviorally compatible but recreates deterministic completed-
pod growth.

## Architecture and compatibility

`init_fetch_pod.py` remains the single trusted composition root. No new
abstraction or connector dependency is introduced. The change is compatible
with the Airflow 2.10 and 3.x provider matrix already exercised by the project.

The Apache Airflow Kubernetes provider documents `delete_succeeded_pod` as the
normal-completion mode intended to delete successful pods while leaving other
pod outcomes for provider-native handling:
[KubernetesPodOperator API](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/_api/airflow/providers/cncf/kubernetes/operators/pod/index.html).

Other requested market systems are `N/A` for this contract: dlt, Airbyte,
Fivetran, Informatica, Pentaho, SSIS, Cosmos and Beam do not own Airflow
KubernetesPodOperator cleanup semantics. Their retention models are therefore
not evidence for this provider-specific decision.

## Alternatives and trade-offs

| Option | Decision | Reason |
| --- | --- | --- |
| `keep_pod` | Rejected as default | Deterministically retains every green pod and grows Kubernetes API state. |
| `delete_pod` | Rejected for this increment | Removes failed init/base pods before operators can diagnose an artifact or Kubernetes failure. |
| `delete_succeeded_pod` | Selected | Reduces green-pod growth while usually preserving failed base pods; requires the separately deployed dpone stale-pod control because cleanup is best-effort. |
| Custom cleanup state machine in dpone provider | Deferred | Would duplicate Kubernetes/provider lifecycle behavior and needs a separate public design and failure model. |

No new module or dependency is added. Changed Python modules remain below the
repository size budget, and architecture clustering is unchanged.

## Test and rollout plan

- Unit: runtime and hook strict composition both preserve `get_logs=false` and
  use `delete_succeeded_pod`.
- Compatibility: pack-owned cleanup/log fields remain fail-closed in strict
  mode; legacy execution-policy tests remain green.
- Matrix: Airflow 2.10/2.11/3.2/3.3 provider tests and DAG serialization.
- Live dev: runtime smoke succeeds and provider cleanup is observed; retained
  failure and cleanup-error behavior is recorded as evidence, not assumed.
- Production: complete the
  [runtime pod retention acceptance](airflow-runtime-pod-retention.md#alerts-and-acceptance-evidence),
  then promote the exact dev-certified package and deployment. Rollback uses
  exact desired deployment identity.

## Cancellation, cleanup errors and evidence

- `on_finish_action` governs normal completion; provider `on_kill` behavior is
  version-specific and may delete the pod.
- A task may fail after a successful base container during XCom extraction or
  callbacks; `delete_succeeded_pod` may already have removed that pod.
- Kubernetes delete errors are not converted into data-task failures because a
  rerun after committed data could be unsafe. They must be detected by the
  external stale-pod control.
- The external control shares one bounded item/byte budget across both terminal
  phase queries. It records a successful conditional request as
  `delete_accepted`, not as observed Pod absence; the next sweep proves
  convergence. Evidence records the selected Kubernetes credential mode and
  never assumes that kubeconfig authentication is a ServiceAccount.
- Structured XCom and dpone audit remain authoritative. Runtime stdout requires
  an independently certified cluster log collector when operators need it.
