# ADR 0050: Exact Airflow launch pins may use KPO durable task state

## Status

Accepted.

## Context

dpone's exact-cache outcome gate must evaluate the immutable launch envelope
from the concrete runtime Pod, not deployment values from a later parse tip.
The original production pointer authority uses Kubernetes ConfigMap
`resourceVersion` CAS and an init-container barrier. It provides strong
cross-worker fencing but requires mutable ConfigMap RBAC.

Some production Airflow tenants deliberately allow KubernetesPodOperator to
manage Pods while denying Secret, ConfigMap, Lease, and controller mutations.
Disabling the launch pin would make long-running, deferred outcome gates unsafe
during an exact-cache tip flip. A tenant loader patch or automatic fallback
would hide a weaker contract and make behavior depend on import order.

Airflow 3.3 introduced a public task state store. CNCF Kubernetes provider
10.20 uses it for KPO `durable=True`: the operator persists the selected Pod
name and namespace before waiting, reconnects to it after worker loss, and
keeps its established label-search reattach path for the narrow crash window
before persistence. Kubernetes gives each Pod occurrence a unique UID and
supports UID-precondition deletion.

## Decision

- Keep `kubernetes_configmap` as the backward-compatible launch-pin backend.
- Add explicit `airflow_task_state` selection. Never auto-fallback between
  backends.
- Require Airflow 3.3+, CNCF provider 10.20+, `durable=True`, and an available
  task-state accessor for `airflow_task_state`; otherwise fail closed.
- Compose with KPO's native durable lifecycle rather than creating a second
  scheduler or persistence layer.
- Keep authority C on the live runtime Pod: immutable launch-envelope
  annotation/env plus server-assigned UID.
- Use task state for the KPO Pod locator and bounded XCom only to transfer the
  occurrence claim to the downstream gate. The gate must re-fetch the exact
  namespace/name/UID and rebuild the envelope and pin digest before accepting
  the claim.
- Freeze the backend and Kubernetes authority into runtime, gate, and cleanup
  tasks. Include the backend in the store authority digest.
- Retain exact UID-precondition Pod cleanup for both backends. Run ConfigMap
  head/per-try cleanup only for the ConfigMap backend.
- Preserve parse compatibility for older Airflow versions; only execution of
  the selected unsupported backend fails.

## Consequences

- Restricted tenants can preserve exact-activation evidence with only the Pod
  mutations already required by KPO.
- Secret/ConfigMap/Lease/DaemonSet write permission is not required by the new
  backend.
- The safety boundary depends on the documented KPO durable contract, so
  provider version and task-state availability become certified platform
  capabilities.
- Existing tenants do not change behavior until they explicitly select the new
  backend.
- The ConfigMap backend remains appropriate where independent Kubernetes CAS
  fencing is required or Airflow/provider versions are older.
- A bounded XCom is still not trusted by itself. Missing live Pod, UID mismatch,
  envelope disagreement, task-state mismatch, and backend contradiction remain
  blockers.

## Rejected alternatives

- Disable launch pins or trust parse-time values: exact-cache tip flips become
  ambiguous.
- Patch tenant DAG loaders: unversioned policy and import-order coupling.
- Kubernetes Lease: requires unavailable additional RBAC.
- Direct Airflow metadata database access: violates Airflow 3 task isolation
  and provider portability.
- XCom-only authority: cannot prove the selected live Pod occurrence.
- Dedicated authority Pod: adds a bespoke scheduled-resource lifecycle.

## References

- [Feature specification](../feature-design-airflow-native-launch-pin-backend-v0740.md)
- [Airflow provider contract](../airflow-pack-provider.md)
- [ADR 0032: exact activation occurrence](0032-airflow-exact-activation-occurrence.md)
- [ADR 0042: runtime Pod deletion evidence](0042-airflow-runtime-pod-retention-evidence.md)
- <https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html#durable-execution>
- <https://airflow.apache.org/docs/apache-airflow/3.3.0/administration-and-deployment/task-and-asset-state-store.html>
- <https://kubernetes.io/docs/concepts/overview/working-with-objects/names/>
