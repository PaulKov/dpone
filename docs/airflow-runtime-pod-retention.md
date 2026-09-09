# Retain and sweep dpone runtime Pods

**Purpose.** Choose and operate the safe path for bounded cleanup of retained dpone runtime Pods.

**Audience.** Airflow/Kubernetes platform engineers, SREs, incident responders, and audit reviewers.

Strict `init_fetch` tasks without a separate `outcome_gate` use
`on_finish_action=delete_succeeded_pod`. Runtime tasks that pin a launch
envelope for a deferred `outcome_gate` force `keep_pod` until
`launch_pin_cleanup` deletes the exact namespace/name/UID after the gate
(or until this sweeper's creation-age floor if the gate never starts).
Provider cleanup is otherwise best-effort. dpone ships a separate executable,
namespace-scoped retention control for failed, interrupted and cleanup-error
Pods. Pod retention is diagnostic hygiene only: structured XCom, runtime
evidence and load-step audit remain the execution/data-success authority.

## Audience and prerequisites

This runbook is for Airflow/Kubernetes platform operators. Workload authors do
not run the sweeper. Before starting, prepare:

- `dpone[kubernetes]`, `kubectl`, `helm`, `jq`, and access to the reviewed
  kubeconfig context;
- namespace-scoped permission to inspect RBAC and create/update the rendered
  ServiceAccount, Role, RoleBinding, CronJob, Job, and optional alert objects;
- an immutable `dpone` maintenance image digest and infrastructure-owned log
  retention for Job output;
- a recorded operator identity for apply evidence and an approved rollback
  window.

Read the [architecture boundary](architecture.md#airflow-deployment-cache-integrity)
and [Kubernetes cache deployment runbook](airflow-cache-kubernetes-deployment.md)
before enabling apply mode. Every example below requires an explicit reviewed
context and namespace; no command is intended to rely on workstation defaults.
The stock CLI publisher is `process_ordered`, not crash-durable. Therefore the
rendered apply CronJob is a non-production certification profile until an
embedding supplies and certifies a `durable_acknowledged` publisher.

## What dpone ships and what infrastructure owns

dpone owns:

- `runtime-pod-retention-plan`, `-apply` and `-render` commands;
- exact provider-owned label and terminal-phase selectors;
- metadata-only inventory, fail-closed classification and conditional deletes;
- stable plan/apply/render JSON schemas;
- deterministic ServiceAccount/RBAC/CronJob and optional alert rules.

Infrastructure owns:

- namespace, immutable image digest, schedule and deployment;
- Kubernetes ServiceAccount identity and rollout approval;
- durable runtime log/evidence retention;
- two dry-run reviews, controlled execute certification and alert routing;
- suspension/rollback during incidents.

Until live dev evidence is collected for an exact image/manifests digest,
cluster readiness is `UNVERIFIED`; local tests are not live certification.

## Safety boundary

The adapter performs two server-side metadata-only queries in one namespace:

```text
labels:
  dpone.dev/managed-by=airflow-provider
  dpone.dev/runtime-contract=init-fetch-v2
  dpone.dev/workload-id exists

fields:
  status.phase=Succeeded
  status.phase=Failed
```

It never uses a name prefix, `status.phase!=Running`, all-namespace inventory,
full Pod bodies or `deletecollection`. `Pending`, `Running`, `Unknown`,
terminating, malformed, ambiguous and uncorrelated objects are preserved.
Every delete uses the observed UID and resourceVersion, so a recreated or
changed Pod receives a safe 409 rather than being deleted accidentally.

## Important TTL limitation

`PartialObjectMetadata` does not expose terminal-condition timestamps. v1 uses
creation time as a conservative, explicitly imperfect age basis and reports:

```yaml
age_basis: creation_timestamp_fallback
terminal_age_exact: false
```

This is **not** “24 hours after completion”. A long-running Pod that became
terminal recently can already exceed the creation-age threshold. Review two
dry-run cycles before apply. Exact terminal-age retention needs a separate
durable first-terminal-observed ledger or wider Pod status/patch permissions;
v1 deliberately does neither.

## Recommended policy

| Setting | Dev default | Production requirement |
| --- | ---: | --- |
| Minimum creation age | 24 hours | Explicit per environment |
| Sweep interval | 1 hour | No longer than half the age floor |
| Maximum deletes/cycle | 100 | Bounded; start lower if API pressure exists |
| Initial mode | `plan` | Two reviewed cycles before `apply` |
| Namespace | one Airflow runtime namespace | Never cluster-wide |
| Alerts | optional render | Required and routed before apply |


## Choose the task

| Task | Canonical guide | Outcome |
| --- | --- | --- |
| Understand ownership and safety | [Review the architecture](airflow-runtime-pod-retention-architecture.md) | Component boundaries and invariants |
| Review or execute one bounded cycle | [Run retention manually](airflow-runtime-pod-retention-manual.md) | Schema-valid plan or apply evidence |
| Embed the policy | [Use the Python API](airflow-runtime-pod-retention-python-api.md) | Explicit capability composition |
| Schedule the control | [Deploy on Kubernetes](airflow-runtime-pod-retention-kubernetes.md) | Reviewed plan-mode CronJob, then controlled apply |
| Upgrade it | [Upgrade the control](airflow-runtime-pod-retention-upgrade.md) | Occurrence-bound plan-first replacement and rollback |
| Diagnose it | [Operate and recover retention](airflow-runtime-pod-retention-operations.md) | Alert, recovery, and stable error procedure |
| Withdraw it | [Withdraw runtime Pod retention](airflow-runtime-pod-retention-withdrawal.md) | Forensic capture and exact-object removal proof |

Follow the guides in that order for a first non-production certification
rollout. Workload authors should not run the control; infrastructure owns
deployment, durable evidence retention, alerts, and rollback. Production apply
remains blocked until a certified crash-durable evidence publisher replaces the
stock `process_ordered` JSONL publisher.

## Run manually

Continue with the canonical [manual plan/apply procedure](airflow-runtime-pod-retention-manual.md#run-manually).

## Expected machine-readable evidence

Review the [plan/apply examples and evidence contract](airflow-runtime-pod-retention-manual.md#expected-machine-readable-evidence).

## Python API

Use the [Python API composition guide](airflow-runtime-pod-retention-python-api.md#python-api).

## Render the Kubernetes control

Follow the [Kubernetes render and activation guide](airflow-runtime-pod-retention-kubernetes.md#render-the-kubernetes-control).

## Classification reference

Use the [classification reference](airflow-runtime-pod-retention-operations.md#classification-reference) when a Pod is preserved or selected.

## Error recovery table

Use the [stable error recovery table](airflow-runtime-pod-retention-operations.md#error-recovery-table).

## Alerts and acceptance evidence

Use the [alert and acceptance procedure](airflow-runtime-pod-retention-operations.md#alerts-and-acceptance-evidence). Apply remains disabled in production until evidence durability is certified as described there.

## Rollback and incident response

Follow the [rollback and incident procedure](airflow-runtime-pod-retention-withdrawal.md#rollback-and-incident-response).

## Public evidence schemas

Inspect and validate the [public evidence schemas](airflow-runtime-pod-retention-operations.md#public-evidence-schemas).

For the next likely platform task, continue with [Kubernetes cache deployment](airflow-cache-kubernetes-deployment.md).
