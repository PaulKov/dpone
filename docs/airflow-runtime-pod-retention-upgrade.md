# Upgrade runtime Pod retention safely

**Purpose.** Replace the retention image, schedule, policy, or alert threshold without losing occurrence identity or creating an unreviewed destructive window.

**Audience.** Airflow platform engineers and SREs operating the namespace-scoped retention control.

[Back to runtime Pod retention overview](airflow-runtime-pod-retention.md) · **Next likely task:** [certify the upgraded Kubernetes control](airflow-runtime-pod-retention-kubernetes.md#activate-apply-after-two-plan-cycles).

## What an upgrade changes

The stable Kubernetes name does not identify one rollout. Evidence must bind:

- immutable maintenance image digest;
- rendered manifest SHA-256 and retention template SHA-256;
- CronJob UID and observed generation;
- namespace UID and reviewed kubeconfig context;
- rollout timestamp, policy bounds, schedule and alert freshness threshold.

Changing only a tag, mutable values file, or live object is not an upgrade
contract. Never edit the active apply CronJob in place and assume its earlier
certification still applies.

## Safe algorithm

1. Record the current evidence bundle and exact rollback manifest.
2. Suspend the current CronJob with UID/resourceVersion preconditions.
3. Wait until its owned Jobs report `active=0`; do not delete monitoring.
4. Render the replacement as `mode=plan` with an immutable image digest and an
   explicit alert threshold appropriate for its schedule.
5. Run server-side dry-run and diff with bounded Kubernetes request timeouts.
6. Apply the reviewed plan manifest and immediately record namespace UID,
   CronJob UID, generation, template digest, manifest digest and rollout time.
7. Observe two distinct successful plan Job occurrences. Each must be recent,
   inactive, terminal, owned by the recorded CronJob occurrence and identical
   to the reviewed Job execution bounds.
8. If production apply is intended, verify the injected evidence publisher is
   live-certified as `durable_acknowledged`. Render and review apply separately;
   the stock `process_ordered` profile is non-production.
9. Activate apply, retain the previous manifest and evidence, and monitor at
   least two cycles before expiring rollback artifacts.

No step mutates an Airflow task, DAG state, XCom, workload data, or scheduler
cache. A failed upgrade leaves the prior CronJob suspended and the previous
manifest available for explicit rollback.

## Rollback

1. Suspend the failed occurrence and prove its owned Jobs are inactive.
2. Reapply the exact previous immutable manifest; do not reconstruct it from
   memory or mutable `latest` references.
3. Record the new CronJob generation/UID occurrence and rollout timestamp.
4. Repeat two plan cycles before restoring apply.
5. Preserve failed rollout evidence through the incident retention period.

Rollback is a new activation occurrence even when the old manifest bytes are
reused. Historical Job evidence must not be presented as proof for the new
occurrence.

## Failure rules

- Diff, identity, freshness, ownership, terminal-state, or template mismatch:
  stop before apply activation.
- Active or late Job/Pod: keep monitoring and access resources, then replan.
- Missing durable ACK: non-production plan may continue; production apply stays
  disabled.
- Alert route not verified: do not activate apply.
- Cache or Airflow outage: unrelated; retention remains suspended until its own
  evidence is complete.
