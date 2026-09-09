# Retain bounded Airflow cache generations

**Purpose.** Explain the deployment-cache retention contract, its durable state, and the safe operator journey without duplicating executable recovery commands.

**Audience.** Airflow platform engineers, SREs, and reviewers approving deletion of immutable cache generations.

[Back to cache sync and recovery overview](airflow-cache-sync.md) · **Next likely task:** [approve one exact retention plan](airflow-cache-retention-approval.md).

## Outcome

Retention removes only validated, inactive deployment generations after an
operator reviews one immutable plan. It never discovers deletion authority from
mutable `latest` state, remote listing, directory age alone, or a previous
successful operation.

```text
fresh plan + protected generations + current loader ACK
  -> reviewed plan_sha256 + approved review_id
  -> durable applying receipt
  -> operation-bound filesystem WAL
  -> candidate deletion
  -> committed receipt and v3 evidence
```

The cache mutation holds the same lock hierarchy as promotion and recovery.
The active deployment, explicit protection set, desired-state checkpoint,
loader ACK, activation history, plan digest, receipt, and WAL must agree before
dpone removes any candidate.

## Durable state

| State | Contract | Role |
| --- | --- | --- |
| Plan | `dpone.deployment-cache-retention-plan.v1` | Closed reviewed candidate and protection set |
| Receipt | `dpone.deployment-cache-retention-apply-receipt.v2` | Per-approved-attempt progress and terminal outcome |
| Recovery WAL | `dpone.deployment-cache-retention-recovery.v2` | Per-occurrence filesystem transaction bound to `operation_id` |
| Apply evidence | `dpone.deployment-cache-retention-apply.v3` | Public committed operation projection |

Recovery WAL v2 keys every transaction by immutable `transaction_id` and binds
it to the exact operation digest. A v1
journal is parsed for compatibility and migrated as **unbound** evidence; dpone
never guesses which receipt owns it. If an incomplete receipt depends on such a
journal, the receipt is durably aborted and a fresh plan and approval are
required. This prevents a rematerialized deployment from being reported as
deleted because an unrelated historical WAL entry happened to use the same
deployment id.

## Replay and drift

- A committed receipt is idempotently returned only for its exact operation.
- A committed WAL occurrence advances only the receipt with the same
  `operation_id` while the original path remains absent.
- A rematerialized original path aborts the old receipt and requires a new plan
  plus a new UUIDv4 review id.
- Protection, desired-state, ACK, current deployment, or history drift closes
  an incomplete receipt as `aborted`; it does not leave a permanent
  `applying` lock.
- WAL-confirmed items remain forensic `deleted` outcomes when a receipt aborts;
  pending items become explicit skips.
- Corrupt, oversized, unsafe, or ambiguous state remains fail-closed and is not
  normalized as ordinary drift.

## Operator journey

1. Produce and schema-validate a fresh plan.
2. Review every protected and delete candidate, then persist `plan_sha256` and
   one UUIDv4 `review_id` for that attempt.
3. Follow the [one-plan approval procedure](airflow-cache-retention-approval.md).
4. Execute the generated v3 apply command from the
   [recovery runbook](airflow-cache-sync-recovery.md#recover-partial-writes).
5. Retain plan, approval change, receipt-backed apply evidence, and withdrawal
   proof together.
6. Build a new plan after any drift or aborted receipt. Never edit local control
   files to force replay.

## Compatibility

Plan v1 and apply v1/v2 remain unchanged. Recovery WAL v1 remains readable but
cannot authorize replay. New writes use occurrence-keyed WAL v2 and cumulative
ACK v2. Existing caches without receipt or recovery files remain valid and
create control state only when destructive retention begins. Downgrade across
WAL v2 requires stopping mutation and restoring a complete older-runtime state
snapshot; editing control files is unsupported.

For stable error handling, continue with
[cache verification and recovery](airflow-cache-sync-recovery.md#recovery-by-error-family).
