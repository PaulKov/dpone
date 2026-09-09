# ADR 0043: Airflow retention binds recovery and approval to occurrences

## Status

Accepted. This ADR refines the identity, recovery, and replay parts of
[ADR 0041](0041-airflow-cache-retention-transaction.md).

## Context

A deployment digest identifies immutable content, but it does not identify one
filesystem occurrence of that content. The same deployment can be deleted,
restored, or materialized again at the original path. A WAL keyed only by the
deployment digest can therefore make a replacement directory look like the
directory deleted by an earlier operation.

The reviewed plan digest has the same limitation for operator intent. After an
attempt is durably aborted, replaying the same plan digest must not silently
reuse the old receipt. A fresh destructive attempt needs fresh approval while
ordinary retries of the same attempt must remain idempotent.

## Decision

### Transaction occurrence

Recovery schema `dpone.deployment-cache-retention-recovery.v2` keys every WAL
entry by `transaction_id`. The digest binds deployment and operation identity,
original and detached paths, device, inode, activation path, and environment.
`deployment_id` remains content identity; it is not the WAL key.

A committed occurrence authorizes a deleted receipt item only while the exact
original path is absent and three paths are byte-for-byte equal: the reviewed
receipt item path, the WAL `original_path`, and the canonical deployment path
under the currently configured cache root. A relocated cache root, a duplicate
terminal WAL occurrence, or any path mismatch invalidates destructive replay.
An applying or committed receipt is atomically rewritten to `aborted` before
the drift error is returned. It remains historical evidence but cannot be
projected as success for the new cache occurrence. If a directory is present
at the canonical path, dpone treats it as a new occurrence, preserves the WAL
for forensics, and requires a fresh plan and review. If that abort cannot be
persisted, dpone reports the durable-control failure and leaves WAL intact; it
never reports a false aborted state.

### Review attempt

Every non-empty destructive apply requires `review_id`, a canonical UUIDv4
created by the approval workflow and stored beside the approved plan digest.
The operation identity is:

```text
sha256(environment, reviewed_plan_sha256, review_id)
```

Retries reuse the same `review_id` and receipt. After `aborted`, the operator
must create a new UUIDv4. Reusing the old UUID returns the old terminal result;
it never starts another deletion cycle.

Public v3 apply evidence includes `review_id`, `operation_id`, the closed
receipt revision, and `transaction_status=committed`. Historical v1 and v2
projections remain unchanged.

### Recovery acknowledgement

Recovery acknowledgement v2 is cumulative and keyed by exact
`transaction_id` values. Unrelated WAL writes do not invalidate an already
acknowledged restored occurrence. A matching legacy v1 acknowledgement is
upgraded durably before the next controlled journal mutation.

### Forensic retention

Unbound migrated v1 WAL and WAL belonging to aborted receipts are never
automatically pruned. Successful operation-bound WAL is pruned only after the
matching receipt is durably committed. Capacity exhaustion fails closed and
requires an explicit reviewed archival/remediation procedure.

## Migration and downgrade

- Recovery v1 remains readable. It is normalized in memory to occurrence-bound
  v2 with `operation_id: null` and is persisted as v2 only on the next
  controlled mutation.
- Recovery ACK v1 remains readable only when its journal revision and restored
  deployment set match exactly. It is then upgraded before journal mutation.
- Apply receipt v1 remains readable as historical audit state. An applying v1
  receipt may reconcile exact, absent, operation-bound terminal WAL outcomes,
  then is durably aborted; it never authorizes another deletion. Remaining
  work requires a fresh v2 receipt and UUIDv4 review. A committed v1 receipt is
  not valid v3 evidence and cannot be replayed as a current success.
- Older runtimes that do not understand recovery v2 must not be started over a
  cache containing v2 state. Before a binary downgrade, stop cache mutation,
  archive receipts, WAL, ACK, activation history, and current-pointer evidence,
  and follow `docs/airflow-cache-retention-state-downgrade.md` for the certified
  `emptyDir` recovery path. Persistent-volume downgrade remains `UNVERIFIED`
  until a separate snapshot/PVC occurrence and evidence contract is accepted.
- Deleting or hand-editing v2 state to make an old runtime start is unsupported.

## Consequences

- Rematerialized content cannot inherit deletion evidence from an earlier
  inode/path occurrence.
- One human approval authorizes one destructive attempt, while crash retries
  remain idempotent.
- Recovery acknowledgement is monotonic across unrelated journal changes.
- Forensic evidence can consume bounded local capacity; retention fails closed
  rather than removing ambiguous evidence automatically.

## Rejected alternatives

- Key WAL by deployment digest: confuses immutable content with a filesystem
  occurrence.
- Key approval only by plan digest: an aborted attempt permanently reserves the
  plan or permits unreviewed reuse.
- Prune aborted or unbound WAL automatically: destroys incident evidence.
- Infer operation ownership for v1 WAL: cannot be proven from deployment
  identity alone.

## References

- [ADR 0041: reviewed crash-safe retention](0041-airflow-cache-retention-transaction.md)
- [Cache retention runbook](../airflow-cache-sync-recovery.md)
- [Approval and withdrawal](../airflow-cache-retention-approval.md)
