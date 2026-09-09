# ADR 0041: Airflow cache retention is a reviewed crash-safe transaction

## Status

Accepted.

Occurrence identity, approval-attempt identity, and recovery acknowledgement
are refined by [ADR 0043](0043-airflow-retention-occurrence-review-identity.md).

## Context

The Airflow pack cache contains the active deployment, rollback generations,
exact activation evidence and parser acknowledgements. Retention must reclaim
bounded local storage without deleting active, unreviewed or unrecoverable
bytes. A process can stop after any filesystem operation, including an atomic
rename, recursive removal or durable journal transition.

The published `dpone.deployment-cache-retention-apply.v1` schema is already a
public compatibility contract. Transaction and activation-history evidence is
useful to strict controllers, but adding required fields to v1 would break
existing consumers.

## Decision

### Authority and ordering

One retention apply holds `reconcile_lock` and then `promotion_lock` for its
entire authority check, validation and mutation. Remote storage is never read
inside this critical section.

For a non-empty deletion plan the applier performs these steps in order:

1. recover or block any interrupted local retention transaction;
2. block a new reviewed operation while any other receipt remains `applying`;
   when replay discovers changed activation authority, close that receipt as
   `aborted` under both locks before requiring a fresh review;
3. rebuild the current plan and compare its reviewed SHA-256 digest;
4. validate desired-state checkpoint, physical `current`, current pointer,
   promotion audit and loader ACK as one exact activation occurrence;
5. validate every deletion candidate and capture its directory identity;
6. derive, without writing, the deterministic bounded-history revision for the
   acknowledged activation occurrence;
7. durably create the replayable operation receipt bound to that prospective
   revision;
8. persist exactly that history revision and refuse any conflicting revision;
9. delete candidates one at a time through the write-ahead transaction;
10. commit the replayable operation receipt;
11. return evidence containing the reviewed plan and activation-history
   revision when v2 was explicitly requested, plus receipt identity when v3
   was explicitly requested.

A candidate-validation failure occurs before activation history, receipt and
filesystem mutation and reports `state_may_have_changed=false`. A receipt-create
failure also precedes those mutations, but reports `state_may_have_changed=true`
when the durable write outcome cannot be proven; operators must inspect or
remove only the exact incomplete receipt occurrence before retrying.
The prospective history revision is deterministic from existing history and
the acknowledged occurrence, including its acknowledgement timestamp. A crash
after receipt creation and before history persistence leaves an `applying`
receipt; replay persists only its bound revision before any deletion. No
deployment, activation snapshot, or trash path is mutated until both receipt
and matching history revision are durable.

### Per-candidate write-ahead transaction

Before detaching a candidate, the coordinator creates or validates a sealed,
fingerprinted activation snapshot for that exact deployment. This applies even
to a generation that was never previously active.

The durable state machine is:

```text
prepared -> detached -> deletion_started -> deployment_deleted
         -> activation_deleted -> committed

prepared|detached|deletion_started|blocked -> restored
```

- `prepared` is committed before activation preparation or detach.
- `detached` follows atomic rename and fsync of both directory parents.
- `deletion_started` is committed after detached-byte revalidation and before
  recursive removal.
- `deployment_deleted` and `activation_deleted` follow the corresponding
  removal and parent-directory fsync.
- `committed` and `restored` are terminal outcomes.
- `blocked` is retryable recovery state and disables new destructive cycles.

Journal advancement first commits a copied next state and mutates in-memory
phase only after the durable write succeeds. A failed terminal write therefore
remains replayable from the last durable phase.

### Recovery

Recovery restores an intact detached inode when it can still be validated. If
deletion may have made it incomplete, that tree is never renamed back. The
coordinator instead validates the sealed activation, copies it through private
staging, seals and fingerprints the copy, atomically publishes it, fsyncs the
parent and validates the published projection. The original activation remains
intact until forward deletion commits.

Journaled and safe unjournaled trash are both processed. If neither trustworthy
detached bytes nor an activation snapshot is available, recovery stays blocked
and retention remains fail-closed. A successful restoration changes recovery
revision, invalidating every previously reviewed plan.

Replay reads activation history without upserting the current ACK. A matching
occurrence and the exact recorded history revision are required. If current
deployment or activation authority changed, the applier durably aborts the old
receipt: WAL-confirmed deletions become `deleted`, remaining `pending` items
become forensic skips, and the old operation can no longer block a fresh plan.

Terminal WAL entries remain available while their operation receipt is still
`applying`. Successful terminal WAL is pruned only after the matching receipt
is durably `committed`; committed-receipt replay idempotently closes a crash
between those writes. When recovery restores any deployment for that operation, the
receipt is durably closed as `aborted`: already committed deletions remain
recorded, pending items become forensic skips, and the old operation can no
longer replay. The changed recovery revision produces a different plan digest;
an operator must review that fresh plan before another destructive attempt.

A later reviewed operation cannot prune or overwrite replay evidence for an
earlier incomplete operation. Capacity pressure is reported fail-closed until
the receipt reaches `committed` through forward replay or `aborted` through
successful restoration. An `aborted` receipt and its terminal WAL are retained
as recovery evidence rather than silently reused or removed.

### Evidence compatibility

- `DeploymentRetentionApplyReport.to_dict()` and CLI default
  `--evidence-version v1` preserve the published v1 projection.
- Strict pre-receipt controllers may request `--evidence-version v2`; v2 adds
  `reviewed_plan_sha256` and `activation_history_revision`.
- Receipt-aware controllers request `--evidence-version v3`; v3 additionally
  requires `review_id`, `operation_id`, `receipt_revision`, and
  `transaction_status=committed` for destructive outcomes. A no-op v3 report
  remains valid without receipt fields because it authorized no deletion.
- The plan stays `dpone.deployment-cache-retention-plan.v1`.
- Existing v1 evidence is audit-compatible but does not replace current plan,
  ACK or activation-history authority.

New public fields are introduced through a new schema version, never by
silently changing v1 or v2.

## Consequences

- A reviewed plan cannot authorize bytes that changed before apply.
- A crash cannot make a partially deleted deployment look healthy.
- Every detached candidate has a verified recovery source before mutation.
- Recovery may preserve extra generations and require a fresh review; it never
  optimizes for deletion at the cost of recoverability.
- v1 and v2 consumers remain compatible. Receipt-aware v3 controllers can
  distinguish a replayed committed operation from a newly planned no-op.
- Cache capacity can remain temporarily over budget when evidence or recovery
  is unavailable. This is intentional fail-closed behavior.

## Rejected alternatives

- Delete directly after validation without a WAL: a process crash leaves no
  deterministic recovery phase.
- Snapshot only previously active generations: never-activated reviewed
  candidates would become unrecoverable after partial deletion.
- Commit activation history before validating all candidates: a corrupt
  candidate would mutate control state even though deletion never started.
- Mutate v1 output to include required transaction fields: this breaks a
  published machine-readable contract.
- Fetch remote desired state during retention: it introduces network I/O into
  the local critical section and does not linearize pod-local cache state.

## References

- [ADR 0032: Exact activation occurrence](0032-airflow-exact-activation-occurrence.md)
- [ADR 0040: Legacy cache authority](0040-airflow-legacy-pack-cache-authority.md)
- [Airflow cache sync and retention runbook](../airflow-cache-sync.md)
- [Kubernetes deployment guide](../airflow-cache-kubernetes-deployment.md)
