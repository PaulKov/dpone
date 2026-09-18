# ClickHouse cluster publication recovery runbook

This runbook is for operators and on-call engineers diagnosing an interrupted
bounded ClickHouse cluster `full_refresh`. It covers both modes, with additional
steps for external per-member staging. It does not authorize live publication
or destructive repair in an unapproved environment.

## Safety rules

Until the exact operation is classified:

- keep the authority row, sealed artifact, candidates, predecessors, and queue
  evidence;
- prevent a different scheduler invocation from targeting the same table;
- use read-only inventory, catalog, authority, and queue observations;
- redact endpoints, credentials, local paths, raw SQL values, and row data from
  incident artifacts; and
- treat unavailable, expired, malformed, or contradictory evidence as unknown.

Never:

- switch to local publication or another replication mode as a workaround;
- repeat an insert into an ambiguous external candidate;
- repeat an exchange or rename after publication dispatch;
- delete or rewrite the Keeper authority row;
- drop a candidate or predecessor based on its name, age, prefix, or row count;
- use partial inventory or `skip_unavailable_shards=1` as proof; or
- advance source state without a durable committed receipt.

## 1. Identify the exact operation

Preserve the scheduler invocation identity and collect the redacted run result.
Confirm the manifest still selects the same cluster, database, target, strategy,
source-byte bound, and replication mode.

Run static inspection without changing the manifest:

```bash
dpone check \
  examples/batch/clickhouse-external-replication-full-refresh.batch.yaml \
  --format json

dpone plan \
  examples/batch/clickhouse-external-replication-full-refresh.batch.yaml \
  --format json
```

For external mode the decision must still show `cluster_external`,
`replication_mode: external`, runtime admission required, and no fallback. A
different plan or inventory digest is drift, not a new recovery authority.

## 2. Collect read-only evidence

Through the owning runtime diagnostics, capture redacted evidence for:

- authority schema version, Keeper version, operation digest, phase, fence,
  dispatch epoch, inventory digest, and generation digest;
- the complete ordered member set using opaque member IDs;
- each target and candidate engine, UUID, schema digest, row count, and canonical
  content digest;
- each member's stage/publication/cleanup state;
- the bound distributed-DDL entry, token digest, normalized query digest, and
  exact host-status coverage; and
- the sealed artifact digest, byte size, row count, schema digest, availability,
  and immutability status.

Do not paste direct catalog output containing endpoints into tickets or durable
evidence. The public diagnostic representation uses opaque member IDs.

## 3. Classify the phase

| Observation | Classification | Safe next action |
| --- | --- | --- |
| Authority is `LOCKED`, no candidate exists, artifact unsupported | pre-mutation blocked | allow the owning runtime to clean exact owned resources and record `ABORTED` |
| Authority is `STAGING`, candidate absent on one member | incomplete staging | retry the same operation so dpone creates and loads that member candidate |
| Candidate exactly matches the bound desired digest | recovered member | retry the same operation; dpone marks the member `READY` without inserting |
| Owned candidate is incomplete and publication never started | ambiguous unpublished stage | same-operation recovery may drop, prove absence, recreate, and replay the sealed artifact |
| Candidate UUID is foreign or content diverges | staging divergence | stop; retain objects and escalate |
| Every member is `READY`, authority is `STAGED` | ready to publish | retry the same operation; only a new acknowledged fence CAS may dispatch |
| Publication queue entry is active and members are mixed | publication in progress | wait or retry observation of the original entry only |
| Queue entry is terminal but member generations are mixed | terminal partial | stop; retain all generations and escalate |
| Every target is desired and the receipt is durable | committed | resume exact predecessor cleanup if required |
| Cleanup entry or object identity is unknown | cleanup unknown | retain predecessors; restore evidence and retry observation |
| Every target is desired and every predecessor is proven absent | completed | verify the authority reaches `COMPLETED` |

Row-count equality is diagnostic only. External staging requires the canonical
typed-content digest and exact owned UUID.

## 4. Retry safely

Submit the same scheduler operation through the normal runtime. Preserve its
original run identity; do not manufacture a new identity by renaming the
candidate, changing the manifest, or incrementing a user-controlled token.

The runtime resumes according to durable authority:

- `STAGING`: observes every member, then accepts an exact desired candidate or
  rebuilds only an exact owned unpublished partial candidate;
- `STAGED`: revalidates the barrier and competes for one fenced dispatch permit;
- `PUBLICATION_DISPATCHING`: observes the bound queue entry and member
  generations without redispatch;
- `COMMITTED`: revalidates predecessor identities and resumes fenced cleanup;
- `CLEANUP_DISPATCHING`: observes the bound cleanup entry and object absence;
  and
- `COMPLETED`: returns the already-proven operation result idempotently.

If a lost Keeper mutation response produced an unknown outcome, the caller has
no dispatch permit even if a later read shows the requested value. Recovery
must begin with a later read-only observation and an allowed fresh CAS from the
newly observed version.

## 5. Verify recovery

Recovery is successful only when:

1. the inventory exactly matches the authority-bound required member set;
2. every target exposes the desired per-member physical UUID and the common
   logical generation;
3. the bound publication entry and member results are complete and consistent;
4. a durable external receipt exists for `COMMITTED` or `COMPLETED`;
5. source state refers to that exact receipt; and
6. any cleanup reports predecessor absence for every required member.

Preserve the redacted final receipt and run result. A successful local synthetic
test may be recorded as `local_synthetic`; it does not change live certification
from `UNVERIFIED`.

## Failure-specific actions

### Mode or inventory mismatch

Do not edit the mode until the physical topology is independently confirmed.
Repair cluster configuration or the manifest through normal change control,
then start a new operation only after any existing authority reaches a safe
terminal phase.

### Unsupported artifact

External staging requires one immutable replayable artifact. Do not convert a
one-shot stream during recovery or reuse a mutable file. End the pre-publication
operation through exact owned cleanup and `ABORTED`, then create a new bounded
operation with a supported artifact producer.

### Staging divergence

Stop all publication attempts. Preserve the sealed artifact, authority, and all
candidates. Compare only redacted schema/content digests and UUID bindings.
Automatic replacement is allowed only for an exact owned unpublished partial
candidate; a foreign UUID or different complete digest requires escalation.

### Terminal partial publication

Do not issue another cluster exchange: it can revert members that already
committed. Retain target, candidate, authority, and queue evidence. Direct
per-member repair is outside this protocol unless a separately approved repair
capability supplies exact fencing and evidence.

### Cleanup unknown

The published target may already be correct. Do not trade availability for a
clean namespace by dropping uncertain predecessors. Restore connectivity and
queue evidence, then let same-operation recovery prove exact predecessor
identity and absence.

## Escalate when

Escalate without mutation when any of these is true:

- the complete inventory cannot be observed;
- the authority is malformed, missing, or has an unknown schema version;
- operation, fence, inventory, artifact, or generation identity changed;
- a candidate or predecessor UUID is foreign;
- content or schema digests diverge;
- the publication entry is missing, duplicated, expired, or contradictory;
- publication is terminal with mixed member generations; or
- cleanup would require guessing ownership.

Include only redacted digests, opaque member IDs, phases, stable error codes,
software version, and certification scope in the escalation package.

Return to the [first-success guide](clickhouse-cluster-publication.md) or consult
the [exact reference](clickhouse-cluster-publication-reference.md).
