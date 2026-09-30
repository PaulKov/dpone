# ClickHouse publication: partition replacement and table exchange

Last verified: 2026-09-30

## Availability and the first decision

The method-aware publication kernel is an **unbound Python capability**, not a
production-enabled ODBC option. Ordinary ClickHouse full-refresh publication
continues to use the existing UUID-v1 EXCHANGE/RENAME protocol. No YAML setting in
this change activates parallel MSSQL ODBC or selects REPLACE PARTITION for it.

Start with the [approved kernel design](feature-design-clickhouse-publication-method.md).
The broader [ODBC range v2 design](feature-design-mssql-columnar-range-parallelism-v2.md)
still requires pre-read memory admission, worker isolation, durable route
composition and exact-commit MSSQL/object-storage/ClickHouse certification.
Do not interpret passing unit tests as a route certificate.

For targets written exclusively by dpone, the next binding is described
in the [dpone-only authority design](feature-design-clickhouse-dpone-only-authority.md).
Its single-host durable journal and conservative closure profile is APPROVED
for implementation, not an implemented backend or an enabled route.
The [authority journal foundation](clickhouse-authority-journal.md) now supplies
storage/CAS building blocks; transport, protected observation and production
composition remain separate, unimplemented gates.

## Selection policy

All choices require complete catalog visibility, local Atomic/plain MergeTree,
sealed staging, verified typed content and protected exclusion of every writer.
Unsupported topology or incomplete evidence blocks publication, not fallback.

| Observed complete snapshot | Method | Reason |
|---|---|---|
| Target does not exist | RENAME | Publish a new table without inventing a partition |
| Equal design, equal typed content/count and partition inventory | No-op | No data mutation is needed |
| Empty candidate replacing nonempty target | EXCHANGE | Empty-partition semantics vary by server version |
| Design/schema differs | EXCHANGE | REPLACE requires compatible physical design |
| Exactly one candidate partition; target has no other partition | REPLACE PARTITION | Preserve target table identity |
| Multiple candidate partitions or any other target partition | EXCHANGE | Replace the entire snapshot atomically |

For an unpartitioned table, ClickHouse's partition expression `tuple()` denotes
its sole partition. It does **not** mean all partitions of a partitioned table.
The kernel records the server's canonical partition ID; a certified executor
may use `REPLACE PARTITION ID 'all'` for the observed unpartitioned ID or render
`tuple()` only after proving the actual partition design. Do not derive the
expression from user input, MSSQL range boundaries or observed row count.

For a partitioned table containing only `202609`, replacement is eligible only
when staging contains precisely that same complete snapshot partition (or the
target is empty). If the target also contains `202608`, replacing `202609` would
leave stale data and is therefore rejected in favor of EXCHANGE. A loop over
partitions is useful for partition-overwrite semantics but is not atomic
whole-table full refresh.

## What atomicity does and does not guarantee

EXCHANGE atomically swaps table names. Its repeated execution can undo the first
swap, so a lost response must trigger reconciliation, not SQL retry. REPLACE
atomically replaces one partition and leaves the staging partition intact.
Repeating it can converge to the same logical data only while staging is sealed
and other writers are excluded. Neither command alone provides a transaction
covering source snapshots, objects, checkpoints or replicated cluster members.

The current kernel deliberately rejects Shared, ReplicatedMergeTree,
Distributed and ON CLUSTER publication. This does not mean those ClickHouse
features are unreliable; their topology and acknowledgement contracts require a
different certified binding. One node's acknowledgement does not prove global
cluster visibility. See the official
[EXCHANGE reference](https://clickhouse.com/docs/reference/statements/exchange),
[partition reference](https://clickhouse.com/docs/reference/statements/alter/partition)
and [distributed DDL reference](https://clickhouse.com/docs/reference/statements/distributed-ddl).

## Developer composition contract

The entry point is
`dpone.runtime.sinks.clickhouse_guarded_publication.GuardedClickHousePublication`.
It requires an implementation of
`dpone.ports.clickhouse_publication.GuardedPublicationBackend`.

```python
from dpone.runtime.sinks.clickhouse_guarded_publication import GuardedClickHousePublication

# supplied_backend must be a deployment-certified protected implementation.
# There is intentionally no insecure default or in-memory production example.
publisher = GuardedClickHousePublication(supplied_backend)
record = publisher.publish(stable_operation_id)
```

This is composition pseudocode: `supplied_backend` and `stable_operation_id` are
platform dependencies, not environment variables auto-discovered by dpone.
Use `publisher.recover(stable_operation_id)` for restart recovery. It never
extracts source rows, creates a new intent or dispatches DDL. `publish` also
routes every existing intent to reconciliation. Terminal records describe the
historical publication outcome, not current target freshness or current quality.

The backend owns six enforceable responsibilities:

1. **Identity and exclusion:** canonical endpoint/database/table ownership across
   aliases and operations; prohibit all competing writers and stale epochs.
2. **Evidence:** complete fresh ordered schema/design, immutable staging UUID,
   canonical active partition inventory and a pinned typed logical multiset.
   Include duplicates, NULLs and type distinctions. The hash proof is
   probabilistic; row count alone or mutable `system.parts` checksums is invalid.
3. **Durable journal:** original full intent bytes, owner/epoch, unique operation
   and query ID, CAS state transitions, immutable claim history and audit trail.
   Workers cannot rewrite protected records. Corrupt/unknown versions block.
4. **Dispatch:** acknowledge a unique CAS winner, revalidate authority at the
   actual dispatch boundary, render one exact quoted statement and disable
   transport retries/failover. No-op never dispatches.
5. **Closure:** permanently close and drain the original publisher, including
   queued HTTP/DDL requests absent from `system.processes`; do not issue a new
   publisher for this intent. An empty process-list sample is insufficient.
6. **Resolution:** independently verify retained closure/content evidence and
   persist outcome before returning. Leaving the execution context never
   releases unresolved target ownership.

The backend aggregates these narrowly scoped services at a deployment composition
root; it must not become a connector with hidden global clients. A typed DTO or
`catalog_complete=True` supplied by application code is not proof of authority.
The kernel checks invariants but cannot authenticate a malicious backend.

No concrete backend is bundled in this increment. `SQLiteWindowStore`, a local
file marker, a scheduler mutex and `before_target_mutation()` are not substitutes
for fencing external SQL writers. The existing router cannot yet select this
kernel. A production integration must additionally adapt receipt identity and
cleanup ownership; v1 assumes staging holds the swapped predecessor, which is
false after REPLACE.

## State and evidence reference

Records use `dpone.clickhouse.guarded-publication.v2`. The intent retains
operation ID, physical subject, original observation, method, reason and selected
partition ID. Query identity is deterministic from the stable operation ID;
the backend must namespace operations and enforce uniqueness within its service.
The immutable `claim_granted` history survives an `unknown` resolution. It must
never be reconstructed from the current state label or a target hash match.

| State | Meaning | Permitted action |
|---|---|---|
| prepared | Intent durable, no acknowledged claim | Close publisher; reconcile without dispatch |
| claimed | One invocation granted one dispatch, ACK may be lost | Close/drain; inspect method-specific outcome |
| committed | Frozen desired outcome proven and durable | Finalize evidence/checkpoint; owned cleanup only |
| not_published | Original outcome proven after closure | Explicit new attempt after controlled cleanup |
| unknown | Neither result proven | Retain exclusion and all resources; reconcile only |

For EXCHANGE, proof includes swapped UUIDs and matching frozen logical/design
evidence for both tables. For RENAME, the desired UUID/content is at target and
staging is absent. For REPLACE, both table UUIDs remain unchanged; target content
must match the desired generation and staging must still match its sealed
original. For no-op, the original matching observation remains unchanged.
The same checks reject a third UUID, changed staging, partial visibility or
unexpected partition content. Background merges do not change logical evidence.

An unclaimed intent can never resolve committed, even after an unknown state.
The kernel never dispatches a second command. A new attempt is explicit and must
respect the retained target authority and source snapshot ordering.

## Operator runbook

### Lost reply or process crash

1. Preserve staging, object inventory, journal and authority. Do not manually
   exchange tables or repeat ALTER. Disable automatic DDL transport retries.
2. Reopen the original operation under the same protected target ownership.
   Source access is unnecessary for publication reconciliation.
3. Run the platform's binding of `recover(operation_id)`; verify publisher
   closure before accepting catalog/content proof.
4. For committed, finish receipt/evidence/checkpoint in their established order.
   For not-published, create a new explicit attempt only after safe cleanup.
5. For `PublicationUnknown`, retain the fence and escalate unavailable closure,
   mismatched UUID/design/content or journal failure. It advertises
   `safe_to_retry=False` and `operator_verification_required=True`.

### Rollback and retention

REPLACE retains the candidate, not a backup of the target's previous partition.
If rollback is required, the platform must retain an independently validated
predecessor snapshot before publication. EXCHANGE leaves the predecessor under
the staging name, but blindly exchanging it back can overwrite later writes.
Both rollback paths require a new guarded, journaled operation.

The kernel does not delete tables, advance checkpoints or release ownership.
Production cleanup must validate exact UUID ownership and committed evidence;
after REPLACE it drops the owned candidate, never assumes that candidate contains
the old target. Retain receipts independently of data cleanup for audit/replay.

## Verification and rollout checklist

- Unit selection/fault tests: `tests/test_clickhouse_guarded_publication.py`.
- Compatibility: unchanged `test_clickhouse_full_refresh_publication*.py`.
- Concrete protected backend, restart/CAS/fencing tests: required, not bundled.
- Live `tuple()`, single partition, stale target partition, empty snapshot,
  concurrent writers, queued requests and lost ACK: UNVERIFIED in this increment.
- Exact source/controller commit and MSSQL/object-storage/ClickHouse route proof:
  required before ODBC activation or a production-readiness claim.

Upgrade preserves old v1 records and default routing. Never reinterpret a v1
UUID receipt as v2 partition evidence. See
[ADR 0076](adr/0076-method-aware-clickhouse-publication.md) for the architectural
decision and limits.
