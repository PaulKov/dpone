# ADR decision handoff: isolated SQL Server partition SWITCH

Status: decision note for DDA-06, not a numbered/accepted repository ADR.
Source authority: approved feature specification at planning dependency
`f3682940f8864563cde0e6b6ecee60f746b49020`, section 6.

## Context and decision

The legacy native SWITCH mixin derives its partition list from prepared values
and counts partitions as replaced rows. It cannot implement an empty authored
window correctly and is not reused. Build a feature-local pure planner, strict
catalog projection/parser and transaction-bound executor. Keep public SWITCH
rejection before I/O and do not register or compose the component into runtime.

Contracts and ports depend only on standard library/contracts. The runtime
catalog adapter is the sole deployment snapshot producer. The executor invokes
that producer under deterministic whole-table locks, compares the frozen plan,
counts exact replaced rows and executes old-out/new-in. It owns no transaction,
receipt, fallback, retry, provisioning or cleanup policy.

The explicit initial SQL Server 2022 profile accepts one finite temporal RANGE
RIGHT partition with identical same-database partitioned rowstore layouts.
Unknown metadata and unsupported dependencies fail closed. Invocation markers
supplement durable caller ownership/fencing; they do not create authority.

## Consequences and invariants

- Empty input retains authored replacement authority.
- Index/object names and allocation IDs differ between compatible objects;
  physical shape and object drift use separate fingerprints.
- Held TABLOCKX/HOLDLOCK protection serializes whole tables; concurrency and
  count/catalog overhead must be measured before any performance claim.
- The caller supplies verify_prepared(plan), invoked after executor locks, and must retain full prepared integrity protection through mutation.
  Catalog fingerprints and counts cannot substitute for typed content digests.
- Old-out/new-in failures require whole caller transaction rollback. No fallback
  or retry is legal after mutation.
- Successful SWITCH empties prepared content. Exact receipt-first recovery is
  mandatory, including the initially empty target case. Unknown outcomes retain
  resources and prohibit replay.
- Target receipt insertion/commit, loaded_at authority, recovery and checkpoint
  ordering stay with the existing finalizer/runtime.
- Snapshot JSON is internal catalog transport, not a public evidence schema.

Rejected alternatives: call the old mixin; infer scope from distinct prepared
keys; trust catalog row estimates; add a second transaction manager; treat table
names or SQL ownership as invocation ownership; activate native SWITCH through a
policy/schema bypass.

## Evidence and future activation

The component's synthetic tests prove planner/SQL projection contracts,
transaction control flow and existing finalizer/resume behavior. Live SQL Server
syntax, physical transfer, locking, rollback and performance remain UNVERIFIED
without an explicitly approved disposable environment. DDA-05 owns those fixtures.

DDA-06 assigns the next unused ADR number and updates architecture/navigation.
Public activation requires separately approved aligned-stage provisioning,
durable resource ownership and retention, a same-session authority bridge,
prepared verification protection, public admission changes and exact-environment
live evidence. This change supplies none of those activation permissions.
