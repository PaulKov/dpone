# Isolated SQL Server partition SWITCH component

Purpose: integrate and test the internal, unregistered partition replacement
component. Audience: platform engineers, connector maintainers and certification
fixture authors. Start with the [native transport guide](../mssql-native-transport.md)
and the [approved delivery specification](../feature-design-data-delivery-acceleration-v1.md).

The component is available for synthetic review and approved disposable fixtures.
The public native SWITCH blocker remains
`mssql.strategy.partition_replace.native_switch`, before I/O. There is no new
CLI flag, manifest field, registry entry or automatic fallback. This component
has no live correctness or performance certification yet.

## Supported catalog profile

The v1 adapter accepts SQL Server **2022, major 16**, EngineEdition 2 or 3,
with database-level `VIEW DEFINITION`, server `VIEW ANY DEFINITION`, and
`SELECT ON OBJECT::sys.sql_expression_dependencies` in the target database.
The catalog query needs this explicit SELECT permission to inspect dependencies;
see [Microsoft's dependency-view permissions](https://learn.microsoft.com/en-us/sql/relational-databases/tables/view-the-dependencies-of-a-table?view=sql-server-ver17#permissions).
The fixture owner provisions these permissions; the component does not grant them
or require `db_owner` membership. Other editions/majors and hidden or incomplete
metadata fail closed. This explicit implementation profile is narrower than the
full set of SWITCH operations supported by SQL Server.

All three existing tables must be in the same database and use the same partition
function and scheme. The authored interval must equal two adjacent finite
boundaries of one `RANGE RIGHT` partition. The partition key is non-nullable
`date` or `datetime2` with scale 0–6. A datetime2 interval uses aware UTC Python
datetimes; a date interval uses Python dates. The adapter preserves the SQL
boundary precision; execution binds proven UTC datetime2 values without timezone
conversion. “Temporal key” means a date/time key, not a system-versioned table.

For boundaries `2026-01-01`, `2026-02-01`, the interval
`[2026-01-01, 2026-02-01)` selects partition 2. An empty prepared table still
replaces that partition with zero rows. Partial intervals, multiple partitions,
unbounded first/last partitions and `RANGE LEFT` are ineligible.

The physical profile accepts aligned heaps and rowstore indexes, with identical
ordered column/type/length/precision/scale/nullability/collation metadata, index
keys/includes/directions/options, partitioning and per-index storage placement.
Corresponding partition compression is NONE, ROW or PAGE. Object/index names and
allocation IDs may differ; full object fingerprints retain them for drift checks.
All partition layouts must match, including partitions outside the selected one.

The adapter excludes MAX/LOB storage, user/assembly types, computed, identity,
encrypted, masked, sparse, hidden and generated columns; columnstore, XML/spatial,
filtered, disabled or hypothetical indexes; constraints and table child objects;
foreign-key and schema-bound dependencies, security predicates and change
tracking, fulltext indexes and bound rules/defaults. Enabled database or server
DDL triggers are excluded because they can mutate data during ALTER TABLE. Temporal/history, CDC/replication, graph, ledger, external, memory
optimized, FILESTREAM and remote archive tables are also excluded. This is
conservative admission, not a claim that SQL Server cannot switch every excluded
layout. Unknown flags and missing partition or storage records are not defaults.

## Prepare owned objects and transaction authority

The fixture owner provisions the target plus two invocation-owned aligned tables:
prepared input and initially empty switch-out. No provisioning or DDL activation
is provided by this component. The complete prepared table must contain only
rows in the authored interval; NULL, below-bound and upper-bound rows reject the
plan. The complete switch-out table must be empty, not only its selected partition.

Bind the database ID, name and catalog generation; exact schema/table names,
object IDs and creation timestamps; invocation ID, positive owner generation and
target mutation ID in `NativeSwitchBinding`. Provisioning writes
`binding.tag("prepared")` and `binding.tag("switch_out")` as the corresponding
table extended property `dpone.native_switch.owner.v1`. An arbitrary existing
caller table is not a disposable resource. A naming convention, SQL principal or
copied ownership marker cannot replace the caller's durable resource ownership
record and current fence validation.

`NativeSwitchSql.query(sql, parameters)` returns named rows on the same session,
using driver-bound `?` parameters. A transaction implementation also supplies:

- `assert_authority(binding)`: raise unless the current invocation/generation,
  exact mutation and target fence are valid. Resolve the exact target receipt
  first; reject already committed or unresolved operations.
- `verify_prepared(plan)`: run the existing complete typed business/framework
  integrity verification against durable prepared authority after executor locks
  are held. Raise on mismatch or unavailable evidence; never mutate or settle
  the transaction in this hook.
- `execute(sql)`: execute once on that same session; propagate errors.

The caller opens one committable transaction, enables `XACT_ABORT` and
`SERIALIZABLE`, and retains its existing target/operation fence through completion.
The approved environment must freeze privileged database/server DDL-trigger
configuration through completion; table locks do not guard global administrative
changes. Nested transactions are excluded. SQL connections must not reconnect, retry or
commit implicitly. The executor acquires `TABLOCKX, HOLDLOCK` on all three tables
in object-ID order, then rereads catalog and content. These locks intentionally
serialize whole tables, including unrelated partitions, and protect revalidation
against independent SQL writers and DDL until the caller settles the transaction.

**Prepared content integrity remains the caller's responsibility.** Supply the
mandatory `verify_prepared(plan)` hook delegating to the existing typed/full
verification service. The executor invokes it after locking all three tables.
Bind the proof to this invocation, generation, mutation and interval, accounting
for the caller's authoritative transaction-clock metadata projection. Counts and catalog fingerprints
cannot detect an in-window value change that preserves row count. A snapshot or
ownership string is neither a prepared-content receipt nor transaction authority.

## Call the feature-local interfaces

These signatures are frozen handoff points:

```python
snapshot = catalog.snapshot(binding, interval=interval)
eligibility = plan_native_switch(snapshot, interval=interval, owner_binding=binding)
if eligibility.plan is None:
    raise NativeSwitchRejected(*eligibility.reasons)
result = execute_native_switch(eligibility.plan, transaction=transaction)
```

`catalog` is `NativeSwitchCatalog` constructed with the injected SQL session;
`binding`, `interval` and `transaction` come from the approved fixture/caller
lifecycle described above. This fragment describes integration, not a standalone
production invocation. The complete executable synthetic example is
`test_two_switches_preserve_outside_rows_and_count_replaced_rows` in
`tests/test_mssql_native_partition_switch_recovery.py`.

The executor revalidates the snapshot and plan after locking, counts matching
**target rows** with `COUNT_BIG`, performs target-to-switch-out followed by
prepared-to-target, and returns `NativeSwitchResult(replaced_rows, inserted_rows)`.
The result is uncommitted. The executor emits no target receipt, file, stdout,
stderr, state update or cleanup request. The caller inserts its existing exact
receipt in the same transaction and commits.

```mermaid
sequenceDiagram
    participant C as Existing caller/finalizer
    participant E as Isolated executor
    participant S as Same SQL transaction
    C->>C: Resolve receipt, fence, transaction-clock projection
    C->>E: execute_native_switch(frozen plan, transaction)
    E->>S: Assert active transaction; lock three tables
    E->>S: Reread catalog, ownership and exact content counts
    E->>E: Replan and reject drift
    E->>C: Verify complete prepared integrity under held locks
    E->>S: Count replaced rows; SWITCH old out
    E->>S: SWITCH prepared in
    E-->>C: Uncommitted row counts
    C->>S: Insert exact receipt; commit
```

## Diagnose stable reasons

Pure planning returns sorted `reasons` and no plan on rejection. The catalog and
executor raise `NativeSwitchRejected` with the same `.reasons` tuple. Driver/SQL
errors propagate unchanged; do not parse server messages as eligibility proof.

| Reason | Meaning and next action |
|---|---|
| `metadata_unknown` | Missing, malformed or ambiguous catalog/count result. Restore visibility or supported metadata; do not assume empty values. |
| `unsupported_server` | Server major/edition outside v1. Obtain a separately reviewed adapter profile. |
| `metadata_visibility_required` | Database/server metadata cannot be fully observed. Have the fixture owner supply approved visibility. |
| `database_binding_mismatch` | Current database differs from the bound identity. Correct the connection and provisioning record. |
| `object_binding_mismatch` | Object identity changed or roles alias one object. Re-establish owned objects after settling any prior outcome. |
| `owner_binding_mismatch` | Invocation, generation, mutation or resource marker differs. Resolve ownership; never relabel foreign resources. |
| `interval_not_one_finite_partition` | Authored bounds/column do not select exactly one finite partition. Use the existing supported replacement path or redesign the fixture. |
| `unsupported_partition_layout` | Partition type, direction, precision, boundary or index alignment is outside v1. Supply an approved aligned fixture. |
| `layout_mismatch` | Complete physical shapes differ. Compare columns, indexes, function/scheme and storage before retrying. |
| `unsupported_column` | Column feature/type falls outside the adapter profile. Preserve the public fallback route. |
| `unsupported_index` | Index feature or metadata falls outside the profile. Do not drop production indexes for this experiment. |
| `unsupported_storage` | Unsupported or incomplete filegroup/compression metadata. Correct disposable provisioning. |
| `unsupported_table_feature` | Table behavior such as CDC/temporal/replication is excluded. Use an isolated eligible fixture. |
| `unsupported_dependency` | Constraint, child object or external dependency is excluded. Review the fixture definition. |
| `switch_out_not_empty` | Switch-out contains rows. Resolve prior transaction outcome and retention before changing it. |
| `prepared_rows_outside_interval` | Prepared table contains NULL or out-of-window rows. Reject the prepared payload. |
| `transaction_authority_invalid` | Missing/changed active, committable, serializable caller transaction. Restore caller authority; never start one in the executor. |
| `catalog_drift` | Protected revalidation differs from the frozen plan, including prepared count or a forged partition number. Stop before mutation and diagnose the change. |

## Failure, retention and recovery

A failure before the first SWITCH causes no component target mutation. A failure
from either SWITCH propagates for **whole-transaction caller rollback**. This
includes a first statement that mutated before its response failed. The executor
never retries, falls back to DELETE/INSERT, rolls back partially or cleans tables.
The caller must not catch a second-SWITCH error and commit the first transfer.

After successful SWITCH the prepared partition is empty. Lost commit
acknowledgement therefore requires an exact fresh receipt probe **before reading
prepared content**. A confirmed matching receipt suppresses another mutation and
allows existing evidence/state completion. A missing, unavailable or mismatching
receipt leaves outcome unknown; retain all resources and block replay. Even when
the target was initially empty and switch-out stays empty, receipt-first recovery
is mandatory: emptiness is not proof that publication has not happened.

The caller owns prepared/switch-out retention. Do not drop, truncate, reuse or
restamp them until transaction outcome and the existing evidence/checkpoint
requirements are settled. After confirmed commit, old rows remain in switch-out
until the fixture owner's retention policy permits cleanup. After rollback,
restore the original invocation's prepared authority or fail closed. Unknown
outcomes require investigation, not a cleanup timer.

## Validate and hand off

Run the hermetic contract matrix from the repository root:

```bash
uv run pytest tests/test_mssql_native_partition_switch.py \
  tests/test_mssql_native_partition_switch_catalog.py \
  tests/test_mssql_native_partition_switch_recovery.py \
  tests/test_runtime_partition_replace_native_contracts.py -q
```

Expected result: all selected tests pass. Tests exercise synthetic catalog/row
transactions and inject SWITCH into the existing finalizer handler seam. The
real resume service proves receipt-first behavior with an emptied prepared
fixture. These are component/control-flow tests; they do not establish live SQL
rollback, locking, identity authority, or a working production transaction bridge.

DDA-05 owns the approved disposable real-row fixture. Requirements and the ADR
handoff are retained under `test_artifacts/delivery-acceleration/dda-04/`.
DDA-06 owns navigation and the numbered ADR. Future public activation requires a
separate contract for aligned-stage provisioning, durable ownership/retention,
transaction bridging, public admission and exact-environment live proof.
Existing imports, manifests, receipts, journals and fallback behavior are
unchanged; no migration is needed. Return to the
[delivery task plan](../data-delivery-acceleration-tasks.md) for integration owners.

The underlying restrictions are documented in Microsoft
[ALTER TABLE SWITCH](https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-table-transact-sql?view=sql-server-ver16),
[catalog metadata visibility](https://learn.microsoft.com/en-us/sql/relational-databases/security/metadata-visibility-configuration?view=sql-server-ver16)
and [DDL triggers](https://learn.microsoft.com/en-us/sql/relational-databases/triggers/ddl-triggers?view=sql-server-ver16).
These rolling references were consulted on 2026-09-10; the supported major-16
profile above is an implementation decision, not live certification.
