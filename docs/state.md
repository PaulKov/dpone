# Runtime State Backends

`dpone` can store runtime state outside the default BigQuery backend.

Supported OSS state backends:

- `mssql`: target-atomic generic MSSQL transaction receipts or XMin
  key-snapshot state in SQL Server.
- `postgres`: XMin checkpoints and run-state rows in PostgreSQL.

## Default behavior

When a manifest omits `state`, dpone uses the safest runtime default for the
selected strategy:

- `full_refresh` and `replace` are stateless for non-MSSQL sinks. They run with
  disabled durable state by default and do not require Vault or BigQuery
  credentials.
- Every MSSQL sink route is governed. A non-`key_snapshot` route requires the
  generic four-table transaction catalog described below, including
  `full_refresh` and `replace`. A PostgreSQL XMin `key_snapshot` route keeps the
  separate six-table state contract.
- Stateful strategies such as `incremental_append`, `incremental_merge`, `xmin`,
  `cdc`, `snapshot_diff`, `scd2`, `partition_replace` and `backfill` keep the
  historical durable default. Configure `state.type` explicitly for production
  routes so checkpoints live in the intended metadata store.

Use an explicit disabled state only for stateless routes:

```yaml
state:
  type: disabled
```

dpone rejects `state.type: disabled` for stateful strategies and MSSQL sink
routes because that would make checkpoints, offsets, reconciliation state, or
the atomic commit authority disappear between runs.

## Backfill campaign journal v2

`backfill.options.state.backend: audit_schema` stores an append-only campaign
journal and an append-only chunk journal. Schema version 2 makes causal order
an explicit database fact. Every row has a dialect-owned, non-null
`journal_id`, and runtime selects the current row exclusively by the greatest
ID for the logical key:

- SQL Server: `bigint IDENTITY(1,1)`;
- PostgreSQL: `bigint GENERATED ALWAYS AS IDENTITY`;
- ClickHouse 24.8+: `UInt64 DEFAULT generateSnowflakeID()` with
  `ReplacingMergeTree(journal_id)`.

`__dpone__loaded_at` remains operational evidence only. It is not a version,
tie-breaker, lease authority, or recovery fallback. Runtime validates the
exact catalog shape before reading or writing either journal table. A table
without the exact causal-ID contract fails with
`DPONE_BACKFILL_JOURNAL_SCHEMA_MIGRATION_REQUIRED`; dpone never alters or
silently accepts that table. Journal reads also reject non-positive,
duplicate, or non-descending IDs with
`DPONE_BACKFILL_JOURNAL_ORDER_INVALID`.

The JSON campaign ledger is version 2 as well. A version-1 local file is one
current snapshot rather than an append-only history, so an operator may stop
the campaign, back up the file, verify its immutable run/chunk shape, and
change only `schema_version` from `1` to `2`. Never change a run key, chunk
boundary, idempotency key, plan hash, or config hash during that conversion.

### Upgrading legacy SQL journal tables

There is deliberately no in-place or automatic migration. The safe operator
procedure is:

1. Stop every writer for the affected campaign tables and retain a read-only
   backup of both legacy tables.
2. For campaign history, prove that `__dpone__loaded_at` is strictly unique
   and increasing within every `run_key`. For chunk history, prove the same
   within every `(run_key, chunk_index)`. Any tie or invalid timestamp makes
   causality unrecoverable: keep the old tables as evidence, provision new v2
   tables under new names, and start a new `backfill_id` boundary.
3. If both proofs succeed, create separately named v2 tables with the exact
   dialect DDL. Assign positive unique IDs offline in a deterministic total
   order that preserves each logical key's strict legacy timestamp order,
   using the dialect's supported identity override for the historical copy.
4. Verify row counts, JSON bytes, positive global ID uniqueness, and strictly
   increasing IDs within every logical key. Switch table names only in a
   maintenance window, then let the database issue every subsequent ID.

Do not break a timestamp tie with a physical row locator, arbitrary sort key,
or ingestion retry order. Those values observe storage; they cannot recover
which transition was causally latest.

## Generic MSSQL transaction state

All non-`key_snapshot` MSSQL sink strategies use one narrow, receipt-backed
state capability. Configure it explicitly:

```yaml
state:
  type: mssql
  connection_ref: mssql_governance
  atomicity: target_atomic
  provisioning: external
```

The deployment connection registry is authoritative for the state `database`
and `schema`. It must resolve to the same SQL Server instance, physical
replica, effective principal, and original login as the target connection.
Host aliases may differ. Runtime verifies those vendor facts and exact
cross-database permissions before source schema or payload I/O.

The complete runtime object set for this generic route is:

- target-local `<target_database>.dbo.dpone_target_identity`, with the approved
  binding for the managed target;
- `dpone_target_fence`, `dpone_load_attempt`, `dpone_load_operation`, and
  `dpone_load_receipt` in the registry-resolved state database/schema.

There is no generic-route dependency on `dpone_source_state`,
`dpone_commit_receipt`, `dpone_repair_authority`,
`dpone_repair_authority_consumption`, `dpone_run_state`, or
`dpone_load_audit`. Those belong to the XMin key-snapshot capability. For a
generic route, the immutable `dpone_load_receipt` is the run, payload,
lifecycle, mutation-plan, before/after catalog, metrics, ACK-recovery, and
replay authority.

Render the canonical versioned DDL and have an operator execute it once. The
database and schema must already exist; runtime never applies DDL:

```bash
dpone state render-mssql-transaction-ddl \
  --database Example_System \
  --schema governance \
  > generic-mssql-transaction-v2.sql

sqlcmd -S <server> -d Example_System -i generic-mssql-transaction-v2.sql
```

Upgrade an existing exact v1 catalog without rewriting evidence rows:

```bash
dpone state render-mssql-transaction-ddl \
  --database Example_System \
  --schema governance \
  --upgrade-from 1 \
  > generic-mssql-transaction-v1-to-v2.sql

sqlcmd -S <server> -d Example_System -i generic-mssql-transaction-v1-to-v2.sql
```

The migration runs with `XACT_ABORT`, SERIALIZABLE isolation, and a
transaction-owned application lock. It accepts only exact v1 or v2, is
idempotent for v2, and fails with
`DPONE_GENERIC_TRANSACTION_V2_DUPLICATE_INVOCATION` before catalog mutation if
v1 contains multiple attempt rows for one scheduler invocation. Resolve that
ambiguity under a reviewed state-repair procedure and start a new invocation;
do not delete or merge rows ad hoc.

The renderer creates exactly the four tables and their four required triggers.
All columns are explicitly non-`IDENTITY`. Preflight rejects missing or extra
columns, indexes, foreign keys, CHECK constraints, defaults, or triggers;
wrong ordered key columns, filter semantics, enable/trust state, trigger body,
partition count, or compression also fail closed. Each table and index must
have one `NONE`-compressed partition. Do not customize the rendered catalog or
add convenience indexes to it.

Admission allocates an immutable attempt generation and an operation lease
before extraction. Finalization then revalidates target identity and exact
planned catalog before and after schema/physical actions, performs business
DML, inserts the receipt, and commits them on the target connection in one SQL
Server transaction. An exact durable receipt suppresses replay. If the commit
ACK is lost, a fresh session must find the exact receipt before dpone reports
success; an absent or mismatching receipt yields
`mssql_transaction.commit_outcome_unknown` and preserves staging evidence for
operator diagnosis.

Generic and XMin MSSQL routes both reject every inline pre/post hook before
route branching. Contract v1 has no provider-issued, preplan-safe,
exactly-once hook receipt, so author-declared read-only/idempotent metadata is
not sufficient authority.

Common authoring failures are reported before runtime hydration:

- `MSSQL_TRANSACTION_STATE_REQUIRED`: add the explicit state block;
- `MSSQL_TRANSACTION_REQUIRES_MSSQL_STATE`: use `state.type: mssql`;
- `MSSQL_TRANSACTION_REQUIRES_TARGET_ATOMIC_STATE`: set
  `atomicity: target_atomic`;
- `MSSQL_TRANSACTION_REQUIRES_EXTERNAL_STATE`: set
  `provisioning: external` and install the rendered catalog out of band.

## MSSQL XMin key-snapshot state

For PostgreSQL XMin `key_snapshot` workloads, reference a deployment-owned
connection and keep database/schema coordinates out of the workload manifest:

```yaml
state:
  type: mssql
  connection_ref: mssql_sample_metrics_state
  atomicity: target_atomic
  provisioning: external
  table: {name: dpone_source_state}
  run_table: {name: dpone_run_state}
  receipt_table: {name: dpone_commit_receipt}
  repair_authority_table: {name: dpone_repair_authority}
  repair_consumption_table: {name: dpone_repair_authority_consumption}
  audit_table: {name: dpone_load_audit}
```

The environment connection registry supplies both `database` and `schema`.
For example, the same manifest can resolve to
`[DWH_Dev].[system].*` in DEV and `[Example_System].[dbo].*` in PROD. All six
tables must resolve to that one three-part location; per-table database/schema
overrides that disagree with the registry fail closed. Exact explicit
coordinates that match the registry remain accepted for compatibility;
omitted coordinates use the registry defaults.

`target_atomic` means that target DML, the XMin compare-and-set operation, and
the commit receipt use one SQL Server transaction and one connection. The
target and state databases must therefore be on the same SQL Server instance
and accessible to the same principal. A snapshot-envelope run is successful
only when its result contains a committed receipt.

`provisioning: external` is required for `target_atomic`. The platform creates
the schemas and versioned state tables as a separate installation operation;
the workload DAG never executes `CREATE SCHEMA` or `CREATE TABLE`. Missing
objects are a readiness failure, not permission to create them at runtime.
Runtime hydration also resolves and validates the exact three-part
`audit_table` before extraction, then injects one load-identity service for the
whole `started -> staged -> committed|failed` lifecycle. `started` and
`staged` audit failures fail closed before target mutation. If target and its
commit receipt are already durably committed, a later audit-write failure is
reported as a repairable warning and does not relabel the load as failed or
trigger a blind retry; the immutable commit receipt is the recovery authority.
The checkpoint row carries a monotonic `state_revision`. Target-atomic CAS
compares both the previous XMin value and revision, then increments revision in
the same transaction. This is required because several valid PostgreSQL
snapshots can share the same safe XMin while a long transaction is open.

### One-shot repair authority

Mass-delete guard overrides and full repair baselines use an expiring
environment-owned authority, never a standing manifest switch. The immutable
authority row contains:

- `authority_id` and its canonical SHA-256 digest;
- the exact `state_key` and `scope_hash`;
- `expected_checkpoint_or_absent` (`absent` or exact XMin plus revision);
- optional all-or-none `transfer_from_state_key`, `transfer_from_xmin`, and
  `transfer_from_revision` fields for a schema/PK/scope identity change;
- a non-empty operator reason and UTC expiry;
- independent upper bounds for `full_baseline`, `max_delete_rows`, and
  `max_delete_ratio`.

The platform provisions `dpone_repair_authority` with the exact enabled
`trg_dpone_repair_authority_immutable` INSTEAD OF UPDATE/DELETE trigger, plus
`dpone_repair_authority_consumption`. The latter has unique keys on authority
ID and commit receipt. Missing tables, trigger drift, mutable authority bytes,
expiry, reuse, checkpoint/scope mismatch, or an exceeded allowance fail before
commit.

Select the approval only for the exceptional invocation:

```bash
dpone run path/to/pipeline.yaml \
  --repair-authority-ref repair-work-item-20260815-001
```

Schedulers may project the same opaque ID through
`DPONE_REPAIR_AUTHORITY_REF`. Do not put `repair_authority_ref` in a workload or
promotion manifest. Strict Airflow `init_fetch` compose injects the variable
from `dag_run.conf` after the pack-env allowlist: pass object
`DPONE_REPAIR_AUTHORITY_REFS` keyed by `task_id` for one multi-process run, or
scalar `DPONE_REPAIR_AUTHORITY_REF`. An empty render is a no-op. Packs must
not bake the ID. Before PostgreSQL I/O, dpone previews the immutable row to
decide extraction intent: only `allow_full_baseline=true` forces a full payload;
a delete-only approval preserves XMin incremental extraction. Under the target
application lock dpone revalidates the complete authority, performs only the
bounded action, advances the checkpoint, creates the commit receipt, and
inserts consumption evidence in one transaction. Full repair remains a
set-based upsert plus key reconciliation; it never truncates tombstones.

Changing a schema hash, primary-key contract, or scope creates a new
`state_key`; it does not silently take ownership of an already managed target.
For that baseline, the authority must bind the exact active old owner through
all three `transfer_from_*` values, bind the new checkpoint as `absent`, and
allow a full baseline. Under the target application lock, dpone verifies that
there is exactly one active owner with that old XMin/revision, marks its
source-state row with `superseded_at_utc` and `superseded_by_state_key`, then
performs baseline DML, inserts the new checkpoint/receipt, and consumes the
authority in the same transaction. Old receipts and checkpoint history remain
queryable. Any mismatch, unnecessary/reused transfer, later CAS failure, or
consumption failure rolls everything back; a stale process using the old key
cannot load or advance the superseded checkpoint.

Raw target coordinate strings are diagnostic only. Before PostgreSQL schema or
payload I/O, dpone resolves an immutable row from
`<target_database>.dbo.dpone_target_identity`. That target-local registry has
`schema_name` and `table_name` declared with `COLLATE DATABASE_DEFAULT`, so SQL
Server itself makes CI aliases converge while preserving distinct CS objects.
The registry row's `binding_id`, SQL-reported database identity, and server
topology produce the opaque `target_identity binary(32)`. The same bytes enter
the v2 `state_key` digest, target applock, ownership/transfer predicates, CAS,
and receipt probes. Dpone never applies Python `lower`/`casefold` to object
names and never creates or mutates registry bindings at runtime.

The external state DDL requires `target_identity binary(32) NOT NULL` and a
unique filtered index (named `uq_dpone_source_state_active_target` in the
reference DDL) on `(target_identity) WHERE superseded_at_utc IS NULL`. It both supports the
`SERIALIZABLE` ownership lock without serializing unrelated targets and makes
the one-active-owner invariant physical.

The registry is an externally provisioned exact contract: clustered PK on
`binding_id`, unique nonclustered `(schema_name, table_name)`, default
`SYSUTCDATETIME()` for `created_at_utc`, `NONE` compression, one partition, and
an enabled `INSTEAD OF UPDATE, DELETE` trigger whose sole body is
`THROW 51000, 'DPONE_TARGET_IDENTITY_IMMUTABLE', 1`. Its two name columns must
have exactly the target database default collation. One binding is installed
for every managed target before its first run. Under the target applock dpone
re-resolves the binding before DDL/DML, and target creation uses the registry's
catalog spelling rather than the manifest alias.

All safety-critical catalog identifiers must use the exact lowercase spelling
shown by the contract. Preflight compares column, index, foreign-key, and
default-column names without Python case folding and rejects any pair of
casefold-equivalent spellings. This prevents a case-sensitive database from
placing constraints on a decoy such as `Binding_ID` while runtime reads
`binding_id`.

Reference target-local DDL (execute once in every target database, then insert
approved bindings with explicit UUIDs):

```sql
CREATE TABLE [dbo].[dpone_target_identity] (
    [binding_id] uniqueidentifier NOT NULL,
    [schema_name] nvarchar(128) COLLATE DATABASE_DEFAULT NOT NULL,
    [table_name] nvarchar(128) COLLATE DATABASE_DEFAULT NOT NULL,
    [created_at_utc] datetime2(7) NOT NULL
        CONSTRAINT [df_dpone_target_identity_created] DEFAULT SYSUTCDATETIME(),
    CONSTRAINT [pk_dpone_target_identity] PRIMARY KEY CLUSTERED ([binding_id]),
    CONSTRAINT [uq_dpone_target_identity_name]
        UNIQUE NONCLUSTERED ([schema_name], [table_name])
);

CREATE TRIGGER [dbo].[trg_dpone_target_identity_immutable]
ON [dbo].[dpone_target_identity]
INSTEAD OF UPDATE, DELETE
AS
    THROW 51000, 'DPONE_TARGET_IDENTITY_IMMUTABLE', 1;
```

Before any source I/O, the target-atomic catalog gate validates all six
external objects: source state, commit receipt, repair authority, authority
consumption, run state, and load audit. It matches primary/unique keys by exact
kind, ordered columns, rowstore type, filter semantics, and absence of included
or descending columns; disabled, hypothetical, or `IGNORE_DUP_KEY` indexes fail
closed. Required foreign keys must reference the exact ordered columns, be
enabled and trusted, and may not use `NOT FOR REPLICATION`. Required CHECK
constraints follow the same enforcement rules. CHECK and default expressions
are parsed and compared by a
restricted SQL-semantic normalizer, so harmless SQL Server parentheses,
brackets, `BETWEEN` expansion, and boolean term ordering do not cause drift,
while comments or unsupported syntax cannot spoof a match. Every table and
index must have one `NONE`-compressed partition. Constraint and index names are
diagnostic only; a familiar name never substitutes for the required semantics.

Every column in those six XMin objects and in the target-local
`dpone_target_identity` registry has an explicit `IDENTITY` contract. All are
non-`IDENTITY` except `dpone_run_state.id`, which must be `IDENTITY`. Shape
preflight rejects both an unexpected `IDENTITY` on any other column and a
missing `IDENTITY` on that run-state key before source I/O.
It also rejects computed, sparse, `ROWGUIDCOL`, generated/hidden, masked,
encrypted, FILESTREAM, column-set, user-defined/CLR-type, legacy bound-rule or
bound-default drift. Character and binary columns must retain canonical
`ANSI_PADDING`, and collatable columns must use the database default collation.

The extracted snapshot envelope also freezes the registry-canonical target
database, schema, and table. Finalization rejects a changed runtime config
before staging, uses an independent config copy populated from that envelope,
and checks the same coordinates again under the binary-identity applock. Thus
one run cannot prove/checkpoint target A while directing DDL or DML to target B.

The canonical authority digest always includes `transfer_from`: `null` for a
normal repair, or `{"state_key":"<64 lowercase hex>","xmin":N,"revision":N}`
for a transfer. The database columns remain the three nullable
`transfer_from_*` fields; partial triples are invalid.

MSSQL run state uses an identity `id` as its clustered primary key and a
`run_state_key binary(32)` unique index. The digest binds `dag_id`,
`process_name`, and `execution_date`; runtime also compares the original
dimensions before mutation so a theoretical SHA-256 collision fails closed.
This avoids SQL Server's 900/1700-byte rowstore index-key limits for long
self-service identifiers.

Run-state identity is selected by route policy, then checked against the table
capability. Legacy v1 `(dag_id, execution_date)` run tables remain readable by
their explicit compatibility APIs, but a governed non-key-snapshot MSSQL sink
never selects that `after_target`/runtime-provisioned path. It uses the generic
four-table capability and does not hydrate a separate run-state store. Do not
add or migrate a run table for a generic route.

`target_atomic` (and therefore `key_snapshot`) requires the process-scoped v2
contract and external provisioning. Pointing that route at v1 fails before
source I/O with `mssql_run_state_v1_to_v2_migration_required`. Provision a
separately named v2 table (for example `dpone_run_state_v2`) with
`render_run_state_v2_create_sql(...)`, retain v1 read-only for history, and
change `state.run_table.name`. dpone cannot infer the missing `process_name`
for old rows, so automatic copy/backfill would be collision-prone.

Before PostgreSQL schema or payload I/O, the target-atomic route reads
SQL-reported server, instance, physical replica, effective principal, and
original login on target and state sessions. It then compiles rollback-only,
zero-row permission probes. The target session proves three-part access to
source state, commit receipt, repair authority, and repair consumption; the
state session proves run-state and load-audit access. Host aliases may differ,
but instance/replica/principal or permission drift fails closed.

Legacy manifests using `connection_id`, `connection_type`, table-level
`schema`, or `run_table.run_name` remain accepted during their compatibility
window. New manifests use `connection_ref`, registry location defaults, and
`run_table.name`. Do not point a new identity at an old two-column state row:
new checkpoint identity also binds environment, process, source/target
coordinates, unique key, schema hash, scope hash, and contract version.

## PostgreSQL state

PostgreSQL-backed state is useful for local OSS deployments or for teams that want metadata close to a Postgres source.

```yaml
state:
  type: postgres
  connection_id: postgres_meta
  connection_type: vault
  table: {schema: etl_state, name: etl_xmin_state}
```

Developer API:

```python
from dpone.runtime.state.postgres import PostgresRunStateStorage, PostgresXMinStateStorage

xmin_state = PostgresXMinStateStorage(connector, schema="etl_state", table="etl_xmin_state")
run_state = PostgresRunStateStorage(connector, schema="etl_state", table="etl_run_state")
```

Both backends expose the same methods used by the runtime:

- `save_state(source_schema, source_table, xmin_state)`
- `load_state(source_schema, source_table)`
- `delete_state(source_schema, source_table)`
- `save_run_state(run_state)`
- `update_run_state(run_state)`
- `get_run_state(dag_id, execution_date)`
- `get_run_states_by_dag(dag_id, execution_date=None, limit=100)`
- `delete_old_states(days_to_keep=30)`

## Postgres XMin state runbook

The detailed XMin guide lives in [Postgres XMin](postgres-xmin.md). Use it when a Postgres source does not have an application-level `updated_at` or numeric cursor and you want dpone to persist transaction-ID checkpoints in BigQuery, Postgres, or MSSQL state storage.

The short rule is: set `source.options.incremental_strategy: xmin` explicitly, or omit `source.options.incremental_column` in legacy manifests, to use XMin for Postgres incremental extraction. Configure `state.type` explicitly when the checkpoint must live outside the default BigQuery state backend.

For a large PostgreSQL→MSSQL first load, use the explicit XMin handoff instead
of allowing the incremental strategy to perform one unbounded baseline. The
manual `xmin_execution.mode: initial` process stores chunk campaign progress
in the generic four-table MSSQL transaction catalog. It also binds a dedicated
XMin handoff storage to the canonical six-table XMin catalog. Both storages
must resolve through the same externally governed MSSQL state connection and
database; this is one physical state plane with two non-overlapping contracts,
not two credentials or an implicit fallback.

The initial campaign writes no XMin checkpoint until every chunk is committed.
Its finalizer then creates XMin revision 1 and the deterministic seed receipt
atomically. A separate `xmin_execution.mode: incremental` process with the
same `handoff_id` is admitted only when that exact receipt exists. Missing,
partial, conflicting, or freeze-expired handoffs fail closed before source
payload I/O. Do not add defaults to state columns, copy a checkpoint manually,
or point one phase at a legacy shared state table.
