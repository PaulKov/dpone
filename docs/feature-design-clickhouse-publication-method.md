# Feature design: guarded ClickHouse publication method

- Status: APPROVED
- Owner: dpone maintainers
- Approval: maintainer instruction in this task, 2026-09-29, to implement the
  researched partition-replacement policy and document it comprehensively.
- Target release: TBD; implementation is not production activation.

Last verified: 2026-09-29

## Problem, scope and customer journey

Architects need full-refresh publication that preserves a table's identity when
one partition can represent the complete new snapshot. Operators need to recover
lost acknowledgements without toggling generations or leaving stale partitions.
Platform engineers must supply durable, protected all-writer authority; a mutex,
caller boolean, TTL lease or a target-local marker alone is not sufficient.

Implement a reusable, dependency-injected publication kernel and a closed plan
selector. This is a prerequisite of the
[ODBC range v2 design](feature-design-mssql-columnar-range-parallelism-v2.md),
not permission to activate its suspended route. Existing UUID-v1 publication and
bounded-window behavior remain unchanged. The kernel has no implicit connection,
credential lookup, CLI flag, manifest switch, source access or default binding.

An integrator discovers the capability in the runbook, implements the protected
backend port, certifies the exact binding, then uses `publish(operation_id)` or
`recover(operation_id)`. Recovery obtains the immutable original record before
any source work. Operators inspect persisted method, reason, state and evidence;
unknown outcomes retain exclusion and require reconciliation, never blind DDL.

## Public contract and architecture

New Python modules are `dpone.contracts.clickhouse_publication`,
`dpone.ports.clickhouse_publication`, and
`dpone.runtime.sinks.clickhouse_guarded_publication`. They depend inward on typed
contracts; no connector SDK is imported. The backend is injected explicitly.
The backend must validate persisted full canonical records, not deserialize
untrusted caller claims as authority. Dataclasses are values, not credentials.

Method selection uses fresh, protected catalog and content observations under
target-wide exclusion. The caller cannot request unsafe forced replacement.
Methods are `exchange`, `replace_partition`, `rename`, and `noop`. A plan freezes
the selected method, reason, target/candidate UUIDs, design fingerprints, typed
content evidence, canonical partition ID and query identity. An empty string
partition key denotes an unpartitioned table; partition IDs come from ClickHouse,
not user SQL. Row ranges used in MSSQL extraction are unrelated to partitions.

No old receipts are reinterpreted. No manifest/schema changes or migration are
required. Changing an existing operation's policy cannot change its frozen plan.
The journal schema is `dpone.clickhouse.guarded-publication.v2`.

## Algorithm and failure semantics

1. Enter backend exclusion for the stable operation and physical target. Resolve
   aliases to the same write subject. Reopen an existing journal first.
2. For a new operation, inspect sealed staging and target. Require local Atomic,
   plain MergeTree, complete catalog visibility and supported side effects.
   Unknown metadata blocks; it must not silently select exchange.
3. Select rename for absent target; exchange for empty staging, incompatible
   design or multiple partitions; replace for matching design, exactly one
   staging partition and no target partition outside it. Same full content with
   equal design selects no-op. Never use counts alone for equivalence.
4. Persist the full immutable intent before attempting the one-shot claim. The
   backend claim must be an acknowledged CAS, never a readback of an uncertain
   winner. Only that invocation may dispatch; losers reconcile.
5. Revalidate original UUIDs, design and sealed content immediately before
   dispatch. Execute one fixed statement without hidden connector retries.
6. Close/drain the exact publisher before observing outcome, including queued
   requests not yet present in `system.processes`. No fresh publisher may be
   issued for the same intent. Unknown closure means unknown outcome.
7. For exchange/rename classify names and UUIDs plus desired content. For replace
   require unchanged UUIDs, unchanged sealed candidate and desired target content.
   Original content after closure means not-published; anything else is unknown.
   No-op requires fresh identical target/candidate proof and no mutation.
8. Persist resolution before returning. Repeated recover never executes DDL.
   A not-published operation needs a new explicit attempt, not a hidden retry.

States: `prepared -> claimed -> committed | not_published | unknown`.
Recovery may close an abandoned prepared record and resolve not-published; it
must never infer committed from desired content before a claim. Unavailable
journal, stale authority, changed staging, third UUID, mutations or incomplete
observation fail closed. Cancellation follows the same closure path. This kernel
does not advance source checkpoints, release target ownership or delete tables.

Evidence uses a versioned typed logical multiset digest including row count,
NULL/type distinctions and duplicate multiplicity. It is probabilistic parity,
not an exact mathematical equality proof. Physical part checksums/names are not
durable content identity: merges rewrite them. Deployments must certify the
digest producer and row-type coverage; the kernel does not trust an arbitrary
caller-provided digest. Publication is O(1) DDL but independent evidence may scan
the complete snapshot. No speed/RSS claims follow from method selection.

Rollback is a new guarded publication. EXCHANGE retains the old generation;
REPLACE retains staging but does not create an old-data backup. Automatic reverse
exchange or restoration is prohibited. Backup retention belongs to the binding.

## Market evidence and adopted patterns

Official sources checked 2026-09-29:

- [ClickHouse EXCHANGE](https://clickhouse.com/docs/reference/statements/exchange):
  atomic name swap, retained for whole multi-partition refresh.
- [ClickHouse partition operations](https://clickhouse.com/docs/reference/statements/alter/partition):
  atomic partition replacement, matching structure/keys/storage policy; adopt
  for a single complete snapshot partition. `tuple()` is the sole partition of
  an unpartitioned table, not a wildcard. Empty-source semantics are versioned;
  this policy deliberately uses exchange for empty snapshots.
- [Official dbt-clickhouse table materialization](https://github.com/ClickHouse/dbt-clickhouse/blob/main/dbt/include/clickhouse/macros/materializations/table.sql)
  uses exchange where supported. Its
  [incremental implementation](https://github.com/ClickHouse/dbt-clickhouse/blob/main/dbt/include/clickhouse/macros/materializations/incremental/incremental.sql)
  uses replace-partition for insert-overwrite. Adopt method selection by scope;
  reject treating a partition loop as globally atomic full refresh.

dlt, Informatica, Airbyte, Fivetran, Pentaho, SSIS, gusty, Astronomer Cosmos and
Apache Beam are N/A to this narrowly scoped ClickHouse DDL recovery primitive;
this is not an assessment of their ingestion capabilities. The measurable goal
is zero second DDL dispatches during lost-ACK recovery and zero retained stale
partitions in admitted replacement fixtures, not an unqualified market ranking.

## Tests, rollout and release gates

Unit tests cover selection, stale partitions, empty snapshots, unchanged data,
UUID/content drift, prepared recovery, CAS losers and lost ACK before/after DDL.
Compatibility tests exercise unchanged local-v1 publication. A fresh reviewer
must inspect the exact diff. Required broad checks follow AGENTS.md.

Production activation additionally requires a concrete protected backend,
source-free router integration, restart-safe journal, no-retry SQL executor,
all-writer fencing/closure tests and exact-commit live ClickHouse certification.
ODBC activation also requires all pre-read memory and MSSQL/object-storage route
gates. Mocked kernel tests are not live certification. This increment may be
reviewed independently; it cannot be represented as a released fast ODBC route.

Operational details: [publication runbook](clickhouse-publication-methods.md).
