# ADR 0076: Recover prepared ClickHouse publication only with strict original-operation authority

Status: Accepted for guarded opt-in (2026-09-30)

## Context

A full-refresh candidate can be fully staged while its publication authority
remains `PREPARED`. Clearing that record or starting a fresh load discards the
original fence. Repeating `EXCHANGE` without proving its previous outcome can
publish twice. A version column in a ReplicatedReplacingMergeTree table does
not provide a linearizable compare-and-swap permit across workers.

## Decision

Recovery is a separate operator workflow for the **same** operation. Its plan
requires the exact authority version and identity, unchanged healthy candidate
and predecessor generations on every replica, matching row counts, complete
query-log coverage from the authenticated operation start time, and no matching
DDL in logs, running processes, or distributed queue. Zero-row and unverified
quality-evidence cases remain blocked. The plan is stored in an owner-private
file and confirmed by digest before execution.

Execution requires an externally provisioned KeeperMap authority admitted
for every replica and every writer sharing the target. It rechecks the plan,
then permits exactly one `PREPARED -> DISPATCHING` CAS and one correlated DDL.
Unknown outcomes reconcile by exact token, digest, queue result and physical
generation; they never issue a second DDL. `COMPLETED` requires terminal
replica proof. A restart uses the original private plan, not a new operation.

The existing ReplacingMergeTree adapter reports the strict capability as
unsupported. Merely copying an old `PREPARED` row to KeeperMap does not prove
strict origin. Legacy records require a separately reviewed migration and
all-writer cutover; this feature will not silently import or repair them.

## Consequences

- Existing business pipelines and default publication behavior are unchanged.
- Operators get a fail-closed, auditable self-service path only after strict
  infrastructure admission. Without it, work remains blocked rather than green.
- Live fault integration needs an admitted strict authority. Synthetic unit
  tests are not a substitute for environment-level acceptance.
- Read-only preflight evidence has retention and clock assumptions; missing
  history or an untrustworthy operation start time blocks dispatch.

See [operator procedure](../clickhouse-prepared-recovery.md),
[ADR 0067](0067-clickhouse-cluster-publication-authority.md), and
[ADR 0073](0073-durable-committed-replay-quality.md).
