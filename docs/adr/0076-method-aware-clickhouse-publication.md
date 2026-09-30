# ADR 0076: Method-aware guarded ClickHouse publication

- Status: Accepted for unbound kernel; production activation pending certification
- Date: 2026-09-30

## Context

EXCHANGE swaps table UUID/name mappings; REPLACE PARTITION preserves table UUIDs.
Reusing UUID-v1 recovery for replacement would misclassify every outcome. A
partition loop cannot provide atomic whole-table refresh. Neither command
eliminates the all-writer exclusion requirement of ADR 0057.

## Decision

Use a separate v2 immutable intent and protected one-shot kernel. Prefer a single
partition replacement only when it covers the entire desired full snapshot and
no other target partitions survive. Preserve EXCHANGE for multiple partitions,
empty snapshots and compatible admitted schema replacement. Absent targets use
RENAME; identical logical snapshots use no-op. Incomplete visibility blocks.

After permanent publisher closure, reconcile EXCHANGE by UUID mapping and content;
reconcile replacement by unchanged identities plus sealed logical content. Keep
acknowledged claim history across unknown states. Recovery never executes DDL.
Legacy v1 behavior and receipts remain unchanged.

## Consequences

The kernel requires an injected, certified protected backend. It ships without a
production binding and does not enable the suspended ODBC route. Tests using fake
authority prove orchestration only. Rollback, checkpoints and cleanup remain
separate controlled operations. Hash evidence is probabilistic, not exact equality.

See the [approved design](../feature-design-clickhouse-publication-method.md) and
[operator/developer guide](../clickhouse-publication-methods.md) for algorithms,
sources, versioned contracts, tests and activation gates.
