# ADR 0075: Govern MSSQL columnar range parallelism with one publication barrier

- Status: Accepted
- Date: 2026-09-28
- Decision owners: dpone maintainers
- Related: ADR 0057, ADR 0067, `feature-design-mssql-columnar-range-parallelism-v1.md`

## Context

### 0.87.1 safety correction

The 0.87.0 implementation acquired its byte reservation after ODBC batch
materialization and did not account for simultaneously retained Python, Arrow,
Parquet, and upload representations. Its byte high-water evidence represented a
configured reservation, not a proven retained-memory or RSS bound. This violates
decisions 4 and 11 and matches the rejected row-only safety claim below.

Until an amended design supplies pre-read admission plus exact-commit live RSS
evidence, 0.87.1 suspends runtime activation: `required` fails before source I/O
with `columnar_range_pre_read_byte_admission_unavailable`, `auto` records that
reason and uses serial execution, and `off` is unchanged. The configuration
schema remains readable; no existing v1 evidence is reinterpreted.

The MSSQL columnar object-storage route is serial even though dpone already has a
typed range AST and source-specific predicate renderers. Adding threads around
the current query would create ungoverned connection multiplication, ambiguous
source consistency, gaps/overlap, and partial-publication risk.

SQL Server snapshot isolation is transaction-scoped. Independent sessions do not
share an exported snapshot identity. SQL Server `uniqueidentifier` ordering is
also not raw-bit ordering, so Python-side UUID arithmetic is unsafe.

## Decision

1. `source.options.partitioning` remains the only public partition contract.
2. Parallel columnar extraction is opt-in and uses canonical immutable
   `RangePartition` values plus `MssqlPartitionPredicateRenderer`.
3. Each reader owns an independent session from `open_session()`.
4. Partition count, reader/upload/load concurrency, topology, and aggregate
   row/byte/inflight budgets are explicit and fingerprinted.
5. Automatic planning is limited to types with certified arithmetic/statistics.
   UUID supports explicit ranges only and predicates use SQL Server comparison.
6. The consistency authority is explicit: immutable source, database snapshot,
   temporal `AS OF`, or write exclusion. Multiple independent snapshot
   transactions are not described as one snapshot.
7. Window/group partitions cannot be split. Unprovable queries fail preflight.
8. `shared_per_run` and `per_partition` are capability-negotiated topologies.
   Per-partition staging must assemble into one authoritative run staging before
   quality checks and publication. Shared staging admits one load worker because
   authoritative per-range receipts require sequential target-count deltas;
   parallel load workers require isolated per-partition staging.
9. All planned ranges must reach EOF and all stage counts must be confirmed before
   the existing ClickHouse quality/publication lifecycle may run.
10. First failure cancels and joins all workers; partial work cannot report
    success. Cleanup touches only run-owned resources and preserves the primary
    failure.
11. Requested and observed concurrency, normalized ranges, high-water resource
    use, topology, and publication receipt are evidence.

## Consequences

The route gains deterministic parallel throughput and auditable failure behavior
without changing defaults. Operators must choose and prove a source consistency
authority. Static ranges may retain skew; dynamic splitting is deferred because
it would change identity and replay semantics. Per-partition topology costs more
target tables and is rejected when the sink cannot assemble them safely.

## Rejected alternatives

- Free-form predicates per worker: injection and coverage cannot be certified.
- Python UUID ordering: differs from SQL Server comparison semantics.
- A row-count-only queue advertised as byte bounded: false safety claim.
- Publishing successful partitions independently: exposes partial business data.
- Hidden tenant overrides or monkey patches: not a public reproducible contract.
