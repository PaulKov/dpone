# SQL Server snapshot research and benchmark protocol

Purpose: help maintainers and platform engineers choose a measured storage and
load strategy. This is a planning companion to the
[roadmap](sqlserver-snapshot-roadmap.md), not configuration guidance for an
implemented feature.

- Status: RESEARCHED; implementation and live experiments require separate approval.
- Owner: maintainers; target release: TBD.
- Last verified: 2026-09-09.
- Repository baseline: `6533c27fbf77c78a00b8013bcf72e2293e281e1f`.
- No performance winner has been established. All workloads below are synthetic.

## Separate the four layers

| Layer | What changes | What it cannot establish |
|---|---|---|
| SQL Server storage | ROW/PAGE rowstore or COLUMNSTORE/COLUMNSTORE_ARCHIVE index storage | Network size, serialized source bytes, process RAM, total log allocation |
| Transport compression | Compression of the selected protocol payload; codec negotiated by that transport | SQL Server compression setting or ClickHouse column codec |
| Intermediate representation | CSV, Arrow, Parquet or Native framing, typing, encoding and optional compression | A compressed file size is not logical data size; a format is not an atomic publication protocol |
| ClickHouse column codecs | Per-column disk representation under the selected table engine | Source storage or transport compression |

Fact: ClickHouse exposes column codecs in CREATE TABLE; some encodings preprocess
values before a general compressor. Fact: Native is a binary, column-oriented
exchange format. Neither fact establishes support in every dpone transport.
Sources: [ClickHouse CREATE TABLE](https://clickhouse.com/docs/reference/statements/create/table),
[Native format](https://clickhouse.com/docs/reference/formats/Native), rolling
open-source documentation checked 2026-09-09; runtime server version must be
recorded by the eventual experiment.

## Primary source register and design decisions

All entries were checked 2026-09-09. Microsoft references use the SQL Server 17.x
view unless explicitly marked 16.x; view selection is not certification of all
versions or Azure editions. Facts below summarize the cited source. Decisions
are dpone proposals, not vendor promises.

| Source and version | Observed fact | Proposed implication |
|---|---|---|
| [Microsoft data compression](https://learn.microsoft.com/en-us/sql/relational-databases/data-compression/data-compression?view=sql-server-ver17), SQL Server 17.x documentation | Rowstore supports ROW/PAGE; columnstore supports ordinary and archival compression; compression can vary by partition and trades CPU for storage | Keep storage family distinct from compression mode. Do not append archival mode to the old rowstore enum without a versioned representation |
| [Microsoft load guidance](https://learn.microsoft.com/en-us/sql/relational-databases/indexes/columnstore-indexes-data-loading-guidance?view=sql-server-ver17), updated 2026-07-20 | Bulk batches of at least 102,400 rows can enter compressed rowgroups; smaller tails enter delta storage. Maximum rowgroup size is 1,048,576 rows, potentially reduced by memory pressure. Partitioning splits rowgroups | Measure effective batches per partition and worker. Client fetch size alone is insufficient. Treat thresholds as experimental axes, not universal tuning defaults |
| [Microsoft design guidance](https://learn.microsoft.com/en-us/sql/relational-databases/indexes/columnstore-indexes-design-guidance?view=sql-server-ver17) | Columnstore suits broad scans; rowstore suits selective access; update/delete activity and small partitions can reduce benefits | Compare hot mutable, cold stable and selective workloads. Do not adopt advertised average compression ratios as dpone evidence |
| [CREATE COLUMNSTORE INDEX](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-columnstore-index-transact-sql?view=sql-server-ver17) | Syntax distinguishes clustered/nonclustered columnstore and archival compression; partition alignment and supported types constrain choices; ALTER table permission is required | Inspect types, server capabilities, topology and exact privileges before mutation. Reject unknown support, rather than trying DDL on the business target |
| [SQL Server 2022 editions](https://learn.microsoft.com/en-us/sql/sql-server/editions-and-components-of-sql-server-2022?view=sql-server-ver16), 16.x | Feature availability and resource ceilings depend on edition; Developer follows Enterprise features but is for development/test | A Developer result cannot certify Standard resource behavior. Record edition, version, compatibility level and online-operation support separately |
| [Index DDL disk requirements](https://learn.microsoft.com/en-us/sql/relational-databases/indexes/disk-space-requirements-for-index-ddl-operations?view=sql-server-ver17), updated 2026-07-20 | Builds/rebuilds can retain old and new structures simultaneously and require sorting/online-operation space | Preflight peak coexistence, not final compressed size; measure both database allocation and filesystem consumption |
| [Index transaction log space](https://learn.microsoft.com/en-us/sql/relational-databases/indexes/transaction-log-disk-space-for-index-operations?view=sql-server-ver17), updated 2026-07-20 | Index operations need log capacity through completion and rollback; concurrent work also consumes log | Keep log and tempdb budgets separate; do not change recovery models or force minimal logging automatically |
| [dbt SQL Server configurations](https://docs.getdbt.com/reference/resource-configs/mssql-configs), rolling docs | Describes columnstore default and post-hook index helpers | Useful discovery, but reject arbitrary hooks in dpone; source at the pinned adapter tag is the execution authority |
| [dbt-sqlserver table materialization](https://github.com/dbt-msft/dbt-sqlserver/blob/v1.11.1/dbt/include/sqlserver/macros/materializations/models/table/table.sql), v1.11.1 | Multiple build paths exist; prebuilt can drop the existing target, while the normal path builds an intermediate relation and renames | Admission must identify the exact materialization branch. Reject destructive prebuilt target replacement for the proposed safe snapshot contract |
| [dbt-sqlserver incremental materialization](https://github.com/dbt-msft/dbt-sqlserver/blob/v1.11.1/dbt/include/sqlserver/macros/materializations/models/incremental/incremental.sql), v1.11.1 | Initial/full refresh and ongoing incremental work have different index and relation lifecycles | Test each branch; table-only admission must not imply steady incremental reconciliation support |
| [ClickHouse RENAME](https://clickhouse.com/docs/reference/statements/rename), rolling docs | Renaming multiple entities is not atomic | Do not certify a multi-rename implementation as atomic snapshot replacement |
| [ClickHouse EXCHANGE](https://clickhouse.com/docs/reference/statements/exchange), rolling docs | Atomic exchange is engine-dependent (Atomic and Shared database engines) | Negotiate engine capability; an ON CLUSTER statement alone does not prove cluster-wide atomic visibility |

## Benchmark design

Run only after an isolated, explicitly approved environment exists. Keep the
harness open source and independent of physical-design implementation: baseline
DDL can be generated by a bounded test harness, but must never be mistaken for
an admitted dpone production feature. No live run occurred during this research.

Use a seeded generator (`seed=2718`) with neutral relation `sample.events` and
columns `event_id`, `event_time`, `category`, `amount`, `payload`. Fix a UTC epoch,
integer distribution, decimal scale, null ratio and UTF-8 string generator.
Publish the generator version and SHA-256 of its parameters. Include Unicode,
null versus empty string, binary values, decimal boundaries and timestamp
precision. Use a separate deliberately unsupported-type case for negative
capability tests. Never sample a real source.

| Axis | Reproducible values / control |
|---|---|
| Synthetic rows | 0; 102,399; 102,400; 1,048,576; 4,194,304; larger tiers only after capacity review |
| Distribution | Low-cardinality repetitive values; seeded high-entropy payload; skewed categories; null ratios 0% and 20% |
| Workload | Cold immutable aggregate scan; hot repeated 5% update/delete cycles; selective key/range lookup; mixed scan/load contention |
| MSSQL storage | Heap NONE; clustered rowstore NONE/ROW/PAGE with same key; CCI ordinary; CCI archival experimental; NCCI plus rowstore deferred tier |
| Build order | CCI before bulk load; heap load then CCI build; heap stage then INSERT SELECT into existing CCI; archival rebuild after ordinary CCI |
| Load method | Existing bounded stream/bulk path; dbt pinned table materialization; baseline direct driver batch where supported; never compare different correctness guarantees without labeling them |
| Batch | 1,024; 102,399; 102,400; 1,048,576 delivered rows per effective load batch; record actual rowgroups and tail |
| Parallelism | 1 and 4 writers, controlled MAXDOP; reject oversubscribed cells under approved resource ceilings |
| Capacity | Baseline versus restricted memory/CPU; tempdb/log/data on known volumes; record free space, latency and recovery model without changing it implicitly |
| Partitioning | Unpartitioned core; four evenly split synthetic partitions in deferred tuning tier; record per-partition cardinality |
| Transfer format | Existing supported baseline first; approved CSV/columnar candidates later, identical logical records and types |
| Compression layer | Vary only one of storage, intermediate codec, wire codec or ClickHouse codec at a time, then selected interactions |
| ClickHouse | Fixed exact server build, database/table engine, ORDER BY, partitioning, codecs, insert/merge settings and topology |

Avoid a wasteful full Cartesian product: first screen storage/build order on
cold and hot datasets at fixed capacity, then examine batch/memory interactions
for nondominated choices. Execute each selected cell five times with one warmup,
randomized seeded order and recorded cooldown policy. Distinguish warm-cache
from cold-cache runs. Do not clear shared caches. Report median, p95 where the
sample supports it, every individual value, and variability; five replicates
are insufficient to infer reliable tail service-level guarantees.

Each cell has immutable workload/run/attempt identity, one explicit namespace,
a target lock, exact owned object inventory, and cleanup evidence. A failed
capacity preflight produces SKIP with cause, not a smaller undocumented dataset.
A limit breach terminates the cell and preserves its failure evidence.

## Measurements and correctness gates

| Category | Required measurements / gate |
|---|---|
| Correctness | Exact row count, unique-key multiplicity, typed canonical row multiset digest including nulls and Unicode; independent aggregate checks; old/new target visibility at publish boundaries |
| Throughput | Extract, spool, load, build, validate, publish and cleanup duration separately; rows/s and declared uncompressed bytes/s; end-to-end inclusive result |
| Queries | Fixed aggregate, range and point queries, result equality, elapsed latency and logical/physical reads; warm/cold regime explicit |
| CPU and memory | Server CPU time/utilization, client peak RSS, server memory grants/pressure, worker count; container and OS limits recorded |
| tempdb/log | Peak allocated and used bytes, log bytes generated, autogrowth events, blocked duration and rollback duration |
| Disk | Peak local staging plus source work objects, destination stage, old/new tables and rebuild scratch; final reserved/used bytes per index and partition; cleanup remainder |
| Columnstore quality | Rowgroup states, total/deleted rows, compressed size, trim reasons, delta-store size, segment distribution before/after maintenance |
| Transfer/ClickHouse | Logical and serialized bytes, wire bytes if measurable, peak local artifacts, final part bytes, merges, query correctness; codec and format versions |
| Operations | Blocking/deadlocks, timeouts, cancellation, maintenance/rebuild cost, retained objects, permissions failure and recoverability |

Storage amplification is `peak simultaneously occupied bytes / final result
bytes` only when both are measurable and the denominator is nonzero. Empty
results report N/A for that ratio. A claimed archive saving must include rebuild
CPU/time, peak-space cost and query regression, not just final table size.
Maintenance is a separate measured operation with its own preflight and lease;
never silently run rebuild/reorganize merely to improve benchmark appearance.

Proposed future evidence directory: `test_artifacts/synthetic-snapshot/<run>/`.
It will contain `environment.json`, `workload.json`, `measurements.jsonl`,
`correctness.json`, `objects.json`, `cleanup.json`, and a generated `report.md`.
These paths are a future artifact contract, not evidence that exists today.
Metrics must record unit, sampling period, scope and missing-value reason. No
credentials, user paths, source SQL containing private identifiers, or real rows
belong in this bundle.

## Decisions and measurable hypotheses

Prefer no automatic compression recommendation in the first release. Present
supported alternatives with the evidence scope and require explicit declarative
selection. Archive is a cold-data candidate only; PAGE and ordinary CCI remain
comparators even when archival produces the smallest file.

```yaml
axis: bounded_correct_snapshot_publication
scenario: synthetic full_refresh exceeds declared serialized byte budget
baseline: repository commit 6533c27fbf77c78a00b8013bcf72e2293e281e1f
metric: target mutations after breach; successful state advancement after breach
target: both zero
procedure: inject boundary crossing and retry at every pre-publish transition
artifact: test_artifacts/synthetic-snapshot/budget-boundaries/correctness.json
limitations: offline proof does not certify server transactions or resource peaks
```

Performance hypotheses use the same generator and exact environment. Acceptance
requires zero correctness mismatches; a proposed tuning recommendation additionally
requires a predeclared improvement threshold and allowable CPU/query regression
approved before measurement. No numeric performance advantage is claimed here.

Next: review the [physical-design proposal](feature-design-sqlserver-physical-design-v1.md)
and [budget/publication proposal](feature-design-snapshot-resource-safety-v1.md).
