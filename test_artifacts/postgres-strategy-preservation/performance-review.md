# Large-partition performance and independent review follow-up

The maintainer requested further refinement, another independent subagent
review, and large-partition performance tests in the available local Docker
environment. The root agent remains the only writer.

## Independent finding and correction

Fresh-context read-only reviewer `/root/postgres_final_independent_review`
reviewed `1266b38c76c20497f3860c9d1a981539c0abe392` against
`d5ad9aaecc900c24df421b160ed36b4cfc726e45`. Verdict: **REQUEST CHANGES**, one P2.
PostgreSQL `snapshot_diff` populated `replaced_rows` with missing-key deletions;
the newly preserved field became `loaded_rows` in the public projection. A
three-old/two-new case reported three loaded rows; an empty snapshot could report
deleted rows as loaded. See `postgres_production.py:40` at the reviewed source.

The correction moves hard deletions to `hard_deleted_rows` while retaining
inserted/updated accounting and preserving all result fields. The regression
first failed in `snapshot-metrics-red.log`. New memory/file real-row cases cover
changed snapshots, replay, empty snapshots and empty replay. No common metric
projection or other engine changed. Compatibility documentation and the approved
design explain the bounded correction; no migration or ADR is needed.

## Performance method

Producer: [partition_benchmark.py](partition_benchmark.py), with
[timing and attribution support](partition_benchmark_support.py). The producer
uses only the already running, campaign-owned `dpone-pg-preservation-ec30`
container and uniquely named `bench_pg_*` schemas. It never stops Docker or
changes another workload. Connection secrets stay in memory.

- PostgreSQL creates 100,000, 1,000,000 and 5,000,000 synthetic business rows.
- Each row has a bigint key, 128-byte text payload and partition date. The target
  has a primary key and a CHECK constraint; 1,000 rows in another child remain
  outside the replacement scope.
- Each size runs once with changed payloads and twice with identical input.
- The real `PostgresSink.load` uses `native_mode: required`; no runtime SQL or
  row count is substituted. Timers observe the production connector calls.
- Measure sink elapsed time, exclusive-lock acquisition to commit acknowledgement,
  the exact old-child count, and a concurrent reader's response. The observer
  verifies that PostgreSQL actually reports the reader blocked by the sink.
- After commit, an independent connection checks all payload values, unique-key
  row coverage, counts, untouched rows, parent OID and staging cleanup. Those
  verification queries are outside the load timer.
- Record source/producer hashes, Docker/PostgreSQL/Python identity and settings.
  Refuse a run with changed execution inputs or producer bytes. A run directory
  is never overwritten.

The warm-up smoke at `1266b38` is retained in
[performance-smoke-1266b38/report.json](performance-smoke-1266b38/report.json).
It passed for 100k rows; it is not the final candidate measurement.

Performance summaries report min/median/max over three observations, without
p95 or cold-cache claims. A PASS establishes completed execution, measurements
and exact fixture correctness, not compliance with an unspecified production SLA.
The lock timer includes a separately measured observer probe before commit.

## Final evidence

The complete first large-partition campaign at
`4b6fe37498d54e386f222443c33e97bb03f2eb8b` passed all nine cases, and its
[verification receipt](verification-performance-4b6fe37.json) independently
checks the complete matrix, source/producer hashes, row truth, operation order,
reader observations and summary arithmetic. The [raw report](performance-4b6fe37/report.json)
contains every attempt, SQL timing and environment setting. The
[42-case direct campaign](verification-direct-4b6fe37.json) also passed.

| Rows | Load seconds, median (min–max) | Exclusive lock seconds, median (min–max) | Old-child COUNT seconds, median |
| --- | --- | --- | --- |
| 100,000 | 0.389 (0.354–0.598) | 0.267 (0.259–0.330) | 0.003 |
| 1,000,000 | 3.780 (2.940–12.155) | 2.789 (2.073–6.207) | 0.023 |
| 5,000,000 | 63.875 (55.391–71.458) | 40.254 (37.995–47.591) | 0.984 |

Environment: PostgreSQL 16.15, Python 3.12.11, psycopg 3.3.4, ARM64 Docker VM
with 10 CPUs and 7.75 GiB RAM; no container CPU/memory quota. PostgreSQL fsync,
full-page writes and synchronous commit were enabled. The 5m-row child occupied
1,029,464,064 bytes including indexes. Caches were not flushed and the host was
not exclusively reserved. Variation across attempts is retained.

The observed old-child COUNT took 0.852–1.244 seconds for 5m rows, about
2.2–2.6% of the measured lock interval. This is the duration of that call,
including its round-trip, not an A/B estimate of whole-load slowdown.
The reader duration includes its own SELECT scan after commit and is not a
pure lock-wait metric. The exclusive lock interval is separately measured.

Latest-head Actions were absent because the MR conflicts with the newly merged
Airflow 0.77.0 baseline (`e15ad32`), not because the benchmark failed. The branch
will incorporate that baseline, retain these exact-source observations, and run
new final-source evidence. Integration and final independent review are pending;
no merge to master, release or production SLA claim is made.
