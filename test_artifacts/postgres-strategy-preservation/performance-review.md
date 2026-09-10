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

## Earlier measurements retained for comparison

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

The second nine-case experiment on the locally integrated source
`69f465a3a3b20e7ba34e5c97eceb13f182953c2d` also passed. Its
[raw report](performance-69f465a/report.json) and
[verification receipt](verification-performance-69f465a.json) retain the
observations. At 5m rows the load median was 11.578 seconds (10.234–13.414),
the lock median 7.414 seconds (6.944–7.459), and COUNT median 0.115 seconds.
The PostgreSQL implementation and benchmark producer were unchanged between
these two experiments. Cache state and host contention were uncontrolled;
the timing difference cannot be attributed to a code improvement or the merge.

## Published source and review lineage

The original draft MR #31 conflicted with the merged Airflow 0.77.0 baseline
`e15ad32b850708207c2f8f1b6faf597ef0f5b0b1`. The local merge was rejected by
the branch's linear-history rule, and its history cannot be force-pushed.
[Replacement draft MR #32](https://github.com/PaulKov/dpone/pull/32) preserves
the complete resolved tree in a linear commit:
`bef37a7752db43dcae42298da7bd62509a186535`, with the baseline as its only parent.
The independent reviewer checked that both commits have identical tree
`a6bcd5501d4f9683af719fe7126c0b7bde658625`. Repository protections were preserved.
The local merge's receipt remains attributed to that local source; the final
campaign below is separately attributed to the published linear commit.

The fresh-context reviewer issued **APPROVE** for `4b6fe37`, closing the
snapshot metric finding. They independently checked all 45 direct evidence
file hashes and the nine-case benchmark with its verifier. They also checked
the integration diff, schema semantics, unchanged PostgreSQL execution paths,
the second benchmark and direct campaign, and independently ran 50 focused
tests. Final published-source evidence and documentation review follow below.

## Final published-source campaign

Source: `bef37a7752db43dcae42298da7bd62509a186535`. The
[42-case direct campaign](verification-direct-bef37a7.json) and
[nine-case performance campaign](verification-performance-bef37a7.json) both
passed; the [raw report](performance-bef37a7/report.json) contains all attempts.
The environment and fixture match the method above. Execution inputs and
producers remained frozen throughout the run.

| Rows | Load seconds, median (min–max) | Exclusive lock seconds, median (min–max) | Old-child COUNT seconds, median (min–max) |
| --- | --- | --- | --- |
| 100,000 | 0.272 (0.233–0.293) | 0.186 (0.165–0.215) | 0.003 (0.003–0.007) |
| 1,000,000 | 1.937 (1.723–2.369) | 1.425 (1.250–1.551) | 0.020 (0.019–0.021) |
| 5,000,000 | 12.059 (11.680–15.395) | 8.139 (7.985–9.435) | 0.143 (0.112–0.166) |

At 5m rows the COUNT took approximately 1.4–2.0% of each measured lock interval.
The final series is a separate observation, not evidence that the implementation
became faster. Across all three complete campaigns, 27 attempts passed exact
row, payload, metric, untouched-partition, parent OID, cleanup and blocked-reader
checks. The first campaign's slower observations remain relevant to planning.
There is no cold-cache, percentile, production SLA or before/after regression
claim. Long reader waits remain an operational limitation of this lock scope.

## Final validation status

| Check | Status | Evidence and limits |
| --- | --- | --- |
| Focused regression and version contracts | PASS | 89 cases on resolved tree; `merged-focused.log`; published tree is identical |
| Direct PostgreSQL | PASS | 42 cases on published source; `verification-direct-bef37a7.json` |
| Large partitions | PASS | Nine attempts on published source; `verification-performance-bef37a7.json` |
| Ruff and format | PASS | `merged-ruff.log`, `merged-format.log`; identical published tree |
| Mypy | PASS | `linear-mypy.log`: 1,166 source files, clean-cache run |
| Imports, layers and module size | PASS | `merged-imports.log`, `merged-layers.log`, `linear-module-size-corrected.log` |
| Documentation, generated references and compatibility | PASS | `performance-docs-final.log`, `performance-docs-language-final.log`, `performance-mkdocs-final.log`, `performance-generated-final.log`, `performance-compatibility-final.log`; strict MkDocs passed |
| Agent-control governance | PASS | `performance-agent-governance-gate.json`; source-bound local gate, separate from the CI attestation |
| Final independent review | PASS | Fresh-context read-only `/root/postgres_final_independent_review`: APPROVE on `bef37a7752db43dcae42298da7bd62509a186535` and final docs/evidence; no actionable defects |
| Final MR CI | UNVERIFIED | Normal CI is running on replacement MR #32; historical source receipts are not final-head passes |
| Other live routes and release | N/A | This follow-up exercises the approved local PostgreSQL environment; no new Kubernetes campaign or publication |

The initial incremental mypy run crashed while resolving a cached OpenTelemetry
symbol. The clean-cache run passed without source or dependency changes. The
first module-size invocation used symbolic refs and was rejected by the checker;
the corrected invocation used full base/head SHAs and passed. Failed command
logs are retained. No check was weakened to obtain a pass.

Documentation now covers the operational lock cost, measured capacity,
reproduction and limits. The snapshot correction changes public row accounting
only: hard deletions cannot inflate loaded rows; inserted/updated behavior and
manifest fields remain unchanged. No migration is required. All review findings
have corrections and are closed by the independent reviewer. The result is ready
for maintainer review; merge remains contingent on the final CI and owner
acceptance. Release readiness is outside this campaign.

The final reviewer independently reran both published-source verifiers,
validated all 45 direct evidence hashes, checked the two documentation tables
against raw summaries and confirmed parent/tree equivalence. Benchmark report
SHA256: `63af92aa46ec82dc783c0ea179cda36bed8e4df22e7eaadadf341f78cedb7c99`.
The reviewer made no edits, skipped duplicate heavy/live suites, and relied on
the integrator's final documentation gates. Their 50 focused tests passed on
the identical resolved tree. This approval does not assert CI success or
human-owner acceptance.

## Reproduce the performance campaign

Use the approved campaign container described above. From the repository root,
after committing all execution inputs and benchmark producers:

```bash
uv run --no-sync python test_artifacts/postgres-strategy-preservation/partition_benchmark.py --output test_artifacts/postgres-strategy-preservation/performance-new
SOURCE_SHA="$(git rev-parse HEAD)"
uv run --no-sync python test_artifacts/postgres-strategy-preservation/verify_partition_benchmark.py test_artifacts/postgres-strategy-preservation/performance-new/report.json --source-sha "$SOURCE_SHA" --output test_artifacts/postgres-strategy-preservation/verification-performance-new.json
```

The producer refuses an existing output directory. Use a different name for
each experiment and keep HEAD, execution inputs and producer files unchanged
throughout the run. Expected result: nine passing cases and a PASS receipt;
measurements and correctness observations are in `report.json`. The synthetic
schema is removed after the run; the dedicated PostgreSQL container remains.
