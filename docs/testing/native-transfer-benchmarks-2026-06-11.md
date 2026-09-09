# Native transfer benchmark artifact: 2026-06-11 Postgres -> MSSQL release suite

This page records the local-live certification run after the latest Postgres ->
MSSQL native-transfer hardening. The run validates the critical Postgres ->
MSSQL path and keeps the downstream MSSQL -> ClickHouse comparison leg in the
same evidence chain.

## Execution metadata

| Field | Value |
| --- | --- |
| Date | 2026-06-11 |
| Actor | Codex GPT-5 |
| Environment | Local Docker services on macOS |
| Services | Postgres 16, SQL Server 2022, ClickHouse 24.8 |
| Native tools | Microsoft `bcp` 18.6, `sqlcmd`, ODBC Driver 18 |
| Runner | `tools/mssql_benchmark_suite.py` |
| Output directory | `test_artifacts/live_certification/benchmarks/postgres_mssql_release_suite_2026_06_11/` |
| Result | passed |

## Command

```bash
PYTHONUNBUFFERED=1 uv run python tools/mssql_benchmark_suite.py \
  --rows 10000,1000000,10000000 \
  --partitions 4 \
  --export-workers 2 \
  --load-workers 2 \
  --batch-size 100000 \
  --bcp-path /opt/homebrew/bin/bcp \
  --optimizer-profile high_throughput_safe \
  --clickhouse-bulk-mode http \
  --clickhouse-http-host 127.0.0.1 \
  --clickhouse-http-port 58123 \
  --output-dir test_artifacts/live_certification/benchmarks/postgres_mssql_release_suite_2026_06_11 \
  --markdown-output test_artifacts/live_certification/benchmarks/postgres_mssql_release_suite_2026_06_11/postgres_mssql_native_benchmark_summary.md
```

## Generated artifacts

| Artifact | Purpose |
| --- | --- |
| `summary.json` | Machine-readable suite result and certification status. |
| `postgres_mssql_native_benchmark_summary.md` | Human-readable release review summary. |
| `rows10000_p4_ew2_lw2.json` | 10k scenario metrics and diagnostics. |
| `rows1000000_p4_ew2_lw2.json` | 1M scenario metrics and diagnostics. |
| `rows10000000_p4_ew2_lw2.json` | 10M scenario metrics and diagnostics. |

## Certification summary

| Scenario | Rows | Partitions | Export workers | Load workers | Run | Certification | PG -> MSSQL rps | MSSQL -> CH rps | Bottleneck |
| --- | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: | --- |
| `rows10000_p4_ew2_lw2` | 10,000 | 4 | 2 | 2 | passed | passed | 13,966.48 | 19,305.02 | `postgres_to_mssql.target_load_finalize` |
| `rows1000000_p4_ew2_lw2` | 1,000,000 | 4 | 2 | 2 | passed | passed | 91,199.27 | 32,860.15 | `mssql_to_clickhouse.source_export` |
| `rows10000000_p4_ew2_lw2` | 10,000,000 | 4 | 2 | 2 | passed | passed | 95,737.76 | 33,149.90 | `mssql_to_clickhouse.source_export` |

## Postgres -> MSSQL phase split

| Rows | Source export seconds | Source export rps | Target load/finalize seconds | Target load/finalize rps | Total seconds | Total rps |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10,000 | 0.066 | 152,394.31 | 0.650 | 15,385.22 | 0.716 | 13,966.48 |
| 1,000,000 | 0.834 | 1,199,264.67 | 10.131 | 98,702.98 | 10.965 | 91,199.27 |
| 10,000,000 | 6.871 | 1,455,317.99 | 97.581 | 102,479.26 | 104.452 | 95,737.76 |

Interpretation:

- PostgreSQL `COPY` is not the bottleneck in this profile.
- SQL Server `bcp` load plus staging/finalization dominates the Postgres ->
  MSSQL leg at 1M and 10M.
- The 10M Postgres -> MSSQL profile is above the current 80k rows/sec
  certification target in this local Docker run.
- The downstream MSSQL -> ClickHouse comparison is dominated by SQL Server
  `bcp queryout`; ClickHouse HTTP ingest itself processed 10M rows in 3.915s.

## Correctness checks

| Check | 10k | 1M | 10M |
| --- | --- | --- | --- |
| Row volume at or above policy | passed | passed | passed |
| MSSQL final count equals source rows | passed | passed | passed |
| ClickHouse final count equals source rows | passed | passed | passed |
| Postgres -> MSSQL certification | passed | passed | passed |
| MSSQL -> ClickHouse comparison certification | passed | passed | passed |

## Tuning notes

For Postgres -> MSSQL, the next tuning pass should continue to focus on SQL
Server target-side mechanics:

- `bulk.bcp.batch_size` and transaction sizing;
- SQL Server recovery/log throughput;
- staging table indexes and constraints;
- `bulk.bcp.table_lock` and table-level lock budget;
- finalizer policy and target index maintenance.

For MSSQL -> ClickHouse, the 10M run shows that source-side SQL Server export is
the slowest comparison leg. If this leg is release-critical, prioritize
`bcp queryout` partitioning, SQL Server temp/log IO, packet size and source
predicate/index tuning before changing ClickHouse ingest.

## Reproduction

1. Start local services:

```bash
docker compose -f docker/docker-compose.integration.yml up -d postgres mssql clickhouse
```

2. Ensure SQL Server benchmark database exists:

```bash
/opt/homebrew/bin/sqlcmd \
  -S 127.0.0.1,51433 \
  -U sa \
  -P 'Dp0ne.Strong.Pw.2026!' \
  -C \
  -Q "IF DB_ID('dpone') IS NULL CREATE DATABASE [dpone];"
```

3. Run the benchmark command above.

4. Review `postgres_mssql_native_benchmark_summary.md` first, then inspect the
scenario JSON for phase-level diagnostics.

## Related docs

- [Performance guide](../performance.md)
- [Postgres -> MSSQL](../source-sink/postgres-to-mssql.md)
- [Native transfer benchmark artifact: 2026-06-09](native-transfer-benchmarks-2026-06-09.md)
- [Native transfer benchmark artifact: 2026-06-08](native-transfer-benchmarks-2026-06-08.md)
- [CI/CD workflows](../cicd/workflows.md)
- [CI/CD runbooks](../cicd/runbooks.md)
