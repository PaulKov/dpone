# Native transfer benchmark artifact: 2026-06-09 Postgres -> MSSQL release suite

This page records the local-live certification run for the Postgres -> MSSQL
native fast path and the downstream MSSQL -> ClickHouse comparison leg.

## Execution metadata

| Field | Value |
| --- | --- |
| Date | 2026-06-09 |
| Actor | Codex GPT-5 |
| Environment | Local Docker services on macOS |
| Services | Postgres 16, SQL Server 2022, ClickHouse 24.8 |
| Native tools | Microsoft `bcp` 18.6, `sqlcmd`, ODBC Driver 18 |
| Runner | `tools/mssql_benchmark_suite.py` |
| Output directory | `test_artifacts/live_certification/benchmarks/postgres_mssql_release_suite_2026_06_09/` |
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
  --output-dir test_artifacts/live_certification/benchmarks/postgres_mssql_release_suite_2026_06_09 \
  --markdown-output test_artifacts/live_certification/benchmarks/postgres_mssql_release_suite_2026_06_09/postgres_mssql_native_benchmark_summary.md
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
| `rows10000_p4_ew2_lw2` | 10,000 | 4 | 2 | 2 | passed | passed | 12,953.37 | 16,949.15 | `postgres_to_mssql.target_load_finalize` |
| `rows1000000_p4_ew2_lw2` | 1,000,000 | 4 | 2 | 2 | passed | passed | 89,493.47 | 29,307.47 | `mssql_to_clickhouse.source_export` |
| `rows10000000_p4_ew2_lw2` | 10,000,000 | 4 | 2 | 2 | passed | passed | 88,727.99 | 29,496.26 | `mssql_to_clickhouse.source_export` |

## Postgres -> MSSQL phase split

| Rows | Source export seconds | Source export rps | Target load/finalize seconds | Target load/finalize rps | Total seconds | Total rps |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10,000 | 0.044 | 227,643.72 | 0.728 | 13,742.09 | 0.772 | 12,953.37 |
| 1,000,000 | 0.720 | 1,389,654.81 | 10.454 | 95,654.37 | 11.174 | 89,493.47 |
| 10,000,000 | 8.015 | 1,247,736.86 | 104.689 | 95,521.13 | 112.704 | 88,728.00 |

Interpretation:

- PostgreSQL `COPY` is not the bottleneck in this profile.
- SQL Server `bcp` load plus staging finalization dominates Postgres -> MSSQL runtime at 1M and 10M.
- The 10M profile remains above the current 80k rows/sec certification target for Postgres -> MSSQL.

## Correctness checks

| Check | 10k | 1M | 10M |
| --- | --- | --- | --- |
| Row volume at or above policy | passed | passed | passed |
| MSSQL final count equals source rows | passed | passed | passed |
| ClickHouse final count equals source rows | passed | passed | passed |
| Postgres -> MSSQL certification | passed | passed | passed |
| MSSQL -> ClickHouse comparison certification | passed | passed | passed |

## Tuning notes

For Postgres -> MSSQL, the next tuning pass should focus on SQL Server:

- `bulk.bcp.batch_size` and transaction sizing;
- SQL Server recovery/log throughput;
- staging table indexes and constraints;
- `bulk.bcp.table_lock` and table-level lock budget;
- finalizer policy and target index maintenance.

For MSSQL -> ClickHouse, the bottleneck remains SQL Server `bcp queryout`, not
ClickHouse HTTP ingest. The ClickHouse load/finalize phase processed 10M rows
in 5.535 seconds in this profile.

## Reproduction

1. Start local services:

```bash
docker compose -f docker/docker-compose.integration.yml up -d postgres mssql clickhouse
```

2. Ensure SQL Server database exists:

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
- [Native transfer benchmark artifact: 2026-06-08](native-transfer-benchmarks-2026-06-08.md)
- [CI/CD workflows](../cicd/workflows.md)
- [CI/CD runbooks](../cicd/runbooks.md)
