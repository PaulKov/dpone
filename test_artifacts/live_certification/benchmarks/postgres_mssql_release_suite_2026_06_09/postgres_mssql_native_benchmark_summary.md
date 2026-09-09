# Native transfer benchmark certification

- generated_at: `2026-06-09T19:38:06.721901+00:00`
- failed: `False`
- certification_passed: `True`

| Scenario | Rows | Partitions | Export workers | Load workers | Run | Certification | PG -> MSSQL rps | MSSQL -> CH rps | Bottleneck |
| --- | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: | --- |
| `rows10000_p4_ew2_lw2` | 10,000 | 4 | 2 | 2 | passed | passed | 12,953.37 | 16,949.15 | `postgres_to_mssql.target_load_finalize` |
| `rows1000000_p4_ew2_lw2` | 1,000,000 | 4 | 2 | 2 | passed | passed | 89,493.47 | 29,307.47 | `mssql_to_clickhouse.source_export` |
| `rows10000000_p4_ew2_lw2` | 10,000,000 | 4 | 2 | 2 | passed | passed | 88,727.99 | 29,496.26 | `mssql_to_clickhouse.source_export` |

## How to interpret

- `PG -> MSSQL rps` is the certified native PostgreSQL COPY -> SQL Server bcp path.
- `MSSQL -> CH rps` is included when the suite also exercises the downstream ClickHouse leg.
- A scenario is release-ready only when both `Run` and `Certification` are `passed`.
- Use bottleneck phase diagnostics to decide whether to tune source export, target load/finalizer, or reconciliation.
