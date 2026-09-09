# Integration tests

`dpone` uses several integration-test layers so contributors can get fast feedback locally while maintainers can still certify real production paths.

## Test layers

| Layer | Marker | Services | Purpose |
|---|---|---|---|
| Unit/contract | none | none | Fast regression tests for planners, factories, SQL builders, codecs, docs contracts, and CLI output. |
| Mock integration | `integration_matrix_mock` | local fakes or Docker services | Source/sink strategy coverage without vendor credentials. |
| Local service integration | `integration_mssql`, `integration_postgres`, `integration_clickhouse`, `integration_kafka` | Docker services | Real database or Kafka behavior with generated fixtures. |
| Replay integration | `integration_replay` | injected local clients by default | Recovery/control-plane coverage for replay adapter ordering, reconciliation, and state commit. |
| Vendor live integration | `integration_live` | external vendor APIs | Connector certification against real providers. |
| Full matrix | `integration_matrix` | Docker plus optional live services | Manual source/sink/strategy matrix coverage. |

## Default local gate

```bash
uv sync --all-extras
uv run pytest -m "not integration_live and not integration_matrix"
```

## Manual source/sink matrix

The matrix runner is split into contract, local, and vendor layers. The contract layer validates all 200 source -> sink -> strategy cases and deterministic strategy semantics without credentials: 100 common base cases plus 25 `snapshot_diff` cases plus 60 DB-target `partition_replace`/`scd2`/`backfill` cases plus Postgres `xmin` and Postgres/MSSQL `cdc`. The local layer runs local/mock-capable cases with Docker services where available. The vendor layer is reserved for managed services and real external APIs.

The matrix registry covers supported source/sink pairs and load strategies. The manual workflow can run against mocked local services without external credentials for Postgres, MSSQL, ClickHouse, Kafka, and REST mock APIs.

```bash
uv run pytest tests/integration/matrix -m integration_matrix_mock
```

For the full manual matrix:

```bash
uv run pytest tests/integration/matrix -m integration_matrix
```

See [Testing runbooks](index.md).

## Local MSSQL integration

The MSSQL tests validate pyodbc connectivity, staging-first loads, bcp command generation, state storage, reconciliation, and type handling.

```bash
DPONE_RUN_INTEGRATION=1 \
uv run pytest tests/integration -m integration_mssql
```

Use Docker SQL Server 2022 for reproducible local runs.

## Local Postgres integration

Postgres tests cover COPY-style flows, XMin state, CDC contracts, schema evolution, and target-side reconciliation.

```bash
DPONE_RUN_INTEGRATION=1 \
uv run pytest tests/integration -m integration_postgres
```

## Local ClickHouse integration

ClickHouse tests cover native insert paths, staging/shadow table behavior, replacing-table delete semantics, and direct TSV/HTTP loading.

```bash
DPONE_RUN_INTEGRATION=1 \
uv run pytest tests/integration -m integration_clickhouse
```

## Local Kafka integration

Kafka tests use local Kafka and Schema Registry when available. They cover JSON, JSON Schema, Avro, Protobuf contracts, bounded offset windows, tombstones, and producer delivery behavior.

```bash
DPONE_RUN_INTEGRATION=1 \
uv run pytest tests/integration -m integration_kafka
```

## Replay integration

Replay tests validate `dpone resync` and `dpone resume` execution paths without requiring vendor credentials. They use injected local clients by default and assert that staging validation, finalization, reconciliation, and state commit happen in the correct order.

```bash
DPONE_RUN_INTEGRATION_REPLAY=1 \
uv run pytest -m integration_replay tests/integration/replay -q
```

See [Replay integration](replay-integration.md) for the full runbook and artifact layout.

## REST mock integration

REST mock tests should use local HTTP servers and deterministic fixtures. Use them for pagination, auth policy, schema inference, error retries, and incremental cursor behavior.

## Vendor live tests

Vendor live tests are manual or scheduled. They require real credentials, vendor availability, and certification-level observability artifacts.

```bash
DPONE_RUN_INTEGRATION=1 \
DPONE_RUN_INTEGRATION_LIVE=1 \
uv run pytest tests/integration -m integration_live
```

Do not make ordinary pull-request CI depend on external vendor APIs.

## Artifacts

Long-running or certification tests must write an artifact with:

- Unique name and date.
- Runner and environment.
- Exact command.
- Source/sink/strategy coverage.
- Row counts, throughput, warnings, and failures.
- Links to run reports and logs.

Recommended path:

```text
test_artifacts/<category>/<YYYY-MM-DD>_<slug>.md
```

## Troubleshooting

If a local integration test fails:

1. Run `dpone doctor --profile local`.
2. Confirm Docker services are healthy.
3. Check native clients such as `bcp`, `sqlcmd`, and `clickhouse-client`.
4. Re-run the smallest failing marker with `-vv`.
5. Save the failing command and diagnostics in the artifact or issue.

## Related docs

- [Testing](overview.md)
- [Manual integration matrix](manual-integration-matrix.md)
- [Testing runbooks](index.md)
- [Connector certification](../connector-certification.md)
