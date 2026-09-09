# Manual Source -> Sink Integration Matrix

This runbook defines the manual integration category for validating every supported source -> sink pair across every public load strategy.

The normal CI gate remains fast. The full matrix is intentionally manual because it may start database containers, Kafka, Schema Registry, and heavier type/strategy datasets.

Credentials are not required for `mock_contract` or `mock_local`. They are required only for `vendor_live` cases that exercise real external systems such as BigQuery, managed APIs, or managed Kafka.

## What the matrix covers

The canonical matrix lives in `dpone.integration_matrix`.

Sources:

- PostgreSQL
- MSSQL / SQL Server
- ClickHouse
- Generic REST API
- Kafka bounded batch topic

Sinks:

- MSSQL / SQL Server
- PostgreSQL
- ClickHouse
- BigQuery
- Kafka topic

Load strategies:

- `full_refresh`
- `incremental_append`
- `incremental_merge`
- `replace`
- `partition_replace`
- `snapshot_diff`
- `scd2`
- `backfill`
- Postgres-only `xmin`
- Postgres/MSSQL `cdc`

That gives `25` source -> sink pairs and `200` strategy cases: `100` common base cases plus `25` `snapshot_diff` cases plus `60` DB-target `partition_replace`/`scd2`/`backfill` cases plus `5` Postgres `xmin` cases and `10` Postgres/MSSQL `cdc` cases.

## Manual CI workflow

GitHub Actions workflow:

```text
.github/workflows/integration-matrix.yml
```

Trigger it from GitHub Actions with **Run workflow**. It is not attached to `push` or `pull_request`.

Run modes:

| Run mode | External credentials | Behavior |
| --- | --- | --- |
| `mock_contract` | no | Run all 200 source -> sink x strategy contract cases without starting services: 100 common base cases plus `snapshot_diff`, DB-target `partition_replace`/`scd2`/`backfill`, Postgres `xmin`, and Postgres/MSSQL `cdc`. |
| `mock_local` | no | Start local services and run local/mock-capable cases. BigQuery target cases remain documented-contract only. |
| `vendor_live` | yes | Use caller-provided real managed/vendor services. |

Useful filters:

```text
source_filter=postgres,mssql
sink_filter=mssql,clickhouse
strategy_filter=incremental_merge,replace
case_id_filter=postgres_to_mssql__incremental_merge
```

Use `*` to run all cases.

## Local command

Run the manual preflight matrix locally:

```bash
DPONE_RUN_INTEGRATION=1 \
DPONE_RUN_INTEGRATION_MATRIX=1 \
uv run pytest -m integration_matrix tests/integration/matrix -q
```

Run a focused case:

```bash
DPONE_RUN_INTEGRATION=1 \
DPONE_RUN_INTEGRATION_MATRIX=1 \
DPONE_MATRIX_CASE_ID=postgres_to_mssql__incremental_merge \
uv run pytest -m integration_matrix tests/integration/matrix -q
```

Write case artifacts:

```bash
DPONE_RUN_INTEGRATION=1 \
DPONE_RUN_INTEGRATION_MATRIX=1 \
DPONE_MATRIX_ARTIFACT_DIR=test_artifacts/integration_matrix \
uv run pytest -m integration_matrix tests/integration/matrix -q
```

## Preflight, mock-local, and live execution

The first matrix layer is a credential-free preflight:

- each source -> sink guide exists;
- each case has install extras;
- each case has required live profiles;
- each case renders a minimal manifest fragment;
- each case writes a JSON artifact when `DPONE_MATRIX_ARTIFACT_DIR` is set.

The second layer is `mock_local`: it uses disposable local services and deterministic mock data. The matrix behavior model defaults to 10,000 source rows, 20% changed rows, 5% physical deletes, and 120 sparse wide columns per sampled row. It should cover Postgres, MSSQL, ClickHouse, Kafka, and REST mock paths without external credentials.

Live database/vendor execution is layered below the same case ids. This keeps the public matrix complete while allowing teams to run only the systems they can provision in a given CI environment.

For focused Postgres -> MSSQL native-transfer debugging, run the dedicated
service test before the full matrix:

```bash
docker compose -f docker/docker-compose.integration.yml up -d postgres mssql
DPONE_RUN_INTEGRATION=1 \
uv run pytest tests/integration/mssql/test_postgres_to_mssql_native_transfer_integration.py -q
```

This validates the concrete high-throughput path: PostgreSQL `COPY`,
`mssql-delimited` file artifacts, SQL Server `bcp`, staging-first finalization,
and `NULL`/empty-string/text-control-character fidelity. The focused gate also
checks MSSQL `incremental_merge` with the default `delete_insert` finalizer and
partitioned full refresh with deterministic partition metadata
(`transfer_partition_id`, partition bounds, export/load worker settings).

## Required profiles

| Profile | Meaning |
| --- | --- |
| `postgres_live` | PostgreSQL source/sink/state endpoint is available. |
| `mssql_live` | SQL Server endpoint, ODBC Driver 18, and `bcp` are available. |
| `clickhouse_live` | ClickHouse native/HTTP endpoint is available. |
| `bigquery_live` | GCP credentials and target dataset are available. |
| `kafka_live` | Kafka broker and Schema Registry are available. |
| `rest_mock` | Generic REST mock source is available. |

## Relationship to other gates

- [Integration tests](integration-tests.md) documents service-specific markers.
- [Testing runbooks](index.md) explains run modes and failure recovery.
- [Local MSSQL mock matrix](local-mssql-mock-matrix.md) documents SQL Server local wide-type strategy testing.
- [Connector certification](../connector-certification.md) summarizes connector capability artifacts.
- [Source -> sink matrix](../source-sink-matrix.md) links the per-flow runbooks.
- [Load strategies](../load-strategies.md) defines strategy semantics and staging rules.
