# First local pipeline

Use this page when you want a realistic local database pipeline without
external vendor credentials. The tutorial uses PostgreSQL and MSSQL; the same
integration stack also provides MySQL, ClickHouse, Kafka, Schema Registry, and
MinIO for the next routes.

## Start local services

```bash
docker compose -f docker/docker-compose.integration.yml up -d postgres mysql mssql clickhouse kafka schema-registry minio
docker compose -f docker/docker-compose.integration.yml ps
```

Default local endpoints:

| Service | Endpoint |
| --- | --- |
| PostgreSQL | `127.0.0.1:55432` |
| MySQL | `127.0.0.1:53306` |
| MSSQL | `127.0.0.1:51433` |
| ClickHouse native | `127.0.0.1:59000` |
| ClickHouse HTTP | `127.0.0.1:58123` |
| Kafka | `127.0.0.1:59092` |
| Schema Registry | `http://127.0.0.1:58081` |
| MinIO | `127.0.0.1:59090` |

## Bootstrap test data

Create a PostgreSQL table:

```bash
PGPASSWORD=dpone psql \
  -h 127.0.0.1 \
  -p 55432 \
  -U dpone \
  -d dpone_it \
  -c "CREATE SCHEMA IF NOT EXISTS demo;"

PGPASSWORD=dpone psql \
  -h 127.0.0.1 \
  -p 55432 \
  -U dpone \
  -d dpone_it \
  -c "DROP TABLE IF EXISTS demo.orders;
      CREATE TABLE demo.orders (
        order_id bigint PRIMARY KEY,
        customer_id bigint,
        amount numeric(18, 2),
        status text,
        updated_at timestamptz
      );
      INSERT INTO demo.orders
      SELECT i, 1000 + i, i * 1.25, CASE WHEN i % 2 = 0 THEN 'paid' ELSE 'new' END, now()
      FROM generate_series(1, 1000) AS i;"
```

Create the target database in MSSQL if needed:

```bash
sqlcmd -S 127.0.0.1,51433 -U sa -P 'Dp0ne.Strong.Pw.2026!' -C \
  -Q "IF DB_ID('dpone_it') IS NULL CREATE DATABASE [dpone_it];"
```

## Configure local credentials

```bash
export DPONE_CONN_POSTGRES_DEMO_HOST=127.0.0.1
export DPONE_CONN_POSTGRES_DEMO_PORT=55432
export DPONE_CONN_POSTGRES_DEMO_DATABASE=dpone_it
export DPONE_CONN_POSTGRES_DEMO_USERNAME=dpone
export DPONE_CONN_POSTGRES_DEMO_PASSWORD=dpone

export DPONE_CONN_MSSQL_DEMO_HOST=127.0.0.1
export DPONE_CONN_MSSQL_DEMO_PORT=51433
export DPONE_CONN_MSSQL_DEMO_DATABASE=dpone_it
export DPONE_CONN_MSSQL_DEMO_USERNAME=sa
export DPONE_CONN_MSSQL_DEMO_PASSWORD='Dp0ne.Strong.Pw.2026!'
export DPONE_CONN_MSSQL_DEMO_TRUST_SERVER_CERTIFICATE=yes
```

## Create a manifest

Create `manifests/local_postgres_to_mssql.yaml`:

```yaml
name: local_postgres_to_mssql

source:
  type: postgres
  connection_id: postgres_demo
  connection_type: env
  table:
    schema: demo
    name: orders
  options:
    incremental_column: updated_at

sink:
  type: mssql
  connection_id: mssql_demo
  connection_type: env
  table:
    schema: landing
    name: orders
  strategy:
    mode: incremental_merge
    unique_key: order_id
  options:
    bulk:
      mode: bcp
    schema_evolution:
      enabled: true

state:
  type: mssql
  connection_id: mssql_demo
  connection_type: env
  table:
    schema: etl_state
    name: dpone_state

runtime:
  compatibility:
    legacy_runtime_connections: explicit_only
```

This local tutorial explicitly enables the temporary compatibility bridge for
direct `connection_id` credentials. Use logical `connection_ref` bindings for
production deployments.

## Plan and run

```bash
dpone doctor --profile local
dpone plan manifests/local_postgres_to_mssql.yaml --format json
dpone run manifests/local_postgres_to_mssql.yaml --format json
```

The `run` JSON is the authoritative result of this execution. The separate
`dpone run-report` command creates a manual/synthetic artifact and does not
support `--latest`.

## Validate the result

```bash
sqlcmd -S 127.0.0.1,51433 -U sa -P 'Dp0ne.Strong.Pw.2026!' -C -d dpone_it \
  -Q "SELECT COUNT(*) AS rows_loaded FROM [landing].[orders];"
```

## Next steps

- Try [Postgres -> MSSQL](../source-sink/postgres-to-mssql.md) with larger batches and reconciliation.
- Try [MSSQL -> ClickHouse](../source-sink/mssql-to-clickhouse.md) for direct analytical loading.
- Learn the safe MSSQL bulk format rules in the [MSSQL guide](../mssql.md).
