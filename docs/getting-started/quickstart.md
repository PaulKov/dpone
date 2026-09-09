# Quickstart

This is the shortest path from an installed package to a planned and executed
pipeline. It keeps credentials simple and points to deeper docs only after the
first manifest is understandable.

Prerequisites: Python 3.11 or 3.12, a shell where you can export environment
variables, and reachable PostgreSQL and MSSQL instances containing the source
table and target database. For a ready-made Docker environment, use
[First local pipeline](first-local-pipeline.md).

## 1. Install dpone

```bash
pip install "dpone[postgres,mssql]"
dpone --help
```

For all public connectors:

```bash
pip install "dpone[full]"
```

## 2. Create a manifest

Create `manifests/orders_to_landing.yaml`:

```yaml
name: orders_to_landing

source:
  type: postgres
  connection_id: postgres_oltp
  connection_type: env
  table:
    schema: public
    name: orders
  options:
    incremental_column: updated_at
    batch_size: 50000

sink:
  type: mssql
  connection_id: mssql_dwh
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
  connection_id: mssql_dwh
  connection_type: env
  table:
    schema: etl_state
    name: dpone_state

runtime:
  compatibility:
    legacy_runtime_connections: explicit_only
```

`explicit_only` is the temporary compatibility bridge for the direct
`connection_id`/`connection_type` credentials used in this minimal local
example. Production deployments should use logical `connection_ref` bindings;
see [Connections and credentials](../connections.md).

## 3. Provide credentials

For local development, environment variables are the most transparent option:

```bash
export DPONE_CONN_POSTGRES_OLTP_HOST=127.0.0.1
export DPONE_CONN_POSTGRES_OLTP_PORT=5432
export DPONE_CONN_POSTGRES_OLTP_DATABASE=app
export DPONE_CONN_POSTGRES_OLTP_USERNAME=app
export DPONE_CONN_POSTGRES_OLTP_PASSWORD=secret

export DPONE_CONN_MSSQL_DWH_HOST=127.0.0.1
export DPONE_CONN_MSSQL_DWH_PORT=1433
export DPONE_CONN_MSSQL_DWH_DATABASE=dwh
export DPONE_CONN_MSSQL_DWH_USERNAME=sa
export DPONE_CONN_MSSQL_DWH_PASSWORD=secret
export DPONE_CONN_MSSQL_DWH_TRUST_SERVER_CERTIFICATE=yes
```

For Airflow, Vault, or inline params, use [Credentials quickstart](credentials-quickstart.md) first and then the complete [Connections and credentials](../connections.md) reference.

## 4. Inspect before writing

```bash
dpone doctor --profile local
dpone plan manifests/orders_to_landing.yaml --format json
```

The plan shows the configured source table and columns, target and staging
policy, load strategy, schema-evolution policy, reconciliation policy, state
backend, and warnings. It validates and explains configuration; it does not
execute or materialize a source query.

## 5. Run

```bash
dpone run manifests/orders_to_landing.yaml --run-id orders_local --format json
```

Exit `0` means the process completed successfully. Exit `1` means execution
started but failed; exit `2` means the manifest or configuration was rejected
before execution. The JSON report on stdout contains the actual row counts and
errors from this run.

`dpone run-report` is a separate manual/synthetic artifact generator. It does
not read or summarize the preceding `dpone run`, so it is intentionally not
part of this execution journey.

## 6. Where to go next

Start from your route (source → sink) when you move from a hand-written manifest
to Airflow: pick the same source/sink pair in the
[source → sink matrix](../source-sink-matrix.md), then follow
[First Airflow DAG](first-airflow-dag.md).

| Need | Guide |
| --- | --- |
| Understand append/upsert/replace/XMin/CDC | [Load strategies](../load-strategies.md) |
| Run from CLI or Python | [Running pipelines](../run.md) |
| Pick a supported source -> sink combination | [Source -> sink matrix](../source-sink-matrix.md) |
| Configure MSSQL `bcp` safely | [MSSQL guide](../mssql.md) |
| Configure schema evolution behavior | [Schema evolution](../schema-evolution.md) |
| Prepare production gates | [Production readiness](../production-readiness.md) |
