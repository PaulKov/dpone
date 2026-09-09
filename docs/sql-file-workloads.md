# SQL file transform workloads

`dpone` supports server-side SQL transform workloads for the pattern:

```text
SELECT-only SQL file -> sink staging -> lineage projection -> quality gates -> atomic finalize
```

This is the recommended OSS replacement for handwritten Airflow operators that
run a large ClickHouse SQL file and then perform separate governance steps.
The manifest owns the workload contract, while Airflow only renders the static
`airflow-pack.json` task graph.

## Manifest

```yaml
source:
  type: clickhouse
  connection_type: airflow
  connection_id: ClickHouse
  table:
    schema: Example_Datamarts
    name: wide_mart_crm_query
  options:
    query:
      mode: sql_file
      sql_file: ../../sql/clickhouse/wide_mart_crm_select.sql
      execution:
        mode: server_side
      render:
        engine: jinja
        context:
          sources:
            mes_task: Example_Datamarts.example__ch__activity_fact
            mes_call: Example_Datamarts.example__ch__contact_fact
            dim_task: Example_Datamarts.example__ch__activity_dimension
            dim_call: Example_Datamarts.example__ch__contact_dimension
      readonly: true

sink:
  type: clickhouse
  connection_type: airflow
  connection_id: ClickHouse
  table:
    schema: Example_Datamarts
    name: wide_mart_crm
  strategy:
    mode: full_refresh
    overwrite_type: exchange
  options:
    lineage:
      enabled: true
      preset: standard
    load_governance:
      enabled: true
      finalization_phase: pre_finalize
      audit:
        enabled: true
        state_schema: etl_state
```

Production manifests should use `sql_file`. Inline SQL stays available for
small snippets and tests.

## Runtime Algorithm

1. Resolve `sql_file` relative to the manifest directory.
2. Reject paths that escape the repository root.
3. Render Jinja context from the manifest.
4. Reject non-read-only or multi-statement SQL.
5. Discover and revalidate the rendered query schema with
   `DESCRIBE SELECT * FROM (<query>)`. The declarative `source.table.name` is a
   workload identity placeholder for this mode and is never inspected through
   `system.columns`.
6. Stage data with `INSERT INTO <staging> SELECT <columns> FROM (<query>)`.
7. Project `__dpone__*` lineage columns in staging.
8. Run quality gates before target mutation.
9. Finalize atomically and cleanup staging artifacts.

The ClickHouse implementation uses `SqlQueryArtifact`, so rows are not
materialized in Python. Future databases can implement the same artifact and
stager contracts without changing Airflow DAG code.

## Airflow Pack

`dpone gitops airflow pack` includes SQL files as workload dependencies:

```json
{
  "workload_dependencies": [
    {
      "kind": "sql_file",
      "path": "dpone_workloads/sql/clickhouse/wide_mart_crm_select.sql",
      "sha256": "..."
    }
  ]
}
```

The compact pack bootstrap archive contains both the manifest and SQL file.
The pack fingerprint changes when the SQL changes, so stale packs are visible
in review and CI.

Airflow DAG code should stay thin:

```python
from dpone_airflow_pack import build_dpone_gitops_task_group_from_pack

build_dpone_gitops_task_group_from_pack(
    "/opt/airflow/dags/.dpone/gitops/airflow/wide_mart_crm/airflow-pack.json",
    dag=dag,
)
```

No repository-local `ClickhouseLoaderOperator` or governance helper is needed.

## Source Preparation

If a transform needs a source-side calculation first, use dbt-style
`source.options.hooks.pre_hook` with `execution.airflow: separate_task`.
The compact pack renders that hook as a visible Airflow task, then the runtime
KPO runs the dpone transfer with already-executed Airflow-separate hooks
skipped. CLI and Python API runs still execute hooks inline.

Hooks support the same SQL-as-file discipline:

```yaml
source:
  options:
    hooks:
      pre_hook:
        - id: refresh_source_snapshot
          kind: source_refresh
          type: sql
          connector: source
          sql_file: ../../sql/mssql/refresh_source_snapshot.sql
          mutates_source: true
          execution:
            airflow: separate_task
```

`sql_file` is resolved relative to the manifest, blocked from escaping the
repository, loaded before mutating-SQL validation, and embedded into
`workload_dependencies` with a SHA-256 hash. This keeps large procedure or
ClickHouse preparation scripts out of YAML while preserving reviewability,
Airflow task visibility and stale-pack detection.

## Product Notes

- dbt-style SQL-as-file and Jinja ergonomics are used, but dpone adds typed
  staging, lineage, DQ and atomic finalization.
- Astronomer Cosmos-style compiled artifacts are used through
  `airflow-pack.json`, avoiding heavy Airflow parse-time generation.
- Airflow remains an orchestrator of visible tasks, not a home for handwritten
  database loader operators.
- Informatica, Pentaho and SSIS expose SQL task plus table output patterns;
  dpone turns that pattern into an auditable OSS workload contract.
