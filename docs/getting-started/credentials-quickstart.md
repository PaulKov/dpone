# Credentials quickstart

This page is the short version. Use it to choose a credential provider quickly, then move to the full [Connections and credentials](../connections.md) guide when you need connector-specific fields or runbooks.

## Provider decision table

| Provider | Use when | Good for |
| --- | --- | --- |
| `env` | Local development, CI smoke jobs, containers | Clear copy-paste setup |
| `params` | Tests, examples, generated manifests | Self-contained demos |
| `airflow` | Airflow DAG deployments | Reusing Airflow Connections |
| `vault` | Production secrets in HashiCorp Vault | Centralized secret rotation |

## Environment variables

Use `connection_type: env` and a normalized connection id.

```yaml
source:
  type: postgres
  connection_id: postgres_oltp
  connection_type: env
```

```bash
export DPONE_CONN_POSTGRES_OLTP_HOST=127.0.0.1
export DPONE_CONN_POSTGRES_OLTP_PORT=5432
export DPONE_CONN_POSTGRES_OLTP_DATABASE=app
export DPONE_CONN_POSTGRES_OLTP_USERNAME=app
export DPONE_CONN_POSTGRES_OLTP_PASSWORD=secret
```

## Inline params

Use `params` for demos, tests, generated examples, or local notebooks. Do not commit real secrets.

```yaml
source:
  type: api
  api_type: rest
  connection_id: demo_api
  connection_type: params
  credentials:
    base_url: https://api.example.com
    token: "${DEMO_API_TOKEN}"
```

## Airflow Connections

Use `connection_type: airflow` when dpone runs inside Airflow and should resolve connection ids from Airflow metadata.

```yaml
sink:
  type: mssql
  connection_id: mssql_dwh
  connection_type: airflow
```

Runtime-only Kubernetes pods do not install `apache-airflow`. In that mode
dpone first tries Airflow `BaseHook` when it is available, then falls back to the
standard Airflow URI environment variable:

```bash
AIRFLOW_CONN_MSSQL_DWH='mssql://etl:secret@sql.example.com:1433/analytics_staging'
```

`dpone gitops airflow runtime-profile` discovers `connection_type: airflow`
manifest refs and, by default, emits a Kubernetes Secret bridge contract. The
generated `pod-contract` injects env refs such as `AIRFLOW_CONN_MSSQL_DWH` from
the configured Secret; dpone artifacts serialize only Secret names and keys,
never connection URI values. Use `--airflow-runtime-mode airflow_image` only for
custom images that explicitly install Airflow and its providers.

## Vault

Use `connection_type: vault` when secrets are stored in HashiCorp Vault through `vault-kv-client`.

```yaml
sink:
  type: clickhouse
  connection_id: clickhouse_dwh
  connection_type: vault
  vault_path: databases/clickhouse_dwh
```

For new Airflow self-service pipelines, use a logical `connection_ref` instead
of putting `vault_path` in pipeline authoring. The platform-owned binding-set
and connection registry select Vault at runtime. The only currently certified
policy shape is `version_policy: latest` with
`resolution_scope: workload_start` — glossary:
[Airflow pipeline glossary](airflow-pipeline-glossary.md#related-enums-and-scopes);
runbook:
[credential lifecycle and recovery](../airflow-credential-resolver-lifecycle.md).

## Connector coverage

The shared credential flow supports all first-class source/sink families:

| Family | Env | Params | Airflow | Vault |
| --- | --- | --- | --- | --- |
| PostgreSQL | yes | yes | yes | yes |
| MSSQL | yes | yes | yes | yes |
| ClickHouse | yes | yes | yes | yes |
| BigQuery | yes | yes | yes | yes |
| Kafka | yes | yes | yes | yes |
| REST API | yes | yes | yes | yes |

## Next steps

- Use the complete [Connections and credentials](../connections.md) guide for field names, examples, and troubleshooting.
- For Airflow runtime rotation and recovery, use the [credential resolver lifecycle](../airflow-credential-resolver-lifecycle.md) runbook.
- Run `dpone doctor --profile local` before a first run.
- Keep secrets out of manifests unless they are placeholders resolved by environment variables.
