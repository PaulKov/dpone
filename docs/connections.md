# Connections and credentials

This guide describes every supported way to pass source, sink, and state
credentials into `dpone`.

`dpone` normalizes all connection providers into one runtime DTO:
`CredentialsConfig`. Runtime connectors should not know whether a secret came
from environment variables, Airflow, Vault, or inline params.

## Supported providers

| `connection_type` | Best for | Secret storage | Notes |
| --- | --- | --- | --- |
| `env` | local dev, Docker, GitHub Actions, Kubernetes secrets | process environment | Recommended for OSS examples and CI without Vault. |
| `airflow` | Airflow production deployments | Airflow Connections | Default runtime provider when `connection_type` is omitted in direct factory usage. |
| `vault` | enterprise production | HashiCorp Vault-compatible KV | Uses the public `vault-kv-client` package. |
| `params` | tests, smoke runs, generated examples | inline JSON string | Do not commit real secrets with this mode. |

## Connector coverage matrix

| Runtime family | `env` | `airflow` | `vault` | `params` |
| --- | --- | --- | --- | --- |
| Postgres source/sink/state | yes | yes | yes | yes |
| MSSQL source/sink/state | yes | yes | yes | yes |
| ClickHouse source/sink | yes | yes | yes | yes |
| BigQuery sink/state | yes | yes | yes | yes |
| Kafka source/sink/state offsets | yes | yes | yes | yes |
| Generic REST API source | yes | yes | yes | yes |

Specialized API connectors can keep their own credential contract, but generic
REST uses the same four providers. For a specialized API connector that only
implements `from_vault()`, use `connection_type: vault` until that connector
adds `from_credentials()`.

## Manifest shape

Every source and sink uses the same connection fields:

```yaml
source:
  type: postgres
  connection_id: postgres_source
  connection_type: env

sink:
  type: mssql
  connection_id: mssql_dwh
  connection_type: env
```

For Vault, add the secret location:

```yaml
source:
  type: postgres
  connection_id: postgres_source
  connection_type: vault
  vault_mount_point: secret
  vault_path: postgres/source
```

For `params`, `connection_id` is a JSON object:

```yaml
sink:
  type: mssql
  connection_type: params
  connection_id: >-
    {"host":"localhost","port":1433,"database":"dpone","username":"sa","password":"secret","driver":"ODBC Driver 18 for SQL Server","trust_server_certificate":"yes","bcp_path":"bcp"}
```

## Environment variables

The environment provider uppercases `connection_id` and reads fields from the
canonical `DPONE_CONN_<CONNECTION_ID>_<FIELD>` namespace.

Use underscores in `connection_id` values, for example `mssql_dwh`, so shell
variable names stay portable.

Legacy `<CONNECTION_ID>_<FIELD>` and `DPONE_<CONNECTION_ID>_<FIELD>` names are
still accepted during migration, but new manifests, docs, and generated
scaffolds use `DPONE_CONN_`.

### Postgres env example

```bash
export DPONE_CONN_POSTGRES_SOURCE_HOST=localhost
export DPONE_CONN_POSTGRES_SOURCE_PORT=5432
export DPONE_CONN_POSTGRES_SOURCE_DATABASE=dpone
export DPONE_CONN_POSTGRES_SOURCE_USERNAME=dpone
export DPONE_CONN_POSTGRES_SOURCE_PASSWORD='secret'
export DPONE_CONN_POSTGRES_SOURCE_SCHEMA=public
```

```yaml
source:
  type: postgres
  connection_id: postgres_source
  connection_type: env
```

### MSSQL env example

```bash
export DPONE_CONN_MSSQL_DWH_HOST=localhost
export DPONE_CONN_MSSQL_DWH_PORT=1433
export DPONE_CONN_MSSQL_DWH_DATABASE=dpone
export DPONE_CONN_MSSQL_DWH_USERNAME=sa
export DPONE_CONN_MSSQL_DWH_PASSWORD='secret'
export DPONE_CONN_MSSQL_DWH_DRIVER='ODBC Driver 18 for SQL Server'
export DPONE_CONN_MSSQL_DWH_ENCRYPT=yes
export DPONE_CONN_MSSQL_DWH_TRUST_SERVER_CERTIFICATE=yes
export DPONE_CONN_MSSQL_DWH_BCP_PATH=/opt/mssql-tools18/bin/bcp
```

```yaml
sink:
  type: mssql
  connection_id: mssql_dwh
  connection_type: env
```

### ClickHouse env example

```bash
export CLICKHOUSE_DWH_HOST=localhost
export CLICKHOUSE_DWH_PORT=9000
export CLICKHOUSE_DWH_DATABASE=dpone
export CLICKHOUSE_DWH_USERNAME=default
export CLICKHOUSE_DWH_PASSWORD='secret'
export CLICKHOUSE_DWH_SECURE=false
export CLICKHOUSE_DWH_COMPRESSION=true
export CLICKHOUSE_DWH_CONNECT_TIMEOUT=10
export CLICKHOUSE_DWH_SEND_RECEIVE_TIMEOUT=300
export CLICKHOUSE_DWH_SETTINGS='{"max_threads":4}'
```

```yaml
sink:
  type: clickhouse
  connection_id: clickhouse_dwh
  connection_type: env
```

### BigQuery env example

Use either inline service-account JSON:

```bash
export BIGQUERY_DWH_PROJECT_ID=demo-project
export BIGQUERY_DWH_SERVICE_ACCOUNT_INFO='{"type":"service_account","project_id":"demo-project","client_email":"svc@example.com","private_key":"-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"}'
```

Or a key file path:

```bash
export BIGQUERY_DWH_PROJECT_ID=demo-project
export BIGQUERY_DWH_SERVICE_ACCOUNT_KEY_FILE=/secrets/bigquery-service-account.json
```

```yaml
sink:
  type: bigquery
  connection_id: bigquery_dwh
  connection_type: env
```

### Kafka env example

```bash
export KAFKA_CLUSTER_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_CLUSTER_SECURITY_PROTOCOL=SASL_SSL
export KAFKA_CLUSTER_SASL_MECHANISM=PLAIN
export KAFKA_CLUSTER_SASL_USERNAME=dpone
export KAFKA_CLUSTER_SASL_PASSWORD='secret'
export KAFKA_CLUSTER_SCHEMA_REGISTRY_URL=http://localhost:8081
export KAFKA_CLUSTER_SCHEMA_REGISTRY_USERNAME=dpone
export KAFKA_CLUSTER_SCHEMA_REGISTRY_PASSWORD='secret'
```

```yaml
source:
  type: kafka
  connection_id: kafka_cluster
  connection_type: env
```

### Generic REST env example

```bash
export REST_ORDERS_ENDPOINT=https://api.example.com
export REST_ORDERS_TOKEN='bearer-token'
export REST_ORDERS_API_KEY='api-key-if-needed'
```

```yaml
source:
  type: api
  api_type: rest
  connection_id: rest_orders
  connection_type: env
  options:
    path: /v1/orders
    records_path: data.items
```

## Airflow Connections

Airflow provider reads standard fields from `BaseHook.get_connection()` and
connector-specific settings from connection `extra` JSON.

### Supported Airflow `conn_type` values

| Runtime family | Airflow `conn_type` values |
| --- | --- |
| Postgres | `postgres` |
| MSSQL | `mssql`, `microsoft mssql`, `sqlserver`, `odbc` |
| ClickHouse | `clickhouse`, `ch` |
| BigQuery | `bigquery`, `google_cloud_platform`, `gcp`, `google_cloud` |
| Kafka | `kafka`, `confluent` |
| Generic REST | `http`, `https`, `rest`, `api`, `generic_rest` |

### Runtime-only Airflow env bridge

The official dpone runtime image does not install `apache-airflow`. When
`BaseHook` is unavailable, `AirflowCredentialsProvider` parses the standard
Airflow URI environment variable instead:

```bash
export AIRFLOW_CONN_MSSQL_DWH='mssql://etl:secret@sql.example.com:1433/analytics_staging?driver=ODBC%20Driver%2018%20for%20SQL%20Server'
```

The GitOps Airflow runner pack owns this bridge for KubernetesPodOperator and
KubernetesPodExecutor deployments. `dpone gitops airflow runtime-profile`
discovers manifest refs with `connection_type: airflow`, emits the
`connection_bridge` contract, and `pod-contract` injects env refs from a
Kubernetes Secret by default:

```bash
dpone gitops airflow runtime-profile .dpone/gitops/bundle/bundle.json \
  --image ghcr.io/acme/dpone:2026.06.17 \
  --airflow-connection-bridge k8s_secret \
  --airflow-connection-secret dpone-airflow-connections \
  --airflow-runtime-mode runtime_only
```

Artifacts contain only Secret names and keys such as
`AIRFLOW_CONN_MSSQL_DWH`; they never contain connection URI values. Use
`--airflow-connection-bridge env` only when another pod policy already injects
the required env vars. Use `--airflow-runtime-mode airflow_image` for custom
images that explicitly install Airflow and providers.

### MSSQL Airflow extra

```json
{
  "driver": "ODBC Driver 18 for SQL Server",
  "encrypt": "yes",
  "trust_server_certificate": "yes",
  "connect_timeout": 10,
  "query_timeout": 300,
  "multi_subnet_failover": "yes",
  "application_intent": "ReadOnly",
  "transparent_network_ip_resolution": "no",
  "bcp_path": "/opt/mssql-tools18/bin/bcp"
}
```

`connect_timeout` is passed to `pyodbc.connect(timeout=...)`. The ODBC
connection string allowlist accepts `multi_subnet_failover`,
`application_intent`, and `transparent_network_ip_resolution`; other raw extra
keys are ignored by the MSSQL connector. Use `multi_subnet_failover=yes` for
SQL Server Always On availability group listeners.

### ClickHouse Airflow extra

```json
{
  "secure": false,
  "compression": true,
  "connect_timeout": 10,
  "send_receive_timeout": 300,
  "settings": {"max_threads": 4}
}
```

### BigQuery Airflow extra

Inline service account:

```json
{
  "project_id": "demo-project",
  "keyfile_dict": {
    "type": "service_account",
    "project_id": "demo-project",
    "client_email": "svc@example.com",
    "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
  }
}
```

Key file path:

```json
{
  "project_id": "demo-project",
  "keyfile_path": "/opt/airflow/secrets/bigquery-service-account.json"
}
```

### REST Airflow extra

```json
{
  "endpoint": "https://api.example.com",
  "token": "bearer-token",
  "api_key": "api-key-if-needed"
}
```

## Vault secrets

Vault secrets use the same logical field names as env and params. Common aliases
are also accepted for database fields.

```json
{
  "host": "localhost",
  "port": 1433,
  "database": "dpone",
  "username": "sa",
  "password": "secret",
  "driver": "ODBC Driver 18 for SQL Server",
  "trust_server_certificate": "yes",
  "bcp_path": "bcp"
}
```

Database aliases accepted by Vault:

| Canonical field | Vault aliases |
| --- | --- |
| `host` | `db_host` |
| `port` | `db_port` |
| `database` | `db_database` |
| `username` | `user`, `db_user` |
| `password` | `db_password` |

BigQuery Vault fields:

```json
{
  "project_id": "demo-project",
  "service_account_info": {
    "type": "service_account",
    "project_id": "demo-project",
    "client_email": "svc@example.com",
    "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
  }
}
```

Generic REST Vault fields:

```json
{
  "endpoint": "https://api.example.com",
  "token": "bearer-token",
  "api_key": "api-key-if-needed"
}
```

## Params JSON

`params` is useful for tests and smoke runs. Avoid real secrets in committed
manifests.

Postgres:

```yaml
source:
  type: postgres
  connection_type: params
  connection_id: >-
    {"host":"localhost","port":5432,"database":"dpone","username":"dpone","password":"secret"}
```

MSSQL:

```yaml
sink:
  type: mssql
  connection_type: params
  connection_id: >-
    {"host":"localhost","port":1433,"database":"dpone","username":"sa","password":"secret","driver":"ODBC Driver 18 for SQL Server","trust_server_certificate":"yes","bcp_path":"bcp"}
```

ClickHouse:

```yaml
sink:
  type: clickhouse
  connection_type: params
  connection_id: >-
    {"host":"localhost","port":9000,"database":"dpone","username":"default","password":"secret","secure":false,"settings":{"max_threads":4}}
```

BigQuery:

```yaml
sink:
  type: bigquery
  connection_type: params
  connection_id: >-
    {"project_id":"demo-project","service_account_key_file":"/secrets/bigquery-service-account.json"}
```

Kafka:

```yaml
sink:
  type: kafka
  connection_type: params
  connection_id: >-
    {"bootstrap_servers":"localhost:9092","schema_registry_url":"http://localhost:8081"}
```

Generic REST:

```yaml
source:
  type: api
  api_type: rest
  connection_type: params
  connection_id: >-
    {"endpoint":"https://api.example.com","token":"bearer-token"}
  options:
    path: /v1/orders
    records_path: data.items
```

## Runbooks

### Incomplete credentials

Check the canonical required fields:

| Runtime family | Required fields |
| --- | --- |
| Postgres | `host`, `database`, `username`, `password` |
| MSSQL | `host`, `database`; `username/password` optional for trusted auth |
| ClickHouse | `host`, `database`, `username` |
| BigQuery | `project_id` plus `service_account_info` or `service_account_key_file` |
| Kafka | `bootstrap_servers` |
| Generic REST | `endpoint` through credentials or `source.options.endpoint` |

### Env connection works locally but fails in CI

Run:

```bash
dpone doctor --profile ci --format md
```

Then verify the exact uppercase prefix. For `connection_id: mssql_dwh`, the
canonical prefix is `DPONE_CONN_MSSQL_DWH_`. If older automation still exports
`MSSQL_DWH_` or `DPONE_MSSQL_DWH_`, keep it only as a temporary migration
fallback.

### Airflow connection extra is ignored

Validate that `extra` is valid JSON. Invalid JSON is intentionally ignored with a
warning so a broken extra block does not crash unrelated imports.

### REST source still asks for Vault

Generic REST supports all providers. Specialized API connectors may still only
support Vault until they implement `from_credentials()`. For new API connectors,
implement both `from_vault()` and `from_credentials()`.

### BigQuery cannot find credentials

Use one of these two patterns:

```bash
BIGQUERY_DWH_SERVICE_ACCOUNT_INFO='{"project_id":"demo-project",...}'
```

or:

```bash
BIGQUERY_DWH_SERVICE_ACCOUNT_KEY_FILE=/absolute/path/key.json
```

If both are present, key file path takes precedence in the runtime factory.

## Developer contract for new connectors

New connectors should accept a normalized `CredentialsConfig` and provide a small
factory method when credentials are not passed directly:

```python
class ExampleConnector:
    @classmethod
    def from_credentials(cls, credentials: CredentialsConfig, **kwargs):
        return cls(
            endpoint=credentials.endpoint,
            token=credentials.token,
            **kwargs,
        )
```

This keeps provider-specific logic inside `dpone.runtime.credentials` and avoids
copying Vault/env/Airflow parsing into connector modules.
