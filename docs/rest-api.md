# Generic REST Pull API

`api_type: rest` is a self-service JSON pull source for APIs that do not need a dedicated provider package.

```yaml
source:
  type: api
  api_type: rest
  connection_type: vault
  vault_path: api/example_provider
  options:
    method: GET
    path: /v1/orders
    records_path: data.items
    pagination:
      type: cursor
      cursor_param: cursor
      next_cursor_path: data.next_cursor
      max_pages: 100
    columns:
      - {name: id, type: bigint}
      - {name: updated_at, type: datetime2}
      - {name: payload, type: nvarchar(max)}

sink:
  type: mssql
  connection_type: vault
  connection_id: mssql-dwh
  vault_path: mssql/demo-dwh
  table: {schema: landing_api, name: example_orders}
  strategy: {mode: incremental_append, only_new_rows: true}
```

Vault secret fields:

- `endpoint` or `base_url`
- `token` / `bearer_token` for Authorization Bearer
- `api_key` for `X-API-Key` by default
- `username` and `password` for basic auth

Supported pagination modes in v1:

- `page`: sends a page parameter.
- `offset`: sends offset and optional limit parameters.
- `cursor`: sends a cursor and reads the next cursor from response JSON.

The generic source intentionally supports JSON only. XML, GraphQL, and OAuth refresh-token flows should be implemented as dedicated API providers.

## Schema evolution with REST sources

REST sources should define `source.options.columns` explicitly when the API
contract matters. Sink-side schema evolution is enabled by default, so newly
declared nullable columns can be added automatically to MSSQL, PostgreSQL,
ClickHouse, or BigQuery targets.

If an API field changes type and you do not want to block ingestion, configure
the sink with:

```yaml
sink:
  options:
    schema_evolution:
      on_type_change: new_column
```

The changed field is then loaded into `__dpone__nc__<field>`. See
[Schema evolution](schema-evolution.md) for runbooks and the complete matrix.
