# DPONE_MSSQL_ASSET_URI_INVALID

**Audience:** pipeline authors and Airflow / platform operators.

dpone refused to publish an Airflow Asset URI for an MSSQL table because the
coordinates are incomplete or unstable. Build/check emits this code as a
**blocker** (fail-closed). Parse-time `build_asset_outlets` only catches provider
`ValueError` as a shield and must not be the primary gate.

Canonical AIP-60 shapes (Airflow `apache-airflow-providers-microsoft-mssql`):

```text
mssql://{host}:{port}/{database}/{schema}/{table}
mssql://{host}:{port}/{INSTANCE}/{database}/{schema}/{table}
```

- `{host}:{port}` is a **deployment-owned** `asset_authority`, never a
  `connection_ref` alias.
- Port is always explicit (default `1433`) so identity does not drift when the
  microsoft-mssql provider is present or absent.
- Path segments are percent-encoded.

## What broke

Usually one of:

1. Registry entry for the `connection_ref` has no `connection.asset_authority`
   (and no fallback `connection.host` / `connection.port`).
2. `sink.table.database` / `source.table.database` (or LoadConfig
   `target_database` / `source_database`) is missing.
3. An explicit inlet/outlet still uses a compact form such as
   `mssql://schema/table` or `mssql://database/schema/table`.
4. A coordinate still contains Jinja (`{{ ... }}`) — inference is blocked.
5. An explicit `mssql://` URI uses a host/port/instance that is not an approved
   physical authority from the active environment registry (wrong port, missing
   named instance, typo host, or ambiguous portless host).

## Next step

### 1. Set deployment-owned authority (SoT)

In the environment connection registry
(`.dpone/registry/connection-registries/<env>.yaml`):

```yaml
connections:
  mssql_example:
    type: mssql
    connection:
      asset_authority:
        host: sql-prod.internal   # stable server identity, NOT connection_ref
        port: 1433
        instance: null            # optional named instance
    credentials:
      resolver: airflow_connection
      connection_id: mssql_example
      execution_mode: operator_bridge
```

Invariant: every `connection_ref` that points at the same physical SQL Server
must share the same `asset_authority`.

Fallback (also valid): omit nested `asset_authority` and set
`connection.host` / `connection.port` / optional `connection.instance`.

Optional assertion on a managed source/sink (must match registry; not an
override):

```yaml
sink:
  type: mssql
  connection_ref: mssql_example
  lineage:
    asset_authority:   # assertion only — must equal registry authority
      host: sql-prod.internal
      port: 1433
```

### 2. Complete table coordinates

```yaml
sink:
  type: mssql
  connection_ref: mssql_example
  table:
    database: dwh_example          # or LoadConfig target_database
    schema: assortment_planning
    name: example_forecast
```

### 3. Re-check

```bash
dpone check workloads/<domain>/pipelines/<pipeline_id>
```

Expected outlet URI:

```text
mssql://sql-prod.internal:1433/dwh_example/assortment_planning/example_forecast
```

Named instance example:

```text
mssql://sql-prod.internal:1433/MSSQL01/dwh_example/assortment_planning/example_forecast
```

## Good / bad examples

| URI | Result |
| --- | --- |
| `mssql://sql-prod.internal:1433/dwh_example/dbo/orders` | OK |
| `mssql://sql-prod.internal:1433/INST/dwh_example/dbo/orders` | OK (named instance) |
| `mssql://sql-prod.internal/dwh_example/dbo/orders` | OK — normalized to `:1433` (pack emits only canonical) |
| `mssql://mssql_example/dwh_example/dbo/orders` | Bad — `connection_ref` as host |
| `mssql://sql-typo.internal:1433/dwh_example/dbo/orders` | Bad — host not in approved registry authorities |
| `mssql://dwh_example/dbo/orders` | Bad — compact form |
| `mssql://dbo/orders` | Bad — compact form |
| `mssql://sql-prod.internal:abc/dwh_example/dbo/orders` | Bad — malformed port |

Do not put credentials into Asset URIs. Runtime secrets stay in the operator
bridge / connection registry credential resolver.

[Domain-first error overview](index.md) ·
[Airflow pack provider](../airflow-pack-provider.md) ·
[Connection registry schema](../schemas/gitops/connection-registry.schema.json)
