# DPONE_LEGACY_CONNECTION_REGISTRY_ENTRY_FOUND

`dpone check --connections` found a legacy platform connection registry entry
that still uses `connection_type` or `vault_path`.

## Why It Blocks

The connection registry must describe a backend-neutral credential resolver
under `credentials.resolver`. For Vault-backed credentials, use `vault_kv` with
a logical Vault path, explicit field mapping, `version_policy`, and
`resolution_scope`.

## Migration Shape

Legacy entry:

```yaml
connections:
  mssql_dev:
    type: mssql
    connection_type: vault
    vault_path: dpone/dev/credentials/mssql_dev
```

Target entry:

```yaml
connections:
  mssql_dev:
    type: mssql
    connection:
      host: mssql.local
      port: 1433
      database: dwh
    credentials:
      resolver: vault_kv
      mount: kv
      kv_version: 2
      path: dpone/dev/credentials/mssql_dev
      fields:
        username: username
        password: password
      version_policy: latest
      resolution_scope: workload_start
```

## Ownership

Registry migration is platform-owned and manual. `dpone fix` intentionally does
not edit connection registries or credentials.

Plain-text check output shows the manual fix id:

```text
fix manual: migrate_legacy_connection_entry
```

The same text output also includes a redacted unified diff for the affected
registry entry. Secret-looking legacy values are replaced before the plan is
serialized or printed.

The fix line is a platform action hint, not an automatic remediation command.
Use the migration shape above and the redacted diff to update the registry
through the normal platform review process.
