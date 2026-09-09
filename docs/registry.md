# Registry

`dpone` uses registries to keep manifests compact while preserving explicit runtime wiring.

## Purpose

A registry stores reusable connection, naming, owner, and provider metadata. Manifests reference registry entries by stable identifiers instead of repeating the same configuration in every table definition.

## What belongs in a registry

- Connection identifiers and connection types.
- Source ownership metadata such as team, domain, and service owner.
- Default table naming conventions.
- Provider-specific defaults for API integrations.
- Environment-specific overrides that should not live directly in a manifest.

## What must not be stored in a registry

- Plain-text secrets.
- Ad-hoc runtime state.
- One-off transformation logic.
- Target table schema drift decisions that belong in schema evolution policy.

## Example

```yaml
connections:
  postgres_oltp:
    type: postgres
    connection_type: vault
    owner: payments

  mssql_dwh:
    type: mssql
    connection_type: vault
    owner: analytics

naming:
  landing_prefix: landing
  technical_prefix: __dpone__
```

A manifest can then reference `postgres_oltp` and `mssql_dwh` without duplicating the connection contract.

## Recommended practice

Keep registry entries small, stable, and environment-aware. A registry should make manifests easier to read; it should not become a hidden second manifest.

## Related docs

- [Manifest guide](manifests-variant-c.md)
- [Conventions](conventions.md)
- [Overrides](overrides.md)
- [State](state.md)
