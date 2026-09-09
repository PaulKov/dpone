# DPONE_MSSQL_REGISTRY_SOURCE_AMBIGUOUS

**Audience:** platform operators.

dpone found MSSQL connection registries for the same environment in both:

- `.dpone/registry/connection-registries/<env>.yaml`
- `platform/connection-registries/<env>.yaml`

Deployment-owned Asset authority must have exactly one source of truth.

## Next step

Keep only one of the two files for that environment, then retry pack/reconcile.
