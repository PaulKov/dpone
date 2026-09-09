# DPONE_CONNECTION_REF_INVALID

`dpone check --connections` found a `connection_ref` that is not a safe logical
alias.

Use a short backend-neutral alias such as `mssql_dev`, `clickhouse_dev`, or
`sales.pg_prod`. A `connection_ref` is not a Vault path, Airflow connection URI,
Kubernetes Secret reference, environment assignment, or secret value.

Allowed characters:

- letters and digits;
- dot `.`;
- underscore `_`;
- hyphen `-`.

The alias must start and end with a letter or digit and be at most 128
characters long.

Fix the alias in the pipeline source, `binding-set.yaml`, or
`connection-registry.yaml`, then rerun:

```bash
dpone check pipelines/orders_daily --connections --environment prod
```

Do not put credentials or secret backend paths in authoring files. Use
`connection_ref` in the pipeline and let the platform-owned binding-set and
connection registry resolve it at runtime.
