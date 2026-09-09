# DPONE_LEGACY_CONNECTION_CONFIG_FOUND

`dpone check` found legacy `connection_type` or `vault_path` fields in a
pipeline authoring source.

## Why It Blocks

Airflow self-service pipelines must use `connection_ref` in the authoring
source. Resolver details such as Vault paths belong to the platform-owned
connection registry. This keeps the pipeline portable, keeps secrets out of
authoring files, and prevents Airflow parse from depending on Vault or other
credential systems.

## Safe Fix

Preview the source-only migration first:

```bash
dpone fix pipelines/orders_daily --plan
```

The text plan includes a redacted unified diff, so it is safe to review in a
terminal, CI log, or pull-request comment. Secret-looking legacy values are not
printed, and the command does not change `pipeline.yaml`.

Apply it after reviewing the plan:

```bash
dpone fix pipelines/orders_daily --apply
```

`dpone fix` only edits the pipeline authoring source. It does not edit
connection registries, binding-sets, credentials, runtime infrastructure,
release-sets, deployment-sets, or published cache artifacts.

## Platform Follow-Up

Make sure the referenced `connection_ref` exists in the environment
`binding-set` and platform `connection-registry`.
