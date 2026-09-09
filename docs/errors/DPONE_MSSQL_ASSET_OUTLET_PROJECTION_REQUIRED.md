# DPONE_MSSQL_ASSET_OUTLET_PROJECTION_REQUIRED

## Meaning

A strict Airflow deployment pack declares a logical MSSQL `asset_ref` outlet, but
the deployment-owned `mssql_asset_outlet_projection` is absent or does not cover
that ref. Asset events must not silently disappear.

## Typical causes

1. Deployment index was built without the MSSQL outlet projection step.
2. Projection was stripped/mutated after materialization.
3. Pack outlets were added after the deployment projection was computed.

## Remediation

1. Rebuild the environment deployment via
   `AirflowDeploymentProjectionService.materialize` (or the CI equivalent).
2. Confirm `deployment.json` and `airflow-index.json` both carry the same
   `dpone.mssql-asset-outlet-projection.v1` document.
3. Re-activate the deployment; do not hand-edit projection digests.
