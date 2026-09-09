# DPONE_MSSQL_ASSET_OUTLET_PROJECTION_MISMATCH

## Meaning

The MSSQL outlet projection disagrees with deployment identity: environment,
`binding_set_ref`, `connection_registry_ref`, `projection_sha256`, or the
deployment↔index mirror. Independently valid projection A in `deployment.json`
and B in `airflow-index.json` is also a mismatch — activation parses each side
separately before accepting `deployment_id`.

## Typical causes

1. `projection_sha256` was edited without recomputing the body digest.
2. Index projection was copied from a different environment deployment.
3. `asset_ref_sha256` does not match the JCS digest of `asset_ref`.
4. Deployment and index projections were patched independently (both digest-valid
   but not byte-identical).
5. Projection `uri` database/schema/table disagree with `asset_ref` (host/port/
   instance stay deployment-authority owned; relation must match).

## Remediation

1. Rebuild the deployment so projection is written into `deployment.json`
   **before** `deployment_id` and mirrored into `airflow-index.json` under the
   closed v3 wire pair.
2. Never hand-edit digests; treat the closed document as content-addressed.
