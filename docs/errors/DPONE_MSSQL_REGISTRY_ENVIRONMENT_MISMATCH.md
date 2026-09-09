# DPONE_MSSQL_REGISTRY_ENVIRONMENT_MISMATCH

**Audience:** platform operators and GitOps authors.

dpone refused to use an MSSQL connection-registry snapshot because environment
identity did not match.

## What broke

One of:

1. A caller passed `env="prod"` with a registry snapshot whose `env` is `"dev"`.
2. The registry YAML declares `environment: dev` while the requested `--env` is
   `prod` (or another mismatch).

## Next step

1. Ensure `requested env == snapshot.env`.
2. When the registry document includes `environment`, keep it identical to the
   filename/`--env` value.
3. Rebuild the pack/reconcile artifact set for the intended environment.
