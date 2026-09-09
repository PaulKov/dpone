# Route certify release

`dpone ops route-certify-release` builds one release-level go/no-go report from
immutable per-route certification bundles.

Use it after [Route certify](route-certify.md) has produced
`route_certification_bundle.json` for every first-class route that the release
claims as certified. The command is a control-plane gate: it reads existing
artifacts, verifies route/profile identity, and writes release review files.
Use [Route release finalize](route-release-finalize.md) when the release also
needs bundle discovery, freshness, provenance, regression, and history checks.

It does not run route tests, Docker, database services, or `route-certify`.

## Required first-class routes

The default release matrix requires these route bundles:

| Route case id | Expected bundle |
| --- | --- |
| `postgres_to_mssql__incremental_merge` | Postgres -> MSSQL `incremental_merge` route certification bundle. |
| `mssql_to_clickhouse__incremental_merge` | MSSQL -> ClickHouse `incremental_merge` route certification bundle. |

Pass `--route` one or more times to override the required matrix for a custom
release train.

## OSS-safe mode

OSS-safe mode is credential-free and suitable for ordinary CI. It accepts
certified route bundles created with local or OSS profiles.

```bash
uv run dpone ops route-certify-release \
  --release v0.9.0-rc1 \
  --profile oss_ci \
  --output-dir test_artifacts/route_certification_release \
  --route-bundle postgres_to_mssql__incremental_merge=test_artifacts/route_certify/postgres_to_mssql/route_certification_bundle.json \
  --route-bundle mssql_to_clickhouse__incremental_merge=test_artifacts/route_certify/mssql_to_clickhouse/route_certification_bundle.json \
  --format json
```

## vendor-live mode

vendor-live mode is opt-in and stricter. Every required bundle must have been
created with `profile=vendor_live`; otherwise the release report fails with a
`<route>.profile_mismatch` blocker.

```bash
uv run dpone ops route-certify-release \
  --release v0.9.0-rc1 \
  --profile vendor_live \
  --output-dir test_artifacts/route_certification_release/vendor_live \
  --route-bundle postgres_to_mssql__incremental_merge=test_artifacts/route_certify/postgres_to_mssql/route_certification_bundle.json \
  --route-bundle mssql_to_clickhouse__incremental_merge=test_artifacts/route_certify/mssql_to_clickhouse/route_certification_bundle.json \
  --format json
```

## Outputs

| File | Description |
| --- | --- |
| `route_certification_release.json` | Stable schema `dpone.route_certification_release.v1` for CI, release gates, and automation. |
| `route_certification_release.md` | Human-readable route matrix, blockers, warnings, artifact index, and Operator runbook. |
| `route_certification_release_notes.md` | Short release-note fragment for GitHub releases or release reviews. |

## Policy

The release fails closed when any required route bundle is missing, malformed,
not certified, profile-mismatched for `vendor_live`, or embeds a different route
identity than the route key passed on the CLI.

Optional bundles are included in the report. A blocked optional route creates a
warning rather than a blocker.

## Operator runbook

Failure: `<route>.missing`.

1. Run `dpone ops route-certify` for the missing route.
2. Attach the generated `route_certification_bundle.json` with
   `--route-bundle <route>=<path>`.
3. Re-run `dpone ops route-certify-release`.

Failure: `<route>.not_certified`.

1. Open the route bundle and inspect its `blockers`.
2. Fix upstream route readiness, live evidence, refresh verification, state
   promotion, or release evidence issues.
3. Regenerate the route bundle; do not edit generated JSON manually.

Failure: `<route>.profile_mismatch`.

1. Re-run the route certification with the same profile requested by this
   release gate.
2. Use `vendor_live` only for opt-in Docker-live or vendor-live release
   candidates.

Failure: `<route>.route_mismatch`.

1. Confirm the CLI route key matches the embedded `route.case_id`.
2. Reattach the correct route bundle before tagging the release.
