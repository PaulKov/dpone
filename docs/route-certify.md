# Route certify

`dpone ops route-certify` builds the release certification bundle for one
`source -> sink -> strategy` route. It is the promotion gate above route
readiness, route refresh execution, snapshot capture, exact verification, and
release evidence.

The command is control-plane only. It does not start Docker, run pytest, open
database connections, or execute refresh chunks. It reads immutable evidence
artifacts that were already produced by specialized commands.

## When to use it

Use `route-certify` before a minor or major release when a route changed or when
release notes claim route-level certification.

Use [Route release candidate orchestrator](route-rc-orchestrator.md) when you
want a reproducible command train. Use `route-certify` when that train or a CI
job has already produced evidence and you need one final promotion artifact.
Use [Route certify release](route-certify-release.md) after every first-class
route has a `route_certification_bundle.json` and the release needs one
matrix-level go/no-go report.

## OSS-safe mode

OSS-safe mode stays credential-free. It requires local JSON evidence only and is
safe for ordinary pull-request CI.

```bash
uv run dpone ops route-certify \
  --release v0.9.0-rc1 \
  --source postgres \
  --sink mssql \
  --strategy incremental_merge \
  --profile oss_ci \
  --output-dir test_artifacts/route_certify/postgres_to_mssql \
  --artifact route_refresh_execution=test_artifacts/live_certification/refresh-executor/postgres-mssql/route_refresh_execution.json \
  --artifact route_refresh_snapshot_capture=test_artifacts/live_certification/refresh-executor/postgres-mssql/route_refresh_snapshot_capture.json \
  --artifact route_refresh_verification=test_artifacts/live_certification/refresh-executor/postgres-mssql/route_refresh_verification.json \
  --artifact route_execution_ledger=test_artifacts/route_execution/postgres_to_mssql/route_execution_ledger.json \
  --artifact state_promotion=test_artifacts/route_state/postgres_to_mssql/state_promotion.json \
  --artifact benchmark_slo=test_artifacts/live_certification/benchmark-slo/benchmark_slo_gate.json \
  --artifact pre_release_checklist=test_artifacts/live_certification/pre-release/pre_release_checklist.json \
  --artifact evidence_chain=test_artifacts/live_certification/evidence-chain/evidence_chain_index.json \
  --format json
```

## vendor-live mode

Vendor-live mode is opt-in. It requires the same OSS-safe evidence plus a
`route_live_evidence_bundle` produced by Docker-live or vendor-live checks.
Missing live evidence blocks the bundle. Pass the content-valid release-set when
the bundle will feed the public six-dimensional route certification matrix.

```bash
uv run dpone ops route-certify \
  --release v0.9.0-rc1 \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --profile vendor_live \
  --release-set test_artifacts/release/release-set.json \
  --artifact route_live_evidence_bundle=test_artifacts/route_live/mssql_to_clickhouse/route_live_certification.json \
  --artifact route_refresh_execution=test_artifacts/live_certification/refresh-executor/mssql-clickhouse/route_refresh_execution.json \
  --artifact route_refresh_snapshot_capture=test_artifacts/live_certification/refresh-executor/mssql-clickhouse/route_refresh_snapshot_capture.json \
  --artifact route_refresh_verification=test_artifacts/live_certification/refresh-executor/mssql-clickhouse/route_refresh_verification.json \
  --artifact pre_release_checklist=test_artifacts/live_certification/pre-release/pre_release_checklist.json \
  --artifact evidence_chain=test_artifacts/live_certification/evidence-chain/evidence_chain_index.json \
  --output-dir test_artifacts/route_certify/mssql_to_clickhouse \
  --format json
```

`--release-set` adds a safe `dpone.route-matrix-claim.v1` to the bundle. The
claim binds the exact release digest and source commit to the registered route
ID, transport, schema-evolution mode, Airflow/runtime mode, and certification
time. It contains no credentials and is required before `dpone certify routes`
can promote the row above `experimental`.

## Outputs

| File | Description |
| --- | --- |
| `route_certification_bundle.json` | Stable schema `dpone.route_certification_bundle.v1` with `certified`, `warning`, or `blocked` decision. |
| `route_certification_bundle.md` | Human-readable release review and Operator runbook. |
| `route-certification-pack/route_certification_pack.json` | Readiness-compatible evidence pack for the route. |
| `route-certification-pack/readiness/route_readiness.json` | Embedded route readiness report. |
| `route-promotion-gate/route_release_gate.json` | Route promotion gate, indexed as `route_promotion_gate`. |
| `release-evidence-pack/release_evidence_pack.json` | Release evidence pack scoped to this route certification run. |

## Required evidence

The default release certification policy requires:

- `route_refresh_execution`
- `route_refresh_snapshot_capture`
- `route_refresh_verification`
- `route_execution_ledger`
- `state_promotion`
- `benchmark_slo`
- `pre_release_checklist`
- `evidence_chain`
- route profile evidence from the source -> sink matrix

`vendor_live` also requires `route_live_evidence_bundle`.

## Operator runbook

Failure: `route_certification_pack.not_passed`.

1. Open `route-certification-pack/readiness/route_readiness.json`.
2. Attach missing route profile evidence such as type matrix, benchmark/SLO,
   docs/runbook, schema evolution, or reconciliation artifacts.
3. Re-run `dpone ops route-certify`.

Failure: `route_promotion_gate.not_passed`.

1. Open `route-promotion-gate/route_release_gate.json`.
2. Fix `.missing`, `.not_passed`, or `.route_mismatch` blockers upstream.
3. Do not tag a release while the route promotion gate is red.

Failure: `release_evidence_pack.not_passed`.

1. Open `release-evidence-pack/release_evidence_pack.json`.
2. Attach release-level evidence such as `pre_release_checklist` and
   `evidence_chain`.
3. Rebuild the bundle from source artifacts; do not edit generated JSON.
