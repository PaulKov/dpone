# Route release finalize

`dpone ops route-release-finalize` is the final route-certified release gate.
It performs bundle discovery, route certification release aggregation,
freshness checks, provenance checks, regression checks, and route certification
history recording.

Use it after `dpone ops route-certify` has produced route bundles and after
`dpone ops route-certify-release` is expected to pass. The finalizer can scan
bundle roots, so release managers do not need to hand-maintain every
`--route-bundle` argument.

The command is control-plane only. It does not run Docker, does not open
database connections, and does not execute route refreshes.

## Example

```bash
uv run dpone ops route-release-finalize \
  --release v0.9.0-rc1 \
  --profile oss_ci \
  --bundle-root test_artifacts/route_certify \
  --history-dir test_artifacts/route_certification_history \
  --output-dir test_artifacts/route_release_finalize \
  --format json
```

Use explicit bundle overrides when a discovered bundle should be replaced:

```bash
--route-bundle postgres_to_mssql__incremental_merge=PATH_TO_BUNDLE
```

## Gates

| Gate | Blocks when |
| --- | --- |
| bundle discovery | a required route bundle is missing after root scan and explicit overrides |
| freshness | a required bundle is older than `--max-age-hours` |
| provenance | a required bundle `release` differs from the requested release |
| regression | a route score is lower than `--baseline-json` for the same route |
| route certify release | nested `route_certification_release.json` is not `release_ready` |

`--baseline-json` accepts a previous finalizer report, a
`route_certification_history_index.json`, or a `route_certification_release.json`
with route scores.

## Outputs

| File | Description |
| --- | --- |
| `route_release_finalizer.json` | Stable final route release gate for CI and release review. |
| `route_release_finalizer.md` | Human-readable checks, blockers, and Operator runbook. |
| `route-certify-release/route_certification_release.json` | Nested release-level route bundle report. |
| `route_certification_history_index.json` | Route scores and levels by release for future regression gates. |

## Operator runbook

Failure: `<route>.missing`.

1. Run `dpone ops route-certify` for the route.
2. Re-run `dpone ops route-release-finalize` with the bundle root or explicit
   `--route-bundle` override.

Failure: `<route>.stale`.

1. Regenerate the route certification bundle for this release candidate.
2. Increase `--max-age-hours` only when the release policy explicitly allows
   older evidence.

Failure: `<route>.release_mismatch`.

1. Open the bundle and confirm its `release` field.
2. Regenerate it for the release id passed to the finalizer.

Failure: `<route>.score_regression`.

1. Compare the route bundle score with the baseline.
2. Fix the degraded evidence or document an accepted release risk before
   publishing.
