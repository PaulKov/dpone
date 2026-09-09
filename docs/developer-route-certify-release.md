# Developer route certify release

`route-certify-release` is the release-level aggregation layer above
per-route certification bundles.

It keeps release go/no-go decisions reusable across all future
`source -> sink -> strategy` routes while preserving the runtime connector
boundary. Runtime sources and sinks still extract/load data; this layer reads
already-produced control-plane artifacts.

## Class taxonomy

| Class | Module | Responsibility |
| --- | --- | --- |
| `RouteCertificationReleaseService` | `dpone.ops.route_certify_release` | Orchestrates bundle loading, normalization, policy evaluation, and report writing. |
| `RouteCertificationReleasePolicy` | `dpone.ops.routes.certify_release_policy` | Pure scoring/blocker policy with no file I/O and no route-specific branches. |
| `RouteCertificationReleaseItem` | `dpone.ops.routes.certify_release_models` | Normalized route bundle row used by JSON, Markdown, and policy. |
| `RouteCertificationReleaseReport` | `dpone.ops.routes.certify_release_models` | Stable public JSON/Markdown/release-notes contract. |

The service receives the policy through dependency injection so tests and future
edition-specific gates can replace policy behavior without changing CLI code.

## Boundary rules

- The service does not run route-certify.
- The service does not run Docker.
- The service does not open database connections or execute route refreshes.
- CLI handlers only parse arguments and call the service.
- No route-specific branches belong in `RouteCertificationReleaseService`.
- Required routes live in the model contract and can be overridden through the
  CLI matrix inputs.

## Extension rules

Add a future first-class route by updating route/profile metadata, docs, and
tests. Command logic should not change unless the public contract changes.

Add profile-specific behavior in a small policy extension, then inject that
policy from the catalog or a focused command. Keep the default policy generic.

Add new output fields only by extending `RouteCertificationReleaseReport` and
covering the JSON/Markdown contract in tests. Preserve existing keys for CI and
release tooling.

## Tests

The release gate is protected by:

- service tests for missing bundles, blocked bundles, profile mismatch, and
  route identity mismatch;
- CLI tests for JSON output and non-zero exit codes;
- docs/workflow contract tests that require user docs, developer docs,
  navigation links, and uploadable CI artifacts;
- architecture and module-size checks in the shared docs/quality gate.

## CI integration

Use `.github/workflows/route-certification-release.yml` as the opt-in workflow.
It stays credential-free when `profile=oss_ci`; `vendor_live` should be used
only after the upstream Docker-live or vendor-live route jobs have produced
matching route bundles.
