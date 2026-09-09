# Developer route release finalize

`route-release-finalize` is the final route release control-plane layer above
`route-certify-release`.

## Taxonomy

| Component | Responsibility |
| --- | --- |
| `RouteCertificationReleaseFinalizerService` | Orchestrates discovery, nested release aggregation, final policy evaluation, history writing, and report writing. |
| `RouteCertificationBundleDiscovery` | Reads bundle roots and maps embedded route identities to bundle paths. |
| `RouteCertificationReleaseFinalizerPolicy` | Pure freshness, provenance, regression, and nested-release blocker policy. |
| `RouteCertificationReleaseFinalizerReport` | Stable JSON/Markdown finalizer contract. |

All collaborators are injected through constructors for dependency injection.
Tests can replace discovery, nested release aggregation, or policy without
touching CLI code.

## Boundary rules

- The finalizer does not run Docker.
- The finalizer does not open database connections.
- The finalizer does not execute `route-certify`, refreshes, or CDC streams.
- CLI handlers parse arguments only.
- No route-specific branches belong in the finalizer service.
- Required first-class route defaults stay in the route certification release
  contract and may be overridden with `--route`.

## Extension rules

Add new final checks in `RouteCertificationReleaseFinalizerPolicy` and cover
them with focused tests. Keep filesystem discovery in
`RouteCertificationBundleDiscovery` and history serialization in the service
or a small extracted writer if it grows.

Add new public fields only through `RouteCertificationReleaseFinalizerReport`.
Update user docs, developer docs, CLI reference, workflow docs, and docs tests
in the same change.

## Tests

Coverage must include:

- bundle discovery with explicit override precedence;
- stale bundle blockers;
- release provenance blockers;
- score regression blockers;
- history index writes;
- CLI exit codes and JSON output;
- docs/workflow navigation contracts.
