# Developer route live certification

`RouteLiveCertificationService` builds the route-scoped live certification
harness and evidence bundle. It belongs to the ops control plane, not the
runtime connector plane.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.ops.route_live_certification` | Service facade that composes route profile lookup, harness step rendering, evidence loading, policy evaluation, and report writing. |
| `dpone.ops.routes.live_certification_models` | `RouteLiveCertificationStep`, `RouteLiveCertificationEvidence`, and `RouteLiveCertificationReport` stable contracts. |
| `dpone.ops.routes.live_certification_policy` | `RouteLiveCertificationPolicy` and pure score/blocker/next-action decisions. |
| `dpone.ops.routes.catalog` | `RouteProfileCatalog` source of route existence and profile metadata. |

The service uses dependency injection for the catalog and policy. Tests can
replace either collaborator without monkeypatching module globals.

## Stable JSON contract

The public schema is `dpone.route_live_certification.v1`. Keep these fields
stable:

- `release`
- `profile`
- `route`
- `route_profile`
- `passed`
- `level`
- `score`
- `required_evidence`
- `evidence`
- `evidence_index`
- `harness_steps`
- `evidence_bundle`
- `json_path`
- `markdown_path`

Additive fields are allowed when tests and docs are updated. Do not rename or
remove fields without a compatibility plan.

## No live I/O rule

`RouteLiveCertificationService` does not open live database connections, start
Docker, call connector clients, or run pytest. It only renders commands and
validates artifacts. Runtime adapters stay in `dpone.runtime.*`; live tests stay
under `tests/integration/*`; workflow execution stays in CI or an operator's
shell.

## Extension rules

To add a new route:

1. Add or update the source -> sink matrix entry.
2. Add route profile metadata through `RouteProfileCatalog`.
3. Add live evidence names to route profile metadata or the small route-live
   evidence map.
4. Add focused tests for required evidence, route mismatch, and harness steps.
5. Update user docs, developer docs, source-sink docs, and architecture docs in
   the same PR.

Do not add route-specific branching to CLI handlers. CLI code only parses
arguments and calls the service.

## Tests

Required coverage:

- service contract tests for successful bundles;
- missing, failed, and route-mismatched evidence tests;
- CLI JSON/Markdown output tests;
- docs contract tests for user docs, developer docs, architecture, CI/CD,
  source-sink matrix, and CLI reference;
- architecture/quality gates for module size and dependency drift.
