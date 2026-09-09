# Developer route release gate

Route release gate is a control-plane aggregation feature. It turns
already-produced evidence into one route-scoped release receipt. It does not
execute connectors, mutate state, run Docker, or perform live certification.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.ops.route_release_gate` | `RouteReleaseGateService`, evidence reading, route identity extraction, required-domain expansion, report writing orchestration. |
| `dpone.ops.routes.release_gate_models` | `RouteReleaseGateEvidence`, `RouteReleaseGateReport`, schema version, JSON/Markdown contract. |
| `dpone.ops.routes.release_gate_policy` | `RouteReleaseGatePolicy`, `RouteReleaseGateDecision`, scoring, blocker, warning, and next-action rules. |
| `dpone.ops.routes.catalog` | `RouteProfileCatalog` and matrix-driven required evidence. |
| `dpone.commands.ops_parsers_core` | CLI argument parser only. |
| `dpone.services.ops.command_handlers_release` | Thin command handler that invokes the catalog service and emits JSON/Markdown. |

## Dependency injection

`RouteReleaseGateService` accepts a `RouteProfileCatalog` and
`RouteReleaseGatePolicy`. Tests can inject either dependency. Production
composition goes through `ReadinessOpsCatalog.route_release_gate` and
`ReleaseOpsCatalog.route_release_gate`.

Keep dependency injection explicit. Do not read environment variables, start
services, load connector clients, or choose route-specific behavior inside the
service.

## No connector imports

No connector imports are allowed in this feature. The service reads JSON or
non-JSON artifacts from disk and evaluates their pass/fail state. Runtime work
belongs to the upstream source, sink, CDC, benchmark, reconciliation, and
certification commands.

## Stable JSON contract

`RouteReleaseGateReport` is a public CI/release contract. Add fields
additively. Do not rename existing keys such as `schema_version`, `release`,
`route`, `passed`, `level`, `score`, `required_evidence`, `evidence`,
`evidence_index`, `blockers`, `warnings`, `next_actions`, `json_path`, or
`markdown_path`.

## Extension rules

- Add route-specific required domains to `RouteProfile.required_evidence`, not
  to CLI branches.
- Add release-specific domains through repeated `--require` flags or a future
  small policy extension.
- Add new pass/fail artifact semantics in one reader helper with tests.
- Keep CLI handlers thin: parse args, call service, emit report.
- Keep policy pure: it should accept normalized evidence and return a decision
  without filesystem access.
- Keep docs and docs-contract tests in the same PR as behavior changes.

## Tests

Focused tests live in:

- `tests/test_route_release_gate.py`
- `tests/test_cli_route_release_gate_command.py`
- `tests/test_route_release_gate_docs_contract.py`

Run these with route readiness/certification/state tests before changing
release-gate behavior:

```bash
uv run pytest \
  tests/test_route_release_gate.py \
  tests/test_cli_route_release_gate_command.py \
  tests/test_route_release_gate_docs_contract.py \
  tests/test_route_readiness.py \
  tests/test_route_certification_pack.py \
  tests/test_route_execution_ledger.py \
  tests/test_route_state_promotion.py \
  -q
```
