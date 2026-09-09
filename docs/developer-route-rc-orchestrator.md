# Developer route release candidate orchestrator

`RouteReleaseCandidateOrchestratorService` composes existing route evidence
services into one release candidate train. It is an ops control-plane service,
not a runtime execution engine.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.ops.route_rc_orchestrator` | Service facade that composes route certification pack, route live certification, route release gate, release evidence pack, and report writing. |
| `dpone.ops.routes.rc_orchestrator_models` | `RouteRcOrchestrationStep`, `RouteRcOrchestrationDecision`, and `RouteRcOrchestrationReport` stable contracts. |
| `dpone.ops.routes.rc_orchestrator_policy` | `RouteRcOrchestrationPolicy` pure score, blocker, warning, and next-action decisions. |
| `dpone.ops.routes.catalog` | Matrix-backed `RouteProfileCatalog` for route existence and metadata. |

The service uses dependency injection for every collaborator. Tests can replace
the route pack, live certification, release gate, release evidence pack, or
policy without monkeypatching module globals.

## Stable JSON contract

The public schema is `dpone.route_rc_orchestrator.v1`. Keep these fields stable:

- `release`
- `profile`
- `row_count`
- `route`
- `route_profile`
- `passed`
- `level`
- `score`
- `blockers`
- `warnings`
- `next_actions`
- `steps`
- `artifact_index`
- `json_path`
- `markdown_path`

Additive fields are allowed when tests and docs are updated.

## No live execution rule

The orchestrator does not start Docker, does not run pytest, and does not open
database connections. It invokes existing evidence services and emits commands
for CI or operators. Runtime connectors stay under `dpone.runtime.*`; live tests
stay under `tests/integration/*`; workflow execution stays in GitHub Actions or
the operator shell. Use `dpone ops route-rc-execute` and
`RouteReleaseCandidateExecutorService` for dry-run or opt-in command execution;
do not move shell execution into this orchestrator.

## Extension rules

To extend the release train:

1. Add a focused evidence producer first.
2. Add that producer's artifact name to route profile or orchestrator required
   evidence.
3. Add tests for green, missing, failed, and route-mismatched evidence.
4. Update user docs, developer docs, architecture docs, CI docs, and generated
   CLI reference in the same PR.

Do not add business logic to CLI handlers. CLI code parses arguments and calls
`RouteReleaseCandidateOrchestratorService`.
