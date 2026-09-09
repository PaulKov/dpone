# Developer route run supervisor

This developer guide explains how the route run supervisor is organized for any
`source -> sink -> strategy` route.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.ops.routes.run_supervisor_models` | Public dataclasses and JSON/Markdown report contracts. |
| `dpone.ops.routes.run_supervisor_contract` | Run-mode taxonomy, ordered execution stages, required evidence defaults, and operator command hints. |
| `dpone.ops.routes.run_supervisor_policy` | Pure status, blocker, warning, retry, and next-action decisions. |
| `dpone.ops.routes.run_supervisor` | Catalog lookup, artifact normalization, contract building, policy evaluation, phase aggregation, and report writing. |
| `dpone.ops.route_run_supervisor` | Small facade module for public imports and service catalog access. |
| `dpone.services.ops.command_handlers_routes` | Thin CLI handler only. |
| `dpone.commands.ops_parsers_routes` | Argument parsing only. |

The service does not execute live databases, run manifests, apply DDL, repair
rows, replay CDC, or promote state. Runtime connectors stay focused on
extract/load/apply. Route run supervision stays focused on evidence, UX, and
go/no-go decisions.

## Commands

- `dpone ops route-run-supervisor` builds a route run receipt for one
  `source -> sink -> strategy` route and run identity.
- `dpone ops route-run-supervisor --run-mode route_refresh` adds an
  `execution_contract` for route refresh execute -> snapshot capture -> verify
  -> ledger -> state promotion evidence.

## Execution contract

Every receipt contains `execution_contract` so CLI, CI, docs, and release gates
can share one stable route run stage contract. `evidence_only` requires
`route_readiness`, `route_execution_ledger`, and `state_promotion`.
`route_refresh` additionally requires `route_refresh_execution`,
`route_refresh_snapshot_capture`, and `route_refresh_verification`.

`RouteRunExecutionContractBuilder` is deliberately generic. It receives a
`RouteKey`, `RouteRunIdentity`, required evidence names, and normalized
artifacts, then returns ordered stages with status, blockers, artifact paths,
and command hints. Future CDC or refresh streams extend the stage taxonomy and
route/profile metadata; they must not add source/sink branching to the CLI
handler or supervisor service.

## Extension rules

1. Add new source -> sink routes to the integration matrix first.
2. Keep route-specific required evidence in route profiles or release-gate
   configuration, not in command handlers.
3. Keep status and retry decisions in `RouteRunSupervisorPolicy`.
4. Keep run-mode and stage ordering in `RouteRunExecutionContractBuilder`.
5. Keep artifact parsing generic in `RouteRunSupervisorService`.
6. Do not import runtime connector implementations into supervisor modules.
7. Add new evidence domains by mapping domain name to a lifecycle phase and
   adding tests for the decision behavior.

## Status taxonomy

| Status | Developer rule |
| --- | --- |
| `ready` | Profile exists and every required normalized artifact passed. |
| `blocked` | Missing, malformed, unsupported, or route-mismatched evidence. |
| `retryable` | Failed runtime evidence is present and no artifact marks retry unsafe. |
| `unsafe_to_retry` | State, schema, repair, release, duplicate, commit, fencing, or token blockers are present. |
| `manual_approval_required` | Upstream evidence has approval/backfill/DDL governance signals. |

Prefer fail-closed behavior when adding evidence types. If the service cannot
understand whether retry is safe, classify as blocked unless an artifact
explicitly marks the failure as retryable.

## Test strategy

Use TDD for new behavior:

- service tests for ready, missing evidence, route mismatch, retryable,
  unsafe-to-retry, manual approval, and first supported routes;
- CLI tests for JSON output, non-zero exits, output files, and help;
- docs contract tests for user docs, developer docs, architecture docs, and
  MkDocs navigation;
- neighboring route readiness, release gate, schema evolution, and repair tests
  to prevent regressions;
- generated CLI reference and quality metrics checks before PR.

## Runbook

When changing route run supervisor modules:

1. Add or update a failing test first.
2. Keep new collaborators small and dependency-injected.
3. Run focused supervisor tests.
4. Run neighboring route tests.
5. Run `uv run dpone docs update-cli-reference --check`; update generated CLI
   reference when parser output changes.
6. Run `uv run dpone docs update-dev-metrics` when Python source files change.
7. Run `uv run dpone docs check-docs` and `uv run mkdocs build --strict`.
