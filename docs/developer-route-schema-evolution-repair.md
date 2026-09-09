# Developer route schema evolution and repair

This developer guide explains how the route-level schema evolution and repair
feature is organized for any `source -> sink -> strategy` route.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.ops.routes.models` | Public dataclasses and JSON/Markdown report contracts. |
| `dpone.ops.routes.schema_evolution` | Route schema evolution policy orchestration over upstream CDC schema artifacts. |
| `dpone.ops.routes.reconciliation_repair` | Route reconciliation repair orchestration over row-level reconciliation evidence. |
| `dpone.ops.route_schema_evolution` | Small facade module for imports and service catalog access. |
| `dpone.ops.route_reconciliation_repair` | Small facade module for imports and service catalog access. |
| `dpone.services.ops.command_handlers_routes` | Thin CLI handlers only. |

The services do not execute live databases, do not own connector extraction or
loading, and do not encode route-specific code paths. Route support comes from
`RouteProfileCatalog`, which is backed by the integration matrix.

## Commands

- `dpone ops route-schema-evolution` builds route-level schema evolution
  evidence for one `source -> sink -> strategy` route.
- `dpone ops route-reconciliation-repair` builds route-level repair evidence for
  one `source -> sink -> strategy` route.

## Extension rules

1. Add new routes to the integration matrix and source -> sink docs first.
2. Add route-specific required evidence to `RouteProfileCatalog` metadata, not
   to command handlers.
3. Keep schema evolution decisions in `RouteSchemaEvolutionService`.
4. Keep repair action mapping in `RouteReconciliationRepairService`.
5. Keep CLI parsers and handlers as argument parsing plus service invocation.

## Test strategy

Use TDD for new behavior:

- service tests for JSON/Markdown contracts and blocker decisions;
- CLI tests for console/file output and exit codes;
- catalog tests for route evidence requirements;
- docs contract tests for user docs, developer docs, and MkDocs navigation;
- existing docs, ruff, mypy, and quality-metrics checks before release.

## Runbook

When changing these modules:

1. Add or update a failing test first.
2. Implement the smallest production change that makes the test pass.
3. Run targeted tests for route schema evolution, reconciliation repair, route
   readiness, and CLI commands.
4. Run `uv run dpone docs update-cli-reference --check`; update the generated
   CLI reference when parser output changes.
5. Run `uv run dpone docs update-dev-metrics` when Python source files change.
