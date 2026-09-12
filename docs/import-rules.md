# Architectural import rules

`dpone` keeps layer boundaries explicit and checks them automatically in tests/CI.

## Why this exists

The project has several layers with different responsibilities:

- `dpone.commands/*` — CLI orchestration
- `dpone.cli_render/*` — presentation / text rendering
- `dpone.services/*` — application services / use-cases
- `dpone.manifest/*` — manifest compilation / validation / explain
- `dpone.dag/*` — DAG graph / dependency semantics / explain
- `dpone.runtime/*` — execution runtime (sources, sinks, ETL, state)
- `dpone.ports/*` — abstract boundaries
- `dpone.adapters/*` — infrastructure adapters

Without automated checks, architectural drift is easy: commands start calling runtime directly, renderers pull business logic into presentation, or manifest/DAG analysis accidentally becomes dependent on optional runtime SDKs.

## Canonical checks

### Deprecated shims are forbidden in canonical code

Outside shim packages themselves, code must not import deprecated paths such as:

- `dpone.source`
- `dpone.sink`
- `dpone.etl`
- `dpone.etl_logging`
- `dpone.credentials`
- `dpone.state`
- `dpone.reconciliation`
- `dpone.sql_helpers`
- `dpone.xmin`
- `dpone.lib.connectors`
- `dpone.yaml_config_handler`
- `dpone.dbt_publish`

Use canonical paths instead:

- `dpone.runtime.*`
- `dpone.dag.*`
- canonical dbt modules under `dpone.contracts`, `dpone.manifest`,
  `dpone.services`, `dpone.readiness`, and `dpone.adapters`

CLI commands import JSON and redacted text output through
`dpone.commands.output_json` and `dpone.commands.output_text`. These two
command-local facades delegate to the shared `dpone.cli_render` implementation.
The historical top-level `dpone.output_json` and `dpone.output_text` paths
remain compatibility re-exports.

### Layer matrix

| Source layer | Must not import |
|---|---|
| `dpone.commands.*` | `dpone.runtime.*`, deprecated shim packages, `dpone.cli.legacy` |
| `dpone.services.*` | `dpone.commands.*`, `dpone.cli.*`, `dpone.cli_render.*`, `dpone.runtime.*`, deprecated shims |
| `dpone.cli_render.*` | `dpone.commands.*`, `dpone.runtime.*`, `dpone.cli.legacy`, `dpone.cli.main`, `dpone.cli.parser` |
| `dpone.manifest.*` | `dpone.runtime.*`, deprecated shims, `dpone.commands.*`, `dpone.cli_render.*`, `dpone.cli.legacy` |
| `dpone.dag.*` | `dpone.runtime.*`, deprecated shims, `dpone.commands.*`, `dpone.cli_render.*`, `dpone.cli.legacy` |
| `dpone.runtime.*` | `dpone.commands.*`, `dpone.cli.*`, `dpone.cli_render.*`, `dpone.services.*`, `dpone.app.*` |
| `dpone.ports.*` | `dpone.runtime.*`, `dpone.commands.*`, `dpone.cli.*`, `dpone.cli_render.*`, `dpone.services.*`, `dpone.adapters.*` |
| `dpone.adapters.*` | `dpone.runtime.*`, `dpone.commands.*`, `dpone.cli.*`, `dpone.cli_render.*`, `dpone.services.*`, `dpone.manifest.*`, `dpone.dag.*`, `dpone.app.*` |

These rules are intentionally pragmatic:
- they protect the most valuable boundaries now;
- they still allow some transitional dependencies while refactoring continues.

## Readiness package initialization

`dpone.readiness` resolves public helpers and compatibility module attributes on
first access through the existing lazy-export mechanism. Importing a diagnostic
leaf such as `dpone.readiness.python_import_health` does not initialize managed
planning or route evaluation. Doctor keeps its existing probe deadlines.

Import helpers normally, for example `from dpone.readiness import CDCConfig`.
The exported object, defining module, pickle identity and static type remain
unchanged. Explicit `TYPE_CHECKING` re-exports preserve editor and type-checker
support. First-use import errors propagate normally and can be retried; failed
imports are not cached as successful exports.

When changing this facade, run `tests/test_readiness_package_import.py` and the
Doctor import integration/startup-control tests. Check fresh-process isolation,
export order, object identity, static types and concurrent first access.

## How to run the checks

### Pytest

```bash
pytest tests/test_layer_boundaries.py tests/test_docs_import_rules_service.py
```

### CLI (CI-friendly)

```bash
dpone docs check-import-rules
```

JSON output:

```bash
dpone docs check-import-rules --format json
```

Exit codes:
- `0` — no violations
- `2` — one or more violations found

## Typical refactoring workflow

1. Move code to canonical package (`dpone.dag.*`, `dpone.runtime.*`, etc.).
2. Replace any remaining deprecated shim imports.
3. Run:
   ```bash
   dpone docs check-import-rules
   pytest
   ```
4. Only then add/expand the rule matrix if the new boundary is stable.


- `runtime-no-core-or-lib`: runtime code must use canonical contracts/runtime support modules instead of legacy `dpone.core` / `dpone.lib` helpers.
