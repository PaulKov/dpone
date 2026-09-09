# Developer CDC compare and repair

CDC compare and repair is the consistency layer for replication-grade streams.
It compares a source current-state snapshot with a sink current-state
projection, writes a bounded repair plan, and can execute that plan through the
same `CdcSinkApplier` abstraction used by runtime apply.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.runtime.cdc.compare_models` | `CdcCompareRow`, `CdcCompareDiff`, `CdcRepairAction`, `CdcRepairPlan`, and `CdcCompareRepairReport`. |
| `dpone.runtime.cdc.compare` | `CdcCompareRepairService`, `CdcRowHasher`, and credential-free in-memory reader. |
| `dpone.runtime.cdc.compare_readers` | `MssqlCdcCompareReader` and `ClickHouseCdcLogCompareReader`. |
| `dpone.runtime.cdc.repair` | `CdcRepairExecutionService` and repair execution report writing. |
| `dpone.ops.cdc.compare_repair` | `CdcCompareRepairOpsService` and `CdcRepairExecutionOpsService` composition for local/live modes. |
| `dpone.services.ops.command_handlers_cdc` | CLI delegation only. |

## Design rules

`CdcCompareRepairService` owns only generic current-state comparison:

- key indexing;
- payload hash comparison;
- diff classification;
- bounded repair-plan rendering.

Do not put route-specific compare rules in CdcCompareRepairService. Route
behavior belongs in injected readers such as `MssqlCdcCompareReader` and
`ClickHouseCdcLogCompareReader`, or in future small policy collaborators.

`CdcRepairExecutionService` loads `cdc_repair_plan.json`, converts replayable
actions to `CDCChange` objects, and applies them through an injected
`CdcSinkApplier`. Do not mutate CDC offsets from repair execution. The report
must always expose `committed=false`; only `CdcRuntimeOrchestrator` owns durable
offset advancement.

## Extension rules

For a new route:

1. Reuse `CdcCompareRow`, `CdcCompareRepairService`, and
   `CdcRepairExecutionService`.
2. Implement source and sink compare readers as small classes with a
   `read_rows()` method.
3. Keep repair execution sink-specific behavior inside a `CdcSinkApplier`.
4. Add local compare tests, repair execution tests, CLI delegation tests, docs
   contract tests, and route docs.
5. Add Docker-live coverage when the source and sink can run locally.

## Test commands

```bash
uv run pytest \
  tests/test_cdc_compare_repair.py \
  tests/test_cdc_repair_execute.py \
  tests/test_cdc_compare_live_readers.py \
  tests/test_cli_cdc_compare_repair_commands.py \
  tests/test_cdc_compare_repair_docs_contract.py -q
```

Full gate:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run pytest -m "not integration_live"
uv run dpone docs update-cli-reference --check
uv run dpone docs update-dev-metrics --check
uv run dpone docs check-docs
uv run dpone docs check-import-rules
uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json
HEAD_SHA="$(git rev-parse HEAD)"; BASE_SHA="$(git merge-base "$HEAD_SHA" origin/master)"; if [ "$BASE_SHA" = "$HEAD_SHA" ]; then BASE_SHA="$(git rev-parse "${HEAD_SHA}^")"; fi
uv run dpone docs check-module-size --baseline docs/module_size_baseline.json --base-ref "$BASE_SHA" --head-ref "$HEAD_SHA"
uv run dpone docs check-architecture-fitness
uv run mkdocs build --strict
uv build
```
