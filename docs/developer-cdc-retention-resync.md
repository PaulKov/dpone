# Developer CDC retention gap auto-resync

CDC retention gap auto-resync is a control-plane feature. It does not replace
runtime readers, sinks, or offset stores. It answers one question before a CDC
stream resumes: is the stored offset still inside source retention, and if not,
what bounded resync work must run before normal CDC ticks continue?

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.runtime.cdc.retention_models` | `CdcRetentionBounds`, `CdcRetentionDecision`, `CdcRetentionReport`, `CdcResyncAction`, `CdcResyncPlan`, `CdcResyncPlanReport`, and `CdcResyncExecutionReport`. |
| `dpone.runtime.cdc.retention` | `CdcRetentionProbe` protocol, `CdcRetentionPolicy`, `CdcRetentionGapService`, and `CdcResyncPlanner`. |
| `dpone.runtime.cdc.retention_probes` | `StaticCdcRetentionProbe`, `MssqlChangeTrackingRetentionProbe`, and `MssqlCdcRetentionProbe`. |
| `dpone.runtime.cdc.resync` | `CdcResyncExecutionService`, which converts resync actions to `CDCChange` objects and applies them through `CdcSinkApplier`. |
| `dpone.ops.cdc.retention_resync` | Local/live composition, connector construction, and mode validation. |
| `dpone.services.ops.command_handlers_cdc` | CLI delegation only. |

## Design rules

Keep the core generic:

- `CdcRetentionGapService` reads one injected `CdcRetentionProbe` and evaluates
  one injected `CdcRetentionPolicy`.
- `CdcRetentionPolicy` compares offsets and returns `healthy`, `at_risk`, or
  `gap_detected`; it never opens network connections.
- `CdcResyncPlanner` turns a retention report plus snapshot rows into
  `CdcResyncAction` records; it does not read source databases.
- `CdcResyncExecutionService` owns only plan loading, `CDCChange` conversion,
  and `CdcSinkApplier` invocation.
- Route-specific SQL belongs in small probe adapters such as
  `MssqlChangeTrackingRetentionProbe`.
- Sink-specific apply semantics belong in the existing `CdcSinkApplier`
  implementations, for example `ClickHouseCdcSinkApplier`.

These rules keep SOLID boundaries explicit: probes handle source introspection,
policy handles decisions, planner handles action creation, executor handles
sink apply, and ops facades handle dependency injection.

## Extension rules

For a new CDC backend:

1. Implement a small `CdcRetentionProbe` with `read_bounds()`.
2. Return `CdcRetentionBounds` with canonical offset tokens and backend.
3. Add offset comparison only when the default numeric or SQL Server LSN
   comparator is insufficient.
4. Reuse `CdcRetentionGapService`, `CdcResyncPlanner`, and
   `CdcResyncExecutionService`.
5. Add local policy tests, probe SQL tests, CLI delegation tests, docs contract
   tests, and route docs.
6. Add Docker-live coverage only as an opt-in gate; ordinary OSS CI must stay
   credential-free.

For a new sink:

1. Implement or reuse a `CdcSinkApplier`.
2. Keep resync execution offset-safe: reports must keep `committed=false`.
3. Verify idempotency at the sink layer, not in the planner.

## Invariants

- Retention checks may fail a release gate but never mutate source or sink
  data.
- Resync execution may write sink CDC events but never writes source offsets.
- A gap decision is fail-closed: normal CDC should not continue until resync
  and compare evidence are green.
- `mssql -> clickhouse` live mode is a first adapter profile, not a hard-coded
  rule inside the runtime services.
- CLI handlers must not contain business logic.

## Test commands

Focused gate:

```bash
uv run pytest \
  tests/test_cdc_retention_gap.py \
  tests/test_cdc_resync_plan_execute.py \
  tests/test_cdc_retention_live_probes.py \
  tests/test_cli_cdc_retention_resync_commands.py \
  tests/test_cdc_retention_resync_docs_contract.py -q
```

Docker-live gate:

```bash
DPONE_RUN_INTEGRATION=1 \
uv run pytest tests/integration/mssql/test_mssql_clickhouse_live_cdc_runtime_integration.py -q
```

Full quality gate:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run pytest -m "not integration"
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
