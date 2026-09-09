# Developer ClickHouse CDC typed materialization

ClickHouse CDC typed materialization is a runtime-plane serving-table builder
layered after the generic CDC runtime. It must stay separate from offset-critical
CDC apply: `ClickHouseCdcSinkApplier` appends durable events, while
`ClickHouseCdcTypedMaterializationService` rebuilds typed current state through
an injected connector.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.runtime.cdc.typed_materialization` | Public facade for typed materialization models, projection, policy, quality evidence, SQL rendering, shadow-table replace, and report writing. |
| `dpone.runtime.cdc.typed_materialization_models` | Immutable value objects for columns, plans, materialization policy, and reports. |
| `dpone.runtime.cdc.typed_materialization_projection` | `ClickHouseCdcPayloadProjector` expressions and parse-failure predicates. |
| `dpone.runtime.cdc.typed_materialization_quality` | `ClickHouseCdcTypedQualityPolicy`, `ClickHouseCdcTypedQualityEvidence`, schema drift evidence, and parse quarantine evidence. |
| `dpone.runtime.cdc.typed_materialization_quarantine_sql` | Parse quarantine DDL, insert SQL, and failure-count SQL. |
| `dpone.runtime.cdc.typed_materialization_service` | Connector-orchestrated shadow-table replace, schema drift checks, parse quarantine checks, and report writing. |
| `dpone.ops.cdc.typed_materialization` | `CdcTypedMaterializationService` composition root for credentials and connector creation. |
| `dpone.commands.ops_parsers_artifacts` | Argument parser only; keep values as strings and avoid runtime construction. |
| `dpone.services.ops.command_handlers_cdc` | Thin CLI handler that delegates to `release.cdc().typed_materialization()`. |
| `dpone.runtime.cdc.live_adapters` | Append-only CDC log apply; do not mix typed serving-table rebuilds into this offset-critical applier. |

Do not add route-specific branches to CdcRuntimeOrchestrator. The typed
materialization layer consumes the existing ClickHouse CDC log and does not
change `CDCReader`, `CdcSinkApplier`, or `CdcOffsetStore` contracts.

## Public classes

`ClickHouseCdcTypedColumn`
: Immutable value object for one projected serving column. It validates safe
ClickHouse identifiers, supported types, payload keys, and CLI
`name=ClickHouseType` declarations.

`ClickHouseCdcPayloadProjector`
: Renders ClickHouse JSON extraction expressions for the typed columns. It has
no connector, route, credential, or filesystem dependencies.

`ClickHouseCdcTypedMaterializationPlan`
: Resolves `cdc_dataset`, `target_dataset`, quoted table names, shadow/old table
names, unique key metadata, typed columns, and artifact URI.

`ClickHouseCdcTypedMaterializationPolicy`
: Holds delete mode and an optional `ClickHouseCdcTypedQualityPolicy`. Delete
modes are `exclude_deleted` and `tombstone`.

`ClickHouseCdcTypedQualityPolicy`
: Immutable quality configuration for parse quarantine, parse error ratio,
schema drift mode, quarantine target, and payload-key sampling. Keep this class
route-neutral; source-specific behavior belongs in the CDC log producer or route
profile metadata.

`ClickHouseCdcTypedQualityEvidence`
: Aggregates schema drift evidence, parse quarantine evidence, quality blockers,
warnings, and metrics. The report embeds this evidence so CLI, CI, release gates,
and support bundles consume one stable contract.

`ClickHouseCdcTypedMaterializationReport`
: Stable JSON/Markdown evidence contract. Keep field names backward-compatible
once published.

`ClickHouseCdcTypedMaterializationService`
: Uses an injected connector to create a typed shadow table, insert the latest
event per `dpone_cdc_unique_key_hash`, project payload JSON into declared
ClickHouse columns, swap the shadow into the target, and write evidence.

`CdcTypedMaterializationService`
: Ops facade that creates the ClickHouse connector from credentials and delegates
to the runtime service.

## SQL design

The runtime service uses shadow-table replace:

1. create the target database if needed;
2. drop stale shadow and old tables;
3. create a typed shadow serving table with CDC metadata and projected columns;
4. sample payload keys and evaluate schema drift before promotion;
5. count typed parse failures and optionally write parse quarantine rows;
6. insert `row_number() OVER (PARTITION BY dpone_cdc_unique_key_hash ...)`
   latest rows from the CDC log;
7. project payload JSON through `ClickHouseCdcPayloadProjector`;
8. apply the delete-mode filter;
9. rename the shadow into the target and drop the old target copy.

Schema drift and parse quarantine run before the final target rename. If a
quality blocker is produced, the service writes `passed=false`, keeps the
current target untouched, and skips the shadow-table replace. This preserves the
industrial replication contract: typed serving promotion is atomic and evidence
driven.

The connector is injected. Do not import `clickhouse_driver` from this module,
and do not create credentials in the runtime package.

## Extension rules

- Add future CDC sources by writing the same normalized `dpone_cdc_*` log
  columns. Do not add source-specific branches to typed materialization.
- Add future projection types in `ClickHouseCdcPayloadProjector` with focused
  tests for rendered SQL and Docker-live behavior when practical.
- Add future quality checks as focused evidence classes and SQL helpers. Do not
  grow `ClickHouseCdcTypedMaterializationService` into a god module.
- Add route-specific serving policies as small policy extensions or profile
  metadata, not as command-handler conditionals.
- Keep the CLI parser free of runtime imports beyond lightweight enum choices
  already used in the commands package.
- Keep failure behavior report-based: return `passed=false` with
  `clickhouse_cdc_typed_materialization.failed` instead of leaking partial
  evidence.
- Keep report writes deterministic: `cdc_typed_materialization.json`,
  `cdc_typed_materialization.md`, and when quality checks run,
  `cdc_typed_parse_quarantine.json` plus `cdc_typed_parse_quarantine.md`.

## Required tests

When changing this layer, run:

```bash
uv run pytest tests/test_cdc_clickhouse_typed_materialization.py
uv run pytest tests/test_cli_cdc_materialize_clickhouse_typed_command.py
uv run pytest tests/test_cdc_clickhouse_typed_materialization_docs_contract.py
docker compose -f docker/docker-compose.integration.yml up -d mssql clickhouse
DPONE_RUN_INTEGRATION=1 uv run pytest tests/integration/mssql/test_mssql_clickhouse_live_cdc_runtime_integration.py -q
uv run pytest -m "not integration_live"
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run mkdocs build --strict
uv run dpone docs check-import-rules
uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json
HEAD_SHA="$(git rev-parse HEAD)"; BASE_SHA="$(git merge-base "$HEAD_SHA" origin/master)"; if [ "$BASE_SHA" = "$HEAD_SHA" ]; then BASE_SHA="$(git rev-parse "${HEAD_SHA}^")"; fi
uv run dpone docs check-module-size --baseline docs/module_size_baseline.json --base-ref "$BASE_SHA" --head-ref "$HEAD_SHA"
uv run dpone docs check-architecture-fitness
```

The Docker test is opt-in and credential-free for public CI by default. It is
intended for local release certification and protected vendor-live workflows.
