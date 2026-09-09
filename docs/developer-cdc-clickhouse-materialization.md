# Developer ClickHouse CDC materialization

ClickHouse CDC materialization is a runtime-plane serving-table builder layered
after the generic CDC runtime. It must stay separate from offset-critical CDC
apply: `ClickHouseCdcSinkApplier` appends durable events, while
`ClickHouseCdcMaterializationService` rebuilds the latest current state through
an injected connector.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.runtime.cdc.materialization` | `ClickHouseCdcMaterializationPlan`, `ClickHouseCdcMaterializationPolicy`, `ClickHouseCdcMaterializationReport`, SQL rendering, shadow-table replace, and report writing. |
| `dpone.ops.cdc.materialization` | `CdcMaterializationService` composition root for credentials and connector creation. |
| `dpone.commands.ops_parsers_artifacts` | Argument parser only; keep choices as strings. |
| `dpone.services.ops.command_handlers_cdc` | Thin CLI handler that delegates to `release.cdc().materialization()`. |
| `dpone.runtime.cdc.live_adapters` | Append-only CDC log apply; do not mix serving-table rebuilds into this offset-critical applier. |

Do not add route-specific branches to CdcRuntimeOrchestrator. The
materialization layer consumes the existing ClickHouse CDC log and does not
change `CDCReader`, `CdcSinkApplier`, or `CdcOffsetStore` contracts.

## Public classes

`ClickHouseCdcMaterializationPlan`
: Resolves `cdc_dataset`, `target_dataset`, quoted table names, artifact URI,
and unique key metadata.

`ClickHouseCdcMaterializationPolicy`
: Holds delete mode and future safety flags. Current modes are
`exclude_deleted` and `tombstone`.

`ClickHouseCdcMaterializationReport`
: Stable JSON/Markdown evidence contract. Keep field names backward-compatible
once published.

`ClickHouseCdcMaterializationService`
: Uses an injected connector to create a shadow table, insert the latest event
per `dpone_cdc_unique_key_hash`, swap the shadow into the target, and write
evidence.

`CdcMaterializationService`
: Ops facade that creates the ClickHouse connector from credentials and then
delegates to the runtime service.

## SQL design

The runtime service uses shadow-table replace:

1. create the target database if needed;
2. drop stale shadow and old tables;
3. create a shadow serving table;
4. insert `row_number() OVER (PARTITION BY dpone_cdc_unique_key_hash ...)`
   latest rows from the CDC log;
5. apply the delete-mode filter;
6. rename the shadow into the target and drop the old target copy.

The connector is injected. Do not import `clickhouse_driver` from this module,
and do not create credentials in the runtime package.

## Extension rules

- Add future CDC sources by writing the same normalized `dpone_cdc_*` log
  columns. Do not add source-specific branches to materialization.
- Add route-specific serving policies as small policy extensions or profile
  metadata, not as command-handler conditionals.
- Keep the CLI parser free of runtime imports beyond lightweight enum choices
  already used in the commands package.
- Keep failure behavior report-based: return `passed=false` with
  `clickhouse_cdc_materialization.failed` instead of leaking partial evidence.
- Keep report writes deterministic: `cdc_materialization.json` and
  `cdc_materialization.md`.

## Required tests

When changing this layer, run:

```bash
uv run pytest tests/test_cdc_clickhouse_materialization.py
uv run pytest tests/test_cli_cdc_materialize_clickhouse_command.py
uv run pytest tests/test_cdc_clickhouse_materialization_docs_contract.py
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
