# Developer CDC live runtime adapters

CDC live runtime adapters are the dependency-injected bridge between the generic
runtime loop and vendor connectors. They must stay thin: readers read bounded
source changes, sink appliers return durable receipts, and offset adapters
translate existing state stores into the `CdcOffsetStore` protocol.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.runtime.cdc.live_adapters` | ClickHouse CDC apply plan, canonical event rows, event hashes, and `ClickHouseCdcSinkApplier`. |
| `dpone.runtime.cdc.live_factory` | `CdcRuntimeLiveAdapterFactory` and `SqlCdcOffsetStoreAdapter` composition helpers. |
| `dpone.runtime.cdc.mssql` | Existing `MSSQLCDCReader` and `MSSQLChangeTrackingReader` implementations. |
| `dpone.ops.cdc.runtime_run` | Ops facade that chooses local JSON mode or live connector mode. |
| `dpone.runtime.cdc.runtime_orchestrator` | Generic read -> apply -> commit loop. |

Do not add route-specific branches to CdcRuntimeOrchestrator. The orchestrator
depends only on `CDCReader`, `CdcSinkApplier`, and `CdcOffsetStore`; live
adapters are injected by the ops facade or future schedulers.

In production composition roots, live adapters are injected; they are not looked
up from inside the orchestrator.

## Adapter taxonomy

`CdcRuntimeLiveAdapterFactory` builds three runtime collaborators:

| Factory method | Output | Notes |
| --- | --- | --- |
| `mssql_reader` | `MSSQLCDCReader` or `MSSQLChangeTrackingReader` | Selected from `CdcRuntimeStream.backend`. |
| `mssql_offset_store` | `SqlCdcOffsetStoreAdapter` | Wraps `MSSQLCDCOffsetStorage` without leaking its argument shape into the orchestrator. |
| `clickhouse_sink_applier` | `ClickHouseCdcSinkApplier` | Implements the `CdcSinkApplier` protocol. |

`SqlCdcOffsetStoreAdapter` maps runtime stream identity to the existing
SQL-backed offset storage contract:

- `pipeline_name`
- `source_schema`
- `source_table`
- `backend`

## ClickHouse apply contract

`ClickHouseCdcSinkApplier` creates an append-only CDC log table when needed and
inserts one normalized row per `CDCChange`. It returns `CdcApplyReceipt` instead
of raising on sink apply errors so the orchestrator can produce a normal failed
runtime report and avoid committing offsets.

The default table model is intentionally conservative:

- `MergeTree`
- order by stream id, unique key hash, source position, and event hash
- canonical JSON payload columns
- explicit delete marker
- deterministic event and batch hashes

Later serving-table strategies can build on this log with materialized views,
ReplacingMergeTree compaction, or route-specific promotion policies without
changing the runtime ports.

## Extension rules

- Add new source readers by implementing `CDCReader`.
- Add new sink appliers by implementing `CdcSinkApplier`.
- Add new offset backends by implementing `CdcOffsetStore`.
- Keep optional database clients behind connector factories or method bodies.
- Keep route-specific SQL or table design in adapter modules, not in
  `CdcRuntimeOrchestrator`.
- Keep CLI handlers limited to argument parsing and service invocation.

## Required tests

When changing live adapters, run:

```bash
uv run pytest tests/test_cdc_live_adapters.py tests/test_cdc_live_factory.py tests/test_cli_cdc_runtime_run_command.py
uv run pytest tests/test_cdc_live_adapters_docs_contract.py
docker compose -f docker/docker-compose.integration.yml up -d mssql clickhouse
DPONE_RUN_INTEGRATION=1 uv run pytest tests/integration/mssql/test_mssql_clickhouse_live_cdc_runtime_integration.py -q
uv run pytest -m "not integration_live"
uv run ruff check .
uv run mypy --config-file mypy.ini
```

Live vendor tests should use Docker SQL Server and ClickHouse fixtures and
verify insert, update, delete, resume after failure, duplicate event protection,
and no offset commit when ClickHouse apply fails.
