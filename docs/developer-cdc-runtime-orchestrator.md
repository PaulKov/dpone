# Developer CDC runtime orchestrator

CDC runtime orchestrator is the generic runtime loop for bounded CDC apply. It
is separate from CDC evidence services: evidence packages prove readiness, while
the runtime orchestrator coordinates one actual `read -> apply -> commit`
iteration.

The default CLI path uses local JSON adapters so CI remains credential-free.
Production live adapters are injected through the same interfaces by the
composition root.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.runtime.cdc.runtime_models` | `CdcRuntimeStream`, `CdcRuntimePolicy`, `CdcApplyReceipt`, and `CdcRuntimeRunReport`. |
| `dpone.runtime.cdc.runtime_ports` | `CdcOffsetStore` and `CdcSinkApplier` protocols. |
| `dpone.runtime.cdc.runtime_orchestrator` | `CdcRuntimeOrchestrator`, idempotency gate, sink receipt evaluation, offset commit decision, and report writing. |
| `dpone.runtime.cdc.local_runtime` | Credential-free JSON reader, file checkpoint store, and local sink applier for tests and examples. |
| `dpone.runtime.cdc.live_factory` | Live adapter factory for MSSQL readers, SQL-backed offset storage, and ClickHouse apply. |
| `dpone.runtime.cdc.live_adapters` | ClickHouse CDC apply plan, event hash helpers, and `ClickHouseCdcSinkApplier`. |
| `dpone.ops.cdc.runtime_run` | Thin ops facade that composes local or live adapters for `dpone ops cdc-runtime-run`. |
| `dpone.services.ops.command_handlers_cdc` | CLI handler only. |

## Interfaces

`CdcRuntimeStream` identifies one CDC loop: pipeline name, source, sink,
backend, source table, target dataset, unique key, route id, and stream id.

`CdcRuntimePolicy` controls one bounded tick: max changes, whether duplicate
event identities block apply, and whether empty batches can commit offsets.

`CdcOffsetStore` loads and saves durable offsets for a stream. Implementations
must save offsets only when the orchestrator asks them to; they must not infer
success from a reader or sink.

`CdcSinkApplier` applies a `CDCBatch` and returns `CdcApplyReceipt`. The receipt
must say whether the sink operation passed, whether it is durable, row counts,
blockers, warnings, metrics, and an artifact URI.

`CdcRuntimeOrchestrator` owns the sequencing:

1. load start offset;
2. call the injected `CDCReader`;
3. evaluate event idempotency;
4. call the injected `CdcSinkApplier`;
5. save next offset only after durable sink success;
6. write `CdcRuntimeRunReport`.

## Extension rules

Do not add route-specific branches to the orchestrator. Route-specific behavior
belongs in injected readers, sink appliers, offset stores, or route/profile
metadata.

For a new live route:

1. Implement or reuse a `CDCReader`.
2. Implement a `CdcSinkApplier` for the target apply semantics.
3. Implement or reuse a `CdcOffsetStore`.
4. Keep retry, quarantine, and observability as small collaborators around the
   orchestrator instead of expanding the orchestrator.
5. Add service tests, CLI/local-adapter tests, docs-contract tests, user docs,
   developer docs, source-sink examples, and runbook entries.
6. Run full CI, docs, import, layer, module-size, and architecture-fitness
   checks.

## MSSQL -> ClickHouse profile

The first profile uses SQL Server CDC offsets (`mssql_cdc`) and ClickHouse
target dataset identity. The local CLI path reads JSON events and writes a
durable local sink receipt, while live production wiring injects
`MSSQLCDCReader` or `MSSQLChangeTrackingReader`, `ClickHouseCdcSinkApplier`,
and a SQL-backed offset store through `CdcRuntimeLiveAdapterFactory`.

The same `CdcRuntimeOrchestrator` is reused in both cases. This keeps the
replication loop testable and prevents CLI or route-specific code from becoming
the hidden runtime engine.
