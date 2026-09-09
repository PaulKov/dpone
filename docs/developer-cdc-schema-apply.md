# Developer CDC schema apply

CDC schema apply is the controlled DDL execution step between
[CDC schema evolution evidence](cdc-schema-evolution-evidence.md) and typed
serving promotion. It turns a normalized schema-change artifact into a
ClickHouse DDL dry-run or apply report, and can optionally run a typed
materialization refresh so operators get one evidence bundle for target DDL and
typed serving correctness.

The feature is deliberately small and control-plane focused. It does not read
source CDC, write CDC log rows, or advance CDC offsets.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.ops.cdc.schema_apply_models` | `CdcSchemaApplyPolicy`, `CdcSchemaApplyPlan`, `CdcSchemaApplyResult`, `CdcSchemaApplyReport`, JSON loading, stable artifact names, and Markdown rendering. |
| `dpone.ops.cdc.schema_apply_clickhouse` | `ClickHouseCdcSchemaDdlPlanner`, source-to-ClickHouse type mapping, safe identifier validation, additive-column DDL, and backfill SQL planning. |
| `dpone.ops.cdc.schema_apply` | `CdcSchemaEvolutionApplyService`, connector composition, DDL execution orchestration, optional typed materialization refresh, and report writing. |
| `dpone.ops.catalog_cdc` | CDC service factory facade used by ops handlers. |
| `dpone.services.ops.command_handlers_cdc` | Thin CLI handler that invokes the service and emits JSON or Markdown. |

## Interfaces

`CdcSchemaApplyPolicy` is the small immutable policy object for DDL execution:
mode, approval requirement, backfill behavior, and nullable-addition policy.

`CdcSchemaApplyPlan` is a pure planner output. It records the change id, target
dataset, operation, source type, mapped ClickHouse type, DDL, optional backfill
SQL, blockers, and warnings. A blocked plan is still serializable so CI and
operators can inspect why no DDL was executed.

`CdcSchemaApplyReport` is the stable public contract. It writes
`cdc_schema_apply_plan.json`, `cdc_schema_apply_result.json`, and
`cdc_schema_apply.md`. When `typed_refresh` is requested, it embeds the typed
materialization refresh evidence path and pass/fail result.

`ClickHouseCdcSchemaDdlPlanner` is the first DDL planner. It is connector-free
and deterministic: schema-change JSON plus target dataset plus policy produces
the same plan every time.

`CdcSchemaEvolutionApplyService` composes the planner, an optional injected
connector, an optional connector factory, and an optional typed materializer. It
does not own route discovery, CDC reads, source offsets, or heavy tests.

## Extension rules

Do not add DDL apply logic to CdcRuntimeOrchestrator. The runtime orchestrator
owns bounded read, sink apply, and durable offset commit only.

Do not mutate CDC offsets from schema apply. A successful DDL apply is evidence
for the promotion lane; offset movement stays behind CDC promotion and runtime
durability checks.

Do not add sink-specific branches to CLI handlers. Add a small planner adapter,
inject it through the service/catalog boundary, and keep parser code limited to
argument parsing.

Do not couple schema apply to a source reader. Future CDC streams should pass
the same normalized `change` artifact, regardless of whether the source is
MSSQL Change Tracking, SQL Server CDC, Postgres logical decoding, Kafka, or a
managed connector feed.

When adding another sink:

1. Add a connector-free DDL planner with safe identifier and type validation.
2. Keep operation support explicit: additive columns first, destructive changes
   blocked unless the route has an approved expand-contract policy.
3. Add planner tests for DDL, backfill SQL, unsafe names, unsupported types, and
   breaking-change blockers.
4. Add service tests with injected fake connectors and fake typed refreshers.
5. Add CLI tests that prove handlers only delegate arguments to the service.
6. Add user docs, developer docs, source-sink examples, runbooks, and docs
   contract tests in the same PR.
7. Add or extend opt-in Docker-live coverage for at least one additive-column
   route before marking the route replication-grade.

## Typed materialization refresh

Schema apply may run a typed materialization refresh after successful DDL. The
service delegates to the existing ClickHouse typed materialization package and
passes through unique keys, declared columns, parse-quarantine policy, and
schema-drift mode. This keeps type projection, parse quarantine, and target
shadow-table replacement in one reusable runtime module.

The service only records the refresh outcome. If typed refresh fails, schema
apply writes a blocked report with `cdc_schema_apply.typed_refresh_failed` so
operators do not promote a serving table whose declared projection is not clean.

## Quality gates

Default OSS CI remains credential-free. The local/default gate must cover model
serialization, planner behavior, service dry-run/apply, CLI delegation, docs
links, import rules, architecture fitness, module size, mypy, ruff, and strict
MkDocs.

Vendor-live behavior is opt-in. The MSSQL -> ClickHouse Docker route must cover
an additive MSSQL column, CDC capture into ClickHouse, `dpone ops
cdc-schema-apply` semantics through `CdcSchemaEvolutionApplyService`, typed
materialization refresh, and a query proving the new ClickHouse typed column is
present with the expected value.
