# CDC Schema Evolution Apply Implementation Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a generic CDC schema evolution apply service that can dry-run/apply ClickHouse DDL for typed CDC serving routes and emit promotion evidence.

**Architecture:** Add a focused control-plane service next to `CdcSchemaEvolutionEvidenceService`. Keep route-specific behavior in small DDL planner/applier adapters and keep runtime CDC readers, sink appliers, and offset stores unchanged.

**Tech Stack:** Python dataclasses, existing `dpone.ops.cdc` service catalog, existing ClickHouse connector protocol, existing typed materialization service, pytest, Docker-live MSSQL + ClickHouse integration tests.

---

### Task 1: RED Tests For Schema Apply Models And ClickHouse DDL Planning

**Files:**
- Create: `tests/test_cdc_schema_evolution_apply.py`
- Create: `src/dpone/ops/cdc/schema_apply_models.py`
- Create: `src/dpone/ops/cdc/schema_apply_clickhouse.py`

- [ ] Write tests proving additive nullable columns render `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, breaking/drop/rename changes block, and reports serialize `cdc_schema_apply_plan.json`.
- [ ] Run `uv run pytest tests/test_cdc_schema_evolution_apply.py -q` and confirm it fails on missing modules.
- [ ] Implement immutable model objects: `CdcSchemaApplyPolicy`, `CdcSchemaApplyPlan`, `CdcSchemaApplyResult`, `CdcSchemaApplyReport`.
- [ ] Implement `ClickHouseCdcSchemaDdlPlanner` with no connector dependency and safe identifier/type validation.

### Task 2: RED Tests For Service Dry-Run, Apply, And Typed Refresh Evidence

**Files:**
- Create: `src/dpone/ops/cdc/schema_apply.py`
- Modify: `src/dpone/ops/catalog_cdc.py`
- Test: `tests/test_cdc_schema_evolution_apply.py`

- [ ] Add fake connector and fake typed materializer tests for `mode="dry_run"` and `mode="apply"`.
- [ ] Assert dry-run writes plan/result JSON and does not execute DDL.
- [ ] Assert apply executes DDL, optionally runs typed materialization refresh, embeds `cdc_typed_materialization.json`, and blocks if typed refresh fails.
- [ ] Implement `CdcSchemaEvolutionApplyService` as a thin orchestrator over planner, applier, optional typed materializer, and report writing.

### Task 3: CLI And Ops Wiring

**Files:**
- Modify: `src/dpone/commands/ops_parsers_artifacts.py`
- Modify: `src/dpone/services/ops/command_handlers_cdc.py`
- Modify: `src/dpone/commands/ops_cmd.py`
- Modify: `src/dpone/commands/registry_ops.py`
- Modify: `src/dpone/ops/cdc/__init__.py`
- Test: `tests/test_cli_cdc_schema_apply_command.py`

- [ ] Add `dpone ops cdc-schema-apply`.
- [ ] Support `--schema-change-json`, `--mode dry_run|apply`, `--sink clickhouse`, `--target-dataset`, `--cdc-dataset`, repeated `--unique-key`, repeated `--column`, `--sink-connection-id`, credentials flags, `--typed-refresh`, quality flags, and `--format`.
- [ ] Keep parser/handler thin: no business logic outside the service.

### Task 4: Docs, Architecture, CI Contracts

**Files:**
- Create: `docs/cdc-schema-apply.md`
- Create: `docs/developer-cdc-schema-apply.md`
- Modify: `docs/README.md`
- Modify: `docs/architecture.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/developer-ci-cd.md`
- Modify: `docs/source-sink/mssql-to-clickhouse.md`
- Modify: `mkdocs.yml`
- Test: `tests/test_cdc_schema_apply_docs_contract.py`

- [ ] Document dry-run/apply workflow, required artifacts, runbook, examples, and Docker-live command.
- [ ] Document developer boundaries and extension rules for future sinks.
- [ ] Update generated CLI reference and quality metrics after parser changes.

### Task 5: Docker-Live Additive Column Apply

**Files:**
- Modify: `tests/integration/mssql/test_mssql_clickhouse_live_cdc_runtime_integration.py`

- [ ] Extend the existing MSSQL -> ClickHouse live fixture: add a new MSSQL nullable column, append a CDC row with that payload key, run schema apply against the typed target, refresh typed materialization, and assert the new ClickHouse column/value exists.
- [ ] Run the test with `DPONE_RUN_INTEGRATION=1`.

### Task 6: Verification

- [ ] `uv run pytest tests/test_cdc_schema_evolution_apply.py tests/test_cli_cdc_schema_apply_command.py tests/test_cdc_schema_apply_docs_contract.py -q`
- [ ] `DPONE_RUN_INTEGRATION=1 uv run pytest tests/integration/mssql/test_mssql_clickhouse_live_cdc_runtime_integration.py -q`
- [ ] `uv run pytest -m "not integration_live"`
- [ ] `uv run ruff check .`
- [ ] `uv run ruff format --check .`
- [ ] `uv run mypy --config-file mypy.ini`
- [ ] `uv run dpone docs check-docs`
- [ ] `uv run dpone docs check-import-rules`
- [ ] `uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json`
- [ ] `uv run dpone docs check-module-size --baseline docs/module_size_baseline.json`
- [ ] `uv run dpone docs check-architecture-fitness`
- [ ] `uv run mkdocs build --strict`
- [ ] `uv build`

## Self-Review

- Scope is one cohesive feature: schema apply for CDC typed serving.
- Runtime CDC offset/apply loops are not modified.
- Route-specific logic is limited to ClickHouse DDL planner/applier and live test fixture.
- Docs, CLI, Docker-live, generated references, and quality metrics are included.
