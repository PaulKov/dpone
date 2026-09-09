# ClickHouse CDC Typed Schema Quarantine Implementation Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add fail-closed schema drift and parse quarantine evidence to ClickHouse CDC typed materialization.

**Architecture:** Keep typed serving materialization split by responsibility. Add small runtime modules for projection diagnostics, schema drift decisions, and quarantine DDL/SQL; extend the existing service and report with optional evidence while keeping the public `dpone.runtime.cdc.typed_materialization` facade stable. CLI and ops remain thin composition layers.

**Tech Stack:** Python dataclasses/protocol-style DI, ClickHouse SQL JSON functions, pytest, Docker Compose MSSQL + ClickHouse integration.

---

### Task 1: Runtime Evidence Models

**Files:**
- Create: `src/dpone/runtime/cdc/typed_materialization_quality.py`
- Modify: `src/dpone/runtime/cdc/typed_materialization.py`
- Test: `tests/test_cdc_clickhouse_typed_schema_quarantine.py`

- [ ] **Step 1: Write RED tests for policy and report evidence**

Add tests that import `ClickHouseCdcTypedQualityPolicy`, `ClickHouseCdcTypedSchemaDrift`, and `ClickHouseCdcTypedParseQuarantine` from the public facade and assert JSON fields for additive fields, missing required fields, parse failures, blockers, warnings, and quarantine artifact names.

- [ ] **Step 2: Run focused RED test**

Run: `uv run pytest tests/test_cdc_clickhouse_typed_schema_quarantine.py -q`

Expected: fails with `ModuleNotFoundError` or missing public classes.

- [ ] **Step 3: Implement minimal dataclasses**

Create immutable dataclasses for:
- `ClickHouseCdcTypedQualityPolicy`
- `ClickHouseCdcTypedSchemaDrift`
- `ClickHouseCdcTypedParseQuarantine`
- `ClickHouseCdcTypedQualityEvidence`

Expose `to_dict()` methods and keep module under the LOC threshold.

- [ ] **Step 4: Export through facade**

Update `src/dpone/runtime/cdc/typed_materialization.py` and `src/dpone/runtime/cdc/__init__.py`.

- [ ] **Step 5: Run focused GREEN test**

Run: `uv run pytest tests/test_cdc_clickhouse_typed_schema_quarantine.py -q`

Expected: passes the model-only tests.

### Task 2: Projection Diagnostics and Quarantine SQL

**Files:**
- Modify: `src/dpone/runtime/cdc/typed_materialization_projection.py`
- Create: `src/dpone/runtime/cdc/typed_materialization_quarantine_sql.py`
- Test: `tests/test_cdc_clickhouse_typed_schema_quarantine.py`

- [ ] **Step 1: Write RED tests for diagnostic SQL**

Assert that diagnostic SQL contains per-column parse-failure expressions such as `isNull(toDecimal128OrNull(...))`, excludes deleted rows in active mode, and renders a quarantine table with payload, column, type, reason, source position, event hash, and ingested timestamp.

- [ ] **Step 2: Run RED test**

Run: `uv run pytest tests/test_cdc_clickhouse_typed_schema_quarantine.py -q`

Expected: fails because diagnostic SQL helpers do not exist.

- [ ] **Step 3: Implement projection expression helpers**

Add methods to reuse conversion expressions without aliases and render parse-failure predicates for non-null input values. Do not duplicate the full projection switch in multiple modules.

- [ ] **Step 4: Implement quarantine SQL renderer**

Create SQL helpers for `CREATE TABLE`, `INSERT INTO quarantine`, and parse diagnostics using `UNION ALL` per projected column.

- [ ] **Step 5: Run GREEN test**

Run: `uv run pytest tests/test_cdc_clickhouse_typed_schema_quarantine.py -q`

Expected: passes diagnostic SQL tests.

### Task 3: Service Integration and Fail-Closed Policy

**Files:**
- Modify: `src/dpone/runtime/cdc/typed_materialization_models.py`
- Modify: `src/dpone/runtime/cdc/typed_materialization_service.py`
- Modify: `src/dpone/ops/cdc/typed_materialization.py`
- Test: `tests/test_cdc_clickhouse_typed_schema_quarantine.py`

- [ ] **Step 1: Write RED service tests**

Use a fake ClickHouse connector to assert:
- report includes `quality_evidence`;
- parse failures write `cdc_typed_parse_quarantine.json`;
- `fail_on_parse_errors=True` returns `passed=false` with `clickhouse_cdc_typed_materialization.parse_quarantine`;
- additive payload fields are warnings;
- missing required payload keys are blockers.

- [ ] **Step 2: Run RED service test**

Run: `uv run pytest tests/test_cdc_clickhouse_typed_schema_quarantine.py -q`

Expected: fails because service does not evaluate quality evidence.

- [ ] **Step 3: Extend policy/report**

Add optional quality policy to `ClickHouseCdcTypedMaterializationPolicy` and `quality_evidence` to `ClickHouseCdcTypedMaterializationReport`.

- [ ] **Step 4: Integrate service**

Before target swap, evaluate schema drift and parse diagnostics using lightweight ClickHouse count/sample queries. Write quarantine evidence JSON/Markdown and fail closed when policy requires it.

- [ ] **Step 5: Run GREEN service test**

Run: `uv run pytest tests/test_cdc_clickhouse_typed_schema_quarantine.py -q`

Expected: passes.

### Task 4: CLI, Docs, and Contracts

**Files:**
- Modify: `src/dpone/commands/ops_parsers_artifacts.py`
- Modify: `src/dpone/services/ops/command_handlers_cdc.py`
- Modify: `src/dpone/ops/cdc/typed_materialization.py`
- Modify: `docs/cdc-clickhouse-typed-materialization.md`
- Modify: `docs/developer-cdc-clickhouse-typed-materialization.md`
- Modify: `docs/architecture.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/developer-ci-cd.md`
- Test: `tests/test_cli_cdc_materialize_clickhouse_typed_command.py`
- Test: `tests/test_cdc_clickhouse_typed_materialization_docs_contract.py`

- [ ] **Step 1: Write RED CLI/docs tests**

Assert CLI forwards `--fail-on-parse-errors`, `--max-parse-error-ratio`, `--quarantine-dataset`, and `--schema-drift-mode`; docs require parse quarantine runbook and developer extension rules.

- [ ] **Step 2: Run RED tests**

Run: `uv run pytest tests/test_cli_cdc_materialize_clickhouse_typed_command.py tests/test_cdc_clickhouse_typed_materialization_docs_contract.py -q`

Expected: fails on missing args/docs strings.

- [ ] **Step 3: Wire parser/handler/ops facade**

Add CLI args and pass them into the typed materialization policy.

- [ ] **Step 4: Update docs**

Document user workflow, runbook, developer boundaries, CI artifact policy, and architecture note. Keep all OSS docs in English.

- [ ] **Step 5: Run GREEN tests**

Run: same focused tests. Expected: passes.

### Task 5: Docker-Live Evidence

**Files:**
- Modify: `tests/integration/mssql/test_mssql_clickhouse_live_cdc_runtime_integration.py`

- [ ] **Step 1: Write RED live assertions**

Extend existing typed materialization live test with a bad decimal payload injected into the ClickHouse CDC log and assert quarantine evidence catches it without advancing into a trusted typed table.

- [ ] **Step 2: Run Docker-live RED/GREEN loop**

Run the opt-in Docker command documented in the user guide. Expected after implementation: `2 passed`.

### Task 6: Final Verification

Run:
- `uv run pytest tests/test_cdc_clickhouse_typed_schema_quarantine.py tests/test_cdc_clickhouse_typed_materialization.py tests/test_cli_cdc_materialize_clickhouse_typed_command.py tests/test_cdc_clickhouse_typed_materialization_docs_contract.py -q`
- Docker-live MSSQL + ClickHouse integration file with `DPONE_RUN_INTEGRATION=1`
- `uv run pytest -m "not integration_live"`
- `uv run ruff check .`
- `uv run ruff format --check .`
- `uv run mypy --config-file mypy.ini`
- `uv run dpone docs update-cli-reference --check`
- `uv run dpone docs update-dev-metrics --check`
- `uv run dpone docs check-docs`
- `uv run dpone docs check-import-rules`
- `uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json`
- `uv run dpone docs check-module-size --baseline docs/module_size_baseline.json`
- `uv run dpone docs check-architecture-fitness`
- `uv run mkdocs build --strict`
- `uv build`
