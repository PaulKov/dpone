# ClickHouse CDC Typed Materialization Implementation Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build typed ClickHouse current-state serving tables from normalized `dpone_cdc_*` append-only logs while preserving the existing JSON materialization path.

**Architecture:** Add a sibling runtime module, `dpone.runtime.cdc.typed_materialization`, with focused plan, column, policy, projector, report, and service classes. Keep connector creation in `dpone.ops.cdc.typed_materialization`, and keep CLI handlers as delegation only. The typed path reuses the existing CDC log contract and shadow-table replace pattern; it does not modify `CdcRuntimeOrchestrator` or live CDC apply.

**Tech Stack:** Python dataclasses, ClickHouse JSON extraction SQL, existing ClickHouse connector facade, argparse CLI, pytest unit/CLI/docs contracts, opt-in Docker MSSQL + ClickHouse integration, MkDocs and quality gates.

---

## File Structure

- Create `src/dpone/runtime/cdc/typed_materialization.py`: typed column specs, projector, plan, policy, report, service and SQL helpers.
- Create `src/dpone/ops/cdc/typed_materialization.py`: credentials-aware typed materialization facade.
- Modify `src/dpone/ops/catalog_cdc.py`: expose `typed_materialization()`.
- Modify `src/dpone/runtime/cdc/__init__.py` and `src/dpone/ops/cdc/__init__.py`: lazy public exports.
- Modify `src/dpone/commands/ops_parsers_artifacts.py`: add `cdc-materialize-clickhouse-typed`.
- Modify `src/dpone/services/ops/command_handlers_cdc.py`: add `cmd_cdc_materialize_clickhouse_typed`.
- Modify `src/dpone/commands/ops_cmd.py` and `src/dpone/commands/registry_ops.py`: import/register the command.
- Create `tests/test_cdc_clickhouse_typed_materialization.py`: plan, projector, service, report and failure contract tests.
- Create `tests/test_cli_cdc_materialize_clickhouse_typed_command.py`: CLI delegation and column parsing tests.
- Create `tests/test_cdc_clickhouse_typed_materialization_docs_contract.py`: user/developer docs and navigation tests.
- Modify `tests/integration/mssql/test_mssql_clickhouse_live_cdc_runtime_integration.py`: add typed serving-table Docker test.
- Create `docs/cdc-clickhouse-typed-materialization.md`.
- Create `docs/developer-cdc-clickhouse-typed-materialization.md`.
- Modify docs index, architecture, CI/CD, source-sink matrix, MSSQL -> ClickHouse guide, `mkdocs.yml`, and generated CLI/quality docs.

### Task 1: Runtime Typed Projection Contract

**Files:**
- Create: `tests/test_cdc_clickhouse_typed_materialization.py`
- Create: `src/dpone/runtime/cdc/typed_materialization.py`
- Modify: `src/dpone/runtime/cdc/__init__.py`

- [ ] **Step 1: Write failing tests**

Test `ClickHouseCdcTypedColumn`, `ClickHouseCdcPayloadProjector`, `ClickHouseCdcTypedMaterializationPlan`, and `ClickHouseCdcTypedMaterializationService`.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/test_cdc_clickhouse_typed_materialization.py -q`
Expected: FAIL because `dpone.runtime.cdc.typed_materialization` does not exist.

- [ ] **Step 3: Implement minimal runtime module**

Support column specs like `order_id Int32`, `amount Decimal(18,2)`, `status Nullable(String)`, and `updated_at DateTime64(3, 'UTC')`. Render ClickHouse expressions with JSON extraction and safe casts. Write `cdc_typed_materialization.json` and `.md`.

- [ ] **Step 4: Verify GREEN**

Run: `uv run pytest tests/test_cdc_clickhouse_typed_materialization.py -q`
Expected: PASS.

### Task 2: Ops Facade and CLI

**Files:**
- Create: `src/dpone/ops/cdc/typed_materialization.py`
- Modify: `src/dpone/ops/catalog_cdc.py`
- Modify: `src/dpone/services/ops/command_handlers_cdc.py`
- Modify: `src/dpone/commands/ops_parsers_artifacts.py`
- Modify: `src/dpone/commands/ops_cmd.py`
- Modify: `src/dpone/commands/registry_ops.py`
- Create: `tests/test_cli_cdc_materialize_clickhouse_typed_command.py`

- [ ] **Step 1: Write failing CLI test**

Command: `dpone ops cdc-materialize-clickhouse-typed --column order_id=Int32 --column amount=Decimal(18,2) ...`.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/test_cli_cdc_materialize_clickhouse_typed_command.py -q`
Expected: FAIL because the command is not registered.

- [ ] **Step 3: Implement CLI/ops delegation**

Parse columns in the ops facade, create connector through injected factory, call runtime service, close connector.

- [ ] **Step 4: Verify GREEN**

Run: `uv run pytest tests/test_cli_cdc_materialize_clickhouse_typed_command.py -q`
Expected: PASS.

### Task 3: Docs and Architecture Contracts

**Files:**
- Create: `tests/test_cdc_clickhouse_typed_materialization_docs_contract.py`
- Create: `docs/cdc-clickhouse-typed-materialization.md`
- Create: `docs/developer-cdc-clickhouse-typed-materialization.md`
- Modify: `docs/README.md`, `docs/architecture.md`, `docs/ci-cd.md`, `docs/developer-ci-cd.md`, `docs/source-sink-matrix.md`, `docs/source-sink/mssql-to-clickhouse.md`, `docs/cdc-clickhouse-materialization.md`, `mkdocs.yml`.

- [ ] **Step 1: Write failing docs contract**

Require CLI examples, delete modes, typed column syntax, evidence artifacts, Docker integration command, developer boundaries, and navigation links.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/test_cdc_clickhouse_typed_materialization_docs_contract.py -q`
Expected: FAIL because docs are missing.

- [ ] **Step 3: Write English docs and links**

Document user workflow, schema/type policy, DDL evidence, extension rules, runbook, CI checks and architecture boundaries.

- [ ] **Step 4: Verify GREEN**

Run: `uv run pytest tests/test_cdc_clickhouse_typed_materialization_docs_contract.py -q`
Expected: PASS.

### Task 4: Docker Integration

**Files:**
- Modify: `tests/integration/mssql/test_mssql_clickhouse_live_cdc_runtime_integration.py`

- [ ] **Step 1: Write integration test**

Use SQL Server Change Tracking to produce insert/update/delete events, append them to ClickHouse CDC log, then materialize typed active and tombstone serving tables.

- [ ] **Step 2: Run opt-in Docker verification**

Run:

```bash
docker compose -f docker/docker-compose.integration.yml up -d mssql clickhouse
DPONE_RUN_INTEGRATION=1 \
DPONE_IT_MSSQL_HOST=127.0.0.1 \
DPONE_IT_MSSQL_PORT=51433 \
DPONE_IT_MSSQL_PASSWORD='Dp0ne.Strong.Pw.2026!' \
DPONE_IT_MSSQL_TRUST_SERVER_CERTIFICATE=yes \
DPONE_IT_CH_HOST=127.0.0.1 \
DPONE_IT_CH_PORT=59000 \
DPONE_IT_CH_DATABASE=dpone_it \
DPONE_IT_CH_USER=default \
DPONE_IT_CH_PASSWORD=dpone \
uv run pytest tests/integration/mssql/test_mssql_clickhouse_live_cdc_runtime_integration.py -q
```

Expected: PASS after implementation.

### Task 5: Full Verification

Run:

```bash
uv run pytest -m "not integration_live"
uv run ruff check
uv run ruff format --check
uv run mypy
uv run mkdocs build --strict
uv run dpone docs check-import-rules
uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json
uv run dpone docs check-module-size --baseline docs/module_size_baseline.json
uv run dpone docs check-architecture-fitness
uv build
```

Regenerate generated docs only through tooling:

```bash
uv run dpone docs update-cli-reference
uv run dpone docs update-dev-metrics
uv run python tools/oss_code_quality_benchmark.py --project dpone --previous-data docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json --allow-stale --runner-name local --workflow-name local --run-id local --branch local --git-sha local
```
