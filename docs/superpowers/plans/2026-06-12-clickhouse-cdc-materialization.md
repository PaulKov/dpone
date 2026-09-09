# ClickHouse CDC Materialization Implementation Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reusable ClickHouse CDC serving-materialization layer that turns append-only `dpone_cdc_*` logs into current-state tables with deterministic delete handling, evidence reports, CLI wiring, docs, and integration tests.

**Architecture:** Keep runtime materialization in `dpone.runtime.cdc.materialization` as focused value objects, SQL rendering, and an injected connector service. Keep credential composition in `dpone.ops.cdc.materialization`, and keep CLI modules limited to parser/handler delegation. The implementation must not add route-specific branches to the orchestrator or live adapters.

**Tech Stack:** Python dataclasses, ClickHouse SQL through the existing connector facade, argparse CLI, pytest unit/contract tests, opt-in Docker integration against MSSQL and ClickHouse, MkDocs documentation gates.

---

## File Structure

- Create `src/dpone/runtime/cdc/materialization.py`: `ClickHouseCdcMaterializationPlan`, `ClickHouseCdcMaterializationPolicy`, `ClickHouseCdcMaterializationReport`, `ClickHouseCdcMaterializationService`, and small SQL/identifier helpers.
- Create `src/dpone/ops/cdc/materialization.py`: credential-aware `CdcMaterializationService` facade that creates a ClickHouse connector through injected factories.
- Modify `src/dpone/ops/catalog_cdc.py`: expose `materialization()`.
- Modify `src/dpone/runtime/cdc/__init__.py` and `src/dpone/ops/cdc/__init__.py`: export public materialization classes.
- Modify `src/dpone/commands/ops_parsers_artifacts.py`: add `cdc-materialize-clickhouse` parser with string-only choices.
- Modify `src/dpone/services/ops/command_handlers_cdc.py`: add `cmd_cdc_materialize_clickhouse`.
- Modify `src/dpone/commands/ops_cmd.py` and `src/dpone/commands/registry_ops.py`: import and register the command.
- Create `tests/test_cdc_clickhouse_materialization.py`: unit tests for plan parsing, delete modes, report rendering, failure receipts, and generated SQL behavior through a fake connector.
- Create `tests/test_cli_cdc_materialize_clickhouse_command.py`: CLI delegation and JSON output tests.
- Create `tests/test_cdc_clickhouse_materialization_docs_contract.py`: documentation linkage tests.
- Modify `tests/integration/mssql/test_mssql_clickhouse_live_cdc_runtime_integration.py`: add opt-in Docker test that runs live CDC and materializes the current-state table.
- Create `docs/cdc-clickhouse-materialization.md` and `docs/developer-cdc-clickhouse-materialization.md`.
- Modify `docs/README.md`, `docs/architecture.md`, `docs/ci-cd.md`, `docs/developer-ci-cd.md`, `docs/source-sink-matrix.md`, `docs/source-sink/mssql-to-clickhouse.md`, `docs/cdc-runtime-orchestrator.md`, and `mkdocs.yml`.

### Task 1: Runtime Materialization Contract

**Files:**
- Create: `tests/test_cdc_clickhouse_materialization.py`
- Create: `src/dpone/runtime/cdc/materialization.py`
- Modify: `src/dpone/runtime/cdc/__init__.py`

- [ ] **Step 1: Write the failing tests**

```python
from dpone.runtime.cdc.materialization import (
    ClickHouseCdcMaterializationPlan,
    ClickHouseCdcMaterializationPolicy,
    ClickHouseCdcMaterializationService,
)

def test_materialization_plan_parses_and_quotes_datasets():
    plan = ClickHouseCdcMaterializationPlan.from_datasets(
        cdc_dataset="analytics.orders_cdc",
        target_dataset="serving.orders_current",
        unique_key=("order_id",),
        default_database="default",
    )
    assert plan.qualified_cdc_table == "`analytics`.`orders_cdc`"
    assert plan.qualified_target_table == "`serving`.`orders_current`"

def test_materialization_replaces_target_with_latest_non_deleted_rows(tmp_path):
    connector = _FakeClickHouseConnector(source_events=3, materialized_rows=1, deleted_rows=1)
    report = ClickHouseCdcMaterializationService(connector).materialize(
        plan=_plan(),
        policy=ClickHouseCdcMaterializationPolicy(delete_mode="exclude_deleted"),
        output_dir=tmp_path,
    )
    assert report.passed is True
    assert report.rows_materialized == 1
    assert report.rows_deleted == 1
    assert (tmp_path / "cdc_materialization.json").exists()
```

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/test_cdc_clickhouse_materialization.py -q`
Expected: FAIL because `dpone.runtime.cdc.materialization` does not exist.

- [ ] **Step 3: Implement the smallest runtime service**

Create a focused module with immutable plan/policy/report dataclasses, safe identifier quoting, window-function latest-row selection, shadow-table replace, and no credential creation.

- [ ] **Step 4: Verify GREEN**

Run: `uv run pytest tests/test_cdc_clickhouse_materialization.py -q`
Expected: PASS.

### Task 2: Ops Facade and CLI Contract

**Files:**
- Create: `src/dpone/ops/cdc/materialization.py`
- Modify: `src/dpone/ops/catalog_cdc.py`
- Modify: `src/dpone/services/ops/command_handlers_cdc.py`
- Modify: `src/dpone/commands/ops_parsers_artifacts.py`
- Modify: `src/dpone/commands/ops_cmd.py`
- Modify: `src/dpone/commands/registry_ops.py`
- Create: `tests/test_cli_cdc_materialize_clickhouse_command.py`

- [ ] **Step 1: Write the failing CLI test**

```python
def test_ops_cdc_materialize_clickhouse_cli_delegates_connection_options(monkeypatch, capsys, tmp_path):
    captured = {}
    monkeypatch.setattr(command_handlers_cdc.OpsServiceCatalog, "default", staticmethod(lambda: SimpleNamespace(release=_Release(captured))))
    with pytest.raises(SystemExit) as exc:
        cli_main.main([
            "ops", "cdc-materialize-clickhouse",
            "--cdc-dataset", "analytics.orders_cdc",
            "--target-dataset", "serving.orders_current",
            "--unique-key", "order_id",
            "--sink-connection-id", "clickhouse-prod",
            "--output-dir", str(tmp_path),
            "--format", "json",
        ])
    assert exc.value.code == 0
    assert captured["cdc_dataset"] == "analytics.orders_cdc"
```

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/test_cli_cdc_materialize_clickhouse_command.py -q`
Expected: FAIL because the command is not registered.

- [ ] **Step 3: Implement parser, handler, catalog, and ops facade**

Add a thin command that calls `OpsServiceCatalog.default().release.cdc().materialization().materialize(...)`; convert CLI strings in the ops facade, not in command parser business logic.

- [ ] **Step 4: Verify GREEN**

Run: `uv run pytest tests/test_cli_cdc_materialize_clickhouse_command.py -q`
Expected: PASS.

### Task 3: Documentation and Architecture Contracts

**Files:**
- Create: `tests/test_cdc_clickhouse_materialization_docs_contract.py`
- Create: `docs/cdc-clickhouse-materialization.md`
- Create: `docs/developer-cdc-clickhouse-materialization.md`
- Modify: `docs/README.md`
- Modify: `docs/architecture.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/developer-ci-cd.md`
- Modify: `docs/source-sink-matrix.md`
- Modify: `docs/source-sink/mssql-to-clickhouse.md`
- Modify: `docs/cdc-runtime-orchestrator.md`
- Modify: `mkdocs.yml`

- [ ] **Step 1: Write the failing docs contract**

```python
def test_materialization_docs_cover_user_developer_runbook_ci_and_matrix():
    user = (DOCS / "cdc-clickhouse-materialization.md").read_text(encoding="utf-8")
    dev = (DOCS / "developer-cdc-clickhouse-materialization.md").read_text(encoding="utf-8")
    assert "dpone ops cdc-materialize-clickhouse" in user
    assert "ClickHouseCdcMaterializationService" in dev
    assert "test_mssql_clickhouse_live_cdc_runtime_integration.py" in user
```

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/test_cdc_clickhouse_materialization_docs_contract.py -q`
Expected: FAIL because docs are not present.

- [ ] **Step 3: Write English user/developer docs and update navigation**

Document delete modes, idempotent shadow replace, evidence files, local command, Docker integration command, extension rules, and architecture boundaries.

- [ ] **Step 4: Verify GREEN**

Run: `uv run pytest tests/test_cdc_clickhouse_materialization_docs_contract.py -q`
Expected: PASS.

### Task 4: Opt-In Docker Integration

**Files:**
- Modify: `tests/integration/mssql/test_mssql_clickhouse_live_cdc_runtime_integration.py`

- [ ] **Step 1: Write the failing integration test**

Add a test that creates MSSQL Change Tracking changes, writes them to a ClickHouse CDC log through the live runtime, runs `ClickHouseCdcMaterializationService`, and asserts:
- `exclude_deleted` target keeps only active latest keys;
- `tombstone` target keeps deleted latest keys with `dpone_cdc_deleted = 1`;
- report JSON/Markdown are written.

- [ ] **Step 2: Verify RED or SKIP Gate**

Run with Docker env:

```bash
DPONE_RUN_INTEGRATION=1 \
DPONE_IT_MSSQL_HOST=127.0.0.1 \
DPONE_IT_MSSQL_PORT=51433 \
DPONE_IT_MSSQL_DATABASE=dpone_it \
DPONE_IT_MSSQL_CDC_HOST=127.0.0.1 \
DPONE_IT_MSSQL_CDC_PORT=51433 \
DPONE_IT_MSSQL_CDC_DATABASE=dpone_it \
DPONE_IT_MSSQL_PASSWORD='Dp0ne.Strong.Pw.2026!' \
DPONE_IT_MSSQL_CDC_PASSWORD='Dp0ne.Strong.Pw.2026!' \
DPONE_IT_MSSQL_TRUST_SERVER_CERTIFICATE=yes \
DPONE_IT_CH_HOST=127.0.0.1 \
DPONE_IT_CH_PORT=59000 \
DPONE_IT_CH_HTTP_PORT=58123 \
DPONE_IT_CH_DATABASE=dpone_it \
DPONE_IT_CH_USER=default \
DPONE_IT_CH_PASSWORD=dpone \
uv run pytest tests/integration/mssql/test_mssql_clickhouse_live_cdc_runtime_integration.py -q
```

Expected before implementation: FAIL in the new materialization assertions, or SKIP only if Docker services are explicitly unavailable.

- [ ] **Step 3: Implement integration support through the runtime service**

No new route-specific runtime branches are allowed. Reuse the same service and injected connector.

- [ ] **Step 4: Verify GREEN**

Run the same Docker command. Expected: PASS.

### Task 5: Full Verification and Generated Docs

**Files:**
- Modify generated docs only through existing tooling where required: `docs/cli-reference.md`, `docs/quality-metrics.md`, benchmark docs if the full gate requires it.

- [ ] **Step 1: Run focused CDC tests**

Run:

```bash
uv run pytest \
  tests/test_cdc_clickhouse_materialization.py \
  tests/test_cli_cdc_materialize_clickhouse_command.py \
  tests/test_cdc_clickhouse_materialization_docs_contract.py \
  tests/test_cdc_live_adapters.py \
  tests/test_cli_cdc_runtime_run_command.py \
  -q
```

- [ ] **Step 2: Run full non-live suite and static gates**

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

- [ ] **Step 3: Regenerate CLI and quality docs if gates report staleness**

Use the existing docs tools instead of manual editing for generated sections:

```bash
uv run dpone docs update-cli-reference --output docs/cli-reference.md
uv run dpone docs update-dev-metrics --output docs/quality-metrics.md
```

- [ ] **Step 4: Re-run affected gates**

Re-run the failing gate commands until outputs are clean or record the exact blocker.
