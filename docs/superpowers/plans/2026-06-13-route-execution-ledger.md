# Route Execution Ledger Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a generic route execution ledger and idempotent commit protocol for any `source -> sink -> strategy` route.

**Architecture:** The feature lives in the ops/control-plane route taxonomy and does not execute connector work. Small model, store, policy, and service modules write stable JSON/Markdown evidence that can be consumed by route readiness, release gates, CI, and incident runbooks.

**Tech Stack:** Python dataclasses, local JSON ledger store, existing `RouteKey`, existing ops CLI parser/handler/catalog patterns, pytest, MkDocs.

---

### Task 1: Core Models And Policy

**Files:**
- Create: `src/dpone/ops/routes/execution_models.py`
- Create: `src/dpone/ops/routes/execution_policy.py`
- Test: `tests/test_route_execution_ledger.py`

- [ ] **Step 1: Write failing tests**

```python
from dpone.ops.routes.execution_models import RouteExecutionStage, RouteExecutionStatus

def test_route_execution_stage_order_is_append_only():
    assert RouteExecutionStage.PLANNED.can_transition_to(RouteExecutionStage.EXTRACTING)
    assert not RouteExecutionStage.FINALIZED.can_transition_to(RouteExecutionStage.LOADED_TO_STAGING)
    assert RouteExecutionStatus.COMMITTED.is_terminal
```

- [ ] **Step 2: Run red test**

Run: `uv run pytest tests/test_route_execution_ledger.py -q`
Expected: fails because `dpone.ops.routes.execution_models` does not exist.

- [ ] **Step 3: Implement minimal models**

Create enums for stages/statuses, immutable route execution identity, immutable step record, and a pure policy that returns blockers for backward stage movement, failed side-effect checks, missing final quality evidence, or state commits before sink success.

- [ ] **Step 4: Run green test**

Run: `uv run pytest tests/test_route_execution_ledger.py -q`
Expected: model tests pass.

### Task 2: Local Ledger Store And Service

**Files:**
- Create: `src/dpone/ops/routes/execution_store.py`
- Create: `src/dpone/ops/route_execution.py`
- Test: `tests/test_route_execution_ledger.py`

- [ ] **Step 1: Write failing tests**

```python
def test_route_execution_service_records_idempotent_steps(tmp_path):
    service = RouteExecutionService()
    report = service.record_step(
        output_dir=tmp_path,
        source="mssql",
        sink="clickhouse",
        strategy="cdc",
        dataset="dbo.orders",
        run_id="run-1",
        stage="loaded_to_staging",
        status="succeeded",
        source_boundary="lsn:001",
        sink_boundary="event_hash:abc",
        artifact_paths={},
    )
    assert report.passed is True
    assert Path(report.json_path).exists()
```

- [ ] **Step 2: Run red test**

Run: `uv run pytest tests/test_route_execution_ledger.py -q`
Expected: fails because service/store are missing.

- [ ] **Step 3: Implement store and service**

Use a local JSON store with one index file and one run-specific ledger file per route/dataset/run. The service should be dependency-injected with store and policy, normalize route identity through `RouteKey`, compute artifact checksums, append idempotent steps, and write `route_execution_ledger.json` plus `route_commit_protocol.md`.

- [ ] **Step 4: Run green test**

Run: `uv run pytest tests/test_route_execution_ledger.py -q`
Expected: all ledger service tests pass.

### Task 3: CLI And Ops Catalog

**Files:**
- Modify: `src/dpone/commands/ops_parsers_artifacts.py`
- Modify: `src/dpone/commands/ops_cmd.py`
- Modify: `src/dpone/commands/registry_ops.py`
- Modify: `src/dpone/ops/catalog_readiness.py`
- Modify: `src/dpone/ops/catalog_release.py`
- Modify: `src/dpone/services/ops/command_handlers_release.py`
- Modify: `src/dpone/services/ops/__init__.py`
- Test: `tests/test_cli_route_execution_ledger_command.py`

- [ ] **Step 1: Write failing CLI test**

```python
def test_ops_route_execution_ledger_cli_outputs_json(monkeypatch, capsys, tmp_path):
    cli_main.main([
        "ops", "route-execution-ledger",
        "--source", "mssql", "--sink", "clickhouse", "--strategy", "cdc",
        "--dataset", "dbo.orders", "--run-id", "run-1",
        "--stage", "loaded_to_staging", "--status", "succeeded",
        "--output-dir", str(tmp_path / "ledger"), "--format", "json",
    ])
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "dpone.route_execution_ledger.v1"
```

- [ ] **Step 2: Run red test**

Run: `uv run pytest tests/test_cli_route_execution_ledger_command.py -q`
Expected: fails because the command is not registered.

- [ ] **Step 3: Wire parser, handler, catalog, and exports**

Keep parser logic limited to argument collection. The handler should call `_ops(ctx).route_execution_ledger().record_step(...)`, emit JSON/Markdown, and return non-zero when blockers exist.

- [ ] **Step 4: Run green test**

Run: `uv run pytest tests/test_cli_route_execution_ledger_command.py -q`
Expected: CLI tests pass.

### Task 4: Documentation, Architecture, And CI Contracts

**Files:**
- Create: `docs/route-execution-ledger.md`
- Create: `docs/developer-route-execution-ledger.md`
- Modify: `docs/architecture.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/developer-ci-cd.md`
- Modify: `docs/ops-cli.md`
- Modify: `docs/source-sink-matrix.md`
- Modify: `mkdocs.yml`
- Test: `tests/test_route_execution_ledger_docs_contract.py`

- [ ] **Step 1: Write failing docs contract**

```python
def test_route_execution_ledger_docs_cover_user_developer_runbook_and_architecture():
    assert "route-execution-ledger.md" in Path("mkdocs.yml").read_text()
    assert "RouteExecutionService" in Path("docs/developer-route-execution-ledger.md").read_text()
```

- [ ] **Step 2: Run red test**

Run: `uv run pytest tests/test_route_execution_ledger_docs_contract.py -q`
Expected: fails because docs are absent.

- [ ] **Step 3: Add docs and links**

Document user workflow, examples for bulk/CDC/resync, operator runbook, developer module boundaries, DI rules, CI evidence, and architecture responsibilities. Keep all OSS docs in English.

- [ ] **Step 4: Run docs checks**

Run: `uv run pytest tests/test_route_execution_ledger_docs_contract.py tests/test_docs_language_contracts.py -q`
Expected: docs contract and English-only docs pass.

### Task 5: Verification

**Files:**
- All feature files from prior tasks.

- [ ] **Step 1: Run focused test suite**

Run: `uv run pytest tests/test_route_execution_ledger.py tests/test_cli_route_execution_ledger_command.py tests/test_route_execution_ledger_docs_contract.py -q`
Expected: all focused tests pass.

- [ ] **Step 2: Run quality gates**

Run:

```bash
uv run ruff check src/dpone/ops/routes/execution_models.py src/dpone/ops/routes/execution_policy.py src/dpone/ops/routes/execution_store.py src/dpone/ops/route_execution.py tests/test_route_execution_ledger.py tests/test_cli_route_execution_ledger_command.py tests/test_route_execution_ledger_docs_contract.py
uv run mypy --config-file mypy.ini
uv run mkdocs build --strict
```

Expected: lint, typing, and docs build pass without warnings.

## Self-Review

- Spec coverage: models, idempotent policy, local ledger store, service, CLI, docs, architecture, CI contracts, and verification are covered.
- Placeholder scan: no TBD/TODO/placeholders remain.
- Type consistency: `RouteExecutionService`, `RouteExecutionStage`, `RouteExecutionStatus`, and `route-execution-ledger` names are used consistently.
