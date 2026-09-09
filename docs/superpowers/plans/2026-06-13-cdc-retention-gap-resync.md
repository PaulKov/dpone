# CDC Retention Gap Auto-Resync Implementation Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a generic CDC retention gap detector and resumable resync planner/executor, with MSSQL -> ClickHouse as the first live route.

**Architecture:** Keep the feature in the CDC control plane. Probes expose source retention bounds, policy turns bounds plus committed offsets into go/no-go decisions, planner emits stable resync actions, and execution reuses the existing `CdcSinkApplier` port without committing source offsets.

**Tech Stack:** Python dataclasses/protocols, existing `CDCOffset`, `CdcRuntimeStream`, `CDCBatch`, `CDCChange`, `CdcSinkApplier`, argparse CLI handlers, pytest, docs contract checks, mkdocs.

---

## File Structure

- Create `src/dpone/runtime/cdc/retention_models.py`: serializable value objects for retention bounds, decisions, reports, resync actions/plans, and execution reports.
- Create `src/dpone/runtime/cdc/retention.py`: generic policy, retention evaluation service, and resync planner.
- Create `src/dpone/runtime/cdc/retention_probes.py`: probe protocol plus static/local, MSSQL Change Tracking, and MSSQL CDC probes.
- Create `src/dpone/runtime/cdc/resync.py`: execution service that turns resync actions into CDC changes and applies them through `CdcSinkApplier`.
- Create `src/dpone/ops/cdc/retention_resync.py`: ops facade for local/live retention checks, plan generation, and execution.
- Create `src/dpone/commands/ops_parsers_cdc_retention_resync.py`: CLI parser definitions.
- Modify `src/dpone/runtime/cdc/__init__.py`: lazy exports only.
- Modify `src/dpone/ops/catalog_cdc_recovery.py`, `src/dpone/ops/catalog_cdc.py`, `src/dpone/commands/ops_cmd.py`, `src/dpone/commands/registry_ops.py`, `src/dpone/services/ops/command_handlers_cdc.py`, and `src/dpone/services/ops/__init__.py`: wire service factories and commands.
- Create tests:
  - `tests/test_cdc_retention_gap.py`
  - `tests/test_cdc_resync_plan_execute.py`
  - `tests/test_cdc_retention_live_probes.py`
  - `tests/test_cli_cdc_retention_resync_commands.py`
  - `tests/test_cdc_retention_resync_docs_contract.py`
- Modify `tests/integration/mssql/test_mssql_clickhouse_live_cdc_runtime_integration.py`: add opt-in live coverage that proves MSSQL Change Tracking bounds can be probed and a resync action can restore ClickHouse without mutating offsets.
- Create docs:
  - `docs/cdc-retention-resync.md`
  - `docs/developer-cdc-retention-resync.md`
- Modify docs indexes and gates: `docs/README.md`, `docs/architecture.md`, `docs/ci-cd.md`, `docs/developer-ci-cd.md`, `docs/ops-cli.md`, `docs/source-sink-matrix.md`, `docs/source-sink/mssql-to-clickhouse.md`, `mkdocs.yml`.

## Task 1: Retention Models and Policy

- [ ] **Step 1: Write failing tests**

Create `tests/test_cdc_retention_gap.py` covering:

```python
def test_retention_policy_detects_healthy_at_risk_and_gap(tmp_path):
    stream = _stream()
    service = CdcRetentionGapService(policy=CdcRetentionPolicy(at_risk_margin=5))
    healthy = service.evaluate(
        output_dir=tmp_path / "healthy",
        stream=stream,
        committed_offset=CDCOffset(CDCBackend.MSSQL_CHANGE_TRACKING, "120", True),
        probe=StaticCdcRetentionProbe(
            CdcRetentionBounds(
                backend=CDCBackend.MSSQL_CHANGE_TRACKING,
                min_available_offset="100",
                high_watermark="150",
                current_offset="150",
                retention_seconds=172800,
            )
        ),
    )
    assert healthy.passed is True
    assert healthy.decision.level == "healthy"
    assert healthy.metrics["offset_margin"] == 20
```

Also cover `at_risk` when margin is below threshold and `gap_detected` when the committed offset is older than `min_available_offset`.

- [ ] **Step 2: Run the red test**

Run: `uv run pytest tests/test_cdc_retention_gap.py -q`

Expected: fail with missing `dpone.runtime.cdc.retention` imports.

- [ ] **Step 3: Implement models and policy**

Add immutable dataclasses with stable `to_dict()`, `to_json()`, `to_markdown()`, and `write()` methods. Keep offset comparison numeric for Change Tracking and hex-aware for CDC LSN strings.

- [ ] **Step 4: Run the green test**

Run: `uv run pytest tests/test_cdc_retention_gap.py -q`

Expected: pass.

## Task 2: Resync Planner and Executor

- [ ] **Step 1: Write failing tests**

Create `tests/test_cdc_resync_plan_execute.py` covering:

```python
def test_resync_planner_creates_bounded_snapshot_plan_for_gap(tmp_path):
    report = _gap_report(tmp_path)
    plan_report = CdcResyncPlanner().plan(
        output_dir=tmp_path / "plan",
        retention_report=report,
        rows_json=tmp_path / "rows.json",
        max_rows=100,
    )
    assert plan_report.passed is True
    assert plan_report.plan.action_count == 1
    assert plan_report.plan.actions[0].operation == "upsert"
```

Also cover execution through an injected fake `CdcSinkApplier`, `committed=False`, and report schema `dpone.cdc_resync_execution.v1`.

- [ ] **Step 2: Run the red test**

Run: `uv run pytest tests/test_cdc_resync_plan_execute.py -q`

Expected: fail with missing resync modules.

- [ ] **Step 3: Implement planner and executor**

Planner emits replayable `CdcResyncAction` records from local snapshot rows. Executor converts each action into `CDCChange` using `CDCOperation.INSERT` or `CDCOperation.DELETE`, applies one bounded batch, and never touches offset stores.

- [ ] **Step 4: Run the green test**

Run: `uv run pytest tests/test_cdc_resync_plan_execute.py -q`

Expected: pass.

## Task 3: Live Probes and Ops Facade

- [ ] **Step 1: Write failing tests**

Create `tests/test_cdc_retention_live_probes.py` with connector stubs proving:

```python
probe = MssqlChangeTrackingRetentionProbe(connector=connector, source_schema="dbo", source_table="orders")
bounds = probe.read_bounds()
assert bounds.min_available_offset == "10"
assert "CHANGE_TRACKING_MIN_VALID_VERSION" in connector.queries[0]
```

Also cover `MssqlCdcRetentionProbe` using `sys.fn_cdc_get_min_lsn` and `sys.fn_cdc_get_max_lsn`.

- [ ] **Step 2: Run the red test**

Run: `uv run pytest tests/test_cdc_retention_live_probes.py -q`

Expected: fail before probe implementation.

- [ ] **Step 3: Implement probe adapters and ops services**

Keep probe logic in `retention_probes.py`; keep connector construction and mode branching in `ops/cdc/retention_resync.py`.

- [ ] **Step 4: Run the green test**

Run: `uv run pytest tests/test_cdc_retention_live_probes.py tests/test_cdc_retention_gap.py tests/test_cdc_resync_plan_execute.py -q`

Expected: pass.

## Task 4: CLI Wiring

- [ ] **Step 1: Write failing CLI tests**

Create `tests/test_cli_cdc_retention_resync_commands.py` for:

- `dpone ops cdc-retention-check`
- `dpone ops cdc-resync-plan`
- `dpone ops cdc-resync-execute`

Assert parsed options are delegated unchanged to injected catalog services and exit codes mirror `report.passed`.

- [ ] **Step 2: Run the red test**

Run: `uv run pytest tests/test_cli_cdc_retention_resync_commands.py -q`

Expected: parser/handler missing.

- [ ] **Step 3: Implement parser, handlers, catalog, and registry wiring**

No business logic in parser/handler files; they only parse args, call services, and emit reports.

- [ ] **Step 4: Run the green test**

Run: `uv run pytest tests/test_cli_cdc_retention_resync_commands.py -q`

Expected: pass.

## Task 5: Docs, Architecture, CI, and Live Evidence

- [ ] **Step 1: Write failing docs contract tests**

Create `tests/test_cdc_retention_resync_docs_contract.py` asserting docs, mkdocs nav, source-sink matrix, architecture, and CI documentation mention:

- `CDC retention gap auto-resync`
- `cdc-retention-check`
- `cdc-resync-plan`
- `cdc-resync-execute`
- `CdcRetentionGapService`
- `CdcResyncPlanner`
- `CdcResyncExecutionService`
- `mssql -> clickhouse`

- [ ] **Step 2: Run the red test**

Run: `uv run pytest tests/test_cdc_retention_resync_docs_contract.py -q`

Expected: fail until docs are added.

- [ ] **Step 3: Add English user/developer docs and update architecture/CI/source-sink docs**

Document local mode, live MSSQL -> ClickHouse mode, failure semantics, offset safety, Docker-live gate, runbooks, examples, developer extension points, and quality expectations.

- [ ] **Step 4: Add live opt-in coverage**

Extend the existing MSSQL + ClickHouse integration test with a lightweight retention probe and resync execution check guarded by `DPONE_RUN_INTEGRATION=1`.

- [ ] **Step 5: Regenerate docs artifacts**

Run:

```bash
uv run dpone docs update-cli-reference
uv run dpone docs update-dev-metrics
```

## Task 6: Full Verification

- [ ] **Step 1: Focused tests**

Run:

```bash
uv run pytest tests/test_cdc_retention_gap.py tests/test_cdc_resync_plan_execute.py tests/test_cdc_retention_live_probes.py tests/test_cli_cdc_retention_resync_commands.py tests/test_cdc_retention_resync_docs_contract.py -q
```

- [ ] **Step 2: CDC regression set**

Run:

```bash
uv run pytest tests/test_cdc_* tests/test_cli_cdc_* -q
```

- [ ] **Step 3: Optional live route test**

Run when local Docker/live dependencies are available:

```bash
DPONE_RUN_INTEGRATION=1 uv run pytest tests/integration/mssql/test_mssql_clickhouse_live_cdc_runtime_integration.py -q
```

- [ ] **Step 4: Full OSS and quality gates**

Run:

```bash
uv run pytest -m "not integration" -q
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run dpone docs update-cli-reference --check
uv run dpone docs update-dev-metrics --check
uv run dpone docs check-docs
uv run dpone docs check-import-rules
uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json
uv run dpone docs check-module-size --baseline docs/module_size_baseline.json
uv run dpone docs check-architecture-fitness
uv run dpone docs check-compatibility
uv run dpone docs update-deprecation-roadmap --check
uv run dpone docs update-shim-removal-plan --check
uv run mkdocs build --strict
uv build
```

## Self-Review

- Spec coverage: retention bounds, gap policy, resync plan, no offset commit, live MSSQL probes, ClickHouse execution through existing sink applier, CLI, docs, CI, architecture, and tests are mapped to tasks.
- Placeholder scan: no open TBD/TODO markers.
- Type consistency: service and model names match the proposed module boundaries and CLI command names.
