# CDC Poison Quarantine Replay Implementation Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a generic CDC poison-event quarantine and replay execution foundation, with `mssql -> clickhouse` as the first live route.

**Architecture:** Keep runtime read/apply/commit semantics in `CdcRuntimeOrchestrator`, but inject small poison classification and quarantine collaborators. Keep replay execution as a separate service that applies quarantined events idempotently and never mutates offsets. Keep ClickHouse replay safety in the ClickHouse CDC sink applier by skipping already-seen event hashes.

**Tech Stack:** Python dataclasses and protocols, existing CDC runtime models, ClickHouse live adapter, existing ops catalog/CLI patterns, pytest, Docker-live MSSQL + ClickHouse integration tests, MkDocs docs gates.

---

### Task 1: RED Tests For Poison Classification And Runtime Policy

**Files:**
- Create: `tests/test_cdc_poison_quarantine.py`
- Create: `src/dpone/runtime/cdc/poison_models.py`
- Create: `src/dpone/runtime/cdc/poison.py`
- Modify: `src/dpone/runtime/cdc/runtime_models.py`
- Modify: `src/dpone/runtime/cdc/runtime_orchestrator.py`

- [ ] Write tests for `CdcPoisonClassifier` proving missing unique keys, unsupported operations, and duplicate event ids produce normalized `CdcPoisonRecord` objects.
- [ ] Write an orchestrator test proving default `poison_mode="fail_closed"` blocks apply and offset commit when poison events are present.
- [ ] Write an orchestrator test proving `poison_mode="quarantine_and_continue"` writes `cdc_poison_quarantine.json`, applies only clean events, and commits the next offset after durable sink apply.
- [ ] Run `uv run pytest tests/test_cdc_poison_quarantine.py -q` and confirm it fails on missing poison modules or policy fields.
- [ ] Implement immutable poison models, JSON/Markdown rendering, file quarantine writer, classifier, and orchestrator integration.
- [ ] Re-run `uv run pytest tests/test_cdc_poison_quarantine.py -q` and confirm it passes.

### Task 2: RED Tests For ClickHouse CDC Replay Dedupe

**Files:**
- Create: `tests/test_clickhouse_cdc_sink_dedupe.py`
- Modify: `src/dpone/runtime/cdc/live_adapters.py`

- [ ] Write a fake ClickHouse connector test proving `ClickHouseCdcSinkApplier` inserts a new event once and skips the same `dpone_cdc_event_hash` on replay.
- [ ] Assert the second receipt is durable, passed, `rows_applied == 0`, and metrics include `duplicate_events_skipped == 1`.
- [ ] Run `uv run pytest tests/test_clickhouse_cdc_sink_dedupe.py -q` and confirm it fails because replay dedupe is not implemented.
- [ ] Add target-side existing-event-hash lookup to the ClickHouse applier after table creation and before insert.
- [ ] Re-run the dedupe test and existing runtime/live focused tests.

### Task 3: RED Tests For Replay Execution And Quarantine Inspect

**Files:**
- Create: `tests/test_cdc_replay_execute.py`
- Create: `src/dpone/runtime/cdc/replay_execution.py`
- Create: `src/dpone/ops/cdc/replay_execute.py`
- Create: `src/dpone/ops/cdc/quarantine.py`
- Modify: `src/dpone/ops/catalog_cdc.py`

- [ ] Write tests proving `CdcReplayExecutionService` reads `cdc_poison_quarantine.json`, reconstructs `CDCChange` records, applies them through an injected sink applier, writes `cdc_replay_execution.json`, and does not touch offsets.
- [ ] Write tests proving `CdcQuarantineInspectionService` summarizes record counts by reason/action and writes `cdc_quarantine_inspection.json`.
- [ ] Run `uv run pytest tests/test_cdc_replay_execute.py -q` and confirm it fails on missing services.
- [ ] Implement replay models/service and quarantine inspection service with injected applier support and local artifact output.
- [ ] Re-run `uv run pytest tests/test_cdc_replay_execute.py -q` and confirm it passes.

### Task 4: CLI Wiring

**Files:**
- Create: `tests/test_cli_cdc_poison_replay_commands.py`
- Create: `src/dpone/commands/ops_parsers_cdc_replay.py`
- Modify: `src/dpone/services/ops/command_handlers_cdc.py`
- Modify: `src/dpone/services/ops/__init__.py`
- Modify: `src/dpone/commands/ops_cmd.py`
- Modify: `src/dpone/commands/registry_ops.py`

- [ ] Write CLI delegation tests for `dpone ops cdc-quarantine-inspect` and `dpone ops cdc-replay-execute`.
- [ ] Run the CLI tests and confirm they fail because parsers/handlers are missing.
- [ ] Add focused parsers for inspect/replay commands; keep handlers thin and delegate into `OpsServiceCatalog.default().release.cdc()`.
- [ ] Re-run CLI tests and focused CDC tests.

### Task 5: Docs, Architecture, CI Contracts

**Files:**
- Create: `tests/test_cdc_poison_quarantine_docs_contract.py`
- Create: `docs/cdc-poison-quarantine.md`
- Create: `docs/developer-cdc-poison-quarantine.md`
- Modify: `docs/README.md`
- Modify: `docs/architecture.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/developer-ci-cd.md`
- Modify: `docs/source-sink/mssql-to-clickhouse.md`
- Modify: `docs/source-sink-matrix.md`
- Modify: `mkdocs.yml`

- [ ] Write docs contract tests requiring user docs, developer docs, CLI names, artifact names, runbook, source-sink guide links, CI docs, and MkDocs nav entries.
- [ ] Run docs contract tests and confirm they fail on missing docs.
- [ ] Add English OSS docs for operator workflow, developer boundaries, extension rules, Docker-live command, and runbook.
- [ ] Update generated CLI reference and quality metrics after parser changes.

### Task 6: Docker-Live Replay Safety Coverage

**Files:**
- Modify: `tests/integration/mssql/test_mssql_clickhouse_live_cdc_runtime_integration.py`

- [ ] Extend the existing MSSQL -> ClickHouse live test to replay an already-applied ClickHouse CDC event through `ClickHouseCdcSinkApplier`.
- [ ] Assert the duplicate replay receipt is durable and passed, but `rows_applied == 0`, `duplicate_events_skipped == 1`, and the ClickHouse CDC log row count does not increase.
- [ ] Run the live test with `DPONE_RUN_INTEGRATION=1`.

### Task 7: Verification

- [ ] `uv run pytest tests/test_cdc_poison_quarantine.py tests/test_clickhouse_cdc_sink_dedupe.py tests/test_cdc_replay_execute.py tests/test_cli_cdc_poison_replay_commands.py tests/test_cdc_poison_quarantine_docs_contract.py -q`
- [ ] `DPONE_RUN_INTEGRATION=1 uv run pytest tests/integration/mssql/test_mssql_clickhouse_live_cdc_runtime_integration.py -q`
- [ ] `uv run pytest -m "not integration_live"`
- [ ] `uv run ruff check .`
- [ ] `uv run ruff format --check .`
- [ ] `uv run mypy --config-file mypy.ini`
- [ ] `uv run dpone docs update-cli-reference --check`
- [ ] `uv run dpone docs update-dev-metrics --check`
- [ ] `uv run dpone docs check-docs`
- [ ] `uv run dpone docs check-import-rules`
- [ ] `uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json`
- [ ] `uv run dpone docs check-module-size --baseline docs/module_size_baseline.json`
- [ ] `uv run dpone docs check-architecture-fitness`
- [ ] `uv run mkdocs build --strict`
- [ ] `uv build`

## Self-Review

- The feature is one cohesive industrial reliability increment: poison classification, quarantine, replay, and idempotent sink safety.
- Runtime connectors stay focused on read/apply. Replay execution is separate and does not mutate offsets.
- Default behavior remains fail-closed, preserving current safety unless the user explicitly enables quarantine-and-continue.
- Route-specific behavior is limited to ClickHouse duplicate-hash lookup and the Docker-live route test.
- Docs, generated references, CI contracts, local tests, and Docker-live tests are included.
