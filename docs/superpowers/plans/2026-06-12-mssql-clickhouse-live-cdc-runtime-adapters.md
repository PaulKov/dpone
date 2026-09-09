# MSSQL ClickHouse Live CDC Runtime Adapters Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add live MSSQL CDC/Change Tracking readers and a ClickHouse durable CDC sink applier behind the existing generic CDC runtime orchestrator.

**Architecture:** Keep `CdcRuntimeOrchestrator` route-agnostic. Add focused adapter modules under `dpone.runtime.cdc` and a thin ops facade under `dpone.ops.cdc.runtime_run` that chooses local JSON mode or live connector mode through DI. The first live profile is `mssql -> clickhouse`, but factories and ports remain backend-driven.

**Tech Stack:** Python dataclasses/protocols, existing `CDCReader` and `CdcSinkApplier` ports, `MSSQLCDCReader`, `MSSQLChangeTrackingReader`, `ClickHouseConnector`, `MSSQLCDCOffsetStorage`, pytest, CLI docs generation.

---

### Task 1: Live Adapter Value Objects

**Files:**
- Create: `src/dpone/runtime/cdc/live_adapters.py`
- Test: `tests/test_cdc_live_adapters.py`

- [ ] Write failing tests for `ClickHouseCdcApplyPlan`, dataset parsing, idempotency hash generation, and rows rendered from `CDCBatch`.
- [ ] Implement minimal value objects and pure helpers.
- [ ] Verify focused tests pass.

### Task 2: ClickHouse Sink Applier

**Files:**
- Modify: `src/dpone/runtime/cdc/live_adapters.py`
- Test: `tests/test_cdc_live_adapters.py`

- [ ] Write failing tests with a fake ClickHouse connector proving the applier creates a CDC target table, inserts append-only CDC rows, returns a durable `CdcApplyReceipt`, and reports blockers on connector failure.
- [ ] Implement `ClickHouseCdcSinkApplier` with small private helpers for SQL rendering and receipt building.
- [ ] Verify focused tests pass.

### Task 3: MSSQL Reader and Offset Factories

**Files:**
- Create: `src/dpone/runtime/cdc/live_factory.py`
- Test: `tests/test_cdc_live_factory.py`

- [ ] Write failing tests proving the factory maps `mssql_cdc` to `MSSQLCDCReader`, `mssql_change_tracking` to `MSSQLChangeTrackingReader`, and wraps `MSSQLCDCOffsetStorage` in the runtime `CdcOffsetStore` port.
- [ ] Implement small factories without importing optional clients at module import time.
- [ ] Verify focused tests pass.

### Task 4: CLI Live Mode Wiring

**Files:**
- Modify: `src/dpone/ops/cdc/runtime_run.py`
- Modify: `src/dpone/commands/ops_parsers_artifacts.py`
- Modify: `src/dpone/services/ops/command_handlers_cdc.py`
- Test: `tests/test_cli_cdc_runtime_run_command.py`

- [ ] Write failing CLI tests for `--mode local` and `--mode live` argument contracts using an injected fake service where possible.
- [ ] Add mode/configuration arguments without putting business logic in handlers.
- [ ] Keep local JSON mode backward compatible.
- [ ] Verify focused CLI tests pass.

### Task 5: Docs, Architecture, CI Contracts

**Files:**
- Create: `docs/cdc-live-runtime-adapters.md`
- Create: `docs/developer-cdc-live-runtime-adapters.md`
- Modify: `docs/cdc-runtime-orchestrator.md`
- Modify: `docs/developer-cdc-runtime-orchestrator.md`
- Modify: `docs/source-sink/mssql-to-clickhouse.md`
- Modify: `docs/source-sink-matrix.md`
- Modify: `docs/architecture.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/developer-ci-cd.md`
- Modify: `docs/README.md`
- Modify: `mkdocs.yml`
- Test: `tests/test_cdc_live_adapters_docs_contract.py`

- [ ] Write docs contract tests for user docs, developer docs, examples, runbook, matrix, architecture, and CI references.
- [ ] Write English docs and update nav/architecture.
- [ ] Regenerate CLI reference and quality metrics if generated files change.
- [ ] Verify docs tests pass.

### Task 6: Full Verification

**Files:**
- Generated docs if required.

- [ ] Run focused tests for live adapters and runtime command.
- [ ] Run non-live test suite.
- [ ] Run ruff, format check, mypy, docs checks, mkdocs strict, architecture/code quality checks, and build.
- [ ] Report exact verification evidence and remaining risks.
