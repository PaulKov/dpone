# Durable Route Execution Ledger Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a generic durable/shared backend for the route execution ledger with atomic compare-and-swap appends and lease fencing.

**Architecture:** Keep route execution as a control-plane feature. Add a small store protocol, keep JSON local storage as the default, and add a SQLite-backed store that uses transactions for shared-runner coordination. The service stays connector-free and delegates persistence concerns to injected stores.

**Tech Stack:** Python stdlib `sqlite3`, existing route execution models/policy/service, pytest, MkDocs, generated CLI reference, quality metrics.

---

### Task 1: Store Protocol And CAS Contract

**Files:**
- Modify: `src/dpone/ops/routes/execution_store.py`
- Modify: `src/dpone/ops/route_execution.py`
- Test: `tests/test_route_execution_ledger_sqlite.py`

- [ ] Write a failing test that calls `append_steps_if_version` twice with the same expected version and expects the second append to fail.
- [ ] Add `RouteExecutionLedgerStore` protocol with `read_steps`, `append_steps_if_version`, `write_steps`, `acquire_lease`, `ledger_path`, and `backend_name`.
- [ ] Update `LocalRouteExecutionLedgerStore` to implement the protocol with best-effort version checks.
- [ ] Update `RouteExecutionService` to call `append_steps_if_version` and return a `route_execution.concurrent_write_conflict` blocker if the compare-and-swap write fails.

### Task 2: SQLite Shared Store

**Files:**
- Create: `src/dpone/ops/routes/execution_store_sqlite.py`
- Modify: `src/dpone/ops/routes/__init__.py`
- Test: `tests/test_route_execution_ledger_sqlite.py`

- [ ] Write failing tests for cross-service persistence and lease fencing using one shared SQLite file.
- [ ] Implement `SqliteRouteExecutionLedgerStore` with `BEGIN IMMEDIATE` transactions, route run rows, lease rows, and deterministic JSON serialization.
- [ ] Ensure expired leases can be taken over and active leases block different owners.
- [ ] Export the store from `dpone.ops.routes`.

### Task 3: CLI Backend Selection

**Files:**
- Create: `src/dpone/ops/routes/execution_store_factory.py`
- Modify: `src/dpone/ops/catalog_readiness.py`
- Modify: `src/dpone/ops/catalog_release.py`
- Modify: `src/dpone/commands/ops_parsers_core.py`
- Modify: `src/dpone/services/ops/command_handlers_release.py`
- Test: `tests/test_cli_route_execution_ledger_command.py`

- [ ] Write a failing CLI test for `--store-backend sqlite --store-uri <path>`.
- [ ] Add a small factory that maps `local_json` and `sqlite` to store instances.
- [ ] Wire backend selection through the ops catalog; keep handlers thin.
- [ ] Regenerate CLI reference.

### Task 4: Docs And Quality Contracts

**Files:**
- Modify: `docs/route-execution-ledger.md`
- Modify: `docs/developer-route-execution-ledger.md`
- Modify: `docs/architecture.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/developer-ci-cd.md`
- Modify: `docs/ops-cli.md`
- Modify: `tests/test_route_execution_ledger_docs_contract.py`

- [ ] Add user docs for local JSON vs SQLite backend, atomic compare-and-swap, shared runner lease fencing, and runbook failures.
- [ ] Add developer docs for store protocol, SQLite schema ownership, extension rules, and no connector imports.
- [ ] Update architecture and CI/CD docs so durable ledger evidence is part of the route control plane.
- [ ] Regenerate quality metrics and CLI docs.

### Task 5: Verification

**Commands:**
- `uv run pytest tests/test_route_execution_ledger.py tests/test_route_execution_ledger_sqlite.py tests/test_cli_route_execution_ledger_command.py tests/test_route_execution_ledger_docs_contract.py -q`
- `uv run ruff check src/dpone/ops/routes/execution_store.py src/dpone/ops/routes/execution_store_sqlite.py src/dpone/ops/routes/execution_store_factory.py src/dpone/ops/route_execution.py tests/test_route_execution_ledger_sqlite.py`
- `uv run mypy --config-file mypy.ini src/dpone/ops/routes/execution_store.py src/dpone/ops/routes/execution_store_sqlite.py src/dpone/ops/routes/execution_store_factory.py src/dpone/ops/route_execution.py`
- `uv run dpone docs update-cli-reference --check`
- `uv run dpone docs update-dev-metrics --check`
- `uv run dpone docs check-docs`
- `uv run mkdocs build --strict`
- `uv run pytest -m "not integration_live" -q`

---

## Self-Review

- Spec coverage: durable shared backend, CAS append, lease fencing, CLI selection, docs, architecture, CI/CD, and verification are covered.
- Placeholder scan: no placeholder tasks remain.
- Scope: focused on route execution ledger persistence only; no runtime connector or source/sink mutation changes.
