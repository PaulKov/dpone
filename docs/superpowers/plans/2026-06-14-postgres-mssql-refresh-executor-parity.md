# Postgres MSSQL Refresh Executor Parity Implementation Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a production-grade `postgres_mssql` route refresh executor with replay-safe live certification and a reusable native refresh pipeline shape for future source -> sink routes.

**Architecture:** Keep `RouteRefreshExecutionService` generic and route-agnostic. Add a small generic refresh pipeline module with prepare/export/load/artifact contracts, then implement `postgres_mssql` as a focused backend composed from Postgres COPY export and MSSQL bcp import adapters. Keep route-specific SQL and config in backend modules, not in CLI or service code.

**Tech Stack:** Python 3.12, pytest, ruff, mypy, MkDocs, Postgres COPY, MSSQL bcp, Docker-live opt-in integration tests.

---

### Task 1: RED contracts

**Files:**
- Modify: `tests/test_route_refresh_executor_registry.py`
- Create: `tests/test_postgres_mssql_refresh_live_certification_docs_contract.py`
- Create: `tests/integration/mssql/test_postgres_mssql_refresh_executor_live_integration.py`

- [ ] **Step 1: Write failing unit tests**

Add tests proving `postgres_mssql` can be selected from `RouteRefreshExecutorRegistry`, calls prepare/export/load in order, writes chunk artifacts, and blocks unsupported routes before side effects.

- [ ] **Step 2: Write failing docs/workflow tests**

Require user docs, developer docs, CI/CD docs, source-sink docs, and `.github/workflows/live-certification.yml` to mention `DPONE_RUN_REFRESH_EXECUTOR_LIVE=1`, `postgres_mssql`, `test_postgres_mssql_refresh_executor_live_integration.py`, and `route_refresh_execution.json`.

- [ ] **Step 3: Verify RED**

Run:

```bash
uv run pytest tests/test_route_refresh_executor_registry.py::test_route_refresh_executor_registry_builds_postgres_mssql_executor_from_config -q
uv run pytest tests/test_postgres_mssql_refresh_live_certification_docs_contract.py -q
```

Expected: both fail because the backend and docs do not exist yet.

### Task 2: Generic native refresh pipeline

**Files:**
- Create: `src/dpone/ops/routes/refresh_executors/native_pipeline.py`
- Test: `tests/test_route_refresh_executor_registry.py`

- [ ] **Step 1: Add thin contracts**

Create dataclasses for `NativeChunkPrepareResult`, `NativeChunkExportResult`, `NativeChunkLoadResult`, and protocols for `NativeChunkPreparer`, `NativeChunkExporter`, `NativeChunkLoader`.

- [ ] **Step 2: Add helper normalization**

Add small normalization helpers so concrete backends can return dataclasses or mapping-like fake results in tests.

- [ ] **Step 3: Verify focused tests**

Run:

```bash
uv run pytest tests/test_route_refresh_executor_registry.py -q
```

Expected: existing `mssql_clickhouse` tests still pass.

### Task 3: Postgres -> MSSQL backend

**Files:**
- Create: `src/dpone/ops/routes/refresh_executors/postgres_mssql_config.py`
- Create: `src/dpone/ops/routes/refresh_executors/postgres_mssql_adapters.py`
- Create: `src/dpone/ops/routes/refresh_executors/postgres_mssql_artifacts.py`
- Create: `src/dpone/ops/routes/refresh_executors/postgres_mssql_executor.py`
- Create: `src/dpone/ops/routes/refresh_executors/postgres_mssql.py`
- Modify: `src/dpone/ops/routes/refresh_executors/__init__.py`
- Modify: `src/dpone/ops/routes/refresh_executors/registry.py`

- [ ] **Step 1: Config**

Parse `source_dataset`, `target_dataset`, `boundary_column`, `columns`, `query_template`, `postgres`, and `mssql` mappings. Reuse safe identifier rules from `mssql_clickhouse_config`.

- [ ] **Step 2: Adapters**

Implement `PostgresCopyChunkExporter` with `PostgresConnector.copy_to_file(format="MSSQL_DELIMITED", compress=False)` and `MssqlBcpChunkLoader` with `BcpRunner.import_file()`.

- [ ] **Step 3: Prepare**

Implement MSSQL bounded cleanup with `DELETE FROM [schema].[table] WHERE [boundary] BETWEEN <start> AND <end>` before bcp import.

- [ ] **Step 4: Artifact**

Write `dpone.postgres_mssql.route_refresh_chunk.v1` per-chunk JSON with route, query hash, prepare/export/load evidence, row counts, transfer checksum, blockers, and redacted commands.

- [ ] **Step 5: Registry**

Register `postgres_mssql` and `postgres-mssql` aliases.

### Task 4: Live certification and docs

**Files:**
- Create: `tests/integration/mssql/test_postgres_mssql_refresh_executor_live_integration.py`
- Modify: `.github/workflows/live-certification.yml`
- Modify: `docs/route-refresh-execute.md`
- Modify: `docs/developer-route-refresh-execute.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/source-sink/postgres-to-mssql.md`
- Modify: `docs/architecture.md`
- Modify: `docs/ops-cli.md`

- [ ] **Step 1: Live test**

Build a small Postgres source table and MSSQL target table, execute the same `postgres -> mssql -> incremental_merge` plan twice, then assert no duplicates, typed hash equality, two chunk artifacts, and replay `route_refresh_execution.json`.

- [ ] **Step 2: Workflow**

Run the test in `Live certification` for non-`type_matrix_certification` profiles, storing evidence under `test_artifacts/live_certification/refresh-executor/postgres-mssql/`.

- [ ] **Step 3: Docs**

Document local Docker env variables, expected artifacts, replay semantics, and release gate usage.

### Task 5: Verification and PR

- [ ] **Step 1: Focused tests**

```bash
uv run pytest tests/test_route_refresh_executor_registry.py tests/test_postgres_mssql_refresh_live_certification_docs_contract.py -q
```

- [ ] **Step 2: Non-live quality**

```bash
uv run pytest -m "not integration_live" -q
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy --config-file mypy.ini
uv run dpone docs update-cli-reference --check
uv run dpone docs update-dev-metrics --check
uv run dpone docs check-docs
uv run dpone docs check-import-rules
uv run dpone docs check-layer-metrics
uv run dpone docs check-module-size
uv run dpone docs check-architecture-fitness
uv run mkdocs build --strict
```

- [ ] **Step 3: Live verification**

```bash
docker compose -f docker/docker-compose.integration.yml up -d postgres mssql
DPONE_RUN_INTEGRATION=1 DPONE_RUN_REFRESH_EXECUTOR_LIVE=1 uv run pytest tests/integration/mssql/test_postgres_mssql_refresh_executor_live_integration.py -q -rs
```

- [ ] **Step 4: Commit and PR**

Commit the implementation, push `codex/postgres-mssql-refresh-executor-parity`, and open a draft PR stacked on `codex/mssql-clickhouse-refresh-live-certification`.
