# Route Refresh Snapshot Capture Implementation Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a generic `RouteRefreshSnapshotCaptureService` that captures typed source/sink snapshots for executed route refresh chunks and feeds `route-refresh-verify` without hand-written snapshot JSON.

**Architecture:** Snapshot capture stays between route refresh execution and verification. The facade consumes `route_refresh_execution.json`, delegates row reads to narrow `RouteRefreshRowsReader` ports, computes typed-hash side snapshots through reusable model helpers, and writes source/sink `route_refresh_snapshot.v1` documents plus a capture receipt. Backend-specific SQL and connector wiring live in small registry/adapters modules, not in the service.

**Tech Stack:** Python dataclasses, Protocol ports, existing route taxonomy, existing refresh execution and verification contracts, pytest, MkDocs, Docker-live MSSQL/Postgres/ClickHouse integration tests.

---

## File Structure

- Create `src/dpone/ops/routes/refresh_snapshot_capture_models.py`
  - Immutable request, side snapshot document, chunk capture result, capture artifact, and capture report contracts.
  - Reuse `RouteRefreshSideSnapshot` and `typed_hash` semantics from verification models.
- Create `src/dpone/ops/routes/refresh_snapshot_capture_policy.py`
  - Pure status/blocker/next-action policy for capture receipts.
- Create `src/dpone/ops/routes/refresh_snapshot_capture_reader.py`
  - `RouteRefreshRowsReader` protocol, JSON rows reader for credential-free CLI/tests, and helper functions that compute row-count/boundary/hash/duplicate/null-key snapshots.
- Create `src/dpone/ops/routes/refresh_snapshot_capture_registry.py`
  - Optional backend registry that builds source/sink readers from an executor-style config JSON.
- Create `src/dpone/ops/routes/refresh_snapshot_capture_adapters.py`
  - Small SQL row-reader adapters for MSSQL, Postgres, and ClickHouse using injected connector-like objects.
- Create `src/dpone/ops/route_refresh_snapshot_capture.py`
  - Thin facade service: load execution, build requests, call readers, write source/sink snapshot JSON and receipt Markdown/JSON.
- Modify CLI/catalog files:
  - `src/dpone/commands/ops_parsers_routes.py`
  - `src/dpone/commands/ops_cmd.py`
  - `src/dpone/commands/registry_ops.py`
  - `src/dpone/services/ops/__init__.py`
  - `src/dpone/services/ops/command_handlers_routes.py`
  - `src/dpone/ops/catalog_readiness.py`
  - `src/dpone/ops/catalog_release_sections.py`
- Modify live certification tests:
  - `tests/integration/mssql/test_mssql_clickhouse_refresh_executor_live_integration.py`
  - `tests/integration/mssql/test_postgres_mssql_refresh_executor_live_integration.py`
- Update docs:
  - `docs/route-refresh-execute.md`
  - `docs/developer-route-refresh-execute.md`
  - `docs/ops-cli.md`
  - `docs/ci-cd.md`
  - `docs/architecture.md`
  - `docs/source-sink/mssql-to-clickhouse.md`
  - `docs/source-sink/postgres-to-mssql.md`
  - generated `docs/cli-reference.md`
  - generated `docs/quality-metrics.md`

## Task 1: RED Tests For Generic Capture Contracts

**Files:**
- Create: `tests/test_route_refresh_snapshot_capture.py`

- [ ] **Step 1: Write failing tests**

Add tests that prove:
- capture service writes `source_route_refresh_snapshot.json`, `sink_route_refresh_snapshot.json`, and `route_refresh_snapshot_capture.json`;
- typed hashes canonicalize declared types through captured rows;
- duplicate and null key counts are recorded;
- failed/non-executed `route_refresh_execution.json` fails closed before readers run;
- service text stays backend-agnostic.

- [ ] **Step 2: Verify RED**

Run:

```bash
uv run pytest tests/test_route_refresh_snapshot_capture.py -q
```

Expected: fail with missing `dpone.ops.route_refresh_snapshot_capture`.

## Task 2: Implement Core Models, Reader Port, Policy, And Service

**Files:**
- Create: `src/dpone/ops/routes/refresh_snapshot_capture_models.py`
- Create: `src/dpone/ops/routes/refresh_snapshot_capture_reader.py`
- Create: `src/dpone/ops/routes/refresh_snapshot_capture_policy.py`
- Create: `src/dpone/ops/route_refresh_snapshot_capture.py`
- Test: `tests/test_route_refresh_snapshot_capture.py`

- [ ] **Step 1: Implement minimal GREEN core**

Implement:
- `RouteRefreshSnapshotCaptureRequest`
- `RouteRefreshSnapshotDocument`
- `RouteRefreshChunkSnapshotCapture`
- `RouteRefreshSnapshotCaptureReport`
- `RouteRefreshRowsReader`
- `JsonRouteRefreshRowsReader`
- `snapshot_from_rows(...)`
- `RouteRefreshSnapshotCapturePolicy`
- `RouteRefreshSnapshotCaptureService.capture(...)`

- [ ] **Step 2: Verify GREEN**

Run:

```bash
uv run pytest tests/test_route_refresh_snapshot_capture.py -q
```

Expected: pass.

## Task 3: CLI, Catalog, And Registry Wiring

**Files:**
- Create: `tests/test_cli_route_refresh_snapshot_capture_command.py`
- Modify: `src/dpone/commands/ops_parsers_routes.py`
- Modify: `src/dpone/commands/ops_cmd.py`
- Modify: `src/dpone/commands/registry_ops.py`
- Modify: `src/dpone/services/ops/__init__.py`
- Modify: `src/dpone/services/ops/command_handlers_routes.py`
- Modify: `src/dpone/ops/catalog_readiness.py`
- Modify: `src/dpone/ops/catalog_release_sections.py`

- [ ] **Step 1: Write failing CLI tests**

Test `dpone ops route-refresh-capture-snapshots` with JSON row inputs:
- matching source/sink row JSON writes source/sink snapshot files and returns exit 0;
- missing or failed execution returns nonzero and does not call readers;
- output JSON contains paths usable by `route-refresh-verify`.

- [ ] **Step 2: Verify RED**

Run:

```bash
uv run pytest tests/test_cli_route_refresh_snapshot_capture_command.py -q
```

Expected: parser does not know `route-refresh-capture-snapshots`.

- [ ] **Step 3: Implement parser/handler/catalog wiring**

Add a thin CLI command with:
- `--route-refresh-execution-json`
- `--runner-id`
- `--source-rows-json`
- `--sink-rows-json`
- `--executor`
- `--executor-config-json`
- repeated `--key`
- `--boundary-column`
- repeated `--column`
- repeated `--type name=type`
- `--format`

Use JSON row readers for credential-free CLI and backend registry readers when `--executor` is provided.

- [ ] **Step 4: Verify GREEN**

Run:

```bash
uv run pytest tests/test_cli_route_refresh_snapshot_capture_command.py tests/test_route_refresh_snapshot_capture.py -q
```

Expected: pass.

## Task 4: Backend Reader Registry And Adapter Tests

**Files:**
- Create: `tests/test_route_refresh_snapshot_capture_registry.py`
- Create: `src/dpone/ops/routes/refresh_snapshot_capture_adapters.py`
- Create: `src/dpone/ops/routes/refresh_snapshot_capture_registry.py`

- [ ] **Step 1: Write failing registry/adapter tests**

Test:
- `mssql_clickhouse` builds an MSSQL source reader and ClickHouse sink reader from existing executor config shape;
- `postgres_mssql` builds a Postgres source reader and MSSQL sink reader;
- generated SQL is bounded by chunk `start/end`, uses configured dataset, configured columns, and safe identifier quoting;
- unknown backend returns unavailable readers with explicit blockers.

- [ ] **Step 2: Verify RED**

Run:

```bash
uv run pytest tests/test_route_refresh_snapshot_capture_registry.py -q
```

Expected: missing registry/adapters.

- [ ] **Step 3: Implement registry/adapters**

Keep adapters small:
- connector-like dependency only needs `get_records(query, as_dict=True)`;
- adapters return rows, not snapshots;
- service owns hashing and report writing;
- route-specific SQL stays in adapters and config helpers.

- [ ] **Step 4: Verify GREEN**

Run:

```bash
uv run pytest tests/test_route_refresh_snapshot_capture_registry.py tests/test_route_refresh_snapshot_capture.py tests/test_cli_route_refresh_snapshot_capture_command.py -q
```

Expected: pass.

## Task 5: Docs, Architecture, And Quality Contracts

**Files:**
- Create: `tests/test_route_refresh_snapshot_capture_docs_contract.py`
- Modify docs listed in File Structure.

- [ ] **Step 1: Write failing docs contract**

Require user docs, developer docs, CI/CD docs, architecture docs, ops CLI docs, and both source-sink docs to mention:
- `route-refresh-capture-snapshots`
- `route_refresh_snapshot_capture.json`
- `source_route_refresh_snapshot.json`
- `sink_route_refresh_snapshot.json`
- `RouteRefreshSnapshotCaptureService`
- `RouteRefreshRowsReader`

- [ ] **Step 2: Verify RED**

Run:

```bash
uv run pytest tests/test_route_refresh_snapshot_capture_docs_contract.py -q
```

Expected: docs contract fails.

- [ ] **Step 3: Update docs**

Document:
- user workflow from execute to capture to verify;
- JSON row-input CLI mode and backend config mode;
- Python API usage;
- developer taxonomy, ports, adapters, extension rules;
- live certification artifacts and CI evidence;
- architecture diagram text.

- [ ] **Step 4: Verify GREEN**

Run:

```bash
uv run pytest tests/test_route_refresh_snapshot_capture_docs_contract.py -q
uv run dpone docs update-cli-reference
uv run dpone docs update-dev-metrics
```

Expected: docs contract passes and generated docs update cleanly.

## Task 6: Wide Live Certification Integration

**Files:**
- Modify: `tests/integration/mssql/test_mssql_clickhouse_refresh_executor_live_integration.py`
- Modify: `tests/integration/mssql/test_postgres_mssql_refresh_executor_live_integration.py`
- Modify: `.github/workflows/live-certification.yml`

- [ ] **Step 1: Update live tests from toy rows to wide exact certification**

Replace static snapshot readers with `RouteRefreshSnapshotCaptureService` and real DB row readers. The default Docker-live fixture must use:
- `10_000` rows;
- approximately `200` columns;
- route-specific physical-contract type coverage for supported scalar, decimal, temporal, boolean, uuid/guid, binary-as-hex/string, and text-like columns;
- exact typed-hash verification for all compared columns;
- enough chunks to prove windowed capture and replay semantics, with `chunks_verified >= 2`;
- source/sink row totals equal to `10_000`;
- capture and verification blockers equal to `[]`.

For MSSQL -> ClickHouse, reuse the same type-policy expectations covered by `tools/mssql_clickhouse_wide_type_certification.py` and `tests/test_mssql_clickhouse_type_fidelity.py`. For Postgres -> MSSQL, reuse the physical mapping expectations from `tests/test_postgres_mssql_type_mapping.py` and the Postgres -> MSSQL native hardening contract. Avoid duplicating mapper truth tables inside the capture service.

- [ ] **Step 2: Add schema evolution live checks**

For each first route, add a small additive schema-evolution phase around the wide fixture:
- create a nullable additive column after the initial target is present;
- apply or pre-create the target-side compatible column using the existing route/schema-evolution contract path available in the test harness;
- rerun capture + verify with the expanded column list;
- assert exact typed-hash verification still passes and the capture receipt records the expanded column count.

- [ ] **Step 3: Run non-live live-test import check**

Run:

```bash
uv run pytest tests/integration/mssql/test_mssql_clickhouse_refresh_executor_live_integration.py tests/integration/mssql/test_postgres_mssql_refresh_executor_live_integration.py -q -rs
```

Expected: skipped without live env, no import errors.

- [ ] **Step 4: Update workflow artifact copy/bundle**

Copy and attach `route_refresh_snapshot_capture.json`, `source_route_refresh_snapshot.json`, and `sink_route_refresh_snapshot.json` when live env is enabled.

## Task 7: Final Verification, Commit, Push, PR

**Files:**
- All changed files.

- [ ] **Step 1: Focused tests**

Run:

```bash
uv run pytest tests/test_route_refresh_snapshot_capture.py tests/test_cli_route_refresh_snapshot_capture_command.py tests/test_route_refresh_snapshot_capture_registry.py tests/test_route_refresh_snapshot_capture_docs_contract.py -q
uv run pytest tests/test_route_refresh_verify.py tests/test_cli_route_refresh_verify_command.py tests/test_route_refresh_execute.py tests/test_route_refresh_executor_registry.py -q
```

- [ ] **Step 2: Static and docs gates**

Run:

```bash
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
git diff --check
```

- [ ] **Step 3: Full non-live tests**

Run:

```bash
uv run pytest -m "not integration_live" -q
```

- [ ] **Step 4: Docker-live tests**

Run both MSSQL -> ClickHouse and Postgres -> MSSQL refresh executor live tests with `DPONE_RUN_INTEGRATION=1` and `DPONE_RUN_REFRESH_EXECUTOR_LIVE=1`, then inspect capture and verification JSON artifacts.

- [ ] **Step 5: Commit and PR**

Commit one focused slice, push `codex/route-refresh-snapshot-capture`, and open a stacked draft PR on top of `codex/route-refresh-verification`.
