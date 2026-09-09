# Route Refresh Verification Implementation Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a generic post-load route refresh verification service that proves executed refresh chunks are reconciled through row counts, boundaries, duplicate-key checks, and typed hashes before state promotion.

**Architecture:** Verification stays separate from chunk execution. `RouteRefreshVerificationService` consumes a stable `route_refresh_execution.json`, delegates source/sink reads to narrow verification ports, evaluates results through a pure policy, and writes `route_refresh_verification.json` plus Markdown. Concrete database adapters remain optional and route-specific only at the edge; the service is generic for future source -> sink routes.

**Tech Stack:** Python dataclasses, Protocol-based dependency injection, existing `RouteKey`/route catalog contracts, pytest, Ruff, mypy, MkDocs, GitHub Actions.

---

### Task 1: Verification Contracts and Policy

**Files:**
- Create: `src/dpone/ops/routes/refresh_verification_models.py`
- Create: `src/dpone/ops/routes/refresh_verification_policy.py`
- Test: `tests/test_route_refresh_verify.py`

- [ ] **Step 1: Write failing tests**

Add tests proving:
- typed hashes are stable across equivalent numeric/string rows;
- matching source/sink chunks pass;
- row-count, min/max boundary, duplicate-key, and typed-hash mismatches produce blockers;
- report JSON and Markdown include a stable schema version and evidence summary.

- [ ] **Step 2: Run tests to verify RED**

Run: `uv run pytest tests/test_route_refresh_verify.py -q`

Expected: fail because the verification modules do not exist.

- [ ] **Step 3: Implement minimal contracts and policy**

Create focused dataclasses for `RouteRefreshVerificationRequest`, `RouteRefreshSideSnapshot`, `RouteRefreshChunkVerification`, `RouteRefreshVerificationDecision`, and `RouteRefreshVerificationReport`. Keep policy pure and deterministic.

- [ ] **Step 4: Run tests to verify GREEN**

Run: `uv run pytest tests/test_route_refresh_verify.py -q`

Expected: pass.

### Task 2: Generic Verification Service

**Files:**
- Create: `src/dpone/ops/route_refresh_verify.py`
- Create: `src/dpone/ops/routes/refresh_verification_verifier.py`
- Modify: `src/dpone/ops/catalog_readiness.py`
- Test: `tests/test_route_refresh_verify.py`

- [ ] **Step 1: Write failing service tests**

Add tests proving `RouteRefreshVerificationService`:
- loads `route_refresh_execution.json`;
- refuses non-succeeded execution receipts;
- delegates each executed chunk to an injected `RouteRefreshVerifier`;
- writes `route_refresh_verification.json` and `.md`;
- remains executor-agnostic and has no backend-specific imports.

- [ ] **Step 2: Run tests to verify RED**

Run: `uv run pytest tests/test_route_refresh_verify.py -q`

Expected: fail because service and protocol are missing.

- [ ] **Step 3: Implement service and DI catalog factory**

Keep the service orchestration-only. Put backend reads behind the `RouteRefreshVerifier` protocol. Add a safe `UnavailableRouteRefreshVerifier` for CLI UX when no backend is configured.

- [ ] **Step 4: Run tests to verify GREEN**

Run: `uv run pytest tests/test_route_refresh_verify.py -q`

Expected: pass.

### Task 3: CLI, Release Gate, and Docs Contracts

**Files:**
- Modify: `src/dpone/commands/ops_parsers_routes.py`
- Modify: `src/dpone/commands/ops_cmd.py`
- Modify: `src/dpone/commands/registry_ops.py`
- Modify: `src/dpone/services/ops/__init__.py`
- Modify: `src/dpone/services/ops/command_handlers_routes.py`
- Modify: `src/dpone/ops/route_release_gate.py`
- Test: `tests/test_cli_route_refresh_verify_command.py`
- Test: `tests/test_route_release_gate.py`
- Test: `tests/test_route_refresh_verify_docs_contract.py`

- [ ] **Step 1: Write failing tests**

Add tests proving:
- `dpone ops route-refresh-verify` emits JSON/Markdown and returns non-zero on blockers;
- release gates accept `route_refresh_verification` as required evidence;
- docs mention user runbook, developer architecture, CI, examples, and live certification evidence.

- [ ] **Step 2: Run tests to verify RED**

Run: `uv run pytest tests/test_cli_route_refresh_verify_command.py tests/test_route_release_gate.py::test_route_release_gate_accepts_route_refresh_verification_required_evidence tests/test_route_refresh_verify_docs_contract.py -q`

Expected: fail because parser/handler/docs are missing.

- [ ] **Step 3: Implement CLI/release gate/docs support**

Add `route-refresh-verify` to the existing ops command registry. Add `route_refresh_verification` to default route release evidence so release candidates require post-load proof by default.

- [ ] **Step 4: Run tests to verify GREEN**

Run: same focused pytest command.

Expected: pass.

### Task 4: Live Certification Wiring

**Files:**
- Modify: `tests/integration/mssql/test_mssql_clickhouse_refresh_executor_live_integration.py`
- Modify: `tests/integration/mssql/test_postgres_mssql_refresh_executor_live_integration.py`
- Modify: `.github/workflows/live-certification.yml`
- Modify: `docs/route-refresh-execute.md`
- Modify: `docs/developer-route-refresh-execute.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/architecture.md`
- Modify: `docs/source-sink/postgres-to-mssql.md`

- [ ] **Step 1: Write failing live/docs contract tests**

Extend live tests to run verification after replay and require `route_refresh_verification.json`. Add docs contract assertions for both first routes.

- [ ] **Step 2: Run focused tests to verify RED**

Run: `uv run pytest tests/test_route_refresh_verify_docs_contract.py tests/integration/mssql/test_mssql_clickhouse_refresh_executor_live_integration.py tests/integration/mssql/test_postgres_mssql_refresh_executor_live_integration.py -q -rs`

Expected: docs contract fails; live tests skip unless opt-in env vars are present.

- [ ] **Step 3: Wire verification artifacts into live tests/workflow/docs**

Live tests should use read-only verifier adapters or test-local verifier ports and attach the resulting artifact to route live certification bundles.

- [ ] **Step 4: Run focused tests to verify GREEN**

Run: same command. Live tests may skip without opt-in env vars; docs contract must pass.

### Task 5: Quality Gates, Commit, and PR

**Files:**
- Modify: `docs/quality-metrics.md`

- [ ] **Step 1: Run full verification**

Run:
- `uv run pytest tests/test_route_refresh_verify.py tests/test_cli_route_refresh_verify_command.py tests/test_route_release_gate.py tests/test_route_refresh_verify_docs_contract.py -q`
- `uv run pytest -m "not integration_live" -q`
- `uv run ruff check src tests && uv run ruff format --check src tests`
- `uv run mypy --config-file mypy.ini`
- `uv run dpone docs update-cli-reference --check`
- `uv run dpone docs update-dev-metrics --check`
- `uv run dpone docs check-docs`
- `uv run dpone docs check-import-rules && uv run dpone docs check-layer-metrics && uv run dpone docs check-module-size && uv run dpone docs check-architecture-fitness`
- `uv run mkdocs build --strict`
- `git diff --check`

- [ ] **Step 2: Run opt-in live verification if local services are available**

Run the MSSQL -> ClickHouse and Postgres -> MSSQL live refresh executor tests with `DPONE_RUN_INTEGRATION=1` and `DPONE_RUN_REFRESH_EXECUTOR_LIVE=1`, then inspect `route_refresh_verification.json` for both routes.

- [ ] **Step 3: Commit, push, and open a draft PR**

Commit one focused slice, push `codex/route-refresh-verification`, and open a stacked draft PR on top of `codex/postgres-mssql-refresh-executor-parity`.
