# Route Refresh Execute Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a generic route refresh execution receipt that consumes `route_refresh_plan.json`, executes or dry-runs bounded chunks through a thin executor interface, and writes release-gate evidence.

**Architecture:** Keep execution control-plane and route-scoped. `RouteRefreshExecutionService` reads the plan, validates policy, delegates chunk work to a `RouteRefreshExecutor` protocol, and writes stable JSON/Markdown evidence. Route-specific data movement belongs behind executor implementations, not in CLI or policy.

**Tech Stack:** Python dataclasses/protocols, existing `RouteKey`/`RouteProfileCatalog`, route refresh plan JSON contract, `dpone ops` command wiring, pytest, ruff, mypy, MkDocs.

---

### Task 1: Red Tests

**Files:**
- Create: `tests/test_route_refresh_execute.py`
- Create: `tests/test_cli_route_refresh_execute_command.py`
- Create: `tests/test_route_refresh_execute_docs_contract.py`
- Modify: `tests/test_route_release_gate.py`

- [ ] Write failing service tests for dry-run execution, explicit execution with an injected executor, blocked/approval-required plans, chunk failure, and route mismatch.
- [ ] Write failing CLI tests for JSON output, dry-run default, `--execute` opt-in, and non-zero blocked execution.
- [ ] Write failing docs tests that require user docs, developer docs, Runbook sections, CLI reference, CI/CD docs, architecture docs, route release gate docs, and mkdocs navigation.
- [ ] Write a release-gate test proving `route_refresh_execution` can be required evidence.
- [ ] Run the focused tests and confirm they fail because the feature is missing.

### Task 2: Execution Contracts

**Files:**
- Create: `src/dpone/ops/routes/refresh_execution_models.py`
- Create: `src/dpone/ops/routes/refresh_execution_policy.py`
- Create: `src/dpone/ops/routes/refresh_execution_executor.py`
- Modify: `src/dpone/ops/routes/__init__.py`

- [ ] Add immutable public models for chunk requests, chunk results, artifacts, and `RouteRefreshExecutionReport`.
- [ ] Add pure policy that returns `dry_run`, `succeeded`, `partial_failure`, `blocked`, or `approval_required`.
- [ ] Add `RouteRefreshExecutor` protocol and `DryRunRouteRefreshExecutor`.
- [ ] Export contracts through the lazy route facade.
- [ ] Run focused tests until model/policy tests pass.

### Task 3: Service And CLI

**Files:**
- Create: `src/dpone/ops/route_refresh_execute.py`
- Modify: `src/dpone/ops/catalog_readiness.py`
- Modify: `src/dpone/ops/catalog_release_sections.py`
- Modify: `src/dpone/commands/ops_parsers_routes.py`
- Modify: `src/dpone/services/ops/command_handlers_routes.py`
- Modify: `src/dpone/services/ops/__init__.py`
- Modify: `src/dpone/commands/ops_cmd.py`
- Modify: `src/dpone/commands/registry_ops.py`

- [ ] Implement `RouteRefreshExecutionService.execute()` around plan loading, route/profile validation, dry-run defaults, injected executor calls, artifact hashing, policy evaluation, and report writing.
- [ ] Wire `dpone ops route-refresh-execute` with `--route-refresh-plan-json`, `--output-dir`, `--execute`, `--runner-id`, `--max-chunks`, `--format`, and optional `--artifact`.
- [ ] Add DI catalog method returning a planning/execution protocol rather than importing concrete services in handlers.
- [ ] Run focused service and CLI tests until green.

### Task 4: Docs And Generated Artifacts

**Files:**
- Create: `docs/route-refresh-execute.md`
- Create: `docs/developer-route-refresh-execute.md`
- Modify: `docs/route-refresh-plan.md`
- Modify: `docs/route-release-gate.md`
- Modify: `docs/operational-control-plane.md`
- Modify: `docs/architecture.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/ops-cli.md`
- Modify: `mkdocs.yml`
- Generated: `docs/cli-reference.md`
- Generated: `docs/quality-metrics.md`

- [ ] Document user workflow, CLI, JSON/Markdown outputs, approval safety, dry-run/execute behavior, and Runbook.
- [ ] Document developer boundaries, interfaces, policy, executor extension points, and tests.
- [ ] Update architecture/control-plane/CI/release-gate references.
- [ ] Regenerate CLI reference and developer quality metrics.
- [ ] Run docs contract tests and strict MkDocs.

### Task 5: Verification And Publish

**Files:** all changed files

- [ ] Run focused tests.
- [ ] Run `uv run ruff check .`.
- [ ] Run `uv run ruff format --check .`.
- [ ] Run `uv run mypy --config-file mypy.ini`.
- [ ] Run docs update checks and `uv run dpone docs check-docs`.
- [ ] Run `uv run mkdocs build --strict`.
- [ ] Run `uv run pytest -m "not integration_live" -q`.
- [ ] Run `DPONE_RUN_INTEGRATION_MATRIX=1 uv run pytest tests/integration/matrix/test_source_sink_strategy_matrix_integration.py -q`.
- [ ] Run `uv build`.
- [ ] Commit, push `codex/route-refresh-execute`, and open a draft stacked PR.
