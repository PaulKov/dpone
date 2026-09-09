# Route Bootstrap Doctor Implementation Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a generic self-service onboarding layer for source -> sink routes: connection diagnostics, source discovery evidence, route bootstrap manifests, and one route doctor report.

**Architecture:** Keep route onboarding in the ops control plane and reuse `RouteKey`, `RouteProfileCatalog`, and existing route readiness contracts. Split contracts, policy, and services into focused `dpone.ops.routes.bootstrap_*` modules, expose small facades under `dpone.ops.route_*`, and keep CLI parser/handler code free of business logic.

**Tech Stack:** Python dataclasses, stdlib JSON/Path/shutil, existing ops catalog DI, argparse command registry, pytest, MkDocs generated CLI reference, English OSS docs.

---

### Task 1: Domain Contracts And Policy

**Files:**
- Create: `src/dpone/ops/routes/bootstrap_models.py`
- Create: `src/dpone/ops/routes/bootstrap_policy.py`
- Modify: `src/dpone/ops/routes/__init__.py`
- Test: `tests/test_route_bootstrap_doctor.py`

- [ ] **Step 1: Write failing tests**

Cover:
- `ConnectionDoctorService` reports extras, tools, env vars, and missing optional checks without DB access.
- `SourceDiscoveryService` reads schema JSON and emits typed table/column profile evidence.
- `RouteBootstrapService` creates route manifest draft, readiness command list, and type-risk summary from the matrix profile.
- `RouteDoctorService` aggregates connection, discovery, bootstrap, and optional readiness artifacts into one go/no-go report.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/test_route_bootstrap_doctor.py -q`
Expected: FAIL on missing route bootstrap modules.

- [ ] **Step 3: Implement contracts and pure policies**

Add small dataclasses for connection checks, discovered tables/columns, route bootstrap outputs, and route doctor reports. Add pure policy helpers for pass/warn/block decisions.

- [ ] **Step 4: Verify GREEN**

Run: `uv run pytest tests/test_route_bootstrap_doctor.py -q`
Expected: PASS.

### Task 2: Services And CLI Wiring

**Files:**
- Create: `src/dpone/ops/connection_doctor.py`
- Create: `src/dpone/ops/source_discovery.py`
- Create: `src/dpone/ops/route_bootstrap.py`
- Create: `src/dpone/ops/route_doctor.py`
- Modify: `src/dpone/ops/catalog_readiness.py`
- Modify: `src/dpone/ops/catalog_release_sections.py`
- Modify: `src/dpone/commands/ops_parsers_routes.py`
- Modify: `src/dpone/services/ops/command_handlers_routes.py`
- Modify: `src/dpone/commands/ops_cmd.py`
- Modify: `src/dpone/commands/registry_ops.py`
- Test: `tests/test_cli_route_bootstrap_doctor_commands.py`

- [ ] **Step 1: Write failing CLI tests**

Cover JSON output and exit codes for:
- `dpone ops connection-doctor`
- `dpone ops source-discover`
- `dpone ops route-bootstrap`
- `dpone ops route-doctor`

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/test_cli_route_bootstrap_doctor_commands.py -q`
Expected: FAIL because commands are not registered.

- [ ] **Step 3: Implement thin facades and parser/handler glue**

Commands accept route identity, schema JSON, environment variable names, executable names, artifact references, output dir, and format. Handlers only parse args, call catalog services, emit JSON/Markdown, and return `0` for pass/warn, `1` for blocked.

- [ ] **Step 4: Verify GREEN**

Run: `uv run pytest tests/test_cli_route_bootstrap_doctor_commands.py tests/test_route_bootstrap_doctor.py -q`
Expected: PASS.

### Task 3: Documentation, Architecture, And CI Contracts

**Files:**
- Create: `docs/route-bootstrap-doctor.md`
- Create: `docs/developer-route-bootstrap-doctor.md`
- Modify: `docs/ops-cli.md`
- Modify: `docs/source-sink-matrix.md`
- Modify: `docs/architecture.md`
- Modify: `docs/developer-ci-cd.md`
- Modify: `mkdocs.yml`
- Modify: `docs/cli-reference.md`
- Test: `tests/test_route_bootstrap_doctor_docs_contract.py`

- [ ] **Step 1: Write failing docs contract tests**

Assert user docs, developer docs, CLI reference, architecture, source-sink matrix, CI/CD docs, and MkDocs nav reference the new onboarding commands and artifact contracts.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/test_route_bootstrap_doctor_docs_contract.py -q`
Expected: FAIL because docs do not exist.

- [ ] **Step 3: Add English docs and generated CLI reference**

Document workflows, artifacts, runbooks, extension rules, DI boundaries, CI artifact uploads, and no-route-specific-command-logic rules.

- [ ] **Step 4: Verify GREEN**

Run:
- `uv run pytest tests/test_route_bootstrap_doctor_docs_contract.py -q`
- `uv run dpone docs update-cli-reference --check`
- `uv run dpone docs check-docs`
- `uv run mkdocs build --strict`

Expected: PASS.

### Task 4: Final Quality Gate

**Files:**
- Modify: `docs/quality-metrics.md`

- [ ] **Step 1: Run focused regression**

Run: `uv run pytest tests/test_route_bootstrap_doctor.py tests/test_cli_route_bootstrap_doctor_commands.py tests/test_route_bootstrap_doctor_docs_contract.py tests/test_route_readiness.py tests/test_cli_route_readiness_command.py -q`
Expected: PASS.

- [ ] **Step 2: Run full verification**

Run:
- `uv run ruff check .`
- `uv run ruff format --check .`
- `uv run mypy --config-file mypy.ini`
- `uv run pytest -q`
- `uv run dpone docs update-cli-reference --check`
- `uv run dpone docs update-dev-metrics --check`
- `uv run dpone docs check-docs`
- `uv run dpone docs check-import-rules`
- `uv run dpone docs check-layer-metrics`
- `uv run dpone docs check-module-size --baseline docs/module_size_baseline.json`
- `uv run dpone docs check-architecture-fitness`
- `uv run mkdocs build --strict`

Expected: PASS.

- [ ] **Step 3: Commit and prepare PR**

Commit message: `Add generic route bootstrap doctor`
