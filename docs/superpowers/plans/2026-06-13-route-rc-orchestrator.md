# Route RC Orchestrator Implementation Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a route-scoped release candidate orchestrator that composes route certification pack, route live certification, route release gate, and release evidence pack into one reproducible release-train receipt.

**Architecture:** Add a thin `RouteReleaseCandidateOrchestratorService` with immutable report models and a pure policy. The service coordinates existing evidence services and emits operator/CI commands, but it does not start Docker, run pytest, or open connector connections.

**Tech Stack:** Python dataclasses, existing route taxonomy, route certification/live/release services, release evidence pack service, ops CLI registry, pytest, MkDocs.

---

### Task 1: RED Tests

**Files:**
- Create: `tests/test_route_rc_orchestrator.py`
- Create: `tests/test_cli_route_rc_orchestrator_command.py`
- Create: `tests/test_route_rc_orchestrator_docs_contract.py`

- [ ] **Step 1: Write service tests**

The service tests import:

```python
from dpone.ops.route_rc_orchestrator import RouteReleaseCandidateOrchestratorService
```

They verify that a complete `mssql -> clickhouse -> incremental_merge` release
train writes route certification, route live certification, route release gate,
and release evidence pack artifacts.

- [ ] **Step 2: Write fail-closed tests**

Missing required release evidence must make the report red and include
machine-readable blockers.

- [ ] **Step 3: Write matrix-driven tests**

Both first routes, `postgres -> mssql` and `mssql -> clickhouse`, must produce a
release candidate orchestration receipt from matrix/profile metadata.

- [ ] **Step 4: Run RED**

```bash
uv run pytest tests/test_route_rc_orchestrator.py tests/test_cli_route_rc_orchestrator_command.py tests/test_route_rc_orchestrator_docs_contract.py -q
```

Expected: fail because the module and CLI command do not exist yet.

### Task 2: Models, Policy, Service

**Files:**
- Create: `src/dpone/ops/routes/rc_orchestrator_models.py`
- Create: `src/dpone/ops/routes/rc_orchestrator_policy.py`
- Create: `src/dpone/ops/route_rc_orchestrator.py`
- Modify: `src/dpone/ops/__init__.py`
- Modify: `src/dpone/ops/routes/__init__.py`

- [ ] **Step 1: Add report models**

Define `RouteRcOrchestrationStep` and `RouteRcOrchestrationReport` with stable
JSON/Markdown contracts.

- [ ] **Step 2: Add pure policy**

Evaluate ordered steps into `passed`, `level`, `score`, `blockers`, and
`next_actions`.

- [ ] **Step 3: Compose existing services**

Call `RouteCertificationPackService`, `RouteLiveCertificationService`,
`RouteReleaseGateService`, and `ReleaseEvidencePackService` with dependency
injection. Do not execute heavy live commands.

- [ ] **Step 4: Run service GREEN**

```bash
uv run pytest tests/test_route_rc_orchestrator.py -q
```

### Task 3: CLI and Catalog Wiring

**Files:**
- Modify: `src/dpone/ops/catalog_readiness.py`
- Modify: `src/dpone/ops/catalog_release_sections.py`
- Modify: `src/dpone/commands/ops_parsers_routes.py`
- Modify: `src/dpone/services/ops/command_handlers_release.py`
- Modify: `src/dpone/services/ops/__init__.py`
- Modify: `src/dpone/commands/ops_cmd.py`
- Modify: `src/dpone/commands/registry_ops.py`

- [ ] **Step 1: Add factory methods**

Expose `route_rc_orchestrator()` from readiness and release catalogs.

- [ ] **Step 2: Add parser and handler**

Register `dpone ops route-rc-orchestrator` with `--release`, `--source`,
`--sink`, `--strategy`, `--profile`, `--row-count`, repeated `--artifact`,
repeated `--require`, and `--format`.

- [ ] **Step 3: Run CLI GREEN**

```bash
uv run pytest tests/test_cli_route_rc_orchestrator_command.py -q
```

### Task 4: Docs and Workflow

**Files:**
- Create: `docs/route-rc-orchestrator.md`
- Create: `docs/developer-route-rc-orchestrator.md`
- Modify: `docs/README.md`
- Modify: `docs/architecture.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/developer-ci-cd.md`
- Modify: `docs/live-certification.md`
- Modify: `docs/release-evidence.md`
- Modify: `docs/ops-cli.md`
- Modify: `docs/source-sink-matrix.md`
- Modify: `mkdocs.yml`
- Modify: `.github/workflows/live-certification.yml`

- [ ] **Step 1: Add user and developer docs**

Document the release-train flow, artifact tree, fail-closed behavior, extension
rules, and no-live-IO rule.

- [ ] **Step 2: Wire workflow**

Add opt-in route release candidate orchestration steps to the live
certification workflow.

- [ ] **Step 3: Regenerate generated docs**

```bash
uv run dpone docs update-cli-reference
uv run dpone docs update-dev-metrics
```

### Task 5: Verification

- [ ] **Step 1: Focused tests**

```bash
uv run pytest tests/test_route_rc_orchestrator.py tests/test_cli_route_rc_orchestrator_command.py tests/test_route_rc_orchestrator_docs_contract.py -q
```

- [ ] **Step 2: Static checks**

```bash
uv run ruff format src/dpone/ops/route_rc_orchestrator.py src/dpone/ops/routes/rc_orchestrator_models.py src/dpone/ops/routes/rc_orchestrator_policy.py tests/test_route_rc_orchestrator.py tests/test_cli_route_rc_orchestrator_command.py tests/test_route_rc_orchestrator_docs_contract.py
uv run ruff check src/dpone/ops/route_rc_orchestrator.py src/dpone/ops/routes/rc_orchestrator_models.py src/dpone/ops/routes/rc_orchestrator_policy.py tests/test_route_rc_orchestrator.py tests/test_cli_route_rc_orchestrator_command.py tests/test_route_rc_orchestrator_docs_contract.py
uv run mypy --config-file mypy.ini src/dpone/ops/route_rc_orchestrator.py src/dpone/ops/routes/rc_orchestrator_models.py src/dpone/ops/routes/rc_orchestrator_policy.py
```

- [ ] **Step 3: Docs and architecture gates**

```bash
uv run dpone docs check-docs
uv run mkdocs build --strict
uv run dpone docs check-import-rules
uv run dpone docs check-layer-metrics
uv run dpone docs check-architecture-fitness
uv run dpone docs check-module-size
```

- [ ] **Step 4: Full non-live suite**

```bash
uv run pytest -m "not integration_live" -q
```
