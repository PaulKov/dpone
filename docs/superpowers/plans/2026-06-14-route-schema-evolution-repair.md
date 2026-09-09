# Route Schema Evolution Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build one route-level schema evolution and reconciliation repair control-plane feature.

**Architecture:** Add small value objects, policy helpers, and facade services under `dpone.ops.routes` and `dpone.ops`. CLI parser and command handlers remain thin. Existing CDC schema evolution and reconciliation services stay focused and are reused rather than duplicated.

**Tech Stack:** Python dataclasses, existing `dpone.ops.routes` taxonomy, existing CDC schema evolution artifacts, existing reconciliation service, pytest, ruff, mypy, MkDocs.

---

### Task 1: Route Schema Evolution Contract

**Files:**
- Modify: `src/dpone/ops/routes/models.py`
- Create: `src/dpone/ops/routes/schema_evolution.py`
- Create: `src/dpone/ops/route_schema_evolution.py`
- Test: `tests/test_route_schema_evolution.py`

- [ ] Write failing tests for route schema evolution pass/fail reports.
- [ ] Implement focused value objects and service orchestration.
- [ ] Verify JSON/Markdown output and blockers.

### Task 2: Route Reconciliation Repair Contract

**Files:**
- Modify: `src/dpone/ops/routes/models.py`
- Create: `src/dpone/ops/routes/reconciliation_repair.py`
- Create: `src/dpone/ops/route_reconciliation_repair.py`
- Test: `tests/test_route_reconciliation_repair.py`

- [ ] Write failing tests for missing, extra, mismatch, and delete repair actions.
- [ ] Reuse existing `ReconciliationService` for row-level semantics.
- [ ] Emit route repair JSON/Markdown artifacts.

### Task 3: CLI and Service Catalog

**Files:**
- Modify: `src/dpone/commands/ops_parsers_routes.py`
- Modify: `src/dpone/commands/ops_cmd.py`
- Modify: `src/dpone/commands/registry_ops.py`
- Modify: `src/dpone/services/ops/command_handlers_routes.py`
- Modify: `src/dpone/services/ops/command_handlers_release_context.py`
- Test: `tests/test_cli_route_schema_evolution_command.py`
- Test: `tests/test_cli_route_reconciliation_repair_command.py`

- [ ] Add parsers for `route-schema-evolution` and `route-reconciliation-repair`.
- [ ] Add command handlers that parse artifacts/row JSON and call services.
- [ ] Verify CLI output to console and files.

### Task 4: Docs and Quality Gates

**Files:**
- Create: `docs/route-schema-evolution.md`
- Create: `docs/route-reconciliation-repair.md`
- Modify: `docs/ops-cli.md`
- Modify: `docs/operational-control-plane.md`
- Modify: `docs/testing/integration-matrix.md`
- Modify: `mkdocs.yml`
- Modify: `docs/quality-metrics.md`
- Test: docs contract tests.

- [ ] Add user docs, developer notes, examples, and runbooks in English.
- [ ] Update generated CLI reference.
- [ ] Update generated quality metrics.
- [ ] Run docs checks and targeted tests.
