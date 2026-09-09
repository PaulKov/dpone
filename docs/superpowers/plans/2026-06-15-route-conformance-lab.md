# Route Conformance Lab Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reusable Route Conformance Lab that proves one `source -> sink -> strategy` route against deterministic synthetic datasets, exact verification contracts, and release-gate evidence.

**Architecture:** Keep the lab in the ops control plane. Runtime connectors still extract and load; conformance services generate deterministic fixtures, compare source/sink snapshots, score contract evidence, and write stable JSON/Markdown reports. The service is matrix-driven through `RouteProfileCatalog`; route-specific behavior lives in profiles or input artifacts, never in CLI handlers.

**Tech Stack:** Python dataclasses, existing route taxonomy, existing ops catalog/CLI patterns, pytest, mkdocs, docs quality gates.

---

### Task 1: Public Contracts And Tests

**Files:**
- Create: `tests/test_route_conformance_lab.py`
- Create: `tests/test_cli_route_conformance_commands.py`
- Create: `tests/test_route_conformance_docs_contract.py`

- [ ] Write failing tests for dataset profiles, generated column contracts, deterministic row sampling, typed hashes, mismatch blockers, service reports, CLI output, and docs links.
- [ ] Run the focused tests and verify they fail because `dpone.ops.route_conformance` and `route-conformance` CLI do not exist.

### Task 2: Focused Conformance Modules

**Files:**
- Create: `src/dpone/ops/routes/conformance_models.py`
- Create: `src/dpone/ops/routes/conformance_dataset.py`
- Create: `src/dpone/ops/routes/conformance_verifier.py`
- Create: `src/dpone/ops/routes/conformance_policy.py`
- Create: `src/dpone/ops/routes/conformance_rendering.py`
- Create: `src/dpone/ops/route_conformance.py`
- Modify: `src/dpone/ops/routes/__init__.py`
- Modify: `src/dpone/ops/__init__.py`

- [ ] Implement immutable models for dataset profiles, column contracts, source/sink snapshots, verification chunks, conformance decisions, and reports.
- [ ] Implement deterministic synthetic dataset generation with wide schema support, nested `id`/`parent_id` fields, nullable edge cases, and schema-evolution plan metadata.
- [ ] Implement exact verification by row count, per-column physical contract, chunk-level typed hash, mismatch samples, and route identity checks.
- [ ] Implement policy scoring with fail-closed blockers for unsupported routes, row-count drift, hash mismatch, physical-contract drift, missing evolution evidence, and insufficient dataset scale.
- [ ] Implement `RouteConformanceService` as the facade that composes catalog lookup, dataset generation/loading, verification, policy, and report writing.

### Task 3: CLI And Catalog Wiring

**Files:**
- Create: `src/dpone/commands/ops_parsers_route_conformance.py`
- Create: `src/dpone/services/ops/command_handlers_route_conformance.py`
- Create: `src/dpone/ops/catalog_route_conformance.py`
- Modify: `src/dpone/ops/catalog_release.py`
- Modify: `src/dpone/commands/ops_cmd.py`
- Modify: `src/dpone/commands/registry_ops.py`
- Modify: `src/dpone/services/ops/__init__.py`

- [ ] Register `dpone ops route-conformance run`.
- [ ] Register `dpone ops route-conformance summarize`.
- [ ] Register `dpone ops route-conformance release-gate`.
- [ ] Keep parsers argument-only and handlers invocation-only.
- [ ] Expose service factories through a dedicated conformance catalog field on `ReleaseOpsCatalog`.

### Task 4: Documentation And Quality Gates

**Files:**
- Create: `docs/route-conformance-lab.md`
- Create: `docs/developer-route-conformance-lab.md`
- Modify: `docs/README.md`
- Modify: `docs/ops-cli.md`
- Modify: `docs/source-sink-matrix.md`
- Modify: `docs/architecture.md`
- Modify: `docs/developer-ci-cd.md`
- Modify: `mkdocs.yml`
- Regenerate: `docs/cli-reference.md`
- Regenerate: `docs/quality-metrics.md`

- [ ] Document user workflow, artifacts, runbook, examples, and release-gate interpretation.
- [ ] Document developer boundaries, extension rules, DI, testing, and how live Docker evidence plugs in as optional input.
- [ ] Link docs from nav, README, architecture, CI/CD guide, source-sink matrix, ops CLI, and generated CLI reference.
- [ ] Regenerate quality metrics and keep architecture fitness gates green.

### Task 5: Verification And PR

**Files:**
- All changed files.

- [ ] Run `uv run ruff check .`.
- [ ] Run `uv run ruff format --check .`.
- [ ] Run `uv run mypy --config-file mypy.ini`.
- [ ] Run `uv run pytest -q`.
- [ ] Run docs and architecture gates.
- [ ] Commit, push `codex/route-conformance-lab`, and create a stacked PR on top of `codex/route-bootstrap-doctor` or the current release branch if PR #75 has merged.
