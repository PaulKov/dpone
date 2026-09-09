# Route Release Gate Implementation Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a route-scoped release gate that aggregates source -> sink -> strategy evidence into one stable go/no-go receipt for release review.

**Architecture:** Add a thin route release gate service above route readiness, certification pack, execution ledger, and state promotion evidence. The service stays control-plane only: it reads already-produced artifacts, validates route identity and required evidence domains, calculates score/level/blockers, and writes JSON/Markdown. CLI and catalogs remain thin composition layers.

**Tech Stack:** Python dataclasses, existing `RouteProfileCatalog`, existing route evidence pass/fail semantics, argparse-based `dpone ops` CLI, pytest, docs contracts, MkDocs.

---

### Task 1: RED Tests

**Files:**
- Create: `tests/test_route_release_gate.py`
- Create: `tests/test_cli_route_release_gate_command.py`
- Create: `tests/test_route_release_gate_docs_contract.py`

- [ ] Add service tests proving a green `mssql -> clickhouse -> incremental_merge` gate passes when required route evidence, route readiness, certification pack, execution ledger, and state promotion artifacts are present.
- [ ] Add blocker tests for missing required evidence, failed upstream artifacts, and route identity mismatch.
- [ ] Add CLI tests for JSON output and nonzero blocked exit.
- [ ] Add docs contract tests requiring user docs, developer docs, architecture, CI/CD, source-sink matrix, ops CLI, route readiness, and MkDocs nav links.
- [ ] Run the tests and verify they fail because `dpone.ops.route_release_gate` and CLI wiring do not exist yet.

### Task 2: Models, Reader, Policy, Service

**Files:**
- Create: `src/dpone/ops/routes/release_gate_models.py`
- Create: `src/dpone/ops/routes/release_gate_policy.py`
- Create: `src/dpone/ops/route_release_gate.py`
- Modify: `src/dpone/ops/routes/__init__.py`
- Modify: `src/dpone/ops/__init__.py`

- [ ] Add `RouteReleaseGateEvidence`, `RouteReleaseGateDecision`, and `RouteReleaseGateReport`.
- [ ] Reuse existing artifact pass/fail semantics through a small reader that records path, sha256, missing, summary, blockers, and route match.
- [ ] Add `RouteReleaseGatePolicy` with generic required-domain scoring: default required domains are `route_readiness`, `route_certification_pack`, `route_execution_ledger`, `state_promotion`, plus matrix-driven `RouteProfile.required_evidence` and optional `--require` domains.
- [ ] Add route identity checks for artifacts that expose `route.case_id`; mismatches become blockers.
- [ ] Add report JSON/Markdown writing with release id, route id, level, score, evidence index, blockers, warnings, and operator runbook.

### Task 3: CLI And Catalog Wiring

**Files:**
- Modify: `src/dpone/ops/catalog_readiness.py`
- Modify: `src/dpone/ops/catalog_release.py`
- Modify: `src/dpone/commands/ops_parsers_core.py`
- Modify: `src/dpone/services/ops/command_handlers_release.py`
- Modify: `src/dpone/services/ops/__init__.py`
- Modify: `src/dpone/commands/ops_cmd.py`
- Modify: `src/dpone/commands/registry_ops.py`

- [ ] Add `route_release_gate()` factory methods.
- [ ] Add `dpone ops route-release-gate` with `--output-dir`, `--release`, `--source`, `--sink`, `--strategy`, repeated `--artifact`, repeated `--require`, and `--format`.
- [ ] Keep handler logic limited to parsing artifacts, invoking the service, emitting output, and returning `0/1`.

### Task 4: Documentation And Generated References

**Files:**
- Create: `docs/route-release-gate.md`
- Create: `docs/developer-route-release-gate.md`
- Modify: `mkdocs.yml`
- Modify: `docs/README.md`
- Modify: `docs/architecture.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/developer-ci-cd.md`
- Modify: `docs/ops-cli.md`
- Modify: `docs/route-readiness.md`
- Modify: `docs/source-sink-matrix.md`
- Generate: `docs/cli-reference.md`
- Generate: `docs/quality-metrics.md`

- [ ] Document user workflow, CLI examples, artifact taxonomy, CDC optional requirements, runbook, and release-review usage.
- [ ] Document developer boundaries, DI, extension rules, no connector imports, tests, and report stability rules.
- [ ] Link docs from architecture, CI/CD, ops CLI, route readiness, source-sink matrix, README, and MkDocs nav.
- [ ] Regenerate CLI reference and quality metrics.

### Task 5: Verification

**Commands:**
- `uv run pytest tests/test_route_release_gate.py tests/test_cli_route_release_gate_command.py tests/test_route_release_gate_docs_contract.py -q`
- `uv run pytest tests/test_route_release_gate.py tests/test_route_readiness.py tests/test_route_certification_pack.py tests/test_route_state_promotion.py tests/test_route_execution_ledger.py -q`
- `uv run ruff check ...`
- `uv run ruff format --check ...`
- `uv run mypy --config-file mypy.ini ...`
- `uv run dpone docs update-cli-reference --check`
- `uv run dpone docs check-docs`
- `uv run mkdocs build --strict`
- `uv run dpone docs update-dev-metrics --check`
- `uv run dpone docs check-import-rules`
- `uv run dpone docs check-module-size`
- `uv run dpone docs check-layer-metrics`
- `uv run dpone docs check-architecture-fitness`
- `uv run pytest tests/test_metrics_contracts.py tests/test_architecture_fitness_gate.py tests/test_docs_yaml_contracts.py tests/test_cicd_docs_contracts.py tests/test_mkdocs_toolchain_contract.py -q`
- `uv run pytest -m "not integration_live" -q`
