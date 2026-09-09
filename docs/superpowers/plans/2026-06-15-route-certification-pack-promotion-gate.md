# Route Certification Pack Promotion Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `dpone ops route-certify`, a route-scoped release certification bundle and promotion gate over existing execute/capture/verify/readiness artifacts.

**Architecture:** Introduce focused route certification models, a pure policy, and a small orchestration service. The service remains matrix-driven, reads immutable evidence artifacts, delegates readiness to `RouteCertificationPackService`, delegates go/no-go evidence checks to `RouteReleaseGateService`, delegates final release bundling to `ReleaseEvidencePackService`, and never runs Docker, pytest, or database work.

**Tech Stack:** Python dataclasses, existing `RouteKey`/`RouteProfileCatalog`, existing ops CLI parser/handler registry, pytest, MkDocs.

---

### Task 1: Red Tests

**Files:**
- Create: `tests/test_route_certify.py`
- Create: `tests/test_cli_route_certify_command.py`
- Create: `tests/test_route_certify_docs_contract.py`
- Modify: `tests/test_route_release_gate.py`

- [ ] Add tests proving a complete route certification bundle produces `level=certified`, writes `route_certification_bundle.json`, includes promotion-gate and release-evidence artifacts, and works for the first matrix routes.
- [ ] Add fail-closed tests for missing snapshot capture evidence and missing vendor-live evidence.
- [ ] Add CLI tests for `dpone ops route-certify`.
- [ ] Add docs contract tests for user, developer, architecture, CI/CD, ops CLI, CLI reference, and source-sink docs.
- [ ] Add a regression test that `route_release_gate.md` renders every evidence row.

### Task 2: Models And Policy

**Files:**
- Create: `src/dpone/ops/routes/certify_models.py`
- Create: `src/dpone/ops/routes/certify_policy.py`

- [ ] Define stable schema `dpone.route_certification_bundle.v1`.
- [ ] Add immutable stage/decision/report dataclasses with JSON and Markdown renderers.
- [ ] Add pure policy returning `certified`, `warning`, or `blocked`.

### Task 3: Service And CLI

**Files:**
- Create: `src/dpone/ops/route_certify.py`
- Modify: `src/dpone/ops/catalog_readiness.py`
- Modify: `src/dpone/ops/catalog_release_sections.py`
- Modify: `src/dpone/services/ops/command_handlers_routes.py`
- Modify: `src/dpone/services/ops/__init__.py`
- Modify: `src/dpone/commands/ops_parsers_routes.py`
- Modify: `src/dpone/commands/ops_cmd.py`
- Modify: `src/dpone/commands/registry_ops.py`

- [ ] Implement `RouteCertificationService.certify(...)` with dependency injection.
- [ ] Compose route certification pack, route promotion gate, and release evidence pack.
- [ ] Require `route_refresh_execution`, `route_refresh_snapshot_capture`, and `route_refresh_verification` by default.
- [ ] Require `route_live_evidence_bundle` only for `vendor_live`.
- [ ] Register `dpone ops route-certify` with thin argument parsing only.

### Task 4: Documentation

**Files:**
- Create: `docs/route-certify.md`
- Create: `docs/developer-route-certify.md`
- Modify: `mkdocs.yml`
- Modify: `docs/README.md`
- Modify: `docs/architecture.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/developer-ci-cd.md`
- Modify: `docs/ops-cli.md`
- Modify: `docs/source-sink-matrix.md`
- Modify: `docs/source-sink/postgres-to-mssql.md`
- Modify: `docs/source-sink/mssql-to-clickhouse.md`
- Generated: `docs/cli-reference.md`
- Generated: `docs/quality-metrics.md`

- [ ] Document OSS-safe and vendor-live modes, inputs, outputs, blockers, and release runbook.
- [ ] Document SOLID module boundaries and extension rules.
- [ ] Update architecture/CI/source-sink navigation and generated references.

### Task 5: Verification And PR

**Files:**
- All touched files.

- [ ] Run focused tests.
- [ ] Run static quality checks, docs checks, architecture fitness, MkDocs strict.
- [ ] Run full non-live pytest.
- [ ] Stage only intentional files, commit, push, and open a stacked PR.
