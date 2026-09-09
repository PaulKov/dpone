# Route Live Certification Implementation Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a generic route-aware live certification harness and release evidence bundle for critical `source -> sink -> strategy` routes.

**Architecture:** Add a small `RouteLiveCertificationService` that reads route profiles, renders opt-in Docker/vendor commands, normalizes existing evidence artifacts, and writes a route-scoped bundle. It does not open database connections or run heavy tests; CI and operators execute the emitted commands and feed resulting artifacts back into the bundle and `route-release-gate`.

**Tech Stack:** Python dataclasses, existing route taxonomy, ops CLI command registry, GitHub Actions workflow docs, pytest docs/CLI contracts.

---

### Task 1: RED Tests

**Files:**
- Create: `tests/test_route_live_certification.py`
- Create: `tests/test_cli_route_live_certification_command.py`
- Create: `tests/test_route_live_certification_docs_contract.py`

- [ ] **Step 1: Write service tests**

```python
from dpone.ops.route_live_certification import RouteLiveCertificationService
```

The tests assert that a complete `mssql -> clickhouse -> incremental_merge`
bundle passes, missing evidence blocks, route mismatches block, and both first
routes produce matrix-driven harness steps.

- [ ] **Step 2: Write CLI tests**

```python
dpone ops route-live-certification --release 0.8.0-rc1 --source mssql --sink clickhouse --strategy incremental_merge
```

The tests assert JSON output, written Markdown/JSON receipts, and non-zero exit
for failed required artifacts.

- [ ] **Step 3: Write docs contract tests**

Require user docs, developer docs, architecture, CI/CD, ops CLI, source-sink
matrix, MkDocs nav, and CLI reference entries.

- [ ] **Step 4: Run RED**

Run:

```bash
uv run pytest tests/test_route_live_certification.py tests/test_cli_route_live_certification_command.py tests/test_route_live_certification_docs_contract.py -q
```

Expected: fail because `dpone.ops.route_live_certification` and CLI wiring do not exist.

### Task 2: Service Models and Policy

**Files:**
- Create: `src/dpone/ops/routes/live_certification_models.py`
- Create: `src/dpone/ops/routes/live_certification_policy.py`
- Create: `src/dpone/ops/route_live_certification.py`
- Modify: `src/dpone/ops/routes/__init__.py`
- Modify: `src/dpone/ops/__init__.py`

- [ ] **Step 1: Add immutable report contracts**

Define `RouteLiveCertificationStep`, `RouteLiveCertificationEvidence`, and
`RouteLiveCertificationReport` with stable JSON and Markdown output.

- [ ] **Step 2: Add generic policy**

Compute `passed`, `level`, `score`, blockers, warnings, and next actions from
required evidence and normalized items.

- [ ] **Step 3: Add service**

Use `RouteProfileCatalog` and profile-derived metadata. Generate deterministic
Docker/vendor command steps and normalize provided artifacts. Do not import
runtime connectors or execute heavy tests.

- [ ] **Step 4: Run GREEN for service tests**

Run:

```bash
uv run pytest tests/test_route_live_certification.py -q
```

Expected: pass.

### Task 3: CLI and Catalog Wiring

**Files:**
- Modify: `src/dpone/ops/catalog_readiness.py`
- Modify: `src/dpone/ops/catalog_release_sections.py`
- Modify: `src/dpone/services/ops/command_handlers_release.py`
- Modify: `src/dpone/services/ops/__init__.py`
- Modify: `src/dpone/commands/registry_ops.py`
- Modify: `src/dpone/commands/ops_cmd.py`
- Modify: `src/dpone/commands/ops_parsers_routes.py`

- [ ] **Step 1: Add catalog factory**

Expose `route_live_certification()` beside route readiness, route certification
pack, route execution ledger, route state promotion, and route release gate.

- [ ] **Step 2: Add CLI parser and handler**

Support `--release`, `--source`, `--sink`, `--strategy`, `--profile`,
`--row-count`, repeated `--artifact`, repeated `--require`, and `--format`.

- [ ] **Step 3: Register command**

Register `dpone ops route-live-certification`.

- [ ] **Step 4: Run CLI GREEN**

Run:

```bash
uv run pytest tests/test_cli_route_live_certification_command.py -q
```

Expected: pass.

### Task 4: Docs and Workflow Evidence

**Files:**
- Create: `docs/route-live-certification.md`
- Create: `docs/developer-route-live-certification.md`
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

- [ ] **Step 1: Add user docs and runbook**

Document opt-in Docker/vendor flow, generated artifacts, route-release-gate
handoff, and failure recovery.

- [ ] **Step 2: Add developer docs**

Document SOLID boundaries, dependency injection, no connector imports, stable
JSON contract, and extension rules for future routes.

- [ ] **Step 3: Update workflow**

Add opt-in route live certification bundle steps for `native_transfer` and
route-release-gate consumption while keeping default OSS CI credential-free.

- [ ] **Step 4: Regenerate references**

Run:

```bash
uv run dpone docs update-cli-reference
uv run dpone docs update-dev-metrics
```

### Task 5: Verification

- [ ] **Step 1: Focused tests**

```bash
uv run pytest tests/test_route_live_certification.py tests/test_cli_route_live_certification_command.py tests/test_route_live_certification_docs_contract.py -q
```

- [ ] **Step 2: Static checks**

```bash
uv run ruff format src/dpone/ops/route_live_certification.py src/dpone/ops/routes/live_certification_models.py src/dpone/ops/routes/live_certification_policy.py tests/test_route_live_certification.py tests/test_cli_route_live_certification_command.py tests/test_route_live_certification_docs_contract.py
uv run ruff check src/dpone/ops/route_live_certification.py src/dpone/ops/routes/live_certification_models.py src/dpone/ops/routes/live_certification_policy.py tests/test_route_live_certification.py tests/test_cli_route_live_certification_command.py tests/test_route_live_certification_docs_contract.py
uv run mypy --config-file mypy.ini src/dpone/ops/route_live_certification.py src/dpone/ops/routes/live_certification_models.py src/dpone/ops/routes/live_certification_policy.py
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
