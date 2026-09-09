# Route Release Finalizer Implementation Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a release-final route certification finalizer that discovers route bundles, validates freshness/provenance/regressions, records route certification history, and writes one release-ready artifact for v0.9.0 promotion.

**Architecture:** Keep route certification bundle aggregation separate from runtime connectors and heavy tests. `RouteCertificationReleaseFinalizerService` composes a bundle discovery reader, the existing `RouteCertificationReleaseService`, a pure finalizer policy, and a history writer. CLI remains thin and all route-specific defaults stay in route certification release contracts.

**Tech Stack:** Python dataclasses, existing `dpone.ops` service catalog, `argparse` CLI handlers, JSON/Markdown artifacts, pytest, ruff, mypy, MkDocs strict docs.

---

### Task 1: Finalizer Contract Tests

**Files:**
- Create: `tests/test_route_certify_release_finalizer.py`
- Create: `tests/test_cli_route_release_finalize_command.py`
- Create: `tests/test_route_release_finalize_docs_contract.py`

- [ ] **Step 1: Write failing service tests**

Add tests that create route certification bundle JSON files under temporary bundle roots and assert:

```python
report = RouteCertificationReleaseFinalizerService().finalize(
    output_dir=tmp_path / "final",
    release="v0.9.0-rc1",
    profile="oss_ci",
    bundle_roots=(tmp_path / "bundles",),
    history_dir=tmp_path / "history",
)
assert report.passed is True
assert report.level == "final_ready"
assert Path(report.release_report_path).exists()
assert Path(report.history_index_path).exists()
```

Also cover stale bundle failure, embedded bundle release mismatch, and route score regression against a previous baseline report.

- [ ] **Step 2: Write failing CLI tests**

Assert `dpone ops route-release-finalize --release v0.9.0-rc1 --bundle-root <root> --history-dir <history> --format json` exits `0` for green bundles and exits `1` for stale or missing required route bundles.

- [ ] **Step 3: Write failing docs contract tests**

Require `docs/route-release-finalize.md`, `docs/developer-route-release-finalize.md`, `mkdocs.yml`, `docs/ops-cli.md`, `docs/ci-cd.md`, `docs/developer-ci-cd.md`, `docs/source-sink-matrix.md`, `docs/route-certify-release.md`, and `.github/workflows/route-release-finalize.yml` to mention `route-release-finalize`.

### Task 2: Models, Discovery, Policy, History

**Files:**
- Create: `src/dpone/ops/routes/certify_release_finalizer_models.py`
- Create: `src/dpone/ops/routes/certify_release_discovery.py`
- Create: `src/dpone/ops/routes/certify_release_finalizer_policy.py`
- Modify: `src/dpone/ops/routes/certify_release_models.py`
- Modify: `src/dpone/ops/route_certify_release.py`

- [ ] **Step 1: Extend normalized release item metadata**

Add optional `bundle_release`, `score`, and `modified_at` fields to `RouteCertificationReleaseItem`. Populate them from bundle payload and filesystem metadata while preserving existing JSON keys and tests.

- [ ] **Step 2: Add discovery reader**

Implement `RouteCertificationBundleDiscovery.discover(roots, explicit)` that walks roots for `route_certification_bundle.json`, extracts embedded route case id, and returns `{route_case_id: path}`. Explicit `--route-bundle` values override discovered bundles.

- [ ] **Step 3: Add finalizer policy**

Implement pure policy checks:
- `route_certification_release.not_passed` when the aggregated release report is blocked;
- `<route>.stale` when bundle age exceeds `max_age_hours`;
- `<route>.release_mismatch` when bundle `release` differs from requested release;
- `<route>.score_regression` when current score is below baseline score for the same route.

- [ ] **Step 4: Add history models**

Write `route_certification_history_index.json` with one entry per release, current route scores/levels, profile, passed status, and finalizer artifact path.

### Task 3: Service and CLI

**Files:**
- Create: `src/dpone/ops/route_certify_release_finalizer.py`
- Create: `src/dpone/commands/ops_parsers_route_release_finalize.py`
- Modify: `src/dpone/ops/catalog_readiness.py`
- Modify: `src/dpone/ops/catalog_release_sections.py`
- Modify: `src/dpone/services/ops/command_handlers_routes.py`
- Modify: `src/dpone/services/ops/__init__.py`
- Modify: `src/dpone/commands/ops_cmd.py`
- Modify: `src/dpone/commands/registry_ops.py`
- Modify: `src/dpone/ops/routes/__init__.py`

- [ ] **Step 1: Implement `RouteCertificationReleaseFinalizerService`**

Compose discovery, `RouteCertificationReleaseService`, policy, and history writer. The service writes `route_release_finalizer.json`, `route_release_finalizer.md`, and nested `route-certify-release/route_certification_release.json`.

- [ ] **Step 2: Add CLI parser/handler**

Expose `dpone ops route-release-finalize` with `--release`, `--profile`, `--output-dir`, `--bundle-root`, `--route-bundle`, `--route`, `--history-dir`, `--baseline-json`, `--max-age-hours`, and `--format`.

- [ ] **Step 3: Register through service catalogs**

Use lazy imports and existing catalog facade methods so tests can inject services and no runtime connector layer depends on finalizer code.

### Task 4: Docs, Workflow, Generated References

**Files:**
- Create: `docs/route-release-finalize.md`
- Create: `docs/developer-route-release-finalize.md`
- Create: `.github/workflows/route-release-finalize.yml`
- Modify: `mkdocs.yml`, `docs/README.md`, `docs/architecture.md`, `docs/ci-cd.md`, `docs/developer-ci-cd.md`, `docs/ops-cli.md`, `docs/source-sink-matrix.md`, `docs/route-certify-release.md`, `docs/cicd/workflows.md`, `docs/cicd/runbooks.md`
- Generate: `docs/cli-reference.md`, `docs/quality-metrics.md`

- [ ] **Step 1: Add user docs and operator runbook**

Document discovery, explicit bundle overrides, freshness, provenance, regression, history, outputs, and release-blocking behavior.

- [ ] **Step 2: Add developer docs**

Document SOLID/DI boundaries, no Docker/no database/no route execution contract, extension rules, and tests.

- [ ] **Step 3: Add opt-in workflow**

Create `route-release-finalize.yml` with `workflow_dispatch`, `uv sync --all-extras`, `dpone ops route-release-finalize`, and `actions/upload-artifact`.

### Task 5: Verification, Commit, PR

**Files:** all touched files.

- [ ] **Step 1: Run focused tests**

Run:

```bash
uv run pytest tests/test_route_certify_release_finalizer.py tests/test_cli_route_release_finalize_command.py tests/test_route_release_finalize_docs_contract.py -q
```

- [ ] **Step 2: Run quality and docs gates**

Run:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run dpone docs update-cli-reference --check
uv run dpone docs update-dev-metrics --check
uv run dpone docs check-docs
uv run dpone docs check-module-size
uv run dpone docs check-architecture-fitness
uv run mkdocs build --strict
uv run pytest -m "not integration_live" -q
uv build
uv tool run twine check dist/*
```

- [ ] **Step 3: Commit and PR**

Commit with `Add route release finalizer` and open a stacked PR against `codex/route-certify-release-automation`.
