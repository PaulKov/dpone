# Airflow Evidence Bundle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a release-grade Airflow evidence bundle collector that correlates one Airflow attempt with the custom dpone Kubernetes pod, runtime evidence, final XCom outcome, smoke evidence, and pod-watch evidence.

**Architecture:** Keep the feature as a control-plane slice: DTOs and pure collection policy in `dpone.gitops`, orchestration in `dpone.services.gitops`, and argparse/rendering in `dpone.commands.gitops`. The collector reads already-produced JSON artifacts, hashes them, validates stable kinds, builds an attempt/pod correlation block, and emits a public JSON/Markdown report without importing Airflow or Kubernetes SDKs.

**Tech Stack:** Python dataclasses, existing GitOps path policy, existing JSON output helpers, JSON Schema contract catalog, pytest, ruff, mypy, mkdocs.

---

### Task 1: Contract Tests First

**Files:**
- Create: `tests/test_gitops_airflow_evidence_bundle.py`
- Modify: `tests/test_cli_gitops_airflow_commands.py`
- Modify: `tests/test_gitops_schema_contracts.py`

- [ ] **Step 1: Write domain tests**
  - Cover artifact digest collection, repo-relative path output, required artifact blockers, kind mismatch warnings/blockers, and attempt/pod correlation fields.
- [ ] **Step 2: Write CLI tests**
  - Cover `dpone gitops airflow evidence-bundle`, optional output writing, Markdown rendering, and no absolute local path leakage.
- [ ] **Step 3: Write schema tests**
  - Require the new `airflow-evidence-bundle` schema in the public schema catalog.
- [ ] **Step 4: Run tests and verify RED**
  - Run `uv run pytest tests/test_gitops_airflow_evidence_bundle.py tests/test_cli_gitops_airflow_commands.py tests/test_gitops_schema_contracts.py -q`.

### Task 2: Domain Collector

**Files:**
- Create: `src/dpone/gitops/airflow_evidence_bundle_models.py`
- Create: `src/dpone/gitops/airflow_evidence_bundle.py`

- [ ] **Step 1: Add DTOs**
  - Add immutable artifact, attempt correlation, pod correlation, and bundle report DTOs with stable `to_jsonable()`/`to_json()` methods.
- [ ] **Step 2: Add collector policy**
  - Load mappings supplied by the service, validate expected artifact kinds, compute SHA-256 and byte sizes, derive pass/fail from child blockers and required artifact presence, and preserve warnings.
- [ ] **Step 3: Run domain tests and verify GREEN**
  - Run `uv run pytest tests/test_gitops_airflow_evidence_bundle.py -q`.

### Task 3: Service, CLI, Rendering

**Files:**
- Create: `src/dpone/services/gitops/airflow_evidence_bundle_service.py`
- Create: `src/dpone/commands/gitops/airflow_evidence_bundle_cmd.py`
- Modify: `src/dpone/commands/gitops/airflow_cmd.py`
- Modify: `src/dpone/gitops/airflow_rendering.py`

- [ ] **Step 1: Add service orchestration**
  - Validate repo-relative paths, load JSON artifacts through the filesystem port, call the collector, and build a `GitOpsView`.
- [ ] **Step 2: Add CLI command**
  - Register `dpone gitops airflow evidence-bundle` with required Airflow attempt fields and artifact path flags.
- [ ] **Step 3: Add Markdown renderer**
  - Render a compact release-review summary with attempt, pod, artifacts, warnings, and blockers.
- [ ] **Step 4: Run CLI tests and verify GREEN**
  - Run focused CLI tests.

### Task 4: Schema And Docs

**Files:**
- Create: `src/dpone/gitops/schema_airflow_evidence_bundle_contracts.py`
- Create: `docs/schemas/gitops/airflow-evidence-bundle.schema.json`
- Modify: `src/dpone/gitops/schema_airflow_contracts.py`
- Modify: `docs/gitops-airflow-runner-pack.md`
- Modify: `docs/developer-gitops-airflow-runner-pack.md`
- Modify: `docs/gitops-control-plane.md`
- Modify: `docs/architecture.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/cli-reference.md`

- [ ] **Step 1: Add schema contract**
  - Register the new public schema and generated schema file.
- [ ] **Step 2: Update user docs**
  - Explain how to collect the bundle after `run-spec-exec`, `outcome-gate`, `k8s-smoke`, and `pod-watch`.
- [ ] **Step 3: Update developer docs and architecture**
  - Document module taxonomy, extension rules, and CI/release usage.
- [ ] **Step 4: Regenerate CLI reference**
  - Run `uv run dpone docs update-cli-reference`.

### Task 5: Verification And PR

**Files:**
- Modify: `docs/quality-metrics.md` if metrics change.

- [ ] **Step 1: Run focused checks**
  - `uv run ruff check .`
  - `uv run mypy --config-file mypy.ini`
  - Focused pytest files.
- [ ] **Step 2: Run docs and architecture gates**
  - CLI reference check, import rules, layer metrics, module size, dev metrics, and mkdocs strict build.
- [ ] **Step 3: Run full non-live test suite**
  - `uv run pytest -m "not integration_live" --cov=src/dpone --cov-report=xml`.
- [ ] **Step 4: Commit, push, and open PR**
  - Commit with a clear message and create a ready PR with release notes.
