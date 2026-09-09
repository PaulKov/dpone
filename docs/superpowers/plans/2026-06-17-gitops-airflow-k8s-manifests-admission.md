# GitOps Airflow Kubernetes Manifests And Admission Check Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a deployable Kubernetes manifest pack and opt-in admission dry-run gate for generated Airflow runtime artifacts.

**Architecture:** Keep Airflow thin and keep dpone as the control-plane owner. Add small GitOps domain DTOs/builders for Kubernetes objects, service orchestration for artifact loading/path validation, and thin CLI handlers for rendering/executing. Admission execution uses an injectable command runner and never reads Kubernetes Secret values.

**Tech Stack:** Python dataclasses, existing dpone GitOps JSON/YAML artifacts, argparse CLI, kubectl dry-run commands, pytest, ruff, mypy, MkDocs.

---

### Task 1: Kubernetes Manifest Pack Contract

**Files:**
- Create: `src/dpone/gitops/airflow_k8s_manifests_models.py`
- Create: `src/dpone/gitops/airflow_k8s_manifests.py`
- Create: `src/dpone/services/gitops/airflow_k8s_manifests_service.py`
- Create: `src/dpone/commands/gitops/airflow_k8s_manifests_cmd.py`
- Modify: `src/dpone/commands/gitops/airflow_cmd.py`
- Test: `tests/test_gitops_airflow_k8s_manifests.py`

- [ ] Write failing tests that build a manifest pack from `runtime-profile.json`, `pod-contract.json`, and `connection-bridge-plan.json`.
- [ ] Assert generated YAML contains `ServiceAccount`, `Role`, `RoleBinding`, Secret skeletons without values, optional `ExternalSecret`, and optional `NetworkPolicy`.
- [ ] Implement DTOs and pure builder with deterministic object order.
- [ ] Implement service path validation and YAML/JSON output.
- [ ] Register `dpone gitops airflow k8s-manifests`.

### Task 2: Admission Dry-Run Gate

**Files:**
- Create: `src/dpone/gitops/airflow_admission_check_models.py`
- Create: `src/dpone/gitops/airflow_admission_check.py`
- Create: `src/dpone/gitops/airflow_admission_check_runner.py`
- Create: `src/dpone/services/gitops/airflow_admission_check_service.py`
- Create: `src/dpone/commands/gitops/airflow_admission_check_cmd.py`
- Modify: `src/dpone/commands/gitops/airflow_cmd.py`
- Test: `tests/test_gitops_airflow_admission_check.py`

- [ ] Write failing tests for `--mode plan` and injected live runner.
- [ ] Assert planned commands use `kubectl apply --dry-run=server --filename <manifest>`.
- [ ] Assert live failure output becomes blockers, while optional files become warnings.
- [ ] Implement injectable runner and sanitized JSON/Markdown-safe report.
- [ ] Register `dpone gitops airflow admission-check`.

### Task 3: Schemas, Artifact Index, Docs, And CI Contracts

**Files:**
- Create: `src/dpone/gitops/schema_airflow_k8s_manifest_contracts.py`
- Create: `src/dpone/gitops/schema_airflow_admission_contracts.py`
- Modify: `src/dpone/gitops/schema_airflow_contracts.py`
- Modify: `src/dpone/gitops/airflow_artifact_index.py`
- Modify: `docs/gitops-airflow-runner-pack.md`
- Modify: `docs/developer-gitops-airflow-runner-pack.md`
- Modify: `docs/architecture.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/gitops-control-plane.md`
- Modify: `tests/test_gitops_airflow_docs_contract.py`
- Modify: `tests/test_gitops_schema_contracts.py`

- [ ] Write failing docs/schema tests requiring the new commands and schemas.
- [ ] Register schemas and regenerate `docs/schemas/gitops/*.schema.json`.
- [ ] Add artifact-index entries for `airflow-k8s-manifests.json`, `airflow-k8s-manifests.yaml`, and `airflow-admission-check.json`.
- [ ] Update English user/developer/architecture/CI docs and generated CLI reference.

### Task 4: Verification And PR

- [ ] Run focused tests for new commands and docs/schema contracts.
- [ ] Run `uv run ruff check .`.
- [ ] Run `uv run mypy --config-file mypy.ini`.
- [ ] Run docs gates: CLI reference check, import rules, module size, layer metrics, architecture fitness.
- [ ] Run `uv run pytest -m "not integration_live" --cov=src/dpone --cov-report=xml`.
- [ ] Run `uv run mkdocs build --strict`.
- [ ] Commit, push `codex/airflow-k8s-manifests-admission`, and open a stacked PR against `codex/airflow-cluster-doctor`.

### Self-Review

- Scope is one cohesive GitOps/Airflow control-plane layer: render deployable Kubernetes objects and validate Kubernetes admission.
- No Kubernetes SDK dependency is introduced.
- Secrets are represented by names and required key names only; values remain out of artifacts.
- Command handlers stay thin and route into services.
