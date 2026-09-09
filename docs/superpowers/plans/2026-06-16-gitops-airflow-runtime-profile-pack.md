# GitOps Airflow Runtime Profile Pack Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an Airflow Runtime Profile Pack for custom dpone images running under KubernetesExecutor, KubernetesPodOperator, and KubernetesPodExecutor-style wrappers.

**Architecture:** Keep runtime placement and scheduler metadata separate from the runtime run-spec. `dpone.gitops.airflow_runtime_profile_models` owns stable DTOs, `dpone.gitops.airflow_runtime_profile` builds and validates profile contracts, `dpone.services.gitops.airflow_runtime_profile_service` owns DI/path/artifact orchestration, and `dpone.commands.gitops.airflow_cmd` stays argparse-only. The generated DAG factory and XCom summary are artifacts, not Airflow runtime dependencies.

**Tech Stack:** Python dataclasses, existing GitOps/Airflow service patterns, JSON Schema catalog split between core/Airflow schemas, pytest, ruff, mypy, mkdocs.

---

### Task 1: RED Tests For Runtime Profile Pack

**Files:**
- Modify: `tests/test_gitops_airflow_runner_pack.py`
- Modify: `tests/test_cli_gitops_airflow_commands.py`
- Modify: `tests/test_gitops_schema_contracts.py`
- Modify: `tests/test_gitops_airflow_docs_contract.py`

- [ ] Add service tests for `GitOpsAirflowRuntimeProfileService` that write `runtime-profile.json`, `xcom-summary.json`, and `airflow_dag_factory.py` with repo-relative paths only.
- [ ] Add release doctor tests proving missing image digest, service account, resources, and artifact sink become blockers under `--runner-policy release`.
- [ ] Add CLI tests for `dpone gitops airflow runtime-profile`, JSON/Markdown output, optional report output, and generated files.
- [ ] Extend schema/docs contract tests to require `airflow-runtime-profile.schema.json`, `runtime-profile.json`, `xcom-summary.json`, `airflow_dag_factory.py`, and `dpone gitops airflow runtime-profile`.
- [ ] Run focused tests and verify RED because the runtime-profile service, command, schema, and docs do not exist yet.

### Task 2: Domain Models And Policy

**Files:**
- Create: `src/dpone/gitops/airflow_runtime_profile_models.py`
- Create: `src/dpone/gitops/airflow_runtime_profile.py`

- [ ] Add immutable DTOs for `GitOpsAirflowRuntimeProfile`, `GitOpsAirflowRuntimeProfileReport`, `GitOpsAirflowRuntimeResourceProfile`, `GitOpsAirflowArtifactSink`, and generated artifact records.
- [ ] Add `GitOpsAirflowRuntimeProfileBuilder` with deterministic defaults: namespace, service account, image, image digest, resources, env, labels, annotations, artifact sink, run-spec path, evidence path, and XCom summary path.
- [ ] Add `GitOpsAirflowRuntimeProfilePolicy` that returns small findings without importing service or CLI modules.

### Task 3: Services, Render Integration, And CLI

**Files:**
- Create: `src/dpone/services/gitops/airflow_runtime_profile_service.py`
- Modify: `src/dpone/services/gitops/airflow_render_service.py`
- Modify: `src/dpone/gitops/airflow_artifacts.py`
- Modify: `src/dpone/gitops/airflow_rendering.py`
- Modify: `src/dpone/commands/gitops/airflow_cmd.py`

- [ ] Implement `GitOpsAirflowRuntimeProfileService.build_view(args)` with repo-relative path validation, profile artifact writing, XCom summary writing, DAG factory writing, and `GitOpsView` construction.
- [ ] Update Airflow render to emit `runtime-profile.json`, `xcom-summary.json`, and `airflow_dag_factory.py` alongside `run-spec.json`.
- [ ] Register `dpone gitops airflow runtime-profile` with thin JSON/Markdown handlers.

### Task 4: Schemas, Docs, Architecture, And Metrics

**Files:**
- Modify: `src/dpone/gitops/schema_airflow_contracts.py`
- Add: `docs/schemas/gitops/airflow-runtime-profile.schema.json`
- Modify: `docs/gitops-airflow-runner-pack.md`
- Modify: `docs/developer-gitops-airflow-runner-pack.md`
- Modify: `docs/gitops-control-plane.md`
- Modify: `docs/architecture.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/cli-reference.md`
- Modify: `docs/quality-metrics.md`

- [ ] Add the public `gitops.airflow_runtime_profile` JSON Schema contract.
- [ ] Update user docs with KubernetesPodExecutor custom image runbook, Runtime Profile Pack artifacts, XCom summary behavior, and release checks.
- [ ] Update developer docs with taxonomy, module boundaries, extension rules, and focused gates.
- [ ] Regenerate CLI reference and quality metrics.

### Task 5: Verification And Publish

**Files:**
- All branch-owned changes.

- [ ] Run focused tests, ruff, ruff format, mypy, docs generated checks, import rules, layer metrics, module size, full non-live pytest with coverage, and mkdocs strict build.
- [ ] Commit with `Add Airflow runtime profile pack`.
- [ ] Push `codex/gitops-airflow-runtime-profile-pack`.
- [ ] Create stacked draft PR against `codex/gitops-airflow-run-spec-evidence`.
- [ ] Watch GitHub checks and report status.

## Self-Review

- Spec coverage: runtime profile, DAG factory, XCom summary, release policy, schemas, docs, architecture, CI/CD, and PR publication are covered.
- Placeholder scan: no placeholder implementation steps remain.
- Type consistency: artifact names and kind strings consistently use `runtime-profile.json`, `xcom-summary.json`, `airflow_dag_factory.py`, and `gitops.airflow_runtime_profile`.
