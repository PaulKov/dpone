# GitOps Airflow Runner Pack Implementation Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a self-service Airflow/Kubernetes runner pack for custom `dpone` images that renders deterministic runner artifacts and doctors bundle, pod template, and image contracts offline.

**Architecture:** Keep command handlers thin under `dpone.commands.gitops`, with orchestration in `dpone.services.gitops` and reusable domain DTO/render/doctor logic in `dpone.gitops`. The pack consumes existing `gitops.bundle` artifacts and does not import or execute Airflow; generated files are handoff artifacts for Airflow `KubernetesExecutor` and `KubernetesPodOperator`.

**Tech Stack:** Python dataclasses, existing `FileSystem`/`YamlCodec` ports, argparse command registry, pytest, ruff, mypy, MkDocs.

---

## File Structure

- Create `src/dpone/gitops/airflow_models.py`: immutable DTOs for image contracts, rendered artifacts, doctor checks, and reports.
- Create `src/dpone/gitops/airflow_artifacts.py`: pure renderers for `pod_template.yaml`, `executor_config.json`, `airflow_task.py`, and `entrypoint.sh`.
- Create `src/dpone/gitops/airflow_doctor.py`: reusable validators for bundle JSON, pod templates, and image contracts.
- Create `src/dpone/gitops/airflow_rendering.py`: Markdown rendering for Airflow render/doctor/image-contract reports.
- Create `src/dpone/services/gitops/airflow_render_service.py`: DI-aware service that loads a bundle and writes runner artifacts.
- Create `src/dpone/services/gitops/airflow_doctor_service.py`: DI-aware service that validates existing Airflow runner artifacts.
- Create `src/dpone/services/gitops/airflow_image_contract_service.py`: DI-aware service that writes/prints image contract JSON.
- Create `src/dpone/commands/gitops/airflow_cmd.py`: `dpone gitops airflow` command group with render, doctor, and image-contract subcommands only.
- Modify `src/dpone/commands/registry_gitops.py`: register the nested Airflow command group.
- Add `tests/test_gitops_airflow_runner_pack.py`: service/domain tests.
- Add `tests/test_cli_gitops_airflow_commands.py`: CLI handler tests.
- Add `tests/test_gitops_airflow_docs_contract.py`: docs, CLI reference, architecture, nav contract.
- Update `docs/gitops-control-plane.md`, `docs/developer-gitops-control-plane.md`, `docs/architecture.md`, `docs/ci-cd.md`, `docs/README.md`, `mkdocs.yml`.
- Regenerate `docs/cli-reference.md` after implementation.
- Update quality baselines only with project commands after new Python files are staged.

## Task 1: Red Tests For Airflow Runner Services

**Files:**
- Create: `tests/test_gitops_airflow_runner_pack.py`

- [ ] **Step 1: Write failing service tests**

Add tests that create an attested `gitops.bundle`, call `GitOpsAirflowRenderService`, assert repo-relative artifacts are emitted, then call `GitOpsAirflowDoctorService` and assert the generated pod template and image contract pass. Add a negative pod-template test that blocks when `spec.containers[0].name` is not `base`.

- [ ] **Step 2: Run tests to verify RED**

Run: `uv run pytest tests/test_gitops_airflow_runner_pack.py -q`

Expected: import errors for missing Airflow service/model modules.

## Task 2: Red Tests For CLI Command Layer

**Files:**
- Create: `tests/test_cli_gitops_airflow_commands.py`

- [ ] **Step 1: Write failing CLI tests**

Add tests that call `cmd_gitops_airflow_render`, `cmd_gitops_airflow_doctor`, and `cmd_gitops_airflow_image_contract` directly with `Namespace`, assert JSON output contracts and exit codes, and assert no local absolute temp path leaks.

- [ ] **Step 2: Run tests to verify RED**

Run: `uv run pytest tests/test_cli_gitops_airflow_commands.py -q`

Expected: import errors for missing command module.

## Task 3: Implement Domain DTOs And Renderers

**Files:**
- Create: `src/dpone/gitops/airflow_models.py`
- Create: `src/dpone/gitops/airflow_artifacts.py`
- Create: `src/dpone/gitops/airflow_doctor.py`
- Create: `src/dpone/gitops/airflow_rendering.py`

- [ ] **Step 1: Add immutable DTOs**

Define report DTOs with `passed` and `to_jsonable()` methods matching the existing GitOps report shape. Keep entries small and stable: `path`, `kind`, `required`, `exists`, `reason` for artifacts; `name`, `passed`, `severity`, `message`, `path`, `source` for checks.

- [ ] **Step 2: Add pure artifact renderers**

Render a KubernetesExecutor-compatible pod template with `metadata.name`, `spec.containers[0].name: base`, and non-empty image. Render KPO Python code as a self-contained example/snippet, and render an `entrypoint.sh` that runs bundle verify, per-plan verify, and `dpone run` for each bundle entry.

- [ ] **Step 3: Add validators**

Validate JSON bundle kind, optional attestation requirement, pod template first-container contract, and image contract fields. Return checks and `GitOpsIssue` blockers/warnings; do not raise for user-facing validation failures.

## Task 4: Implement Services And CLI

**Files:**
- Create: `src/dpone/services/gitops/airflow_render_service.py`
- Create: `src/dpone/services/gitops/airflow_doctor_service.py`
- Create: `src/dpone/services/gitops/airflow_image_contract_service.py`
- Create: `src/dpone/commands/gitops/airflow_cmd.py`
- Modify: `src/dpone/commands/registry_gitops.py`

- [ ] **Step 1: Add services**

Use existing `FileSystem`, `YamlCodec`, `safe_relative_path`, `GitOpsView`, and `build_gitops_meta`. Keep output repo-relative. Default output directory is `.dpone/gitops/airflow`.

- [ ] **Step 2: Add command group**

Register `dpone gitops airflow render`, `dpone gitops airflow doctor`, and `dpone gitops airflow image-contract`. Support `--format json|markdown`, `--output`, `--output-dir`, `--image`, `--image-contract`, `--pod-template`, and `--require-attestation` where appropriate.

- [ ] **Step 3: Run GREEN tests**

Run: `uv run pytest tests/test_gitops_airflow_runner_pack.py tests/test_cli_gitops_airflow_commands.py -q`

Expected: all tests pass.

## Task 5: Docs Contracts And Generated CLI Reference

**Files:**
- Create: `tests/test_gitops_airflow_docs_contract.py`
- Modify: `docs/gitops-control-plane.md`
- Modify: `docs/developer-gitops-control-plane.md`
- Modify: `docs/architecture.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/README.md`
- Modify: `mkdocs.yml`
- Generate: `docs/cli-reference.md`

- [ ] **Step 1: Write failing docs contract test**

Assert user/developer docs mention `dpone gitops airflow render`, `doctor`, `image-contract`, `KubernetesExecutor`, `KubernetesPodOperator`, `pod_template_file`, `base`, custom `dpone` image, and bundle attestation.

- [ ] **Step 2: Update English docs**

Add user runbook examples for Airflow/Kubernetes runners and developer extension rules for keeping Airflow out of runtime imports.

- [ ] **Step 3: Regenerate CLI reference**

Run: `uv run dpone docs update-cli-reference`

- [ ] **Step 4: Run docs contract**

Run: `uv run pytest tests/test_gitops_airflow_docs_contract.py -q`

Expected: pass.

## Task 6: Quality Gates, Commit, Push, PR

**Files:**
- Modify generated quality baselines if project commands require it.

- [ ] **Step 1: Stage new Python files before metrics update**

Run: `git add src/dpone/gitops/airflow_*.py src/dpone/services/gitops/airflow_*.py src/dpone/commands/gitops/airflow_cmd.py`

- [ ] **Step 2: Update dev metrics if needed**

Run: `uv run dpone docs update-dev-metrics`

- [ ] **Step 3: Run verification gate**

Run:
`uv run ruff check .`
`uv run mypy --config-file mypy.ini`
`uv run pytest tests/test_gitops_airflow_runner_pack.py tests/test_cli_gitops_airflow_commands.py tests/test_gitops_airflow_docs_contract.py -q`
`uv run dpone docs update-cli-reference --check`
`uv run dpone docs check-import-rules`
`uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json`
`uv run dpone docs check-module-size --baseline docs/module_size_baseline.json`
`uv run pytest -m "not integration_live" --cov=src/dpone --cov-report=xml`
`uv run mkdocs build --strict`

- [ ] **Step 4: Commit and PR**

Commit with message `Add Airflow Kubernetes GitOps runner pack`, push branch, and create a draft PR with base `codex/gitops-bundle-verify-schema`.
