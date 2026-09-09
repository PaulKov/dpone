# GitOps Airflow Policy And Schemas Implementation Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add release-grade Airflow runner policy checks and public JSON Schema contracts for the GitOps Airflow runner pack.

**Architecture:** Keep base Airflow validation in `GitOpsAirflowDoctor`, add profile-specific gate logic in a new `dpone.gitops.airflow_policy` module, and keep CLI handlers argparse-only. Extend the existing GitOps schema catalog so Airflow contracts share the same documentation and validation surface as plan/verify/bundle artifacts.

**Tech Stack:** Python dataclasses, existing FileSystem/YamlCodec ports, argparse, pytest, ruff, mypy, JSON Schema dictionaries, MkDocs.

---

## File Structure

- Create `src/dpone/gitops/airflow_policy.py`: profile names and release/pr/advisory checks.
- Modify `src/dpone/gitops/airflow_models.py`: add `runner_policy` to `GitOpsAirflowDoctorReport`.
- Modify `src/dpone/gitops/airflow_artifacts.py`: render resources and security context defaults.
- Modify `src/dpone/services/gitops/airflow_doctor_service.py`: apply profile policy after base artifact validation.
- Modify `src/dpone/commands/gitops/airflow_cmd.py`: add `--runner-policy {advisory,pr,release}` to doctor and optional image metadata flags to render.
- Modify `src/dpone/gitops/schema_contracts.py`: add `airflow-render`, `airflow-doctor`, and `airflow-image-contract` contracts.
- Add generated files under `docs/schemas/gitops/`.
- Update Airflow user/developer docs, GitOps docs, architecture, CI/CD, CLI reference, and quality metrics.
- Extend `tests/test_gitops_airflow_runner_pack.py`, `tests/test_cli_gitops_airflow_commands.py`, `tests/test_gitops_airflow_docs_contract.py`, and `tests/test_gitops_schema_contracts.py`.

## Task 1: RED Tests For Release Policy

- [ ] Add tests that `runner_policy=release` blocks missing image digest, resources requests/limits, service account, and root security context.
- [ ] Add tests that generated render artifacts pass `runner_policy=release` when an image digest is provided.
- [ ] Run `uv run pytest tests/test_gitops_airflow_runner_pack.py -q` and confirm RED due to missing `runner_policy`.

## Task 2: RED Tests For CLI And Schemas

- [ ] Add CLI tests for `--runner-policy release` and rendered metadata flags.
- [ ] Extend schema contract tests to expect Airflow schema names and docs schema files.
- [ ] Run `uv run pytest tests/test_cli_gitops_airflow_commands.py tests/test_gitops_schema_contracts.py -q` and confirm RED.

## Task 3: Implement Policy And Schema Contracts

- [ ] Implement `GitOpsAirflowRunnerPolicyEvaluator` in `dpone.gitops.airflow_policy`.
- [ ] Wire policy into doctor service and report JSON.
- [ ] Add release-friendly pod template defaults and render metadata flags.
- [ ] Add Airflow schema contracts and schema files.
- [ ] Run focused tests to GREEN.

## Task 4: Docs And Quality Gates

- [ ] Update user/developer docs and docs contracts.
- [ ] Regenerate CLI reference and quality metrics.
- [ ] Run full verification:
  - `uv run ruff check .`
  - `uv run ruff format --check .`
  - `uv run mypy --config-file mypy.ini`
  - `uv run pytest tests/test_gitops_airflow_runner_pack.py tests/test_cli_gitops_airflow_commands.py tests/test_gitops_airflow_docs_contract.py tests/test_gitops_schema_contracts.py -q`
  - `uv run dpone docs update-cli-reference --check`
  - `uv run dpone docs update-dev-metrics --check`
  - `uv run dpone docs check-import-rules`
  - `uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json`
  - `uv run dpone docs check-module-size --baseline docs/module_size_baseline.json`
  - `uv run pytest -m "not integration_live" --cov=src/dpone --cov-report=xml`
  - `uv run mkdocs build --strict`

## Task 5: Commit, Push, PR

- [ ] Commit with message `Add Airflow runner release policy schemas`.
- [ ] Push `codex/gitops-airflow-policy-schemas`.
- [ ] Create draft stacked PR with base `codex/gitops-airflow-runner-pack`.
