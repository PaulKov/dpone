# GitOps Airflow Run Spec Evidence Implementation Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an Airflow runtime contract and runtime evidence verification layer for custom dpone images running under KubernetesExecutor, KubernetesPodOperator, and KubernetesPodExecutor-style wrappers.

**Architecture:** Keep the slice control-plane only. `dpone.gitops.airflow_runtime_models` owns stable DTOs, `dpone.gitops.airflow_run_spec` builds pure run-spec contracts, `dpone.gitops.airflow_runtime_evidence` verifies runtime evidence, and `dpone.services.gitops.airflow_runtime_service` composes filesystem, bundle loading, path validation, and views. `dpone.commands.gitops.airflow_cmd` stays a thin argparse/rendering adapter.

**Tech Stack:** Python dataclasses, existing GitOps DTOs, existing repo-relative path policy, JSON Schema catalog under `dpone.gitops.schema_contracts`, pytest, ruff, mypy, mkdocs.

---

### Task 1: Red Tests For Run Spec And Evidence

**Files:**
- Modify: `tests/test_gitops_airflow_runner_pack.py`
- Modify: `tests/test_cli_gitops_airflow_commands.py`
- Modify: `tests/test_gitops_schema_contracts.py`
- Modify: `tests/test_gitops_airflow_docs_contract.py`

- [ ] **Step 1: Add service tests before production code**

Add tests proving that `GitOpsAirflowRunSpecService` writes `.dpone/gitops/airflow/run-spec.json`, that render artifacts include `run-spec.json` and `runtime-evidence.json`, that generated `entrypoint.sh` executes `dpone gitops airflow run-spec-exec`, and that `GitOpsAirflowEvidenceVerifyService` accepts a passing evidence file and blocks failed steps.

- [ ] **Step 2: Add CLI tests before production code**

Add tests for `cmd_gitops_airflow_run_spec` and `cmd_gitops_airflow_evidence_verify`, and assert `gitops_group()` still registers the nested `airflow` command family.

- [ ] **Step 3: Add schema/docs contract tests before production code**

Extend schema catalog tests to require `airflow-run-spec` and `airflow-runtime-evidence`. Extend docs contract tests to require `dpone gitops airflow run-spec`, `dpone gitops airflow evidence-verify`, `run-spec.schema.json`, and `runtime-evidence.schema.json`.

- [ ] **Step 4: Run the focused tests and verify RED**

Run:

```bash
uv run pytest tests/test_gitops_airflow_runner_pack.py tests/test_cli_gitops_airflow_commands.py tests/test_gitops_schema_contracts.py tests/test_gitops_airflow_docs_contract.py -q
```

Expected: fail because the new service classes, command handlers, schemas, and docs do not exist yet.

### Task 2: Domain Contracts And Runtime Policy

**Files:**
- Create: `src/dpone/gitops/airflow_runtime_models.py`
- Create: `src/dpone/gitops/airflow_run_spec.py`
- Create: `src/dpone/gitops/airflow_runtime_evidence.py`
- Modify: `src/dpone/gitops/airflow_models.py`

- [ ] **Step 1: Implement immutable DTOs**

Add `GitOpsAirflowRunSpecEntry`, `GitOpsAirflowRunSpec`, `GitOpsAirflowRunSpecReport`, `GitOpsAirflowRuntimeStep`, `GitOpsAirflowRuntimeEvidence`, and `GitOpsAirflowEvidenceVerifyReport`. Each DTO must expose `to_jsonable()` and `to_json()` where appropriate and must not expose absolute paths.

- [ ] **Step 2: Implement pure run-spec builder**

Add `GitOpsAirflowRunSpecBuilder.build(...)` to produce deterministic commands, evidence path defaults, Airflow env names, bundle digest, image digest, and per-manifest entries from an existing `gitops.bundle`.

- [ ] **Step 3: Implement pure evidence verifier**

Add `GitOpsAirflowRuntimeEvidenceVerifier.verify(...)` to check expected kind, run-spec path match, failed steps, missing required steps, and manifest entry failures. Return warnings/blockers as `GitOpsIssue` tuples.

- [ ] **Step 4: Run focused model tests and verify GREEN for domain**

Run the focused Airflow runner test file. Expected: new model and verifier tests pass after the service layer exists, while CLI/docs tests may still fail until later tasks complete.

### Task 3: Services, Render Integration, And CLI

**Files:**
- Create: `src/dpone/services/gitops/airflow_runtime_service.py`
- Modify: `src/dpone/services/gitops/airflow_render_service.py`
- Modify: `src/dpone/gitops/airflow_artifacts.py`
- Modify: `src/dpone/gitops/airflow_rendering.py`
- Modify: `src/dpone/commands/gitops/airflow_cmd.py`

- [ ] **Step 1: Implement runtime services**

Add `GitOpsAirflowRunSpecService.build_view(args)` and `GitOpsAirflowEvidenceVerifyService.build_view(args)`. Services own path validation, bundle/evidence JSON loading, artifact writes, and `GitOpsView` construction.

- [ ] **Step 2: Integrate render output**

Update `GitOpsAirflowRenderService` to write `run-spec.json`, include it in artifacts, generate an entrypoint that executes `dpone gitops airflow run-spec-exec .dpone/gitops/airflow/run-spec.json --evidence-output .dpone/gitops/airflow/runtime-evidence.json`, and expose the command in the render report.

- [ ] **Step 3: Add CLI subcommands**

Register `run-spec` and `evidence-verify` under `dpone gitops airflow`. Keep handlers thin and reuse existing JSON/Markdown output conventions.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run:

```bash
uv run pytest tests/test_gitops_airflow_runner_pack.py tests/test_cli_gitops_airflow_commands.py -q
```

Expected: Airflow service and CLI tests pass.

### Task 4: Schemas, Docs, Architecture, And Quality Contracts

**Files:**
- Modify: `src/dpone/gitops/schema_contracts.py`
- Add: `docs/schemas/gitops/airflow-run-spec.schema.json`
- Add: `docs/schemas/gitops/airflow-runtime-evidence.schema.json`
- Modify: `docs/gitops-airflow-runner-pack.md`
- Modify: `docs/developer-gitops-airflow-runner-pack.md`
- Modify: `docs/gitops-control-plane.md`
- Modify: `docs/architecture.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/cli-reference.md`
- Modify: `docs/quality-metrics.md`

- [ ] **Step 1: Extend schema catalog**

Add contracts for `gitops.airflow_run_spec` and `gitops.airflow_runtime_evidence`, then write the generated schema files in `docs/schemas/gitops/`.

- [ ] **Step 2: Update public docs**

Document the user runbook, Airflow/KubernetesPodExecutor handoff, custom image behavior, runtime evidence artifacts, and troubleshooting. Keep all OSS docs in English.

- [ ] **Step 3: Update developer and architecture docs**

Document class taxonomy, module boundaries, extension rules, CI/CD gates, and quality metrics. Regenerate CLI reference and developer metrics.

- [ ] **Step 4: Run docs/schema focused tests and verify GREEN**

Run:

```bash
uv run pytest tests/test_gitops_schema_contracts.py tests/test_gitops_airflow_docs_contract.py -q
uv run dpone docs update-cli-reference --check
uv run dpone docs update-dev-metrics --check
```

Expected: schema and docs contracts pass.

### Task 5: Final Verification And Publish

**Files:**
- All changed files in the branch.

- [ ] **Step 1: Run quality gates**

Run:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run dpone docs check-import-rules
uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json
uv run dpone docs check-module-size --baseline docs/module_size_baseline.json
uv run pytest -m "not integration_live" --cov=src/dpone --cov-report=xml
uv run mkdocs build --strict
```

Expected: all gates pass.

- [ ] **Step 2: Commit, push, and create stacked draft PR**

Stage only branch-owned files, commit with `Add Airflow run spec runtime evidence`, push `codex/gitops-airflow-run-spec-evidence`, and create a draft PR with base `codex/gitops-airflow-policy-schemas`.

- [ ] **Step 3: Watch GitHub checks**

Run `gh pr checks <number> --watch --interval 10` and report remote status.

## Self-Review

- Spec coverage: the plan covers runtime contract generation, entrypoint handoff, evidence verification, CLI, schema contracts, user/developer docs, architecture, CI, and final PR publication.
- Placeholder scan: no `TBD`, `TODO`, or open-ended implementation placeholders remain.
- Type consistency: `run-spec`, `runtime-evidence`, service, CLI, schema, and docs names are consistent across tasks.
