# Airflow Pod Launch Evidence Implementation Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `dpone gitops airflow pod-watch` as an opt-in Airflow/Kubernetes control-plane gate that records pod launch/completion evidence for custom dpone images used by KubernetesPodExecutor, KubernetesExecutor, and KubernetesPodOperator.

**Architecture:** Keep the feature split into domain models, parser/planner, runner port, service orchestration, CLI adapter, schema, and docs. The CLI only parses args and renders output; live Kubernetes access stays behind an injectable runner so OSS CI remains credential-free.

**Tech Stack:** Python dataclasses, existing `GitOpsIssue`/`GitOpsView` contracts, argparse command adapters, JSON Schema contract helpers, pytest TDD, ruff/mypy/MkDocs quality gates.

---

### Task 1: Domain And CLI Red Tests

**Files:**
- Create: `tests/test_gitops_airflow_pod_launch_evidence.py`
- Modify: `tests/test_cli_gitops_airflow_commands.py`

- [ ] **Step 1: Write failing domain tests**

Add tests that import `GitOpsAirflowPodLaunchEvidencePlanner` and prove:
- plan mode emits `kubectl get pod`, `kubectl get events`, and `kubectl logs` commands without local absolute paths;
- live mode parses pod JSON, events JSON, and logs into normalized evidence;
- release mode blocks mismatched service account, wrong image digest, failed events, restarts, non-zero termination, and missing observed pod JSON.

- [ ] **Step 2: Write failing CLI tests**

Add CLI tests that import `cmd_gitops_airflow_pod_watch` and a static runner. Verify:
- `pod-watch --mode plan` writes JSON output with planned commands;
- `pod-watch --mode live` uses the injected runner and records results;
- failed Kubernetes evidence returns exit code `1`;
- `airflow_group()` registers `pod-watch`.

- [ ] **Step 3: Verify RED**

Run:

```bash
uv run pytest tests/test_gitops_airflow_pod_launch_evidence.py tests/test_cli_gitops_airflow_commands.py -q
```

Expected: import/registration failures for the missing pod launch evidence modules and command.

### Task 2: Domain Models, Planner, Runner, Service, CLI

**Files:**
- Create: `src/dpone/gitops/airflow_pod_launch_evidence_models.py`
- Create: `src/dpone/gitops/airflow_pod_launch_evidence_runner.py`
- Create: `src/dpone/gitops/airflow_pod_launch_evidence.py`
- Create: `src/dpone/services/gitops/airflow_pod_launch_evidence_service.py`
- Create: `src/dpone/commands/gitops/airflow_pod_launch_evidence_cmd.py`
- Modify: `src/dpone/commands/gitops/airflow_cmd.py`
- Modify: `src/dpone/gitops/airflow_rendering.py`

- [ ] **Step 1: Implement models**

Add immutable DTOs for checks, commands, command results, observed containers, events, and `GitOpsAirflowPodLaunchEvidenceReport`. Every DTO exposes `to_jsonable()`.

- [ ] **Step 2: Implement runner port**

Add `AirflowPodLaunchEvidenceRunner` protocol, `StaticAirflowPodLaunchEvidenceRunner`, and `SubprocessAirflowPodLaunchEvidenceRunner`. Subprocess timeouts return exit code `124` instead of raising.

- [ ] **Step 3: Implement planner/parser**

Build commands from repo-relative artifact paths and parse `kubectl` JSON stdout. Normalize:
- pod phase, pod name, namespace, service account, node name;
- base container image, image id, restart count, ready state, exit code, reason, message;
- event reason/type/message/count;
- logs tail.

Release checks must fail closed on image/service-account drift, bad events, restarts, non-zero exit, missing pod JSON, and missing immutable image digest.

- [ ] **Step 4: Implement service and CLI**

Load `runtime-profile.json`, `pod-contract.json`, optional `image-contract.json`, `runtime-evidence.json`, `xcom-summary.json`; validate safe repo-relative paths; run live commands only when `--mode live` and plan checks pass; render JSON/Markdown; keep `airflow_cmd.py` limited to subcommand registration.

- [ ] **Step 5: Verify GREEN**

Run:

```bash
uv run pytest tests/test_gitops_airflow_pod_launch_evidence.py tests/test_cli_gitops_airflow_commands.py -q
```

Expected: all focused domain/CLI tests pass.

### Task 3: Schema And Docs Contracts

**Files:**
- Modify: `src/dpone/gitops/schema_airflow_contracts.py`
- Modify: `src/dpone/gitops/schema_airflow_runtime_contracts.py`
- Create: `docs/schemas/gitops/airflow-pod-launch-evidence.schema.json`
- Modify: `tests/test_gitops_schema_contracts.py`
- Modify: `tests/test_gitops_airflow_docs_contract.py`

- [ ] **Step 1: Write failing schema/docs assertions**

Require `airflow-pod-launch-evidence` in the schema catalog and require user/developer docs, architecture docs, CI docs, and CLI reference to mention `dpone gitops airflow pod-watch`, the schema file, `AirflowPodLaunchEvidenceRunner`, and `GitOpsAirflowPodLaunchEvidenceService`.

- [ ] **Step 2: Implement schema and generate docs schema**

Add the contract to `airflow_schema_contracts()` and write the generated JSON schema to `docs/schemas/gitops/airflow-pod-launch-evidence.schema.json`.

- [ ] **Step 3: Verify schema/docs tests**

Run:

```bash
uv run pytest tests/test_gitops_schema_contracts.py tests/test_gitops_airflow_docs_contract.py -q
```

Expected: schema catalog and docs contract tests pass.

### Task 4: User Docs, Developer Docs, Architecture, CI, CLI Reference

**Files:**
- Modify: `docs/gitops-airflow-runner-pack.md`
- Modify: `docs/developer-gitops-airflow-runner-pack.md`
- Modify: `docs/architecture.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/cli-reference.md`
- Modify: `docs/quality-metrics.md`

- [ ] **Step 1: Update user docs**

Document `pod-watch` plan/live examples, evidence fields, troubleshooting, and runbook placement after `k8s-smoke`.

- [ ] **Step 2: Update developer docs and architecture**

Document class taxonomy, module boundaries, runner port, parser extension rules, schema contract, and CI expectations.

- [ ] **Step 3: Regenerate references and metrics**

Run:

```bash
uv run dpone docs update-cli-reference
uv run dpone docs update-dev-metrics
```

- [ ] **Step 4: Verify docs**

Run:

```bash
uv run dpone docs update-cli-reference --check
uv run dpone docs update-dev-metrics --check
uv run mkdocs build --strict
```

Expected: all docs checks pass.

### Task 5: Final Quality, Commit, Push, PR

**Files:**
- All files changed above.

- [ ] **Step 1: Run focused and architecture gates**

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run pytest tests/test_gitops_airflow_pod_launch_evidence.py tests/test_cli_gitops_airflow_commands.py tests/test_gitops_schema_contracts.py tests/test_gitops_airflow_docs_contract.py -q
uv run dpone docs check-import-rules
uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json
uv run dpone docs check-module-size --baseline docs/module_size_baseline.json
```

- [ ] **Step 2: Run full non-live gate**

```bash
uv run pytest -m "not integration_live" --cov=src/dpone --cov-report=xml
uv run mkdocs build --strict
rm -rf dist && uv build && uv tool run twine check dist/*
```

- [ ] **Step 3: Commit and open PR**

Stage only files belonging to this feature, commit, push `codex/airflow-pod-launch-evidence`, open a ready PR to `master`, and wait for required checks.

---

## Self-Review

- Scope is one feature: Airflow/Kubernetes pod launch evidence.
- No runtime connector or database logic is introduced.
- Live Kubernetes calls are isolated behind a runner port.
- CLI remains a thin adapter.
- Docs, schemas, architecture, quality metrics, and tests are part of the implementation.
