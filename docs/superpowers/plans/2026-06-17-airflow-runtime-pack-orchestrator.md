# Airflow Runtime Pack Orchestrator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `dpone gitops airflow pack` as a self-service golden-path report for Airflow/Kubernetes GitOps runtime artifacts and verify it with focused, full, and opt-in Minikube/Airflow smoke checks.

**Architecture:** Keep Airflow artifact generation owned by existing commands. The new pack slice only plans and verifies the canonical artifact directory using `AIRFLOW_ARTIFACT_SPECS`, stable DTOs, a small domain planner, and a thin service/CLI facade.

**Tech Stack:** Python dataclasses, argparse, existing GitOps service/view patterns, JSON schemas, pytest, MkDocs, Minikube, Helm, Apache Airflow, KubernetesPodOperator.

---

### Task 1: Red Tests For Pack Domain And CLI

**Files:**
- Create: `tests/test_gitops_airflow_pack.py`
- Modify: `tests/test_cli_gitops_airflow_commands.py`
- Modify: `tests/test_gitops_schema_contracts.py`
- Modify: `tests/test_gitops_airflow_docs_contract.py`

- [ ] **Step 1: Add failing domain/service/CLI tests**

Add tests that assert:
- plan mode emits `kind=gitops.airflow_pack`, deterministic steps, no absolute paths, and writes `airflow-runtime-pack.json`.
- verify mode blocks missing required Airflow artifacts.
- verify mode passes once minimal required artifacts exist.
- the nested `dpone gitops airflow` command catalog exposes `pack`.

- [ ] **Step 2: Add failing schema/docs tests**

Add tests that assert:
- `airflow-pack` exists in the GitOps schema catalog and docs schema directory.
- user and developer docs mention `dpone gitops airflow pack`, `airflow-runtime-pack.json`, `GitOpsAirflowPackService`, and `GitOpsAirflowPackPlanner`.

- [ ] **Step 3: Run focused tests and confirm RED**

Run:

```bash
uv run pytest tests/test_gitops_airflow_pack.py tests/test_cli_gitops_airflow_commands.py::test_gitops_group_registers_airflow_nested_command tests/test_gitops_schema_contracts.py tests/test_gitops_airflow_docs_contract.py -q
```

Expected: failures for missing command, modules, schema, and docs strings.

### Task 2: Implement Pack DTOs And Planner

**Files:**
- Create: `src/dpone/gitops/airflow_pack_models.py`
- Create: `src/dpone/gitops/airflow_pack.py`
- Modify: `src/dpone/gitops/airflow_artifact_index.py`

- [ ] **Step 1: Add immutable DTOs**

Define `GitOpsAirflowPackArtifact`, `GitOpsAirflowPackStep`, and `GitOpsAirflowPackReport` with `to_jsonable()` methods, `passed`, `warnings`, `blockers`, and `next_actions`.

- [ ] **Step 2: Add pure planner**

Define `GitOpsAirflowPackPlanner.plan(...)` that:
- consumes artifact specs and loaded artifact contents.
- preserves deterministic artifact and command order.
- emits plan-mode warnings for missing required artifacts.
- emits verify-mode blockers for missing or invalid required artifacts.
- includes live-gate steps only when requested.

- [ ] **Step 3: Register pack artifact**

Add `airflow_runtime_pack` to `AIRFLOW_ARTIFACT_SPECS` as optional `airflow-runtime-pack.json` with expected kind `gitops.airflow_pack`.

### Task 3: Implement Service, CLI, And Schema

**Files:**
- Create: `src/dpone/services/gitops/airflow_pack_service.py`
- Create: `src/dpone/commands/gitops/airflow_pack_cmd.py`
- Create: `src/dpone/gitops/schema_airflow_pack_contracts.py`
- Modify: `src/dpone/commands/gitops/airflow_cmd.py`
- Modify: `src/dpone/gitops/schema_airflow_contracts.py`

- [ ] **Step 1: Add service**

Implement `GitOpsAirflowPackService` that validates repo-relative paths, reads known artifacts without exposing absolute paths, invokes the planner, and writes the default output artifact when paths are valid.

- [ ] **Step 2: Add thin CLI**

Implement `register_pack_parser` and `cmd_gitops_airflow_pack` with flags:
- `--artifact-dir`
- `--bundle-path`
- `--image`
- `--image-digest`
- `--mode {plan,verify}`
- `--runner-policy {advisory,pr,release}`
- `--include-live-gates`
- `--output-path`
- `--output`
- `--format {json,markdown}`

- [ ] **Step 3: Register nested command lazily**

Add `pack` to `airflow_group()` through lazy wrappers in `airflow_cmd.py` and keep command module SLOC under 400.

- [ ] **Step 4: Add JSON schema contract**

Register `airflow-pack.schema.json` with required public fields and generate docs schema output.

### Task 4: Update English Documentation

**Files:**
- Modify: `docs/gitops-airflow-runner-pack.md`
- Modify: `docs/developer-gitops-airflow-runner-pack.md`
- Modify: `docs/architecture.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/gitops-control-plane.md`
- Modify: `docs/cli-reference.md`
- Create: `docs/schemas/gitops/airflow-pack.schema.json`

- [ ] **Step 1: Document user workflow**

Add a self-service section showing `pack --mode plan`, `pack --mode verify`, the default artifact path, and how it fits with `artifact-index`, `preflight`, `cluster-doctor`, `admission-check`, `k8s-smoke`, `pod-watch`, and `evidence-bundle`.

- [ ] **Step 2: Document developer taxonomy**

Add `GitOpsAirflowPackService`, `GitOpsAirflowPackPlanner`, DTO boundaries, and extension rules. State that pack does not parse manifests, mutate PodSpecs, compute git-sync auth, or start pods.

- [ ] **Step 3: Regenerate generated docs**

Run `uv run dpone docs update-cli-reference` and schema generation.

### Task 5: Local And Minikube/Airflow Verification

**Files:**
- No production files unless verification reveals a bug.

- [ ] **Step 1: Run focused and full local gates**

Run the focused test set, ruff, mypy, docs checks, module/layer/architecture metrics, non-live pytest with coverage, strict MkDocs, and package build checks.

- [ ] **Step 2: Attempt local Minikube + Airflow smoke**

Use installed local tooling where available:
- `minikube status` or `minikube start`
- `helm repo add apache-airflow https://airflow.apache.org`
- install or upgrade an Airflow namespace for a minimal local smoke
- generate pack artifacts and run plan/verify commands
- execute the supported KPO/pod-template smoke path where cluster resources and local image availability allow it

- [ ] **Step 3: Record evidence honestly**

If Minikube, Helm, Docker, image pull, or cluster resources block live execution, record the exact command and blocker. Do not claim live success without a fresh passing command.
