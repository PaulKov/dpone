# Airflow K8s Live Runner Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in Airflow Kubernetes smoke contract for custom `dpone` images so release candidates can prove pod placement, image pinning, run-spec execution, runtime evidence, and final XCom outcome without making OSS CI require a live cluster.

**Architecture:** Keep this as a control-plane gate layered on top of the `v0.11.0` GitOps/Airflow artifacts. Domain models live in `dpone.gitops`, service orchestration lives in `dpone.services.gitops`, CLI handlers only parse arguments and render reports, and live command execution is isolated behind a runner protocol so tests can use deterministic fakes.

**Tech Stack:** Python dataclasses, existing `GitOpsIssue` and `GitOpsView` contracts, argparse CLI, JSON/Markdown renderers, JSON Schema generation, pytest, MkDocs.

---

### Task 1: Airflow K8s Smoke Domain

**Files:**
- Create: `src/dpone/gitops/airflow_k8s_smoke.py`
- Test: `tests/test_gitops_airflow_k8s_smoke.py`

- [ ] **Step 1: Write failing tests**

Test the report contract, plan-mode checks, release blockers for unpinned images, command rendering, and deterministic redaction.

- [ ] **Step 2: Implement minimal domain code**

Define immutable DTOs for smoke commands, command results, smoke checks, and the public report. Add a planner that reads normalized artifact mappings and emits checks plus command plans without importing Airflow or Kubernetes SDKs.

- [ ] **Step 3: Verify focused tests**

Run:

```bash
uv run pytest tests/test_gitops_airflow_k8s_smoke.py -q
```

Expected: all focused domain tests pass.

### Task 2: Service And CLI

**Files:**
- Create: `src/dpone/services/gitops/airflow_k8s_smoke_service.py`
- Modify: `src/dpone/commands/gitops/airflow_cmd.py`
- Modify: `src/dpone/gitops/airflow_rendering.py`
- Test: `tests/test_cli_gitops_airflow_commands.py`

- [ ] **Step 1: Write failing CLI tests**

Cover `dpone gitops airflow k8s-smoke` in plan mode, live mode with a static runner, invalid path blockers, markdown output, and command registration.

- [ ] **Step 2: Implement service and command**

Load `pod-contract.json`, `run-spec.json`, `runtime-profile.json`, optional `image-contract.json`, and optional `xcom-summary.json`; produce a stable report and write optional output. In `live` mode execute only the generated smoke commands through an injected runner.

- [ ] **Step 3: Verify focused CLI tests**

Run:

```bash
uv run pytest tests/test_cli_gitops_airflow_commands.py -q
```

Expected: all Airflow CLI tests pass.

### Task 3: Schemas, Docs, And CI References

**Files:**
- Modify: `src/dpone/gitops/schema_airflow_runtime_contracts.py`
- Modify: `src/dpone/gitops/schema_airflow_contracts.py`
- Create: `docs/schemas/gitops/airflow-k8s-smoke.schema.json`
- Modify: `docs/gitops-airflow-runner-pack.md`
- Modify: `docs/developer-gitops-airflow-runner-pack.md`
- Modify: `docs/architecture.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/README.md`
- Modify: `mkdocs.yml`
- Test: `tests/test_gitops_schema_contracts.py`
- Test: `tests/test_gitops_airflow_docs_contract.py`

- [ ] **Step 1: Write failing docs/schema tests**

Require the new schema, CLI reference entry, user docs, developer docs, architecture mention, and opt-in live-gate wording.

- [ ] **Step 2: Implement schema and docs**

Generate the schema through the existing GitOps schema catalog, document plan/live mode, Airflow/KubernetesPodExecutor usage, artifact expectations, and release-gate commands.

- [ ] **Step 3: Verify docs gates**

Run:

```bash
uv run pytest tests/test_gitops_schema_contracts.py tests/test_gitops_airflow_docs_contract.py -q
uv run dpone docs update-cli-reference --check
uv run mkdocs build --strict
```

Expected: tests and docs build pass.

### Task 4: Release Cutover

**Files:**
- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Modify: `README.md`
- Modify: `CHANGELOG.md`
- Modify: `tests/test_snapshot_version.py`
- Modify: `tests/test_oss_release_readiness.py`

- [ ] **Step 1: Run full pre-release verification**

Run the local gates documented in `docs/ci-cd.md`, including full non-live pytest with coverage and package build.

- [ ] **Step 2: Prepare v0.12.0 release metadata**

Bump package metadata to `0.12.0`, add changelog highlights, update version contract tests, and rebuild quality metrics if needed.

- [ ] **Step 3: Merge and release**

Open a PR, wait for GitHub checks, merge to `master`, wait for post-merge checks, tag `v0.12.0`, wait for release and runtime image workflows, and run PyPI resolver smoke.

### Self-Review

- The scope is one bounded control-plane feature: Airflow/Kubernetes smoke contract for custom `dpone` images.
- The feature does not import Airflow, Kubernetes, Docker, or cloud SDKs in core code.
- The live path is opt-in and is covered by injected runner tests.
- Public docs remain English-only.
