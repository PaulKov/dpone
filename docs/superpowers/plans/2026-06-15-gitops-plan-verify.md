# GitOps Plan And Verify Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `dpone gitops plan` and `dpone gitops verify` as one cohesive control-plane contract for sparse checkout runners.

**Architecture:** Keep `dpone manifest sparse-paths` as the low-level manifest dependency primitive. Add a focused `dpone.gitops.*` domain that composes sparse-path reports into GitOps plan and verification reports, with thin command handlers and service-layer DTOs. The feature must not import runtime connectors, Airflow, Kubernetes clients, Git clients, or database SDKs.

**Tech Stack:** Python dataclasses, existing argparse command registry, existing `FileSystem` and `YamlCodec` ports, existing JSON/text output helpers, pytest, ruff, mypy, MkDocs.

---

### Task 1: Domain Contracts And RED Tests

**Files:**
- Create: `tests/test_gitops_plan_verify.py`
- Create: `tests/test_cli_gitops_plan_verify_commands.py`
- Create: `tests/test_gitops_docs_contract.py`

- [ ] Write tests for `GitOpsPlanService` producing a stable JSON contract with `kind`, `manifest`, `workload_root`, `sparse_paths`, `runner`, `command`, `warnings`, `blockers`, and `provenance`.
- [ ] Write tests for `GitOpsVerifyService` detecting missing required sparse paths in a worktree.
- [ ] Write CLI tests proving `dpone gitops plan` and `dpone gitops verify` return JSON and exit code `2` on blockers.
- [ ] Write docs contract tests requiring user docs, developer docs, architecture mention, CLI reference, and MkDocs nav.
- [ ] Run the new tests and confirm they fail because the modules and command group do not exist.

### Task 2: Domain Implementation

**Files:**
- Create: `src/dpone/gitops/models.py`
- Create: `src/dpone/gitops/plan.py`
- Create: `src/dpone/gitops/verify.py`
- Create: `src/dpone/gitops/rendering.py`
- Create: `src/dpone/gitops/__init__.py`

- [ ] Implement immutable dataclasses for `GitOpsIssue`, `GitOpsSparsePath`, `GitOpsRunnerContract`, `GitOpsPlanReport`, and `GitOpsVerifyReport`.
- [ ] Implement `GitOpsPlanBuilder` that consumes a `SparsePathReport` and builds a runner-safe report.
- [ ] Implement `GitOpsPlanVerifier` that checks a plan against a repo-relative worktree without exposing local absolute paths.
- [ ] Implement Markdown rendering helpers for plan and verification reports.
- [ ] Run domain tests and keep modules below module-size warning thresholds.

### Task 3: Services And CLI

**Files:**
- Create: `src/dpone/services/gitops/plan_service.py`
- Create: `src/dpone/services/gitops/verify_service.py`
- Create: `src/dpone/services/gitops/views.py`
- Create: `src/dpone/services/gitops/__init__.py`
- Create: `src/dpone/commands/gitops/plan_cmd.py`
- Create: `src/dpone/commands/gitops/verify_cmd.py`
- Create: `src/dpone/commands/gitops/__init__.py`
- Create: `src/dpone/commands/registry_gitops.py`
- Modify: `src/dpone/commands/registry.py`

- [ ] Add service Protocols over `settings.repo_root`, `fs`, and `yaml`; do not import `AppContext`.
- [ ] Add `dpone gitops plan MANIFEST` with sparse-path options, `--runner`, `--run-command`, `--output`, and `--format {json,markdown}`.
- [ ] Add `dpone gitops verify PLAN` with `--worktree`, `--output`, and `--format {json,markdown}`.
- [ ] Wire the `gitops` group into the top-level command registry.
- [ ] Run CLI tests, import rules, and architecture fitness checks.

### Task 4: Documentation And Generated References

**Files:**
- Create: `docs/gitops-control-plane.md`
- Create: `docs/developer-gitops-control-plane.md`
- Modify: `docs/README.md`
- Modify: `docs/developers.md`
- Modify: `docs/architecture.md`
- Modify: `mkdocs.yml`
- Generate: `docs/cli-reference.md`
- Generate: `docs/quality-metrics.md`

- [ ] Document user workflows for sparse checkout, Airflow/KubernetesPodOperator style handoff, plan artifacts, and verify artifacts.
- [ ] Document developer taxonomy, module boundaries, extension rules, and tests.
- [ ] Update architecture docs to record the GitOps control-plane slice.
- [ ] Regenerate CLI reference and developer metrics.
- [ ] Run docs contract tests and strict MkDocs build.

### Task 5: Verification And Publication

- [ ] Run `uv run ruff check .`.
- [ ] Run `uv run ruff format --check .`.
- [ ] Run `uv run mypy --config-file mypy.ini`.
- [ ] Run targeted GitOps and sparse-path tests.
- [ ] Run docs/import/layer/module/architecture gates.
- [ ] Run `uv run pytest -m "not integration_live" --cov=src/dpone --cov-report=xml`.
- [ ] Stage only feature files, commit, push, and update the draft PR.
