# GitOps Bundle Policy Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `dpone gitops bundle` as a deterministic scheduler handoff artifact that combines impacted manifests, emitted plans, verification reports, and policy gate blockers.

**Architecture:** Keep the command layer thin. `GitOpsBundleService` orchestrates existing affected/plan/verify services and writes a repo-relative output directory. Bundle DTOs, policy decisions, and Markdown rendering stay in focused modules so future GitOps/control-plane gates can reuse the same contract without scheduler-specific branching.

**Tech Stack:** Python, argparse, dataclasses, existing filesystem/YAML ports, GitOps affected/plan/verify services, pytest, ruff, mypy, MkDocs.

---

### Task 1: RED Bundle Service And CLI Tests

**Files:**
- Create: `tests/test_gitops_bundle.py`
- Create: `tests/test_cli_gitops_bundle_command.py`
- Modify: `tests/test_gitops_docs_contract.py`

- [ ] Add service tests for `GitOpsBundleService` producing `bundle.json`, `affected.json`, per-manifest `gitops_plan.json`, per-manifest `gitops_verify.json`, and `summary.md`.
- [ ] Add service tests for `--fail-on-empty-impact`, `--fail-on-warnings`, and `--require-lock` policy blockers.
- [ ] Add CLI tests for JSON output, Markdown output, and output artifact writing.
- [ ] Extend docs contract to require `dpone gitops bundle`, `--fail-on-empty-impact`, `--fail-on-warnings`, and `--require-lock`.
- [ ] Run: `uv run pytest tests/test_gitops_bundle.py tests/test_cli_gitops_bundle_command.py tests/test_gitops_docs_contract.py -q`.
- [ ] Expected: tests fail because bundle service and command do not exist.

### Task 2: GREEN Bundle Domain And Service

**Files:**
- Create: `src/dpone/gitops/bundle_models.py`
- Create: `src/dpone/gitops/bundle_policy.py`
- Create: `src/dpone/services/gitops/bundle_service.py`

- [ ] Add bundle DTOs with `kind`, `output_dir`, `affected_path`, `entries`, `warnings`, `blockers`, `policy`, and `passed`.
- [ ] Add `GitOpsBundlePolicy` that can block empty impact, warnings, and missing lock entries.
- [ ] Implement `GitOpsBundleService` by composing `GitOpsAffectedService` and `GitOpsVerifyService`.
- [ ] Write deterministic artifacts under `<output-dir>/affected.json`, `<output-dir>/manifests/<slug>/gitops_plan.json`, `<output-dir>/manifests/<slug>/gitops_verify.json`, `<output-dir>/bundle.json`, and `<output-dir>/summary.md`.
- [ ] Run: `uv run pytest tests/test_gitops_bundle.py -q`.
- [ ] Expected: bundle service tests pass.

### Task 3: GREEN Bundle CLI And Rendering

**Files:**
- Create: `src/dpone/commands/gitops/bundle_cmd.py`
- Create: `src/dpone/gitops/bundle_rendering.py`
- Modify: `src/dpone/commands/registry_gitops.py`

- [ ] Add `dpone gitops bundle` with changed-file input flags, sparse-path include flags, runner, `--verify-lock`, `--worktree`, policy flags, output-dir, output, and format flags.
- [ ] Add JSON and Markdown output rendering.
- [ ] Register the command in the GitOps command group.
- [ ] Run: `uv run pytest tests/test_cli_gitops_bundle_command.py tests/test_gitops_bundle.py -q`.
- [ ] Expected: CLI and service tests pass.

### Task 4: Docs And Generated References

**Files:**
- Modify: `docs/gitops-control-plane.md`
- Modify: `docs/developer-gitops-control-plane.md`
- Modify: `docs/architecture.md`
- Generate: `docs/cli-reference.md`
- Generate: `docs/quality-metrics.md`

- [ ] Document bundle quickstart, artifact layout, policy flags, and runbook.
- [ ] Update developer taxonomy and architecture notes.
- [ ] Regenerate CLI reference and quality metrics.
- [ ] Run targeted docs/tests and full verification gates.
- [ ] Commit only bundle-scope files, push `codex/manifest-sparse-paths`, and update PR #80.
