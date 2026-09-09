# GitOps Affected And Lock Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `dpone gitops affected` and a lock/provenance foundation for `gitops.plan`.

**Architecture:** Keep `gitops.plan` as the per-manifest runner contract and add lock entries with repo-relative path hashes. Add `gitops.affected` as a separate impact-analysis layer that builds a reverse dependency index from sparse-path reports and returns impacted manifests plus suggested plan commands. The feature stays scheduler-free, connector-free, and Git-client-free.

**Tech Stack:** Python dataclasses, existing argparse command registry, existing filesystem/YAML ports, sparse-path planner, pytest, ruff, mypy, MkDocs.

---

### Task 1: RED Tests

**Files:**
- Modify: `tests/test_gitops_plan_verify.py`
- Create: `tests/test_gitops_affected.py`
- Create: `tests/test_cli_gitops_affected_command.py`
- Modify: `tests/test_gitops_docs_contract.py`

- [ ] Add a failing assertion that `gitops.plan` includes a `lock` contract with sha256 for existing file paths.
- [ ] Add failing tests for `GitOpsAffectedService`: changed manifest files, convention files, and dependency files impact the owning manifest; unrelated files do not.
- [ ] Add failing CLI tests for `dpone gitops affected --changed-files ...`, Markdown output, invalid path blockers, and `--emit-plans`.
- [ ] Extend docs contract tests to require `dpone gitops affected`, impact analysis docs, and lock/provenance docs.

### Task 2: Lock Foundation

**Files:**
- Modify: `src/dpone/gitops/models.py`
- Create: `src/dpone/gitops/lock.py`
- Modify: `src/dpone/gitops/plan.py`
- Modify: `src/dpone/services/gitops/plan_service.py`

- [ ] Add `GitOpsLockEntry` and `GitOpsLockReport` DTOs.
- [ ] Add a lock builder that computes sha256 for existing file entries and keeps directories/missing paths hashless.
- [ ] Wire the lock into `GitOpsPlanReport` without changing sparse-path semantics.

### Task 3: Affected Domain And Service

**Files:**
- Create: `src/dpone/gitops/affected.py`
- Create: `src/dpone/services/gitops/affected_service.py`
- Modify: `src/dpone/gitops/rendering.py`

- [ ] Add immutable affected-report DTOs and analyzer logic.
- [ ] Build manifest sparse-path reports for workload manifests through the existing sparse-path planner.
- [ ] Match changed files against file entries and directory entries, preserving reasons.
- [ ] Optionally emit per-manifest `gitops_plan.json` artifacts when `--emit-plans` is set.

### Task 4: CLI And Docs

**Files:**
- Create: `src/dpone/commands/gitops/affected_cmd.py`
- Modify: `src/dpone/commands/registry_gitops.py`
- Modify: `docs/gitops-control-plane.md`
- Modify: `docs/developer-gitops-control-plane.md`
- Modify: `docs/architecture.md`
- Generate: `docs/cli-reference.md`
- Generate: `docs/quality-metrics.md`

- [ ] Add `dpone gitops affected --changed-files FILE...`.
- [ ] Support `--workload-root`, `--manifest-glob`, sparse include flags, `--runner`, `--emit-plans`, `--output-dir`, `--output`, and `--format {json,markdown}`.
- [ ] Update docs and generated references.

### Task 5: Verification And Publication

- [ ] Run lint, format, mypy, targeted tests, docs gates, architecture gates, full non-live pytest with coverage.
- [ ] Stage only intended files, commit, push, and update PR #80.
