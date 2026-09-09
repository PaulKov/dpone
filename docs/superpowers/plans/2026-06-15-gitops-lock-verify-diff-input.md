# GitOps Lock Verify And Diff Input Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add deterministic lock verification to `dpone gitops verify` and Git-native changed-file inputs to `dpone gitops affected`.

**Architecture:** Keep CLI commands thin and add small domain services. `GitOpsLockVerifier` validates `gitops.plan.lock` entries against a sparse worktree without importing Git or manifest internals. `GitChangedFilesResolver` normalizes changed-file inputs from direct CLI values, a repo-relative file, or `git diff --name-only` refs before the existing impact analyzer runs.

**Tech Stack:** Python, argparse, dataclasses, existing filesystem/YAML ports, local `git` subprocess calls isolated in a small GitOps port, pytest, ruff, mypy, MkDocs.

---

### Task 1: RED Tests For Lock Verification

**Files:**
- Modify: `tests/test_gitops_plan_verify.py`
- Modify: `tests/test_cli_gitops_plan_verify_commands.py`

- [ ] Add service tests proving `verify_lock=True` detects file digest drift, missing locked files, and passes when files match.
- [ ] Add CLI tests proving `dpone gitops verify --verify-lock` returns blocker JSON for drift and includes no local absolute paths.
- [ ] Run: `uv run pytest tests/test_gitops_plan_verify.py tests/test_cli_gitops_plan_verify_commands.py -q`
- [ ] Expected: tests fail because `verify_lock` and lock verification behavior do not exist yet.

### Task 2: GREEN Lock Verification Domain

**Files:**
- Modify: `src/dpone/gitops/models.py`
- Create: `src/dpone/gitops/lock_verify.py`
- Modify: `src/dpone/gitops/verify.py`
- Modify: `src/dpone/services/gitops/verify_service.py`
- Modify: `src/dpone/commands/gitops/verify_cmd.py`
- Modify: `src/dpone/gitops/rendering.py`

- [ ] Add `GitOpsLockCheck` DTO and include `lock_checks` in `GitOpsVerifyReport`.
- [ ] Implement `GitOpsLockVerifier` with exact SHA-256 comparison for present file entries, existence checks for locked files, and warnings for directory/null digest entries.
- [ ] Wire `verify_lock` through service and CLI using `--verify-lock`.
- [ ] Keep path validation repo-relative through `dpone.gitops.paths`.
- [ ] Run: `uv run pytest tests/test_gitops_plan_verify.py tests/test_cli_gitops_plan_verify_commands.py -q`
- [ ] Expected: lock verification tests pass.

### Task 3: RED Tests For Git Diff And File Inputs

**Files:**
- Modify: `tests/test_gitops_affected.py`
- Modify: `tests/test_cli_gitops_affected_command.py`

- [ ] Add service tests for `changed_files_file`, `from_ref` + `to_ref`, deduped deterministic order, and blockers when no changed-file source is provided.
- [ ] Add CLI tests for `--changed-files-file` and `--from-ref/--to-ref`.
- [ ] Run: `uv run pytest tests/test_gitops_affected.py tests/test_cli_gitops_affected_command.py -q`
- [ ] Expected: tests fail because the new changed-file inputs do not exist yet.

### Task 4: GREEN Changed File Resolver

**Files:**
- Create: `src/dpone/gitops/changed_files.py`
- Modify: `src/dpone/services/gitops/affected_service.py`
- Modify: `src/dpone/commands/gitops/affected_cmd.py`

- [ ] Implement `GitChangedFilesResolver` with injectable command runner.
- [ ] Resolve changed files from direct `--changed-files`, newline-delimited `--changed-files-file`, and `git diff --name-only <from-ref> <to-ref>`.
- [ ] Return `GitOpsIssue` blockers for invalid refs, missing file reads, failed git commands, and empty input sets.
- [ ] Preserve deterministic first-seen order and do not expose absolute local paths in public reports.
- [ ] Run: `uv run pytest tests/test_gitops_affected.py tests/test_cli_gitops_affected_command.py -q`
- [ ] Expected: changed-file resolver tests pass.

### Task 5: Docs, Metrics, And Verification

**Files:**
- Modify: `docs/gitops-control-plane.md`
- Modify: `docs/developer-gitops-control-plane.md`
- Modify: `docs/architecture.md`
- Generate: `docs/cli-reference.md`
- Generate: `docs/quality-metrics.md`
- Modify: `tests/test_gitops_docs_contract.py`

- [ ] Document `--verify-lock`, `--changed-files-file`, and `--from-ref/--to-ref`.
- [ ] Update developer taxonomy and architecture notes for lock verification and changed-file resolution.
- [ ] Regenerate CLI reference and quality metrics.
- [ ] Run targeted and full verification gates.
- [ ] Commit only files from this feature, push `codex/manifest-sparse-paths`, and update PR #80.
