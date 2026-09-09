# Release RC Collector Implementation Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a release-candidate collection command that turns GitHub CLI PR JSON exports and evidence references into stable `merge_train.json` and finalizer input artifacts.

**Architecture:** Keep final go/no-go decisions in `ReleaseRcFinalizerPolicy`; add a separate collector service that only normalizes PR/evidence inputs and writes reviewable artifacts. GitHub API/subprocess calls stay outside core policy and finalizer logic; the CLI consumes files produced by `gh pr view --json ...`.

**Tech Stack:** Python dataclasses, existing ops CLI/catalog patterns, JSON/Markdown artifacts, pytest, ruff, mypy, MkDocs.

---

### Task 1: RED Tests

**Files:**
- Create: `tests/test_release_rc_collector.py`
- Create: `tests/test_cli_release_rc_collect_command.py`
- Create: `tests/test_release_rc_collector_docs_contract.py`

- [ ] **Step 1: Write collector service tests**

Cover GitHub CLI-shaped payloads with `baseRefName`, `headRefName`, `mergeStateStatus`, `isDraft`, and `statusCheckRollup`. Assert that the collector writes `merge_train.json`, `release_rc_inputs.json`, and Markdown with a finalizer command.

- [ ] **Step 2: Write CLI tests**

Assert that `dpone ops release-rc-collect --format json` exits zero for a clean ordered train and non-zero for missing PR inputs.

- [ ] **Step 3: Write docs contract tests**

Require user docs, developer docs, CI docs, ops CLI docs, architecture docs, mkdocs nav, and generated CLI reference to mention `release-rc-collect`.

- [ ] **Step 4: Run RED**

Run:

```bash
uv run pytest tests/test_release_rc_collector.py tests/test_cli_release_rc_collect_command.py tests/test_release_rc_collector_docs_contract.py -q
```

Expected: fail because `dpone.ops.release_rc_collector` and the CLI command do not exist yet.

### Task 2: Collector Models And Payload Parsing

**Files:**
- Create: `src/dpone/ops/release_rc_payloads.py`
- Create: `src/dpone/ops/release_rc_collector_models.py`
- Modify: `src/dpone/ops/release_rc_finalizer.py`

- [ ] **Step 1: Extract payload normalization**

Move reusable `merge_train`/PR/check normalization into `release_rc_payloads.py`. Support both existing `checks` arrays and GitHub CLI `statusCheckRollup` arrays.

- [ ] **Step 2: Add collector report models**

Create immutable dataclasses for evidence refs and collection reports. Reports must render stable JSON/Markdown and include output paths, blockers, warnings, and finalizer command arguments.

- [ ] **Step 3: Update finalizer to reuse parser**

Replace private finalizer parsing helpers with imports from `release_rc_payloads.py`.

- [ ] **Step 4: Run focused tests**

Run:

```bash
uv run pytest tests/test_release_rc_finalizer.py tests/test_release_rc_collector.py -q
```

Expected: pass after implementation.

### Task 3: Collector Service And CLI Wiring

**Files:**
- Create: `src/dpone/ops/release_rc_collector.py`
- Modify: `src/dpone/ops/catalog_release_gates.py`
- Modify: `src/dpone/ops/catalog_release_sections.py`
- Modify: `src/dpone/services/ops/command_handlers_release.py`
- Modify: `src/dpone/services/ops/__init__.py`
- Modify: `src/dpone/commands/ops_parsers_core.py`
- Modify: `src/dpone/commands/ops_cmd.py`
- Modify: `src/dpone/commands/registry_ops.py`

- [ ] **Step 1: Implement `ReleaseRcCollectorService`**

Inputs: output dir, release/version context, base/head branch, ordered PR JSON files, evidence refs, required artifacts, finalizer output dir, and mode. Outputs: `merge_train.json`, `release_rc_inputs.json`, and `release_rc_collect.md`.

- [ ] **Step 2: Add catalog method**

Expose `release_rc_collector()` lazily, matching the finalizer factory style.

- [ ] **Step 3: Add CLI command**

Add `dpone ops release-rc-collect` with `--pull-request-json`/`--pr-json`, `--base-branch`, `--head-branch`, `--release`, `--previous-release`, `--package-version`, `--artifact`, `--require-artifact`, `--mode`, `--finalizer-output-dir`, and `--format`.

- [ ] **Step 4: Run CLI focused tests**

Run:

```bash
uv run pytest tests/test_cli_release_rc_collect_command.py -q
```

Expected: pass.

### Task 4: Docs And Generated References

**Files:**
- Create: `docs/release-rc-collector.md`
- Create: `docs/developer-release-rc-collector.md`
- Modify: `docs/README.md`
- Modify: `docs/ops-cli.md`
- Modify: `docs/release-rc-finalizer.md`
- Modify: `docs/developer-release-rc-finalizer.md`
- Modify: `docs/release.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/developer-ci-cd.md`
- Modify: `docs/architecture.md`
- Modify: `mkdocs.yml`
- Generate: `docs/cli-reference.md`
- Generate: `docs/quality-metrics.md`

- [ ] **Step 1: Add user and developer docs**

Document `gh pr view --json ... > pr-75.json`, `release-rc-collect`, generated artifacts, and the no-GitHub-API core boundary.

- [ ] **Step 2: Update release/CI/architecture navigation**

Add the collector to release runbooks, CI gates, architecture docs, and mkdocs nav.

- [ ] **Step 3: Regenerate references**

Run:

```bash
uv run dpone docs update-cli-reference
uv run dpone docs update-dev-metrics
```

- [ ] **Step 4: Run docs contract tests**

Run:

```bash
uv run pytest tests/test_release_rc_collector_docs_contract.py -q
```

Expected: pass.

### Task 5: Verification And Publication

**Files:**
- All changed files from prior tasks.

- [ ] **Step 1: Run full local verification**

Run:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run pytest -q
uv run dpone docs update-cli-reference --check
uv run dpone docs update-dev-metrics --check
uv run dpone docs check-docs
uv run dpone docs check-import-rules
uv run dpone docs check-layer-metrics
uv run dpone docs check-module-size --baseline docs/module_size_baseline.json
uv run dpone docs check-architecture-fitness
uv run mkdocs build --strict
```

- [ ] **Step 2: Commit and push**

Commit only collector-related code/docs/tests and push branch `codex/release-rc-train-generator`.

- [ ] **Step 3: Open stacked PR**

Create a draft PR with base `codex/rc-integration-finalizer`.
