# Release RC Finalizer Implementation Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a release-level RC integration gate that validates a stacked merge train, version context, and release evidence before v0.10.0 tagging.

**Architecture:** Keep route-level RC orchestration under `dpone.ops.routes.*`; add a release-level finalizer under focused `dpone.ops.release_rc_*` modules. CLI remains a thin parser/handler that delegates to `ReleaseRcFinalizerService` through `ReleaseGateCatalog`.

**Tech Stack:** Python dataclasses, `EvidenceArtifactReader`, argparse CLI, pytest, MkDocs, existing docs/architecture quality gates.

---

### Task 1: Service And Policy RED Tests

**Files:**
- Create: `tests/test_release_rc_finalizer.py`
- Create: `tests/test_cli_release_rc_finalizer_command.py`
- Create: `tests/test_release_rc_finalizer_docs_contract.py`

- [ ] **Step 1: Write failing service tests**

Add tests that build a synthetic contiguous merge train, passing evidence artifacts, and assert `ReleaseRcFinalizerService.finalize(...)` writes `release_rc_finalizer.json` and `release_rc_finalizer.md` with schema `dpone.release_rc_finalizer.v1`.

- [ ] **Step 2: Write failing blocker tests**

Add tests for broken merge train order, failed PR checks, missing required evidence, and package version mismatch.

- [ ] **Step 3: Write failing CLI tests**

Add tests for `dpone ops release-rc-finalize --format json` returning zero for a green train and non-zero for blocked evidence.

- [ ] **Step 4: Write failing docs contract tests**

Require user docs, developer docs, architecture, ops CLI, and CI/CD docs to mention `release-rc-finalize`, merge train JSON, version context, and required evidence.

- [ ] **Step 5: Verify RED**

Run:

```bash
uv run pytest tests/test_release_rc_finalizer.py tests/test_cli_release_rc_finalizer_command.py tests/test_release_rc_finalizer_docs_contract.py -q
```

Expected: failures because modules, parser, handler, and docs do not exist yet.

### Task 2: Focused Release RC Taxonomy

**Files:**
- Create: `src/dpone/ops/release_rc_models.py`
- Create: `src/dpone/ops/release_rc_policy.py`
- Create: `src/dpone/ops/release_rc_finalizer.py`

- [ ] **Step 1: Add immutable public contracts**

Define `ReleaseRcCheckRun`, `ReleaseRcPullRequest`, `ReleaseRcMergeTrain`, `ReleaseRcFinalizerCheck`, `ReleaseRcFinalizerReport`, and stable JSON/Markdown rendering.

- [ ] **Step 2: Add pure policy**

Define `ReleaseRcFinalizerPolicy.evaluate(...)` with no filesystem, CLI, or GitHub API logic. It validates release version progression, package version match, merge train chain, PR state/draft/merge state, check rollups, and artifact statuses.

- [ ] **Step 3: Add service orchestration**

Define `ReleaseRcFinalizerService.finalize(...)` that reads merge train JSON, reads evidence through `EvidenceArtifactReader`, delegates decisions to policy, writes JSON/Markdown, and remains credential-free.

- [ ] **Step 4: Verify GREEN for service tests**

Run:

```bash
uv run pytest tests/test_release_rc_finalizer.py -q
```

Expected: service tests pass.

### Task 3: CLI And DI Wiring

**Files:**
- Modify: `src/dpone/commands/ops_parsers_core.py`
- Modify: `src/dpone/commands/registry_ops.py`
- Modify: `src/dpone/commands/ops_cmd.py`
- Modify: `src/dpone/services/ops/command_handlers_core.py`
- Modify: `src/dpone/services/ops/__init__.py`
- Modify: `src/dpone/ops/catalog_release_gates.py`
- Modify: `src/dpone/ops/catalog_release_sections.py`

- [ ] **Step 1: Add parser**

Add `release-rc-finalize` with `--release`, `--previous-release`, `--package-version`, `--merge-train-json`, `--artifact`, `--require-artifact`, `--mode pre_merge|post_merge`, and `--format`.

- [ ] **Step 2: Add handler**

Delegate all business logic to `ReleaseRcFinalizerService.finalize(...)`; return exit code `0` when passed and `1` when blocked.

- [ ] **Step 3: Add catalog factory**

Expose the service from `ReleaseGateCatalog` and `ReleaseGateOps`.

- [ ] **Step 4: Verify CLI GREEN**

Run:

```bash
uv run pytest tests/test_cli_release_rc_finalizer_command.py -q
```

Expected: CLI tests pass.

### Task 4: Documentation And Contract Updates

**Files:**
- Create: `docs/release-rc-finalizer.md`
- Create: `docs/developer-release-rc-finalizer.md`
- Modify: `docs/README.md`
- Modify: `docs/ops-cli.md`
- Modify: `docs/release.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/developer-ci-cd.md`
- Modify: `docs/architecture.md`
- Modify: `mkdocs.yml`

- [ ] **Step 1: Add user docs**

Document purpose, JSON input shape, CLI examples, output artifacts, and runbook blockers.

- [ ] **Step 2: Add developer docs**

Document module boundaries, DI, policy/service split, and extension rules.

- [ ] **Step 3: Update navigation and architecture**

Link the command from docs nav, ops CLI, release, CI/CD, developer CI/CD, and architecture evidence lane.

- [ ] **Step 4: Verify docs GREEN**

Run:

```bash
uv run pytest tests/test_release_rc_finalizer_docs_contract.py -q
```

Expected: docs contract tests pass.

### Task 5: Full Verification And Publication

**Files:**
- Generated: `docs/cli-reference.md`
- Generated: `docs/quality-metrics.md`

- [ ] **Step 1: Regenerate CLI docs and metrics**

Run:

```bash
uv run dpone docs update-cli-reference
uv run dpone docs update-dev-metrics
```

- [ ] **Step 2: Run focused suite**

Run:

```bash
uv run pytest tests/test_release_rc_finalizer.py tests/test_cli_release_rc_finalizer_command.py tests/test_release_rc_finalizer_docs_contract.py -q
```

- [ ] **Step 3: Run quality gates**

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

- [ ] **Step 4: Commit and publish**

Commit with:

```bash
git add ...
git commit -m "Add release RC integration finalizer"
git push -u origin codex/rc-integration-finalizer
gh pr create --base codex/route-conformance-lab --head codex/rc-integration-finalizer --title "Add release RC integration finalizer"
```
