# GitOps Policy Attestation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add release-grade GitOps bundle policy profiles and bundle attestation while fixing the current generated quality-metrics CI blocker.

**Architecture:** Keep `dpone gitops bundle` as a thin CLI over `GitOpsBundleService`. Add reusable GitOps DTOs for policy profiles and attestations in the existing GitOps contract layer, keep profile resolution and attestation digest building in focused domain modules, and let the service compose them without scheduler-specific logic.

**Tech Stack:** Python dataclasses, argparse, SHA-256 digests, existing GitOps affected/verify services, existing filesystem/YAML ports, pytest, ruff, mypy, MkDocs.

---

### Task 1: Reproduce And Fix Quality Metrics CI Blocker

**Files:**
- Modify: `docs/quality-metrics.md`
- Modify: `docs/gitops-control-plane.md`
- Modify: `docs/developer-gitops-control-plane.md`

- [ ] Run `uv run dpone docs update-dev-metrics --check` and confirm it fails on current HEAD.
- [ ] Run `uv run dpone docs update-dev-metrics` after the bundle Python files are tracked.
- [ ] Run `uv run dpone docs update-dev-metrics --check` and confirm it passes.
- [ ] Document the tracked-file ordering rule: stage or commit new Python files before regenerating quality metrics.

### Task 2: RED Tests For Policy Profiles And Attestation

**Files:**
- Modify: `tests/test_gitops_bundle.py`
- Modify: `tests/test_cli_gitops_bundle_command.py`
- Modify: `tests/test_gitops_docs_contract.py`

- [ ] Add a service test for `policy_profile="release"` proving it enables `verify_lock`, `fail_on_empty_impact`, `fail_on_warnings`, and `require_lock`.
- [ ] Add a service test for bundle attestation proving `bundle.json` contains repo-relative provenance, artifact SHA-256 digests, and a deterministic bundle digest.
- [ ] Add a CLI test for `--policy-profile release` and `--attest`.
- [ ] Extend docs contract assertions for `--policy-profile`, `--attest`, `attestation`, and `bundle_digest`.
- [ ] Run the targeted tests and confirm they fail because profiles and attestations do not exist yet.

### Task 3: Implement GitOps Policy Profiles

**Files:**
- Create: `src/dpone/gitops/bundle_profiles.py`
- Modify: `src/dpone/gitops/models.py`
- Modify: `src/dpone/services/gitops/bundle_service.py`
- Modify: `src/dpone/commands/gitops/bundle_cmd.py`

- [ ] Add profile resolution for `custom`, `advisory`, `pr`, and `release`.
- [ ] Preserve explicit boolean flags as overrides after profile defaults.
- [ ] Add `policy_profile` to the public JSON contract and CLI metadata.
- [ ] Keep invalid profile handling as a blocker, not an argparse crash in service tests.

### Task 4: Implement Bundle Attestation

**Files:**
- Create: `src/dpone/gitops/bundle_attestation.py`
- Modify: `src/dpone/gitops/models.py`
- Modify: `src/dpone/services/gitops/bundle_service.py`
- Modify: `src/dpone/gitops/rendering.py`

- [ ] Add artifact digest DTOs and attestation DTOs.
- [ ] Digest `affected.json`, per-manifest `gitops_plan.json`, per-manifest `gitops_verify.json`, and `summary.md`.
- [ ] Compute a stable `bundle_digest` over artifact digest records.
- [ ] Include sanitized provenance: producer, schema version, source refs, changed-file inputs, runner, policy profile, and output directory.
- [ ] Do not expose local absolute filesystem paths.

### Task 5: Docs, CLI Reference, And Verification

**Files:**
- Modify: `docs/gitops-control-plane.md`
- Modify: `docs/developer-gitops-control-plane.md`
- Modify: `docs/architecture.md`
- Generate: `docs/cli-reference.md`
- Generate: `docs/quality-metrics.md`

- [ ] Update user runbook with policy profiles and attestation examples.
- [ ] Update developer docs and architecture boundaries.
- [ ] Regenerate CLI reference and quality metrics.
- [ ] Run targeted tests, static checks, docs gates, architecture gates, and full non-live pytest.
- [ ] Commit only feature-scope files, push the branch, update PR #80, and re-check GitHub CI.
