# GitOps Bundle Verify And Schema Contracts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add offline GitOps bundle verification and public JSON Schema contracts for GitOps artifacts.

**Architecture:** Keep `dpone gitops bundle` backward compatible while adding `dpone gitops bundle verify PATH` as a verification mode in the existing command facade. Put JSON Schema definitions in a small catalog module, structural payload validation in a separate validator, attestation digest verification in a focused domain module, and use a thin service for filesystem orchestration and report rendering.

**Tech Stack:** Python dataclasses, JSON, SHA-256, existing filesystem ports, argparse, pytest, ruff, mypy, MkDocs.

---

### Task 1: RED Tests For Public Contracts

**Files:**
- Create: `tests/test_gitops_bundle_verify.py`
- Create: `tests/test_cli_gitops_bundle_verify_command.py`
- Create: `tests/test_gitops_schema_contracts.py`
- Modify: `tests/test_gitops_docs_contract.py`

- [ ] Add service tests proving `GitOpsBundleVerifyService` passes a freshly attested bundle, blocks digest drift, blocks missing artifacts, and warns or blocks missing attestation depending on policy.
- [ ] Add CLI tests proving `dpone gitops bundle verify .dpone/gitops/bundle/bundle.json` prints JSON, supports Markdown, and validates missing `PATH` with a fail-closed exit.
- [ ] Add schema contract tests proving every public GitOps artifact kind has a JSON Schema with `$schema`, `$id`, `title`, `type`, `required`, and repo docs copy.
- [ ] Extend docs contract tests for `dpone gitops bundle verify`, `--require-attestation`, JSON Schema docs, and schema files.
- [ ] Run the targeted tests and confirm they fail because the command, service, and schemas are missing.

### Task 2: Schema Catalog And Validator

**Files:**
- Create: `src/dpone/gitops/schema_contracts.py`
- Create: `src/dpone/gitops/schema_validation.py`
- Create: `docs/schemas/gitops/affected.schema.json`
- Create: `docs/schemas/gitops/plan.schema.json`
- Create: `docs/schemas/gitops/verify.schema.json`
- Create: `docs/schemas/gitops/bundle.schema.json`
- Create: `docs/schemas/gitops/attestation.schema.json`

- [ ] Add `GitOpsSchemaContract` DTO and catalog functions for `affected`, `plan`, `verify`, `bundle`, and `attestation`.
- [ ] Keep schemas additive, Draft 2020-12 compatible, and focused on stable public fields rather than every internal invariant.
- [ ] Add a small validator that checks object shape, `kind`, required fields, field types, and nested attestation basics without adding a mandatory runtime dependency.
- [ ] Copy the schema catalog into docs schema files and test that source and docs stay identical.

### Task 3: Bundle Verify Domain And Service

**Files:**
- Modify: `src/dpone/gitops/models.py`
- Create: `src/dpone/gitops/bundle_verify.py`
- Create: `src/dpone/services/gitops/bundle_verify_service.py`
- Modify: `src/dpone/gitops/rendering.py`

- [ ] Add `GitOpsBundleArtifactCheck`, `GitOpsBundleSchemaCheck`, `GitOpsBundleAttestationCheck`, and `GitOpsBundleVerifyReport` DTOs.
- [ ] Implement digest verification by reading repo-relative artifact paths from `bundle.json.attestation.artifacts`.
- [ ] Recompute artifact SHA-256, byte sizes, and stable bundle digest using the existing attestation builder semantics.
- [ ] Return blockers for invalid bundle path, unreadable JSON, schema failures, missing required attestation, missing artifacts, artifact digest drift, and bundle digest drift.
- [ ] Return warnings for missing attestation when it is not required.
- [ ] Add Markdown rendering for self-service CI comments.

### Task 4: CLI Integration

**Files:**
- Modify: `src/dpone/commands/gitops/bundle_cmd.py`
- Modify: `src/dpone/commands/registry_gitops.py` only if a separate schema command is needed.

- [ ] Add backward-compatible positional parsing for `dpone gitops bundle verify PATH`.
- [ ] Add `--require-attestation` for verification mode.
- [ ] Route verify mode to `GitOpsBundleVerifyService`; keep build mode on `GitOpsBundleService`.
- [ ] Keep output behavior consistent with other GitOps commands: stdout plus optional `--output`, `--format json|markdown`.

### Task 5: Docs And Generated References

**Files:**
- Modify: `docs/gitops-control-plane.md`
- Modify: `docs/developer-gitops-control-plane.md`
- Modify: `docs/architecture.md`
- Modify: `docs/README.md` if schema index navigation needs it
- Generate: `docs/cli-reference.md`
- Generate: `docs/quality-metrics.md`

- [ ] Add user runbook for verifying a bundle before scheduler handoff or release promotion.
- [ ] Document JSON Schema files and their compatibility promise.
- [ ] Document developer boundaries and extension rules.
- [ ] Regenerate CLI reference and quality metrics after staging new Python files.

### Task 6: Verification And Publishing

**Files:**
- Commit only feature-scope files on `codex/gitops-bundle-verify-schema`.

- [ ] Run targeted GitOps tests.
- [ ] Run `uv run ruff check .`, `uv run ruff format --check .`, and `uv run mypy --config-file mypy.ini`.
- [ ] Run generated docs checks, import rules, layer metrics, module size, architecture fitness, docs checks, and strict MkDocs.
- [ ] Run `uv run pytest -m "not integration_live" --cov=src/dpone --cov-report=xml`.
- [ ] Commit, push, and create a stacked PR against `codex/manifest-sparse-paths` unless that branch has already merged into `master`.
