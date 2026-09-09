# Release Attestation Verification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden the release supply-chain path so CI uses a pinned `uv` runtime, release metadata checks come from the project lockfile, and GitHub Artifact Attestations are verified before artifacts are published.

**Architecture:** Keep the change declarative and workflow-scoped. Governance tests enforce the release contract, GitHub Actions perform the verification with `gh attestation verify`, and docs explain the operator-facing evidence flow.

**Tech Stack:** GitHub Actions, `astral-sh/setup-uv`, `uv`, Twine, GitHub Artifact Attestations, GitHub CLI, pytest, MkDocs.

---

## Task 1: Governance contracts

- [x] Add workflow governance coverage requiring every `astral-sh/setup-uv` step to set an explicit `version`.
- [x] Add release workflow coverage requiring `uv run twine check dist/*` instead of `uv tool run twine`.
- [x] Add release workflow coverage requiring GitHub attestation verification receipts before PyPI publish and GitHub Release creation.
- [x] Verify the new tests fail before implementation.

## Task 2: Workflow hardening

- [x] Pin the `uv` runtime version in every GitHub workflow setup step.
- [x] Run Twine from the synced project environment during release metadata validation.
- [x] Verify each built wheel and source distribution with `gh attestation verify`.
- [x] Upload JSON verification receipts as GitHub Actions evidence.

## Task 3: Documentation

- [x] Update supply-chain and release docs with the pinned-toolchain and attestation-verification flow.
- [x] Replace stale `uv tool run twine check dist/*` guidance with the lockfile-backed command.

## Task 4: Validation

- [ ] Run focused workflow governance tests.
- [ ] Run workflow YAML parsing checks.
- [ ] Run lint, type, docs, packaging, and broad non-live tests selected for this diff.
- [ ] Publish a PR with a clear summary and verification evidence.
