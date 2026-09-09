# Industrial Readiness Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the next release-readiness layer that aggregates local matrix, correctness, reliability, performance lab, UX, and governance evidence.

**Architecture:** Add a focused `dpone.ops.industrial_readiness` service with immutable report models, a thin `dpone ops industrial-readiness` CLI wrapper, a scheduled/manual workflow, and self-service docs. Specialized heavy gates remain independent; this gate verifies their evidence artifacts.

**Tech Stack:** Python dataclasses, existing `dpone.ops` CLI registry, pytest, GitHub Actions, MkDocs.

---

### Task 1: Service and report models

- [x] Add failing tests for green/red industrial readiness artifacts.
- [x] Implement immutable report/domain/matrix models.
- [x] Write JSON and Markdown artifacts.

### Task 2: CLI integration

- [x] Add failing CLI test for `dpone ops industrial-readiness`.
- [x] Add thin parser and command wrapper.
- [x] Register command under `dpone ops`.

### Task 3: Workflow and docs

- [x] Add discoverability tests for workflow/docs.
- [x] Add `.github/workflows/industrial-readiness.yml`.
- [x] Add operator guide, CI/CD references, and runbook links.

### Task 4: Verification

- [ ] Run focused tests.
- [ ] Run full lint/type/docs/test/build/twine gate.
- [ ] Commit, push, and update/create PR.
