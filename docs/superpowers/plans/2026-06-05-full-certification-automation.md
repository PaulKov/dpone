# Full Certification Automation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Automate recurring source -> sink certification with benchmark, lineage, evidence, tamper-evident chain, and release-review artifacts.

**Architecture:** Reuse existing focused services instead of creating a workflow god module. Add a small certification automation plan service for UX/contract visibility, then wire GitHub Actions to existing commands: matrix report, benchmark baseline, run registry, lineage export, evidence bundle, strategy bundle, certification suite, artifact index, and evidence chain.

**Tech Stack:** Python 3.11+, dataclasses, existing `dpone ops` commands, GitHub Actions, pytest, Ruff, mypy, MkDocs.

---

## Task 1: Certification automation plan service and CLI

- [ ] Add tests for `CertificationAutomationPlanService` expected steps and artifacts.
- [ ] Add CLI test for `dpone ops certification-automation-plan`.
- [ ] Implement service in `src/dpone/ops/certification_automation.py`.
- [ ] Implement thin command adapter and registry entry.
- [ ] Run focused tests and static checks.

## Task 2: Scheduled full certification workflow

- [ ] Add CI docs contract test for `.github/workflows/full-certification.yml`.
- [ ] Implement workflow with `workflow_dispatch` and weekly `schedule`.
- [ ] Run mock-contract source/sink matrix and build certification report.
- [ ] Build benchmark, run registry, lineage, evidence bundle, strategy certification bundle, certification suite, artifact index, and evidence chain.
- [ ] Upload full certification artifacts.

## Task 3: Documentation and runbooks

- [ ] Update `docs/cicd/workflows.md`, `docs/cicd/runbooks.md`, `docs/certification-suite.md`, `docs/ops-cli.md`, and `docs/ci-cd.md`.
- [ ] Document local reproduction and failure triage.
- [ ] Add docs contracts for required links and commands.

## Task 4: Full validation and commits

- [ ] Run focused tests.
- [ ] Run Ruff, format, mypy.
- [ ] Run docs checks and MkDocs strict.
- [ ] Run non-live pytest.
- [ ] Build package and run Twine check.
- [ ] Commit focused changes.

## Current progress

- [x] Task 1
- [x] Task 2
- [x] Task 3
- [x] Task 4
