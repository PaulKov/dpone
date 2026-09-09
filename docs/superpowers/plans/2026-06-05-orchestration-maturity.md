# Orchestration Maturity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a production-grade orchestration profile around canonical `dpone run`: retries, concurrency, locks, resumable jobs, scheduler handoff, run evidence, docs, and CI coverage.

**Architecture:** Keep orchestration as a thin application layer around `RunManifestService`. Split responsibilities into focused modules: local locks own acquisition/release, job state owns durable state transitions, handoff owns scheduler snippets, and run service coordinates lock -> state -> execution -> artifacts.

**Tech Stack:** Python 3.11+, dataclasses, local JSON state store, existing CLI command registry, pytest, Ruff, mypy, MkDocs.

---

## File structure

- Modify `src/dpone/orchestration/run.py`: enrich `OrchestratedRunService` with job state transitions, previous-state diagnostics, and resumable metadata.
- Modify `src/dpone/orchestration/handoff.py`: generate scheduler snippets that call `dpone orchestrate run`, not bare `dpone run`, when orchestration controls are required.
- Modify `src/dpone/orchestration/locks.py`: keep local lock semantics focused; add only metadata needed by state reports if required.
- Create `src/dpone/orchestration/state.py`: durable local JSON job state store with started/running/committed/failed/blocked/cancelled transitions.
- Modify `src/dpone/orchestration/__init__.py`: expose state primitives for Python users.
- Modify `src/dpone/commands/orchestrate_cmd.py`: add state-dir/profile options and pass them into services.
- Modify `tests/test_orchestration.py`: TDD coverage for state transitions, stale/blocked locks, scheduler handoff, CLI options, resumable metadata.
- Modify `docs/orchestration.md`: user guide, algorithms, Mermaid diagrams, scheduler snippets, runbooks.
- Modify `docs/architecture.md`: architecture map for orchestration state and DI boundaries.
- Modify `docs/cli-reference.md`: command reference for new options.

## Task 1: Durable orchestration state foundation

- [ ] Write failing tests for `LocalJobStateStore`: started -> running -> committed, failed, blocked, and resumable lookup.
- [ ] Run focused tests and verify RED.
- [ ] Implement `src/dpone/orchestration/state.py` with JSON files per `run_id`, atomic writes, typed records, and stable blocker codes.
- [ ] Wire `OrchestratedRunService` to write started/running/committed/failed/blocked transitions.
- [ ] Run focused tests and verify GREEN.
- [ ] Update docs and CLI reference for `--state-dir`.
- [ ] Commit only files touched by Task 1.

## Task 2: Scheduler handoff profiles

- [ ] Write failing tests that cron/Airflow/Dagster snippets use `dpone orchestrate run` with manifest, selector, lock, state, retry, and output options.
- [ ] Implement scheduler profile rendering without embedding scheduler execution logic.
- [ ] Add docs and runbooks for cron, Airflow, Dagster, Kubernetes CronJob copy-paste handoff.
- [ ] Run focused tests and docs checks.
- [ ] Commit only files touched by Task 2.

## Task 3: Resumable job UX

- [ ] Write failing tests for `dpone orchestrate run --resume-policy fail|resume|restart` behavior against previous job state.
- [ ] Implement resume policy planning and clear diagnostics.
- [ ] Add docs for when to resume vs restart vs fail-closed.
- [ ] Run focused tests and docs checks.
- [ ] Commit only files touched by Task 3.

## Task 4: CI and release evidence

- [ ] Add tests for CLI reference and docs coverage.
- [ ] Add or update CI docs/runbooks describing orchestration gates.
- [ ] Run Ruff, format, mypy, docs, pytest non-live, build, and twine.
- [ ] Commit only files touched by Task 4.

## Current progress

- [x] Task 1
- [x] Task 2
- [x] Task 3
- [x] Task 4
