# Route Conformance Live Runner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend Route Conformance Lab with a vendor-live runner contract that seeds a source, executes a route, reads source/sink snapshots, and reuses exact conformance verification.

**Architecture:** Keep live execution behind small injected ports: source seeder, route executor, snapshot readers, and adapter registry. The default OSS implementation is an in-memory deterministic adapter for local tests; real Postgres, MSSQL, and ClickHouse Docker adapters can plug in later without changing CLI or policy logic.

**Tech Stack:** Python dataclasses and Protocols, existing `RouteConformanceService`, existing conformance dataset/verifier contracts, pytest, MkDocs docs gates.

---

### Task 1: Live Runner Tests

**Files:**
- Create: `tests/test_route_conformance_live.py`
- Modify: `tests/test_cli_route_conformance_commands.py`
- Modify: `tests/test_route_conformance_docs_contract.py`

- [ ] Add RED tests for `RouteConformanceLiveService` using an in-memory adapter.
- [ ] Add a RED test that live evidence fails when the adapter introduces downstream row drift.
- [ ] Add CLI RED tests for `dpone ops route-conformance live-run`.
- [ ] Add docs-contract RED checks for live artifacts, opt-in Docker profile, and developer port boundaries.

### Task 2: Live Models And Ports

**Files:**
- Create: `src/dpone/ops/routes/conformance_live_models.py`
- Create: `src/dpone/ops/routes/conformance_live_ports.py`
- Create: `src/dpone/ops/routes/conformance_live_adapters.py`

- [ ] Add immutable models for live adapter steps, live run config, live evidence, and live reports.
- [ ] Add Protocol ports for `RouteConformanceSourceSeeder`, `RouteConformanceRouteExecutor`, and `RouteConformanceSnapshotReader`.
- [ ] Add `InMemoryRouteConformanceLiveAdapter` for credential-free tests and local CI.

### Task 3: Live Service And CLI

**Files:**
- Create: `src/dpone/ops/route_conformance_live.py`
- Modify: `src/dpone/ops/catalog_route_conformance.py`
- Modify: `src/dpone/ops/__init__.py`
- Modify: `src/dpone/ops/routes/__init__.py`
- Modify: `src/dpone/commands/ops_parsers_route_conformance.py`
- Modify: `src/dpone/services/ops/command_handlers_route_conformance.py`

- [ ] Implement `RouteConformanceLiveService` that composes route profile lookup, dataset generation, adapter seed/execute/read, exact verifier, policy, and report writing.
- [ ] Register `route-conformance live-run` with `--adapter in_memory`, dataset profile arguments, release minimums, schema-evolution requirement, and `--drift` for negative local tests.
- [ ] Return exit code `0` for verified evidence and `1` for blockers.

### Task 4: Documentation And Verification

**Files:**
- Modify: `docs/route-conformance-lab.md`
- Modify: `docs/developer-route-conformance-lab.md`
- Modify: `docs/developer-ci-cd.md`
- Modify: `docs/architecture.md`
- Regenerate: `docs/cli-reference.md`
- Regenerate: `docs/quality-metrics.md`

- [ ] Document `route_conformance_live.json`, `live_source_snapshot.json`, `live_sink_snapshot.json`, and opt-in Docker/vendor-live workflow.
- [ ] Document that real database adapters are ports and must not be embedded in CLI handlers.
- [ ] Run focused tests, full tests, docs checks, architecture gates, and MkDocs strict.
