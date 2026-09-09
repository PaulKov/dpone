# Route State Promotion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a generic route state promotion service that advances source state only after a verified sink commit receipt, route execution ledger, and optional fencing token match.

**Architecture:** Keep promotion in the ops control plane. Runtime connectors keep extracting/loading; route promotion reads immutable evidence, evaluates a pure policy, and writes state through a small store protocol. Provide local JSON and SQLite state stores so future durable backends can be added without changing command logic.

**Tech Stack:** Python stdlib JSON/SQLite, existing `RouteKey` and route execution ledger report contract, pytest, MkDocs, generated CLI docs, quality metrics.

---

### Task 1: Models And Policy

**Files:**
- Create: `src/dpone/ops/routes/state_promotion_models.py`
- Create: `src/dpone/ops/routes/state_promotion_policy.py`
- Test: `tests/test_route_state_promotion.py`

- [ ] Write failing tests for success, missing durable sink evidence, boundary mismatch, fencing mismatch, and idempotent replay.
- [ ] Add `RouteCommitReceipt`, `RouteStateRecord`, `RouteStatePromotionDecision`, and `RouteStatePromotionReport`.
- [ ] Add `RouteStatePromotionPolicy` with fail-closed blocker codes and next actions.

### Task 2: State Stores

**Files:**
- Create: `src/dpone/ops/routes/state_store.py`
- Create: `src/dpone/ops/routes/state_store_sqlite.py`
- Create: `src/dpone/ops/routes/state_store_factory.py`
- Test: `tests/test_route_state_promotion_sqlite.py`

- [ ] Write failing tests for local JSON compare-and-swap and SQLite shared persistence.
- [ ] Add `RouteStateStore` protocol and `LocalRouteStateStore`.
- [ ] Add `SqliteRouteStateStore` with `BEGIN IMMEDIATE` compare-and-swap.
- [ ] Add `RouteStateStoreFactory` for `local_json` and `sqlite`.

### Task 3: Service And CLI

**Files:**
- Create: `src/dpone/ops/route_state_promotion.py`
- Modify: `src/dpone/ops/catalog_readiness.py`
- Modify: `src/dpone/ops/catalog_release.py`
- Modify: `src/dpone/commands/ops_parsers_core.py`
- Modify: `src/dpone/services/ops/command_handlers_release.py`
- Modify: `src/dpone/commands/ops_cmd.py`
- Modify: `src/dpone/commands/registry_ops.py`
- Test: `tests/test_cli_route_state_promotion_command.py`

- [ ] Add `RouteStatePromotionService.promote`.
- [ ] Add `dpone ops route-state-promote` parser and handler.
- [ ] Keep backend selection in the ops catalog/factory, not in the handler.
- [ ] Regenerate CLI reference.

### Task 4: Route Readiness, Docs, And Quality

**Files:**
- Modify: `src/dpone/ops/routes/catalog.py`
- Create: `docs/route-state-promotion.md`
- Create: `docs/developer-route-state-promotion.md`
- Modify: `docs/route-readiness.md`
- Modify: `docs/source-sink-matrix.md`
- Modify: `docs/ops-cli.md`
- Modify: `docs/architecture.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/developer-ci-cd.md`
- Modify: `mkdocs.yml`
- Test: `tests/test_route_state_promotion_docs_contract.py`

- [ ] Add `state_promotion` as a route-readiness evidence domain.
- [ ] Add user docs, developer docs, runbook, examples, and architecture notes.
- [ ] Add docs contracts for user docs, developer docs, nav, architecture, CI/CD, source-sink matrix, and route readiness.
- [ ] Regenerate quality metrics and CLI reference.

### Task 5: Verification

**Commands:**
- `uv run pytest tests/test_route_state_promotion.py tests/test_route_state_promotion_sqlite.py tests/test_cli_route_state_promotion_command.py tests/test_route_state_promotion_docs_contract.py -q`
- `uv run pytest tests/test_route_execution_ledger.py tests/test_route_execution_ledger_sqlite.py tests/test_route_readiness.py tests/test_route_certification_pack.py -q`
- `uv run ruff check src/dpone/ops/routes/state_promotion_models.py src/dpone/ops/routes/state_promotion_policy.py src/dpone/ops/routes/state_store.py src/dpone/ops/routes/state_store_sqlite.py src/dpone/ops/routes/state_store_factory.py src/dpone/ops/route_state_promotion.py`
- `uv run mypy --config-file mypy.ini src/dpone/ops/routes/state_promotion_models.py src/dpone/ops/routes/state_promotion_policy.py src/dpone/ops/routes/state_store.py src/dpone/ops/routes/state_store_sqlite.py src/dpone/ops/routes/state_store_factory.py src/dpone/ops/route_state_promotion.py`
- `uv run dpone docs update-cli-reference --check`
- `uv run dpone docs update-dev-metrics --check`
- `uv run dpone docs check-docs`
- `uv run mkdocs build --strict`
- `uv run pytest -m "not integration_live" -q`

---

## Self-Review

- Spec coverage: models, policy, local/SQLite stores, service, CLI, route readiness, docs, architecture, CI/CD, and verification are covered.
- Placeholder scan: no placeholder tasks remain.
- Scope: limited to state promotion evidence and state-store mutation; no connector or runtime extraction/apply behavior changes.
