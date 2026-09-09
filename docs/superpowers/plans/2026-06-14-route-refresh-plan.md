# Route Refresh Plan Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a generic route-level refresh/backfill/resync planning capability for every supported `source -> sink -> strategy` route.

**Architecture:** Add focused route refresh plan contracts, a pure policy, a small facade service, and a thin CLI command. The service remains control-plane only: it writes planning evidence and never executes queries, mutates sinks, replays CDC, repairs rows, or promotes source state.

**Tech Stack:** Python dataclasses, existing `RouteKey`/`RouteProfileCatalog`, argparse CLI, pytest, ruff, mypy, MkDocs.

---

### Task 1: Failing Service Tests

**Files:**
- Create: `tests/test_route_refresh_plan.py`

- [ ] **Step 1: Write failing tests**

Cover ready integer chunk planning, approval-required state rewind, destructive refresh approval, invalid windows, chunk cap blockers, missing required evidence, route mismatch, malformed JSON, and first supported routes.

- [ ] **Step 2: Run red test**

Run:

```bash
uv run pytest tests/test_route_refresh_plan.py -q
```

Expected: import failure for `dpone.ops.route_refresh_plan`.

### Task 2: Failing CLI Tests

**Files:**
- Create: `tests/test_cli_route_refresh_plan_command.py`

- [ ] **Step 1: Write failing tests**

Cover successful JSON output and non-zero blocked output for `dpone ops route-refresh-plan`.

- [ ] **Step 2: Run red test**

Run:

```bash
uv run pytest tests/test_cli_route_refresh_plan_command.py -q
```

Expected: argparse rejects unknown `route-refresh-plan`.

### Task 3: Failing Docs Contract Tests

**Files:**
- Create: `tests/test_route_refresh_plan_docs_contract.py`

- [ ] **Step 1: Write failing tests**

Require user docs, developer docs, ops CLI docs, CI/CD docs, operational control plane, route release gate, and MkDocs navigation.

- [ ] **Step 2: Run red test**

Run:

```bash
uv run pytest tests/test_route_refresh_plan_docs_contract.py -q
```

Expected: missing docs and references fail.

### Task 4: Models, Policy, and Service

**Files:**
- Create: `src/dpone/ops/routes/refresh_plan_models.py`
- Create: `src/dpone/ops/routes/refresh_plan_policy.py`
- Create: `src/dpone/ops/route_refresh_plan.py`
- Modify: `src/dpone/ops/routes/__init__.py`
- Modify: `src/dpone/ops/__init__.py`

- [ ] **Step 1: Implement immutable public contracts**

Define window, chunk, state rewind, approval, evidence, decision, and report dataclasses.

- [ ] **Step 2: Implement generic service**

Resolve route catalog, normalize evidence, build chunks, create approval/state guidance, evaluate policy, and write JSON/Markdown.

- [ ] **Step 3: Implement pure policy**

Evaluate unsupported route, invalid window, chunk count cap, missing/failed evidence, route mismatch, malformed artifacts, destructive refresh, and state rewind approval.

- [ ] **Step 4: Run green service tests**

Run:

```bash
uv run pytest tests/test_route_refresh_plan.py -q
```

Expected: pass.

### Task 5: CLI and Catalog Wiring

**Files:**
- Modify: `src/dpone/commands/ops_parsers_routes.py`
- Modify: `src/dpone/services/ops/command_handlers_routes.py`
- Modify: `src/dpone/services/ops/__init__.py`
- Modify: `src/dpone/commands/ops_cmd.py`
- Modify: `src/dpone/commands/registry_ops.py`
- Modify: `src/dpone/ops/catalog_readiness.py`
- Modify: `src/dpone/ops/catalog_release_sections.py`

- [ ] **Step 1: Add parser**

Add `route-refresh-plan` with route identity, dataset, reason, window, chunk, state, evidence, approval, and output arguments.

- [ ] **Step 2: Add handler**

Handler only parses artifacts and invokes the service.

- [ ] **Step 3: Add DI catalog access**

Expose `route_refresh_plan()` in readiness and release readiness catalogs.

- [ ] **Step 4: Run green CLI tests**

Run:

```bash
uv run pytest tests/test_cli_route_refresh_plan_command.py -q
```

Expected: pass.

### Task 6: Release Gate Regression

**Files:**
- Modify: `tests/test_route_release_gate.py`

- [ ] **Step 1: Add generic evidence test**

Prove `route_refresh_plan` works through `--require route_refresh_plan` and route-matched JSON evidence without special release-gate command logic.

### Task 7: User and Developer Docs

**Files:**
- Create: `docs/route-refresh-plan.md`
- Create: `docs/developer-route-refresh-plan.md`
- Modify: `docs/ops-cli.md`
- Modify: `docs/operational-control-plane.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/route-release-gate.md`
- Modify: `mkdocs.yml`

- [ ] **Step 1: Write self-service user docs**

Include command examples, reasons, window kinds, approval behavior, artifact contract, release-gate usage, Python API, and runbook.

- [ ] **Step 2: Write developer docs**

Include class taxonomy, boundaries, extension rules, DI, tests, and anti-god-module rules.

- [ ] **Step 3: Run docs contract test**

Run:

```bash
uv run pytest tests/test_route_refresh_plan_docs_contract.py -q
```

Expected: pass.

### Task 8: Generated Docs and Quality Metrics

**Files:**
- Modify: `docs/cli-reference.md`
- Modify: `docs/quality-metrics.md`

- [ ] **Step 1: Stage new Python files before metrics**

Run:

```bash
git add src/dpone/ops/route_refresh_plan.py src/dpone/ops/routes/refresh_plan_models.py src/dpone/ops/routes/refresh_plan_policy.py
```

- [ ] **Step 2: Refresh generated docs**

Run:

```bash
uv run dpone docs update-cli-reference
uv run dpone docs update-dev-metrics
```

Expected: generated docs updated.

### Task 9: Verification

- [ ] **Step 1: Targeted tests**

```bash
uv run pytest tests/test_route_refresh_plan.py tests/test_cli_route_refresh_plan_command.py tests/test_route_refresh_plan_docs_contract.py -q
```

- [ ] **Step 2: Route regression tests**

```bash
uv run pytest tests/test_route_release_gate.py tests/test_route_data_quality.py tests/test_cli_route_data_quality_command.py -q
```

- [ ] **Step 3: Quality checks**

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run dpone docs update-cli-reference --check
uv run dpone docs update-dev-metrics --check
uv run dpone docs check-docs
uv run mkdocs build --strict
```

- [ ] **Step 4: Broad tests**

```bash
uv run pytest -m "not integration_live" -q
```

### Task 10: Commit and PR

- [ ] **Step 1: Review git diff**

```bash
git status --short
git diff --check
git diff --stat
```

- [ ] **Step 2: Commit**

```bash
git add <intended files>
git commit -m "Add route refresh planning evidence"
```

- [ ] **Step 3: Push and open draft PR**

```bash
git push -u origin codex/route-refresh-plan
gh pr create --draft --base codex/route-data-quality-exceptions --head codex/route-refresh-plan --title "Add route refresh planning evidence" --body-file <generated-body>
```
