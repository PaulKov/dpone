# Route Data Quality Exceptions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build generic route-level Data Quality scorecards and exception management evidence for every supported `source -> sink -> strategy` route.

**Architecture:** Add focused route DQ contracts, evidence normalization, pure policy evaluation, and a small facade service. Wire it through the existing route ops CLI, readiness catalog, release gate, docs, and generated references without route-specific command logic.

**Tech Stack:** Python dataclasses, existing `RouteKey`/`RouteProfileCatalog`, argparse CLI, pytest, ruff, mypy, MkDocs.

---

### Task 1: Failing Service Tests

**Files:**
- Create: `tests/test_route_data_quality.py`

- [ ] **Step 1: Write failing tests**

Cover:

- a ready report with required `data_contract`, `quarantine`, and `reconciliation` evidence;
- missing required evidence blocks;
- route identity mismatch blocks;
- malformed JSON blocks;
- quarantine SLA breach produces `quarantine_sla_breached`;
- waiver evidence produces `waiver_required`;
- first supported routes are matrix-driven.

- [ ] **Step 2: Run red test**

Run:

```bash
uv run pytest tests/test_route_data_quality.py -q
```

Expected: import failure for `dpone.ops.route_data_quality`.

### Task 2: Failing CLI Tests

**Files:**
- Create: `tests/test_cli_route_data_quality_command.py`

- [ ] **Step 1: Write failing tests**

Cover successful JSON output and non-zero blocked output for `dpone ops route-data-quality`.

- [ ] **Step 2: Run red test**

Run:

```bash
uv run pytest tests/test_cli_route_data_quality_command.py -q
```

Expected: argparse rejects unknown `route-data-quality`.

### Task 3: Failing Docs Contract Tests

**Files:**
- Create: `tests/test_route_data_quality_docs_contract.py`

- [ ] **Step 1: Write failing tests**

Require:

- `docs/route-data-quality.md`
- `docs/developer-route-data-quality.md`
- `docs/ops-cli.md` references `dpone ops route-data-quality`
- `docs/operational-control-plane.md` references `route-data-quality`
- `docs/ci-cd.md` references the route DQ gate
- `docs/route-release-gate.md` references `route_data_quality`
- `mkdocs.yml` has navigation entry

- [ ] **Step 2: Run red test**

Run:

```bash
uv run pytest tests/test_route_data_quality_docs_contract.py -q
```

Expected: missing docs and references fail.

### Task 4: Models, Evidence Reader, Policy, and Service

**Files:**
- Create: `src/dpone/ops/routes/data_quality_models.py`
- Create: `src/dpone/ops/routes/data_quality_evidence.py`
- Create: `src/dpone/ops/routes/data_quality_policy.py`
- Create: `src/dpone/ops/route_data_quality.py`
- Modify: `src/dpone/ops/routes/__init__.py`

- [ ] **Step 1: Implement immutable public contracts**

Use small dataclasses for threshold, evidence, dimension, exception summary, decision, and report.

- [ ] **Step 2: Implement artifact normalization**

Reader handles JSON/non-JSON, pass/fail semantics, route extraction, dimension extraction, exception metrics, blockers, warnings, waiver flags, and checksums.

- [ ] **Step 3: Implement pure policy**

Policy evaluates unsupported routes, missing required domains, route mismatches, malformed artifacts, critical blockers, score thresholds, quarantine thresholds, and waiver-required state.

- [ ] **Step 4: Implement facade service**

Service composes `RouteProfileCatalog`, reader, and policy, writes `route_data_quality.json` and `route_data_quality.md`.

- [ ] **Step 5: Run green service tests**

Run:

```bash
uv run pytest tests/test_route_data_quality.py -q
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

Add `route-data-quality` with `--source`, `--sink`, `--strategy`, repeated `--artifact`, repeated `--require`, `--min-score`, `--warning-score`, `--max-exception-ratio`, `--max-quarantine-rows`, `--max-exception-age-hours`, `--output-dir`, and `--format`.

- [ ] **Step 2: Add handler**

Handler only parses artifacts and invokes the service.

- [ ] **Step 3: Add DI catalog access**

Expose `route_data_quality()` in readiness and release readiness catalogs.

- [ ] **Step 4: Run green CLI tests**

Run:

```bash
uv run pytest tests/test_cli_route_data_quality_command.py -q
```

Expected: pass.

### Task 6: Release Gate Reuse

**Files:**
- Modify: `src/dpone/ops/route_release_gate.py`
- Modify: `docs/route-release-gate.md`

- [ ] **Step 1: Ensure `route_data_quality` is accepted as normal evidence**

No route-specific release gate branch is needed. The existing `--require route_data_quality --artifact route_data_quality=<path>` contract should work.

- [ ] **Step 2: Add regression test if needed**

Extend existing release gate tests only if current generic evidence semantics do not cover this evidence domain.

### Task 7: User and Developer Docs

**Files:**
- Create: `docs/route-data-quality.md`
- Create: `docs/developer-route-data-quality.md`
- Modify: `docs/ops-cli.md`
- Modify: `docs/operational-control-plane.md`
- Modify: `docs/ci-cd.md`
- Modify: `docs/route-release-gate.md`
- Modify: `mkdocs.yml`

- [ ] **Step 1: Write self-service user docs**

Include command examples, artifact contract, runbook, thresholds, CI usage, and release gate usage.

- [ ] **Step 2: Write developer docs**

Include class taxonomy, boundaries, extension rules, DI, tests, and anti-god-module rules.

- [ ] **Step 3: Run docs contract test**

Run:

```bash
uv run pytest tests/test_route_data_quality_docs_contract.py -q
```

Expected: pass.

### Task 8: Generated Docs and Quality Metrics

**Files:**
- Modify: `docs/cli-reference.md`
- Modify: `docs/quality-metrics.md`

- [ ] **Step 1: Stage new Python files before metrics**

Run:

```bash
git add src/dpone/ops/routes/data_quality_models.py src/dpone/ops/routes/data_quality_evidence.py src/dpone/ops/routes/data_quality_policy.py src/dpone/ops/route_data_quality.py
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
uv run pytest tests/test_route_data_quality.py tests/test_cli_route_data_quality_command.py tests/test_route_data_quality_docs_contract.py -q
```

- [ ] **Step 2: Regression route tests**

```bash
uv run pytest tests/test_route_run_supervisor.py tests/test_route_release_gate.py tests/test_cli_route_run_supervisor_command.py tests/test_cli_route_release_gate_command.py -q
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
git commit -m "Add route data quality exception evidence"
```

- [ ] **Step 3: Push and open draft PR**

```bash
git push -u origin codex/route-data-quality-exceptions
gh pr create --draft --base codex/route-run-supervisor --head codex/route-data-quality-exceptions --title "Add route data quality exception evidence" --body-file <generated-body>
```
