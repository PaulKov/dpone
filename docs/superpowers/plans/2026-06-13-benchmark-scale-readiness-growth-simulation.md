# Benchmark Scale Readiness Growth Simulation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Scale Readiness & Growth Simulation to the OSS code-quality benchmark so readers can see whether dpone remains maintainable as it grows toward dlt, Airbyte, Pentaho Kettle and Apache Hop scale.

**Architecture:** Keep the new business logic in `tools/oss_benchmark/scale_readiness.py`, markdown rendering in `tools/oss_benchmark/renderers/scale_readiness.py`, SVG rendering in `tools/oss_benchmark/renderers/scale_svg.py`, and PR summary rendering in `tools/oss_benchmark/pr_scale.py`. `core.py`, `renderers/markdown.py`, `regression_gate.py`, `pr_summary.py`, and the CLI wrapper only orchestrate stable interfaces.

**Tech Stack:** Python standard library, existing benchmark payload helpers, pytest contract tests, deterministic SVG text rendering, existing benchmark CLI generator.

---

### Task 1: RED Tests

**Files:**
- Modify: `tests/test_oss_code_quality_benchmark.py`
- Modify: `tests/test_cicd_docs_contracts.py`

- [ ] **Step 1: Add scale-readiness behavior test**

Add `test_scale_readiness_growth_simulates_headroom_and_runway()` that builds a payload with dpone plus Airbyte/dlt comparator shapes, calls `tool.build_scale_readiness(payload)`, and asserts:
- `schema_version == 1`
- `summary["dpone"]` has `architecture_runway_score`, `growth_ceiling_sloc`, `quality_headroom`, and `overall_status`
- scenarios include `dlt-scale`, `airbyte-scale`, and `hop-scale`
- each scenario includes projected SLOC, projected max module LOC, projected p90 fan-out, projected maintainability, and risk level
- headroom includes connector slots before yellow/red threshold
- warning gate data includes low-runway warnings when thresholds are crossed

- [ ] **Step 2: Add scale-readiness rendering test**

Add `test_scale_readiness_markdown_svg_pr_and_regression_gate_warning()` that injects a minimal `scale_readiness` payload and asserts:
- markdown contains `## Scale Readiness & Growth Simulation`
- markdown links `assets/oss-scale-readiness.svg`, `assets/oss-architecture-runway.svg`, and `assets/oss-quality-headroom.svg`
- markdown contains `Quality headroom`, `Architecture runway`, `Scale scenarios`, and `Comparator-scale projection`
- each new SVG renderer returns `<svg`
- PR summary contains `Scale Readiness & Growth Simulation`
- `build_pr_regression_gate()` emits a warning when dpone runway is below the warning threshold

- [ ] **Step 3: Extend module taxonomy test**

Extend `test_benchmark_tooling_has_thin_collector_and_renderer_taxonomy()` with:
- `tools/oss_benchmark/scale_readiness.py`
- `tools/oss_benchmark/pr_scale.py`
- `tools/oss_benchmark/renderers/scale_readiness.py`
- `tools/oss_benchmark/renderers/scale_svg.py`

- [ ] **Step 4: Extend benchmark document contract**

Require the generated benchmark document and raw evidence to include:
- `Scale Readiness & Growth Simulation`
- `Quality headroom`
- `Architecture runway`
- `Scale scenarios`
- `Comparator-scale projection`
- `docs/benchmarks/assets/oss-scale-readiness.svg`
- `docs/benchmarks/assets/oss-architecture-runway.svg`
- `docs/benchmarks/assets/oss-quality-headroom.svg`
- raw JSON key `scale_readiness`

- [ ] **Step 5: Extend CI/CD docs contract**

Require manual benchmark workflow docs to mention:
- `Scale Readiness & Growth Simulation`
- `oss-scale-readiness.svg`
- `oss-architecture-runway.svg`
- `oss-quality-headroom.svg`
- `quality headroom`
- `architecture runway`

- [ ] **Step 6: Verify RED**

Run:

```bash
uv run pytest tests/test_oss_code_quality_benchmark.py::test_scale_readiness_growth_simulates_headroom_and_runway tests/test_oss_code_quality_benchmark.py::test_scale_readiness_markdown_svg_pr_and_regression_gate_warning -q
```

Expected: FAIL because `build_scale_readiness` and the new renderers are not exported yet.

### Task 2: Scale Readiness Builder

**Files:**
- Create: `tools/oss_benchmark/scale_readiness.py`
- Modify: `tools/oss_code_quality_benchmark.py`
- Modify: `tools/oss_benchmark/evidence_trust.py`

- [ ] **Step 1: Implement builder contract**

Create `build_scale_readiness(payload: dict[str, Any]) -> dict[str, Any]` returning:
- `schema_version`
- `methodology`
- `summary`
- `quality_headroom`
- `architecture_runway`
- `scale_scenarios`
- `warnings`

- [ ] **Step 2: Compute quality headroom**

For dpone, compute headroom to yellow/red budgets for:
- max module LOC
- p90 fan-out
- total production SLOC
- semantic maintainability
- normalized calibration score

Translate headroom into estimated connector slots before yellow/red using conservative per-connector SLOC and fan-out assumptions.

- [ ] **Step 3: Compute comparator-scale scenarios**

Build deterministic `dlt-scale`, `airbyte-scale`, `pentaho-scale`, and `hop-scale` scenarios when comparator data exists. Project dpone SLOC, max module LOC, p90 fan-out, maintainability score, and risk level from current dpone metrics plus growth multiplier.

- [ ] **Step 4: Compute runway warning contract**

Emit warnings when:
- architecture runway score is below `70`
- connector slots before yellow are below `5`
- projected Airbyte-scale maintainability is below `70`

- [ ] **Step 5: Add provenance**

Add `scale_readiness` as a derived metric group in evidence provenance.

### Task 3: Renderers and PR Gate

**Files:**
- Create: `tools/oss_benchmark/renderers/scale_readiness.py`
- Create: `tools/oss_benchmark/renderers/scale_svg.py`
- Create: `tools/oss_benchmark/pr_scale.py`
- Modify: `tools/oss_benchmark/renderers/markdown.py`
- Modify: `tools/oss_benchmark/pr_summary.py`
- Modify: `tools/oss_benchmark/regression_gate.py`
- Modify: `tools/oss_code_quality_benchmark.py`

- [ ] **Step 1: Implement markdown section**

Render:
- `## Scale Readiness & Growth Simulation`
- `### Quality headroom`
- `### Architecture runway`
- `### Scale scenarios`
- `### Comparator-scale projection`

Include three SVG images and compact tables.

- [ ] **Step 2: Implement SVG renderers**

Render deterministic SVGs:
- `render_scale_readiness_svg`
- `render_architecture_runway_svg`
- `render_quality_headroom_svg`

- [ ] **Step 3: Implement PR summary**

Render dpone runway score, connector slots before yellow/red, worst scenario, and first warning.

- [ ] **Step 4: Add PR regression warning**

Extend `build_pr_regression_gate()` so `scale_readiness.warnings[]` adds warning checks, not blockers.

### Task 4: Generator Wiring and Docs

**Files:**
- Modify: `tools/oss_benchmark/core.py`
- Modify: `docs/ci-cd.md`
- Modify: `docs/cicd/workflows.md`

- [ ] **Step 1: Enrich payload**

Call `build_scale_readiness(payload)` after scoring calibration and before quality gates / PR regression gate.

- [ ] **Step 2: Write assets**

Write:
- `docs/benchmarks/assets/oss-scale-readiness.svg`
- `docs/benchmarks/assets/oss-architecture-runway.svg`
- `docs/benchmarks/assets/oss-quality-headroom.svg`

Add them to benchmark provenance artifact paths.

- [ ] **Step 3: Update CI/CD self-service docs**

Document that manual benchmark refresh includes architecture runway, quality headroom, scale scenarios, and generated SVG assets.

### Task 5: Regenerate and Verify

**Files:**
- Generated: `docs/benchmarks/oss-code-quality-benchmark-2026-06-12.md`
- Generated: `docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json`
- Generated: `docs/benchmarks/data/oss-benchmark-provenance.json`
- Generated: `docs/benchmarks/assets/*.svg`
- Generated: `test_artifacts/oss-code-quality-benchmark/pr-comment.md`
- Generated: `docs/layer_metrics_baseline.json`
- Generated: `docs/quality-metrics.md`

- [ ] **Step 1: Regenerate benchmark**

Run:

```bash
uv run python tools/oss_code_quality_benchmark.py --workspace . --project dpone --previous-data docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json --baseline-data docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json --allow-stale --runner-name codex-local --workflow-name local-scale-readiness --max-stale-days 30 --pr-summary test_artifacts/oss-code-quality-benchmark/pr-comment.md --enforce-quality-gates
```

- [ ] **Step 2: Regenerate dev metrics**

Run:

```bash
uv run dpone docs update-dev-metrics
```

- [ ] **Step 3: Full verification**

Run:

```bash
uv run ruff check tools/oss_code_quality_benchmark.py tools/oss_benchmark tests/test_oss_code_quality_benchmark.py tests/test_cicd_docs_contracts.py
uv run pytest tests/test_oss_code_quality_benchmark.py -q
uv run pytest tests/test_docs_language_contracts.py tests/test_cicd_docs_contracts.py -q
uv run dpone docs check-architecture-fitness --format json --top 30
uv run dpone docs update-dev-metrics --check
uv run --with mkdocs --with mkdocs-material --with pymdown-extensions mkdocs build --strict
```

Expected: all commands exit `0`. If unrelated pre-existing failures appear, report the exact command and output.
