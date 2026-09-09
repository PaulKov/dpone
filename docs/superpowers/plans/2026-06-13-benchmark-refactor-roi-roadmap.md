# Benchmark Refactor ROI Roadmap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the OSS benchmark with a Refactor ROI Roadmap that ranks quality-improvement work by impact, effort, debt reduction, and target architecture recommendation.

**Architecture:** Add one pure analysis module, one markdown renderer, one SVG renderer, and one PR-summary section. `core.py` remains orchestration-only: it attaches `payload["refactor_roi"]`, writes `oss-refactor-roi-roadmap.svg`, and lets renderers consume the merged evidence payload.

**Tech Stack:** Python 3.11, existing benchmark payload schema, stdlib-only deterministic scoring, MkDocs.

---

### Task 1: RED Tests

**Files:**
- Modify: `tests/test_oss_code_quality_benchmark.py`
- Modify: `tests/test_cicd_docs_contracts.py`

- [ ] **Step 1: Add failing ROI tests**

Add tests for:
- `build_refactor_roi_roadmap(payload)` producing `schema_version`, `summary`, `items`, `quadrants`, and debt estimates.
- ROI ordering from synthetic complexity/boundary risk data.
- Target architecture recommendation strings for command, sink, manifest, DAG, and renderer hotspots.
- Markdown section `## Refactor ROI Roadmap`.
- SVG function `render_refactor_roi_svg`.
- PR summary section `### Refactor ROI Roadmap`.
- Docs contract strings for `oss-refactor-roi-roadmap.svg` and quality debt.

- [ ] **Step 2: Verify RED**

Run:

```bash
uv run pytest tests/test_oss_code_quality_benchmark.py::test_refactor_roi_roadmap_ranks_quality_economics tests/test_oss_code_quality_benchmark.py::test_refactor_roi_markdown_svg_and_pr_summary -q
```

Expected: FAIL because ROI functions/renderers are not defined.

### Task 2: ROI Analysis Module

**Files:**
- Create: `tools/oss_benchmark/refactor_roi.py`
- Modify: `tools/oss_benchmark/core.py`
- Modify: `tools/oss_code_quality_benchmark.py`

- [ ] **Step 1: Implement scoring**

Create `build_refactor_roi_roadmap(payload)` that reads:
- `complexity_boundary.summary[].risk_register`
- `complexity_boundary.risk_register`
- `architecture_risk.hotspots`
- `remediation_backlog.items`

Each item gets:
- `impact_score`
- `effort_score` where higher means easier
- `debt_points`
- `roi_score`
- `quadrant`
- `target_architecture`
- `recommended_action`

- [ ] **Step 2: Implement project debt summary**

Compute per-project:
- `debt_points`
- `top_debt_driver`
- `quick_win_count`
- `strategic_refactor_count`
- `status`

- [ ] **Step 3: Wire payload and CLI exports**

Set `payload["refactor_roi"] = build_refactor_roi_roadmap(payload)` after remediation backlog generation. Re-export from `tools/oss_code_quality_benchmark.py`.

### Task 3: Renderers And PR Summary

**Files:**
- Create: `tools/oss_benchmark/renderers/refactor_roi.py`
- Create: `tools/oss_benchmark/renderers/roi_svg.py`
- Create: `tools/oss_benchmark/pr_roi.py`
- Modify: `tools/oss_benchmark/renderers/markdown.py`
- Modify: `tools/oss_benchmark/pr_summary.py`

- [ ] **Step 1: Render benchmark section**

Render:
- executive debt estimate table;
- ROI-ranked roadmap;
- target architecture recommendations;
- sales-friendly explanation that quality is managed as a portfolio.

- [ ] **Step 2: Render SVG**

Render `oss-refactor-roi-roadmap.svg` as a quadrant/scorecard view using the top ROI items.

- [ ] **Step 3: Render PR summary block**

Add a compact top-3 ROI block with status, top quick win, debt points, and target architecture.

### Task 4: Evidence And Docs

**Files:**
- Modify generated benchmark docs/data/assets under `docs/benchmarks/`
- Modify: `docs/ci-cd.md`
- Modify: `docs/cicd/workflows.md`

- [ ] **Step 1: Document CI artifacts**

Mention Refactor ROI Roadmap, quality debt, and `oss-refactor-roi-roadmap.svg` in CI/CD docs.

- [ ] **Step 2: Regenerate evidence**

Run:

```bash
uv run python tools/oss_code_quality_benchmark.py --workspace . --project dpone --previous-data docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json --allow-stale --runner-name codex-local --workflow-name local-refactor-roi --max-stale-days 30 --pr-summary test_artifacts/oss-code-quality-benchmark/pr-comment.md
uv run dpone docs update-dev-metrics
```

### Task 5: Verification

**Files:**
- All modified benchmark, docs, tests, generated assets.

- [ ] **Step 1: Run code and test verification**

```bash
uv run ruff check tools/oss_code_quality_benchmark.py tools/oss_benchmark tests/test_oss_code_quality_benchmark.py tests/test_cicd_docs_contracts.py
uv run pytest tests/test_oss_code_quality_benchmark.py -q
uv run pytest tests/test_docs_language_contracts.py tests/test_cicd_docs_contracts.py -q
```

- [ ] **Step 2: Run docs verification**

```bash
uv run dpone docs check-architecture-fitness --format json --top 30
uv run dpone docs update-dev-metrics --check
uv run --with mkdocs --with mkdocs-material --with pymdown-extensions mkdocs build --strict
```

- [ ] **Step 3: Inspect scope**

```bash
git status --short -- .github/workflows/oss-code-quality-benchmark.yml docs/benchmarks docs/ci-cd.md docs/cicd/workflows.md docs/quality-metrics.md test_artifacts/oss-code-quality-benchmark tests/test_cicd_docs_contracts.py tests/test_oss_code_quality_benchmark.py tools/oss_code_quality_benchmark.py tools/oss_benchmark docs/superpowers/plans/2026-06-13-benchmark-refactor-roi-roadmap.md
```
