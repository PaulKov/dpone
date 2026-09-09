# Benchmark Complexity Boundary Discipline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the OSS benchmark with Complexity & Boundary Discipline evidence: static complexity hotspots, boundary/DI scoring, a maintainability risk register, visual assets, PR-summary support, docs, and tests.

**Architecture:** Keep the benchmark modular. Add one analysis module for complexity and boundary scoring, one markdown renderer, and one SVG renderer function; keep `core.py` as an orchestrator only. Reuse existing payload utilities, freshness merging, project model serialization, PR summary, and docs contract patterns.

**Tech Stack:** Python 3.11, stdlib static parsing via `ast`/regex, existing `uv` test and docs commands, MkDocs.

---

### Task 1: Complexity And Boundary Tests

**Files:**
- Modify: `tests/test_oss_code_quality_benchmark.py`
- Modify: `tests/test_cicd_docs_contracts.py`

- [ ] **Step 1: Write failing tests**

Add tests for:
- `analyze_complexity_boundary_discipline(projects)` returning `schema_version`, `summary`, `projects`, and `risk_register`.
- Python cyclomatic complexity proxy counting `if`, `for`, boolean operators, exception handlers, and comprehensions.
- Boundary violation detection for renderer-to-collector imports and direct implementation imports.
- Markdown rendering with `## Complexity & Boundary Discipline`.
- SVG rendering with `oss-complexity-boundary.svg`.
- PR summary including complexity/boundary gate status.
- Tooling taxonomy guardrail for new files.

- [ ] **Step 2: Verify RED**

Run:

```bash
uv run pytest tests/test_oss_code_quality_benchmark.py::test_complexity_boundary_analysis_scores_complexity_and_contracts tests/test_oss_code_quality_benchmark.py::test_complexity_boundary_markdown_and_svg_render tests/test_oss_code_quality_benchmark.py::test_pr_summary_renders_quality_gates_deltas_and_hotspots -q
```

Expected: FAIL because the new module, renderer, SVG, and PR summary fields do not exist yet.

### Task 2: Static Analysis Module

**Files:**
- Create: `tools/oss_benchmark/complexity_boundary.py`
- Modify: `tools/oss_benchmark/core.py`
- Modify: `tools/oss_code_quality_benchmark.py`

- [ ] **Step 1: Implement focused analyzers**

Create pure functions:
- `measure_python_complexity(text: str) -> dict[str, Any]`
- `measure_generic_complexity(text: str) -> dict[str, Any]`
- `analyze_file_complexity(path, root) -> dict[str, Any]`
- `analyze_complexity_boundary_discipline(projects) -> dict[str, Any]`
- `project_complexity_boundary(project) -> dict[str, Any]`

The module must stay self-contained and read source files only through existing project paths. It must not mutate project payloads.

- [ ] **Step 2: Score contracts and DI discipline**

For each project, compute:
- `complexity_score` 0-100 from average, P90, max function complexity, and god-risk counts.
- `boundary_score` 0-100 from known benchmark boundary violations and direct implementation import pressure.
- `di_score` 0-100 from interface/protocol density and direct concrete implementation imports.
- `overall_score` as weighted complexity/boundary/DI score.
- `top_complex_units` and `boundary_violations`.
- `risk_register` items with priority, project, module, reason, and recommended action.

- [ ] **Step 3: Wire payload and exports**

In `core.write_benchmark_outputs`, set `payload["complexity_boundary"]` before quality gates and render `assets/oss-complexity-boundary.svg`. Re-export the public functions from `tools/oss_code_quality_benchmark.py` for tests and self-service imports.

### Task 3: Renderers And PR Summary

**Files:**
- Create: `tools/oss_benchmark/renderers/complexity_boundary.py`
- Modify: `tools/oss_benchmark/renderers/markdown.py`
- Modify: `tools/oss_benchmark/renderers/svg.py`
- Modify: `tools/oss_benchmark/pr_summary.py`

- [ ] **Step 1: Render markdown section**

Add a TOC entry and render:
- executive score table;
- top complex units;
- boundary violations;
- maintainability risk register;
- customer-facing explanation of why lower complexity and thinner boundaries reduce enterprise change risk.

- [ ] **Step 2: Render SVG**

Add `render_complexity_boundary_svg(payload)` with per-project bars for overall complexity/boundary score and boundary violation count.

- [ ] **Step 3: Extend PR summary**

Add a compact `Complexity & Boundary Discipline` block with status, score, top risk, and blocker/warning count.

### Task 4: Generated Evidence And Docs Contracts

**Files:**
- Modify generated benchmark docs and JSON under `docs/benchmarks/`
- Modify: `docs/ci-cd.md`
- Modify: `docs/cicd/workflows.md`
- Modify: `tests/test_cicd_docs_contracts.py`

- [ ] **Step 1: Update docs references**

Document that the manual benchmark workflow publishes complexity/boundary evidence, `oss-complexity-boundary.svg`, raw JSON fields, and PR summary risk register.

- [ ] **Step 2: Regenerate benchmark**

Run:

```bash
uv run python tools/oss_code_quality_benchmark.py --workspace . --project dpone --previous-data docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json --allow-stale --runner-name codex-local --workflow-name local-complexity-boundary --max-stale-days 30 --pr-summary test_artifacts/oss-code-quality-benchmark/pr-comment.md
uv run dpone docs update-dev-metrics
```

### Task 5: Verification

**Files:**
- All modified benchmark, docs, and tests.

- [ ] **Step 1: Run targeted checks**

```bash
uv run ruff check tools/oss_code_quality_benchmark.py tools/oss_benchmark tests/test_oss_code_quality_benchmark.py tests/test_cicd_docs_contracts.py
uv run pytest tests/test_oss_code_quality_benchmark.py -q
uv run pytest tests/test_docs_language_contracts.py tests/test_cicd_docs_contracts.py -q
```

- [ ] **Step 2: Run docs and architecture checks**

```bash
uv run dpone docs check-architecture-fitness --format json --top 30
uv run dpone docs update-dev-metrics --check
uv run --with mkdocs --with mkdocs-material --with pymdown-extensions mkdocs build --strict
```

- [ ] **Step 3: Inspect scope**

Run:

```bash
git status --short -- .github/workflows/oss-code-quality-benchmark.yml docs/benchmarks docs/ci-cd.md docs/cicd/workflows.md docs/quality-metrics.md test_artifacts/oss-code-quality-benchmark tests/test_cicd_docs_contracts.py tests/test_oss_code_quality_benchmark.py tools/oss_code_quality_benchmark.py tools/oss_benchmark docs/superpowers/plans/2026-06-13-benchmark-complexity-boundary-discipline.md
```

Expected: only benchmark/docs/test/tooling files are in this goal scope; unrelated dirty files remain untouched.
