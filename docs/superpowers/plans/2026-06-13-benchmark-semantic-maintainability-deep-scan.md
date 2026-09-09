# Benchmark Semantic Maintainability Deep Scan Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the OSS code-quality benchmark with a semantic maintainability deep scan for god modules/classes/functions, SOLID/DI/Clean Code/DRY/KISS proxies, boundary discipline, and sales-ready visuals.

**Architecture:** Add a focused analyzer module that derives semantic evidence from existing project payloads and source files, a markdown renderer, an SVG renderer, and a compact PR-summary section. `core.py` remains an orchestrator: it attaches `payload["semantic_maintainability"]`, writes SVG assets, and lets the existing evidence-trust/provenance flow checksum the new files.

**Tech Stack:** Python 3.11+ stdlib only, existing benchmark payload schema, AST parsing for Python, lightweight regex proxies for JVM/JS-style source files, MkDocs.

---

### Task 1: RED Tests

**Files:**
- Modify: `tests/test_oss_code_quality_benchmark.py`
- Modify: `tests/test_cicd_docs_contracts.py`

- [ ] **Step 1: Add analyzer and renderer tests**

Add tests for:
- `analyze_semantic_maintainability(projects)` scoring god modules/classes/functions, DI, interface density, DRY/KISS and boundary violations.
- `analyze_semantic_file(path, root)` using Python AST for class/function LOC, branching and constructor DI signals.
- `render_semantic_maintainability_section(payload)` including `## Semantic Maintainability Deep Scan`.
- `render_semantic_maintainability_svg(payload)` and `render_god_object_radar_svg(payload)`.
- `render_semantic_maintainability_pr_section(payload)`.

- [ ] **Step 2: Add generated-doc contract tests**

Require the benchmark document and evidence to contain:
- `semantic_maintainability`;
- `Semantic Maintainability Deep Scan`;
- `God object radar`;
- `SOLID/DI/Clean Code evidence`;
- `DRY/KISS responsibility signals`;
- `docs/benchmarks/assets/oss-semantic-maintainability.svg`;
- `docs/benchmarks/assets/oss-god-object-radar.svg`.

- [ ] **Step 3: Verify RED**

Run:

```bash
uv run pytest tests/test_oss_code_quality_benchmark.py::test_semantic_maintainability_deep_scan_scores_god_objects_and_contracts tests/test_oss_code_quality_benchmark.py::test_semantic_maintainability_markdown_svg_and_pr_summary -q
```

Expected: FAIL because semantic maintainability functions/renderers do not exist yet.

### Task 2: Analyzer

**Files:**
- Create: `tools/oss_benchmark/semantic_maintainability.py`
- Modify: `tools/oss_code_quality_benchmark.py`

- [ ] **Step 1: Implement file-level analysis**

Create:
- `analyze_semantic_file(path, root)`;
- `measure_python_semantics(text)`;
- `measure_generic_semantics(text)`.

Measured signals:
- class LOC;
- function LOC;
- branch count;
- constructor/Protocol/ABC/Interface hints;
- direct implementation imports;
- responsibility tags from module path/name.

- [ ] **Step 2: Implement project-level scoring**

Create:
- `analyze_semantic_maintainability(projects)`;
- `project_semantic_maintainability(project)`.

Project output includes:
- `overall_score`;
- `god_object_score`;
- `solid_di_score`;
- `dry_kiss_score`;
- `boundary_score`;
- `god_module_count`;
- `god_class_count`;
- `god_function_count`;
- `interface_density`;
- `direct_implementation_imports`;
- `responsibility_spread`;
- `top_god_objects`;
- `solid_di_findings`;
- `dry_kiss_findings`;
- `boundary_findings`;
- `risk_register`.

### Task 3: Renderers And PR Summary

**Files:**
- Create: `tools/oss_benchmark/renderers/semantic_maintainability.py`
- Create: `tools/oss_benchmark/renderers/semantic_svg.py`
- Create: `tools/oss_benchmark/pr_semantic.py`
- Modify: `tools/oss_benchmark/renderers/markdown.py`
- Modify: `tools/oss_benchmark/pr_summary.py`
- Modify: `tools/oss_benchmark/core.py`

- [ ] **Step 1: Render markdown section**

Add “Semantic Maintainability Deep Scan” with:
- score table;
- God object radar;
- SOLID/DI/Clean Code evidence;
- DRY/KISS responsibility signals;
- boundary/cycle discipline summary;
- risk register.

- [ ] **Step 2: Render SVG assets**

Write:
- `docs/benchmarks/assets/oss-semantic-maintainability.svg`;
- `docs/benchmarks/assets/oss-god-object-radar.svg`.

- [ ] **Step 3: Render PR summary**

Add a compact PR block with dpone semantic score, god object counts, SOLID/DI score, DRY/KISS score, and top risk.

### Task 4: Regenerate Evidence And Docs

**Files:**
- Generated docs/data/assets under `docs/benchmarks/`
- `docs/ci-cd.md`
- `docs/cicd/workflows.md`

- [ ] **Step 1: Update self-service docs**

Mention semantic maintainability, god-object radar, SOLID/DI/Clean Code evidence, DRY/KISS signals, and the two new SVG assets in CI/CD documentation.

- [ ] **Step 2: Regenerate benchmark**

Run:

```bash
uv run python tools/oss_code_quality_benchmark.py --workspace . --project dpone --previous-data docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json --baseline-data docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json --allow-stale --runner-name codex-local --workflow-name local-semantic-maintainability --max-stale-days 30 --pr-summary test_artifacts/oss-code-quality-benchmark/pr-comment.md --enforce-quality-gates
uv run dpone docs update-dev-metrics
```

### Task 5: Verification

**Files:**
- All benchmark, tooling, docs, tests.

- [ ] **Step 1: Run focused checks**

```bash
uv run ruff check tools/oss_code_quality_benchmark.py tools/oss_benchmark tests/test_oss_code_quality_benchmark.py tests/test_cicd_docs_contracts.py
uv run pytest tests/test_oss_code_quality_benchmark.py -q
uv run pytest tests/test_docs_language_contracts.py tests/test_cicd_docs_contracts.py -q
```

- [ ] **Step 2: Run docs/architecture verification**

```bash
uv run dpone docs check-architecture-fitness --format json --top 30
uv run dpone docs update-dev-metrics --check
uv run --with mkdocs --with mkdocs-material --with pymdown-extensions mkdocs build --strict
```

- [ ] **Step 3: Inspect scoped status**

```bash
git status --short -- .github/workflows/oss-code-quality-benchmark.yml docs/benchmarks docs/ci-cd.md docs/cicd/workflows.md docs/quality-metrics.md test_artifacts/oss-code-quality-benchmark tests/test_cicd_docs_contracts.py tests/test_oss_code_quality_benchmark.py tools/oss_code_quality_benchmark.py tools/oss_benchmark docs/superpowers/plans/2026-06-13-benchmark-semantic-maintainability-deep-scan.md
```
