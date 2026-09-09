# Benchmark Scoring Validity Calibration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a reproducible Scoring Validity & Calibration layer to the OSS code-quality benchmark so raw scores, normalized scores, language/repo profiles, sensitivity, anti-gaming guardrails, and score explanation cards are visible in evidence, markdown, SVGs, and PR summaries.

**Architecture:** Keep the benchmark taxonomy thin: scoring math lives in `tools/oss_benchmark/scoring_calibration.py`, markdown rendering lives in `tools/oss_benchmark/renderers/scoring_calibration.py`, SVG rendering lives in `tools/oss_benchmark/renderers/calibration_svg.py`, and PR text lives in `tools/oss_benchmark/pr_calibration.py`. `core.py`, `renderers/markdown.py`, and `pr_summary.py` only orchestrate these interfaces.

**Tech Stack:** Python standard library, existing `tools.oss_benchmark` payload helpers, pytest contract tests, SVG-as-text renderers, existing CLI generator.

---

### Task 1: RED Tests

**Files:**
- Modify: `tests/test_oss_code_quality_benchmark.py`
- Modify: `tests/test_cicd_docs_contracts.py`

- [ ] **Step 1: Add scoring-calibration behavior test**

Add a test that creates a small dpone-style payload and a large mixed comparator payload, calls `tool.build_scoring_calibration(payload)`, and asserts:
- schema version is `1`
- dpone receives a focused Python framework normalization profile
- Airbyte receives a large/very-large monorepo scale profile
- normalized scores are present and bounded
- sensitivity reports threshold swing and rank stability
- guardrails include `micro_module_pressure`, `hollow_interface_pressure`, and `score_gaming_resistance`
- explanation cards include positive drivers, negative drivers, and next best action

- [ ] **Step 2: Add scoring-calibration rendering test**

Add a test that injects a minimal `scoring_calibration` payload and asserts:
- benchmark markdown contains `## Scoring Validity & Calibration`
- markdown links `assets/oss-score-calibration.svg`, `assets/oss-score-sensitivity.svg`, and `assets/oss-normalized-vs-raw.svg`
- markdown contains `Language/repo normalization`, `Sensitivity analysis`, `Anti-gaming guardrails`, and `Score explanation cards`
- each new SVG renderer returns `<svg`
- PR summary contains the calibration section and normalized scores

- [ ] **Step 3: Extend benchmark document contract**

Extend the existing benchmark document test to require:
- `Scoring Validity & Calibration`
- the three new SVG asset paths
- raw JSON evidence key `scoring_calibration`
- generated SVG files exist

- [ ] **Step 4: Extend CI/CD documentation contract**

Extend the workflow/docs contract to require the manual benchmark workflow documentation to mention:
- `Scoring Validity & Calibration`
- `Language/repo normalization`
- `Sensitivity analysis`
- `Anti-gaming guardrails`
- the three new SVG asset names

- [ ] **Step 5: Verify RED**

Run:

```bash
uv run pytest tests/test_oss_code_quality_benchmark.py::test_scoring_calibration_normalizes_profiles_and_guardrails tests/test_oss_code_quality_benchmark.py::test_scoring_calibration_markdown_svg_and_pr_summary -q
```

Expected: FAIL because `build_scoring_calibration` and the new renderers are not implemented/exported yet.

### Task 2: Calibration Builder

**Files:**
- Create: `tools/oss_benchmark/scoring_calibration.py`
- Modify: `tools/oss_benchmark/evidence_trust.py`
- Modify: `tools/oss_code_quality_benchmark.py`

- [ ] **Step 1: Implement profile and score contracts**

Create `build_scoring_calibration(payload: dict[str, Any]) -> dict[str, Any]` returning:
- `schema_version`
- `methodology`
- `summary`
- `sensitivity`
- `guardrails`
- `explanation_cards`

Use existing payload values only. Do not collect new source data in this module.

- [ ] **Step 2: Implement language and repo normalization**

Derive language mix from top module file extensions. Classify profiles as focused Python framework, Python library, JVM platform, TS/JS platform, or mixed monorepo. Classify repo scale as focused framework, product codebase, large monorepo, or very-large monorepo.

- [ ] **Step 3: Implement normalized scoring**

Compute raw score from industrial maintainability, semantic maintainability, and coverage confidence. Apply profile thresholds for module size, p90 fan-out, semantic floor, and anti-gaming penalties. Clamp final score to `0..100`.

- [ ] **Step 4: Implement sensitivity and guardrails**

Report score swing under +/-10% threshold movement, stability band, most sensitive metric, and guardrail statuses for micro-module pressure, hollow-interface pressure, and score-gaming resistance.

- [ ] **Step 5: Export and provenance**

Re-export the builder from `tools/oss_code_quality_benchmark.py` and add `scoring_calibration` to derived evidence provenance.

### Task 3: Markdown, SVG, and PR Renderers

**Files:**
- Create: `tools/oss_benchmark/renderers/scoring_calibration.py`
- Create: `tools/oss_benchmark/renderers/calibration_svg.py`
- Create: `tools/oss_benchmark/pr_calibration.py`
- Modify: `tools/oss_benchmark/renderers/markdown.py`
- Modify: `tools/oss_benchmark/pr_summary.py`
- Modify: `tools/oss_code_quality_benchmark.py`

- [ ] **Step 1: Implement markdown section**

Render a compact public section with four subsections:
- `Language/repo normalization`
- `Sensitivity analysis`
- `Anti-gaming guardrails`
- `Score explanation cards`

Include the three SVG images and tables for all projects.

- [ ] **Step 2: Implement SVG renderers**

Render:
- `render_score_calibration_svg`
- `render_score_sensitivity_svg`
- `render_normalized_vs_raw_svg`

SVGs must be deterministic text, readable in docs, and based only on merged evidence.

- [ ] **Step 3: Implement PR summary section**

Render a short review section showing dpone normalized score, rank stability, strongest driver, and guardrail status.

- [ ] **Step 4: Wire renderers**

Add imports and calls in `renderers/markdown.py`, `pr_summary.py`, and `tools/oss_code_quality_benchmark.py`.

### Task 4: Generator Wiring and Docs

**Files:**
- Modify: `tools/oss_benchmark/core.py`
- Modify: `docs/ci-cd.md`
- Modify: `docs/cicd/workflows.md`

- [ ] **Step 1: Enrich payload**

Call `build_scoring_calibration(payload)` after semantic maintainability is available and before quality gates and explanatory sections.

- [ ] **Step 2: Write assets**

Write:
- `docs/benchmarks/assets/oss-score-calibration.svg`
- `docs/benchmarks/assets/oss-score-sensitivity.svg`
- `docs/benchmarks/assets/oss-normalized-vs-raw.svg`

Add them to benchmark provenance artifact paths.

- [ ] **Step 3: Update public docs**

Update manual workflow documentation to tell users the refresh now includes score calibration, raw-vs-normalized evidence, sensitivity, and anti-gaming guardrails.

### Task 5: Regenerate and Verify

**Files:**
- Generated: `docs/benchmarks/oss-code-quality-benchmark-2026-06-12.md`
- Generated: `docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json`
- Generated: `docs/benchmarks/data/oss-benchmark-provenance.json`
- Generated: `docs/benchmarks/assets/*.svg`
- Generated: `test_artifacts/oss-code-quality-benchmark/pr-comment.md`
- Generated: `docs/layer_metrics_baseline.json`

- [ ] **Step 1: Regenerate benchmark**

Run:

```bash
uv run python tools/oss_code_quality_benchmark.py --workspace . --project dpone --previous-data docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json --baseline-data docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json --allow-stale --runner-name codex-local --workflow-name local-scoring-calibration --max-stale-days 30 --pr-summary test_artifacts/oss-code-quality-benchmark/pr-comment.md --enforce-quality-gates
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

Expected: all commands exit `0`. If unrelated pre-existing failures appear, document exact command output and do not hide it.
