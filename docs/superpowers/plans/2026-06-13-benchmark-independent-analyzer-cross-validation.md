# Benchmark Independent Analyzer Cross-Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an Independent Analyzer Cross-Validation & Audit Pack to the OSS benchmark so public quality claims are backed by external analyzer command metadata, LOC/SLOC agreement checks, complexity validation confidence, raw evidence, SVG visuals, and PR summary signals.

**Architecture:** Keep analyzer audit logic in `tools/oss_benchmark/independent_validation.py`, markdown rendering in `tools/oss_benchmark/renderers/independent_validation.py`, SVG rendering in `tools/oss_benchmark/renderers/validation_svg.py`, and PR summary rendering in `tools/oss_benchmark/pr_validation.py`. `core.py`, `renderers/markdown.py`, `pr_summary.py`, `evidence_trust.py`, and the CLI wrapper only orchestrate stable interfaces.

**Tech Stack:** Python standard library, optional local command discovery for `tokei`, `cloc`, `radon`, and `lizard`, existing benchmark payload helpers, pytest contract tests, deterministic SVG text rendering, existing benchmark CLI generator.

---

### Task 1: RED Tests

**Files:**
- Modify: `tests/test_oss_code_quality_benchmark.py`
- Modify: `tests/test_cicd_docs_contracts.py`

- [ ] **Step 1: Add independent-validation behavior test**

Add `test_independent_validation_builds_analyzer_audit_pack()` that builds a payload with dpone metrics and explicit analyzer results:
- `tokei` fresh LOC/SLOC result with small delta
- `radon` fresh complexity result
- `lizard` unavailable result

Assert `tool.build_independent_validation(payload)` returns:
- `schema_version == 1`
- `summary["dpone"]["confidence_score"] >= 80`
- `summary["dpone"]["validation_band"] in {"audit-ready", "high"}`
- analyzer commands include command, status, exit code, version, and generated timestamp
- LOC/SLOC validation includes benchmark value, external value, delta percent, and status
- complexity validation includes external average complexity and status
- unavailable tools are retained in analyzer coverage instead of fabricating values

- [ ] **Step 2: Add rendering and PR summary test**

Add `test_independent_validation_markdown_svg_and_pr_summary()` with a minimal `independent_validation` payload and assert:
- markdown contains `## Independent Analyzer Cross-Validation & Audit Pack`
- markdown links `assets/oss-independent-validation.svg` and `assets/oss-analyzer-confidence.svg`
- markdown contains `Analyzer command ledger`, `LOC/SLOC cross-check`, `Complexity cross-check`, and `Validation confidence`
- both SVG renderers return `<svg`
- PR summary contains `Independent Analyzer Cross-Validation`

- [ ] **Step 3: Extend module taxonomy contract**

Add thin module constraints:
- `tools/oss_benchmark/independent_validation.py`
- `tools/oss_benchmark/pr_validation.py`
- `tools/oss_benchmark/renderers/independent_validation.py`
- `tools/oss_benchmark/renderers/validation_svg.py`

- [ ] **Step 4: Extend benchmark document contract**

Require generated Markdown, raw JSON, and assets to include:
- `Independent Analyzer Cross-Validation & Audit Pack`
- `Analyzer command ledger`
- `LOC/SLOC cross-check`
- `Complexity cross-check`
- `Validation confidence`
- `docs/benchmarks/assets/oss-independent-validation.svg`
- `docs/benchmarks/assets/oss-analyzer-confidence.svg`
- raw JSON key `independent_validation`

- [ ] **Step 5: Extend CI/CD docs contract**

Require manual benchmark workflow docs to mention:
- `Independent Analyzer Cross-Validation`
- `oss-independent-validation.svg`
- `oss-analyzer-confidence.svg`
- `Analyzer command ledger`
- `external analyzer`

- [ ] **Step 6: Verify RED**

Run:

```bash
uv run pytest tests/test_oss_code_quality_benchmark.py::test_independent_validation_builds_analyzer_audit_pack tests/test_oss_code_quality_benchmark.py::test_independent_validation_markdown_svg_and_pr_summary -q
```

Expected: FAIL because `build_independent_validation` and SVG exports do not exist yet.

### Task 2: Analyzer Audit Builder

**Files:**
- Create: `tools/oss_benchmark/independent_validation.py`
- Modify: `tools/oss_code_quality_benchmark.py`
- Modify: `tools/oss_benchmark/evidence_trust.py`

- [ ] **Step 1: Implement builder contract**

Create `build_independent_validation(payload: dict[str, Any]) -> dict[str, Any]` returning:
- `schema_version`
- `methodology`
- `summary`
- `analyzer_commands`
- `loc_sloc_cross_checks`
- `complexity_cross_checks`
- `analyzer_coverage`
- `warnings`

- [ ] **Step 2: Accept injected analyzer evidence**

Read optional `payload["external_analyzer_results"]` so tests and CI can pass already-collected analyzer data without requiring tools to be installed. Each result includes tool, status, command, version, exit code, generated_at, project, metric values, and error.

- [ ] **Step 3: Add lightweight local discovery fallback**

If no injected analyzer evidence exists, create deterministic command ledger entries for `tokei`, `cloc`, `radon`, and `lizard` using `shutil.which`. Mark unavailable tools as `unavailable`; do not fabricate metric values. The builder must remain safe when no analyzer is installed.

- [ ] **Step 4: Compute validation confidence**

Compare external LOC/SLOC values against benchmark values when available. Score small deltas as `passed`, larger deltas as `warning`, unavailable as `unavailable`. For complexity, use available external average complexity and internal complexity/semantic evidence to produce a confidence status.

- [ ] **Step 5: Add provenance**

Add `independent_validation` as an inferred/derived audit metric group in evidence provenance.

### Task 3: Renderers and PR Summary

**Files:**
- Create: `tools/oss_benchmark/renderers/independent_validation.py`
- Create: `tools/oss_benchmark/renderers/validation_svg.py`
- Create: `tools/oss_benchmark/pr_validation.py`
- Modify: `tools/oss_benchmark/renderers/markdown.py`
- Modify: `tools/oss_benchmark/pr_summary.py`
- Modify: `tools/oss_code_quality_benchmark.py`

- [ ] **Step 1: Implement markdown section**

Render:
- `## Independent Analyzer Cross-Validation & Audit Pack`
- `### Validation confidence`
- `### Analyzer command ledger`
- `### LOC/SLOC cross-check`
- `### Complexity cross-check`

Include two SVG images and compact audit tables.

- [ ] **Step 2: Implement SVG renderers**

Render deterministic SVGs:
- `render_independent_validation_svg`
- `render_analyzer_confidence_svg`

- [ ] **Step 3: Implement PR summary section**

Render dpone confidence score, validation band, LOC/SLOC status, complexity status, and unavailable analyzer count.

- [ ] **Step 4: Wire renderer calls**

Call the markdown section after Evidence Trust & Auditability and call PR summary after Evidence Trust so the audit material sits near reproducibility evidence.

### Task 4: Generator Wiring and Docs

**Files:**
- Modify: `tools/oss_benchmark/core.py`
- Modify: `docs/ci-cd.md`
- Modify: `docs/cicd/workflows.md`

- [ ] **Step 1: Enrich payload**

Call `build_independent_validation(payload)` before `build_evidence_trust_summary(payload)` so evidence trust/provenance can reference the independent validation block.

- [ ] **Step 2: Write assets**

Write:
- `docs/benchmarks/assets/oss-independent-validation.svg`
- `docs/benchmarks/assets/oss-analyzer-confidence.svg`

Add them to benchmark provenance artifact paths.

- [ ] **Step 3: Update CI/CD docs**

Document that the manual benchmark workflow publishes external analyzer command metadata, analyzer confidence, and validation SVGs.

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
uv run python tools/oss_code_quality_benchmark.py --workspace . --project dpone --previous-data docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json --baseline-data docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json --allow-stale --runner-name codex-local --workflow-name local-independent-validation --max-stale-days 30 --pr-summary test_artifacts/oss-code-quality-benchmark/pr-comment.md --enforce-quality-gates
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

Expected: all commands exit `0`. If a local optional analyzer is unavailable, the validation block must still render with explicit unavailable status.
