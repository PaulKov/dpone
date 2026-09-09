# Benchmark Evidence Trust Auditability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the OSS code-quality benchmark with audit-ready evidence confidence, metric provenance, reproducibility metadata, checksums, and optional third-party LOC/SLOC cross-checks.

**Architecture:** Add a focused `evidence_trust.py` module for confidence/provenance/reproducibility data, a small markdown renderer, a small SVG renderer, and a PR summary block. `core.py` remains an orchestrator: it attaches `payload["evidence_trust"]`, writes `oss-evidence-confidence.svg`, then writes `oss-benchmark-provenance.json` after generated artifacts exist so checksums are real.

**Tech Stack:** Python 3.11 stdlib only, existing benchmark payload schema, optional `tokei`/`cloc` if available in PATH, MkDocs.

---

### Task 1: RED Tests

**Files:**
- Modify: `tests/test_oss_code_quality_benchmark.py`
- Modify: `tests/test_cicd_docs_contracts.py`

- [ ] **Step 1: Add failing behavior tests**

Add tests for:
- `build_evidence_trust_summary(payload)` with per-project confidence scores and metric evidence modes.
- `build_provenance_export(payload, artifact_paths=...)` with artifact SHA-256 checksums, run context, analyzer schema versions, and optional cross-check status.
- `render_evidence_trust_section(payload)` including `## Evidence Trust & Auditability`.
- `render_evidence_confidence_svg(payload)`.
- `render_evidence_trust_pr_section(payload)`.
- Document contracts for `oss-benchmark-provenance.json`, `oss-evidence-confidence.svg`, and “measured vs derived vs inferred”.

- [ ] **Step 2: Verify RED**

Run:

```bash
uv run pytest tests/test_oss_code_quality_benchmark.py::test_evidence_trust_summary_scores_measured_derived_inferred_and_stale tests/test_oss_code_quality_benchmark.py::test_evidence_trust_markdown_svg_pr_and_provenance_export -q
```

Expected: FAIL because evidence trust functions/renderers do not exist yet.

### Task 2: Evidence Trust Module

**Files:**
- Create: `tools/oss_benchmark/evidence_trust.py`
- Modify: `tools/oss_benchmark/config.py`
- Modify: `tools/oss_benchmark/core.py`
- Modify: `tools/oss_code_quality_benchmark.py`

- [ ] **Step 1: Implement confidence scoring**

Create:
- `build_evidence_trust_summary(payload)`
- `build_metric_provenance(payload)`
- `confidence_for_project(project, payload)`

Confidence modes:
- `measured`: raw source-code metrics and local repo evidence.
- `derived`: scores built from measured metrics.
- `inferred`: feature/comparator posture from docs or static signals.
- `stale`: previous evidence retained after refresh failure.
- `closed-core note`: feature posture for Fivetran/Informatica where code is unavailable.

- [ ] **Step 2: Implement reproducibility and checksums**

Create:
- `build_provenance_export(payload, artifact_paths, root=ROOT)`
- `checksum_artifacts(paths, root=ROOT)`
- `run_external_loc_cross_check(project, root=ROOT)` returning unavailable when `tokei`/`cloc` is absent.

- [ ] **Step 3: Wire outputs**

Add `PROVENANCE_PATH = docs/benchmarks/data/oss-benchmark-provenance.json`.
Attach `payload["evidence_trust"]` before main JSON write.
Write the provenance export after generated docs/assets/trust/PR artifacts exist.

### Task 3: Renderers And PR Summary

**Files:**
- Create: `tools/oss_benchmark/renderers/evidence_trust.py`
- Create: `tools/oss_benchmark/renderers/evidence_svg.py`
- Create: `tools/oss_benchmark/pr_evidence.py`
- Modify: `tools/oss_benchmark/renderers/markdown.py`
- Modify: `tools/oss_benchmark/pr_summary.py`

- [ ] **Step 1: Render markdown section**

Add “Evidence Trust & Auditability” with:
- evidence confidence scorecard;
- measured/derived/inferred/stale/closed-core table;
- audit ledger link;
- cross-check status;
- reproducibility manifest summary.

- [ ] **Step 2: Render SVG**

Render `oss-evidence-confidence.svg` with per-project confidence bars and evidence-mode mix.

- [ ] **Step 3: Render PR summary**

Add a compact PR block with confidence score, provenance ledger path, checksum manifest status, and cross-check status.

### Task 4: Regenerate Evidence And Docs

**Files:**
- Generated docs/data/assets under `docs/benchmarks/`
- `docs/ci-cd.md`
- `docs/cicd/workflows.md`

- [ ] **Step 1: Update docs references**

Mention evidence confidence, provenance ledger, reproducibility manifest, checksums, optional cross-checks, `oss-benchmark-provenance.json`, and `oss-evidence-confidence.svg`.

- [ ] **Step 2: Regenerate benchmark**

Run:

```bash
uv run python tools/oss_code_quality_benchmark.py --workspace . --project dpone --previous-data docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json --allow-stale --runner-name codex-local --workflow-name local-evidence-trust --max-stale-days 30 --pr-summary test_artifacts/oss-code-quality-benchmark/pr-comment.md
uv run dpone docs update-dev-metrics
```

### Task 5: Verification

**Files:**
- All benchmark, tooling, docs, tests.

- [ ] **Step 1: Run code/tests/docs contracts**

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

- [ ] **Step 3: Inspect scope**

```bash
git status --short -- .github/workflows/oss-code-quality-benchmark.yml docs/benchmarks docs/ci-cd.md docs/cicd/workflows.md docs/quality-metrics.md test_artifacts/oss-code-quality-benchmark tests/test_cicd_docs_contracts.py tests/test_oss_code_quality_benchmark.py tools/oss_code_quality_benchmark.py tools/oss_benchmark docs/superpowers/plans/2026-06-13-benchmark-evidence-trust-auditability.md
```
