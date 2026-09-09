# Benchmark v3 Runtime Certification Claims Ledger Implementation Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the OSS benchmark into a release-aware trust product with verified claims, runtime certification evidence, BI-ready exports, and quality budgets as code.

**Architecture:** New features attach to the existing merged benchmark payload after collection and freshness merging. Collectors, evaluators, renderers, exporters, and gates stay separated through small modules; renderers consume only final payload data.

**Tech Stack:** Python stdlib, pytest, ruff, existing `tools/oss_benchmark` pipeline, existing dpone CLI/doc commands.

---

### Task 1: Hygiene Cut For Near-God Benchmark Modules

**Files:**
- Modify: `tools/oss_benchmark/evidence_trust.py`
- Create: `tools/oss_benchmark/evidence_trust_sources.py`
- Create: `tools/oss_benchmark/evidence_trust_scoring.py`
- Modify: `tools/oss_benchmark/semantic_maintainability.py`
- Create: `tools/oss_benchmark/semantic_hotspots.py`

- [ ] Move evidence source extraction helpers out of `evidence_trust.py`.
- [ ] Move confidence score calculation out of `evidence_trust.py`.
- [ ] Move semantic hotspot helpers out of `semantic_maintainability.py`.
- [ ] Preserve public function names imported by `outputs.py`.
- [ ] Run `uv run ruff check tools/oss_benchmark/evidence_trust.py tools/oss_benchmark/semantic_maintainability.py`.

### Task 2: Claims Ledger

**Files:**
- Create: `tools/oss_benchmark/claims.py`
- Create: `tools/oss_benchmark/renderers/claims.py`
- Modify: `tools/oss_benchmark/outputs.py`
- Modify: `tools/oss_benchmark/renderers/markdown.py`
- Test: `tests/test_oss_benchmark_v3_trust_product.py`

- [ ] Add claim contracts with `claim_id`, `title`, `project_id`, `evidence_refs`, `status`, `confidence`, `freshness`, and `gate_impact`.
- [ ] Resolve evidence references against payload JSON paths, artifact paths, and public URLs.
- [ ] Classify claims as `verified`, `stale`, or `unverified`.
- [ ] Add `claims_ledger` to the final payload before rendering.
- [ ] Render a Claims Ledger section in Markdown.

### Task 3: Runtime Certification Matrix

**Files:**
- Create: `tools/oss_benchmark/certification.py`
- Create: `tools/oss_benchmark/renderers/certification.py`
- Modify: `tools/oss_benchmark/outputs.py`
- Modify: `tools/oss_benchmark/renderers/markdown.py`
- Test: `tests/test_oss_benchmark_v3_trust_product.py`

- [ ] Define deterministic benchmark certification scenarios for CLI, Python API, nested lineage, and artifact evidence.
- [ ] Evaluate scenarios from existing payload/runtime evidence without mutating production APIs.
- [ ] Persist scenario status, contract checks, artifact paths, freshness, and last error.
- [ ] Render certification matrix by `source -> strategy -> sink`.

### Task 4: Evidence Warehouse Export

**Files:**
- Create: `tools/oss_benchmark/exports.py`
- Modify: `tools/oss_benchmark/outputs.py`
- Test: `tests/test_oss_benchmark_v3_trust_product.py`

- [ ] Export BI-ready CSV files for projects, metric groups, gates, claims, runtime certification, release deltas, and debt.
- [ ] Include schema version, release version, release SHA, generated timestamp, project id, and freshness columns.
- [ ] Ensure exports are generated only from the final merged payload.

### Task 5: Quality Budget As Code

**Files:**
- Create: `docs/benchmarks/quality_budgets.yml`
- Create: `tools/oss_benchmark/budgets.py`
- Create: `tools/oss_benchmark/renderers/budgets.py`
- Modify: `tools/oss_benchmark/outputs.py`
- Modify: `tools/oss_benchmark/renderers/markdown.py`
- Test: `tests/test_oss_benchmark_v3_trust_product.py`

- [ ] Load global and per-layer quality budgets from YAML.
- [ ] Evaluate SLOC, LOC, fan-out, clustering, and stale-age budgets.
- [ ] Classify findings as `passed`, `warning`, or `failed`.
- [ ] Treat new debt as a release blocker and legacy warning debt as visible ledger.
- [ ] Render Quality Budget and Debt Ledger sections.

### Task 6: Contracts, Docs, And Release Verification

**Files:**
- Modify: `tests/test_docs_language_contracts.py`
- Modify: `tests/test_cicd_docs_contracts.py`
- Modify: `docs/benchmarks/oss-code-quality-benchmark-2026-06-12.md`
- Modify: `docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json`

- [ ] Assert Markdown includes Claims Ledger, Runtime Certification Matrix, Evidence Warehouse Export, Quality Budget, and Debt Ledger.
- [ ] Assert raw JSON has `claims_ledger`, `runtime_certification`, `quality_budgets`, and `evidence_exports`.
- [ ] Assert no new claim is published without evidence.
- [ ] Run full verification:

```bash
uv run ruff check tools/oss_code_quality_benchmark.py tools/oss_benchmark tests
uv run pytest tests/test_oss_benchmark_v3_trust_product.py -q
uv run pytest tests/test_oss_code_quality_benchmark.py tests/test_docs_language_contracts.py tests/test_cicd_docs_contracts.py -q
uv run dpone docs check-module-size --baseline docs/module_size_baseline.json --max-sloc 400
uv run dpone docs check-architecture-fitness --target-avg-clustering 0.18 --max-avg-clustering 0.18
uv run dpone docs update-dev-metrics --check
uv run --with mkdocs --with mkdocs-material --with pymdown-extensions mkdocs build --strict
```
