# Public Evidence Integrity Pack Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the public OSS benchmark safe and verifiable by redacting machine-specific evidence, adding claim-level source coverage, and surfacing an evidence-integrity score in Markdown, raw JSON, PR summaries and CI docs.

**Architecture:** Add a focused `tools/oss_benchmark/public_evidence_integrity.py` module that sanitizes public payload fields and builds an integrity summary/claim ledger. Add small Markdown/SVG/PR renderers, then keep `tools/oss_benchmark/core.py` as the orchestrator that applies sanitization before writing public artifacts.

**Tech Stack:** Python stdlib dictionaries/regex/path handling, existing benchmark renderers, pytest contract tests, GitHub Actions manual workflow documentation.

---

### Task 1: Public Payload Redaction

**Files:**
- Modify: `tests/test_oss_code_quality_benchmark.py`
- Create: `tools/oss_benchmark/public_evidence_integrity.py`
- Modify: `tools/oss_code_quality_benchmark.py`

- [ ] **Step 1: Write failing sanitizer test**

Add a test that passes a payload containing local home-directory, temporary-directory, and benchmark-cache checkout paths. Assert the sanitizer replaces public fields with `$WORKSPACE` or `$BENCHMARK_CACHE` and returns zero redaction violations after sanitization.

- [ ] **Step 2: Run RED**

```bash
uv run pytest tests/test_oss_code_quality_benchmark.py::test_public_evidence_integrity_sanitizes_machine_specific_paths -q
```

Expected: fail because the sanitizer API is not exported.

- [ ] **Step 3: Implement sanitizer**

Implement `sanitize_public_payload(payload) -> dict[str, Any]` and `find_public_redaction_violations(payload) -> list[dict[str, Any]]`. Preserve metric values while replacing only public path-like strings.

- [ ] **Step 4: Run GREEN**

Run the same target. Expected: pass.

### Task 2: Claim Evidence Ledger

**Files:**
- Modify: `tests/test_oss_code_quality_benchmark.py`
- Modify: `tools/oss_benchmark/public_evidence_integrity.py`

- [ ] **Step 1: Write failing claim ledger test**

Add a test for `build_public_evidence_integrity(payload)` that asserts claim coverage includes feature parity, closed-core notes, trust center, quality gates, analyzer execution and dpone position claims with confidence/source metadata.

- [ ] **Step 2: Run RED**

```bash
uv run pytest tests/test_oss_code_quality_benchmark.py::test_public_evidence_integrity_builds_claim_ledger_and_score -q
```

Expected: fail because the integrity builder is missing.

- [ ] **Step 3: Implement ledger**

Build deterministic `claim_evidence` entries with `claim_id`, `section`, `claim`, `confidence`, `source`, `last_checked_at`, and `status`. Compute `claim_coverage_percent`, `redaction_violation_count`, `score`, and `status`.

- [ ] **Step 4: Run GREEN**

Run the claim ledger target. Expected: pass.

### Task 3: Renderers, PR Summary, CI Docs

**Files:**
- Modify: `tests/test_oss_code_quality_benchmark.py`
- Modify: `tests/test_cicd_docs_contracts.py`
- Create: `tools/oss_benchmark/renderers/public_evidence_integrity.py`
- Create: `tools/oss_benchmark/renderers/public_integrity_svg.py`
- Create: `tools/oss_benchmark/pr_public_integrity.py`
- Modify: `tools/oss_benchmark/renderers/markdown.py`
- Modify: `tools/oss_benchmark/pr_summary.py`
- Modify: `.github/workflows/oss-code-quality-benchmark.yml`
- Modify: `docs/ci-cd.md`
- Modify: `docs/cicd/workflows.md`

- [ ] **Step 1: Write failing render/docs tests**

Assert Markdown contains `Public Evidence Integrity`, `claim evidence ledger`, `redaction violations`, and `oss-public-evidence-integrity.svg`. Assert PR summary and CI docs mention public artifact redaction and claim coverage.

- [ ] **Step 2: Run RED**

Run targeted benchmark and CI docs contract tests. Expected: fail on missing renderer/workflow/docs fragments.

- [ ] **Step 3: Implement renderers**

Render a compact integrity table, claim ledger preview, redaction policy, SVG scorecard and PR-summary block.

- [ ] **Step 4: Wire docs and workflow**

Add the new section to the table of contents, write the SVG asset, include it in provenance checksums, and document public artifact redaction in CI docs.

- [ ] **Step 5: Run GREEN**

Run targeted tests. Expected: pass.

### Task 4: Regeneration And Verification

**Files:**
- Generated: `docs/benchmarks/oss-code-quality-benchmark-2026-06-12.md`
- Generated: `docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json`
- Generated: `docs/benchmarks/assets/oss-public-evidence-integrity.svg`
- Generated: `test_artifacts/oss-code-quality-benchmark/pr-comment.md`
- Generated: `docs/quality-metrics.md`

- [ ] **Step 1: Regenerate benchmark**

Run the local dpone refresh with stale preservation and external analyzer timeout.

- [ ] **Step 2: Refresh dev metrics**

```bash
uv run dpone docs update-dev-metrics
```

- [ ] **Step 3: Full verification**

Run ruff, benchmark tests, docs contracts, architecture fitness, metrics check and strict MkDocs build.

- [ ] **Step 4: Report**

Report generated files, redaction status, claim coverage, analyzer status and verification commands.
