# Benchmark v1 Freeze Release Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Freeze the OSS benchmark as a v1 evidence product and add a release-readiness pack that tells reviewers whether it is ready to ship.

**Architecture:** Add one focused domain module that derives release readiness from existing benchmark evidence. Keep rendering separate: Markdown section, SVG seal, PR summary block, and standalone release-readiness artifacts are formatting-only consumers of the merged payload.

**Tech Stack:** Python stdlib, existing `tools/oss_benchmark` package, pytest, MkDocs, GitHub Actions.

---

### Task 1: Release Readiness Domain

**Files:**
- Create: `tools/oss_benchmark/release_readiness.py`
- Test: `tests/test_oss_code_quality_benchmark.py`

- [ ] **Step 1: Write the failing release readiness test**

```python
def test_benchmark_release_readiness_seals_verified_v1_evidence() -> None:
    payload = {
        "generated_at": "2026-06-14T00:00:00+00:00",
        "run_context": {"updated_by": "codex-local", "git_sha": "abc123", "branch": "benchmark"},
        "quality_gates": {"status": "passed"},
        "public_evidence_integrity": {"status": "verified", "score": 100},
        "source_verification": {"status": "verified", "summary": {"source_health_score": 100}},
        "evidence_trust": {"overall_confidence_score": 96, "overall_band": "audit-ready"},
        "trust_center": {"status": "verified"},
    }

    readiness = tool.build_benchmark_release_readiness(payload)

    assert readiness["status"] == "release-ready"
    assert readiness["evidence_seal"]["label"] == "Benchmark v1 verified"
    assert "loc_sloc" in readiness["freeze_policy"]["stable_metric_groups"]
    assert "scale_readiness" in readiness["freeze_policy"]["experimental_metric_groups"]
    assert all(check["status"] == "passed" for check in readiness["release_checks"])
```

- [ ] **Step 2: Implement minimal domain logic**

Implement `build_benchmark_release_readiness(payload)` with:
- deterministic `schema_version`
- `release_stage="benchmark-v1"`
- `status` as `release-ready`, `watch`, or `blocked`
- `evidence_seal`
- stable and experimental metric group taxonomy
- release checks for quality gates, public evidence integrity, source verification, evidence trust, trust center, and generated artifacts

- [ ] **Step 3: Run focused test**

Run: `uv run pytest tests/test_oss_code_quality_benchmark.py -q -k release_readiness`

Expected: release readiness test passes.

### Task 2: Renderers and PR Summary

**Files:**
- Create: `tools/oss_benchmark/renderers/release_readiness.py`
- Create: `tools/oss_benchmark/renderers/release_readiness_svg.py`
- Create: `tools/oss_benchmark/pr_release_readiness.py`
- Modify: `tools/oss_benchmark/renderers/markdown.py`
- Modify: `tools/oss_benchmark/pr_summary.py`
- Test: `tests/test_oss_code_quality_benchmark.py`

- [ ] **Step 1: Write failing renderer test**

```python
def test_release_readiness_markdown_svg_and_pr_summary() -> None:
    payload = {
        "benchmark_release_readiness": {
            "status": "release-ready",
            "evidence_seal": {"label": "Benchmark v1 verified", "score": 100},
            "freeze_policy": {
                "stable_metric_groups": ["loc_sloc"],
                "experimental_metric_groups": ["scale_readiness"],
            },
            "release_checks": [{"label": "Quality gates", "status": "passed", "evidence": "passed"}],
        }
    }

    markdown = tool.render_release_readiness_section(payload)
    svg = tool.render_release_readiness_svg(payload)
    pr = tool.render_pr_summary(payload)

    assert "Benchmark v1 Freeze & Release Readiness" in markdown
    assert "Benchmark v1 verified" in markdown
    assert "<svg" in svg
    assert "Benchmark v1 release readiness" in svg
    assert "Benchmark v1 Release Readiness" in pr
```

- [ ] **Step 2: Implement renderers**

Render the seal, stable/experimental metric policy, release checks, and recommended release action.

- [ ] **Step 3: Wire renderers**

Add the section to the benchmark document, table of contents, visual asset list, and generated PR summary.

### Task 3: Orchestration, Standalone Artifacts, Workflow and Docs

**Files:**
- Modify: `tools/oss_benchmark/config.py`
- Modify: `tools/oss_benchmark/core.py`
- Modify: `tools/oss_code_quality_benchmark.py`
- Modify: `tools/oss_benchmark/evidence_trust.py`
- Modify: `.github/workflows/oss-code-quality-benchmark.yml`
- Modify: `docs/ci-cd.md`
- Modify: `docs/cicd/workflows.md`
- Test: `tests/test_oss_code_quality_benchmark.py`
- Test: `tests/test_cicd_docs_contracts.py`

- [ ] **Step 1: Write failing contract tests**

Add assertions that:
- raw JSON contains `benchmark_release_readiness`
- benchmark Markdown includes `Benchmark v1 Freeze & Release Readiness`
- `docs/benchmarks/assets/oss-release-readiness-seal.svg` exists
- `docs/benchmarks/oss-benchmark-release-readiness-2026-06-12.md` exists
- `docs/benchmarks/data/oss-benchmark-release-readiness-2026-06-12.json` exists
- workflow uploads the release readiness artifacts
- CI/CD docs mention benchmark v1 freeze and release readiness pack

- [ ] **Step 2: Implement orchestration**

Build readiness after source verification, write the SVG seal, standalone Markdown/JSON release-readiness pack, and include new files in provenance checksums.

- [ ] **Step 3: Run focused contracts**

Run:

```bash
uv run pytest tests/test_oss_code_quality_benchmark.py -q -k "release_readiness or benchmark_document_contract"
uv run pytest tests/test_cicd_docs_contracts.py -q -k "oss_code_quality_benchmark"
```

### Task 4: Regenerate and Verify

**Files:**
- Modify generated benchmark Markdown, JSON, SVG, trust/provenance data, PR summary and docs metrics.

- [ ] **Step 1: Regenerate benchmark outputs**

Run:

```bash
uv run python tools/oss_code_quality_benchmark.py --workspace . --project dpone --previous-data docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json --baseline-data docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json --allow-stale --runner-name codex-local --workflow-name local-benchmark-v1-release-readiness --max-stale-days 30 --external-analyzer-timeout 60 --pr-summary test_artifacts/oss-code-quality-benchmark/pr-comment.md --enforce-quality-gates
uv run dpone docs update-dev-metrics
```

- [ ] **Step 2: Full verification**

Run:

```bash
uv run ruff check tools/oss_code_quality_benchmark.py tools/oss_benchmark tests/test_oss_code_quality_benchmark.py tests/test_cicd_docs_contracts.py
uv run pytest tests/test_oss_code_quality_benchmark.py -q
uv run pytest tests/test_docs_language_contracts.py tests/test_cicd_docs_contracts.py -q
uv run dpone docs check-architecture-fitness --format json --top 30
uv run dpone docs update-dev-metrics --check
uv run --with mkdocs --with mkdocs-material --with pymdown-extensions mkdocs build --strict
```

- [ ] **Step 3: Public redaction check**

Run:

```bash
rg -n "/Users/|/private/tmp|<repo-name>" docs/benchmarks test_artifacts/oss-code-quality-benchmark
```

Expected: no matches.
