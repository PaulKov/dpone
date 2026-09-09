# External Analyzer Execution Pack Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Execute real external analyzers (`tokei`, `cloc`, `radon`, `lizard`) for the OSS code-quality benchmark, normalize their outputs, preserve prior analyzer values as stale when refresh fails, and publish the evidence in docs/CI artifacts.

**Architecture:** Add a focused `tools/oss_benchmark/external_analyzers.py` module with parser classes, a subprocess runner, and a normalized result collector. Keep `tools/oss_benchmark/independent_validation.py` responsible for scoring/cross-check aggregation, while `tools/oss_benchmark/core.py` only orchestrates collection and rendering.

**Tech Stack:** Python stdlib (`json`, `subprocess`, `xml.etree.ElementTree`), existing benchmark payload dictionaries, pytest, GitHub Actions manual workflow.

---

### Task 1: Parser And Runner Contracts

**Files:**
- Modify: `tests/test_oss_code_quality_benchmark.py`
- Create: `tools/oss_benchmark/external_analyzers.py`
- Modify: `tools/oss_code_quality_benchmark.py`

- [ ] **Step 1: Write failing parser tests**

Add tests that call `TokeiParser`, `ClocParser`, `RadonParser`, and `LizardParser` through the CLI wrapper and assert normalized `total_sloc`, `total_lines`, `avg_complexity`, `max_complexity`, and `files`.

- [ ] **Step 2: Verify RED**

Run:

```bash
uv run pytest tests/test_oss_code_quality_benchmark.py::test_external_analyzer_parsers_normalize_tool_outputs -q
```

Expected: fail because parser classes are not exported yet.

- [ ] **Step 3: Implement parsers**

Create parser classes with one method each: `parse(stdout: str) -> dict[str, float | int]`. Use JSON for `tokei`, `cloc`, `radon`; use XML for `lizard --xml`.

- [ ] **Step 4: Verify GREEN**

Run the same pytest target. Expected: pass.

### Task 2: Analyzer Collection And Stale Preservation

**Files:**
- Modify: `tests/test_oss_code_quality_benchmark.py`
- Modify: `tools/oss_benchmark/external_analyzers.py`
- Modify: `tools/oss_benchmark/independent_validation.py`

- [ ] **Step 1: Write failing collector test**

Add a test with an injected fake command runner and tool resolver. It should prove fresh analyzer output is marked `fresh`, missing tools with previous values are marked `stale`, and missing tools without previous evidence are `unavailable`.

- [ ] **Step 2: Verify RED**

Run:

```bash
uv run pytest tests/test_oss_code_quality_benchmark.py::test_external_analyzer_collection_executes_tools_and_preserves_stale_values -q
```

Expected: fail because the collector does not exist.

- [ ] **Step 3: Implement collector**

Add `collect_external_analyzer_results(projects, generated_at, previous_payload=None, command_runner=None, tool_resolver=shutil.which, timeout_seconds=60)`. Return normalized dictionaries compatible with `build_independent_validation`.

- [ ] **Step 4: Extend validation statuses**

Teach independent validation that `stale` analyzer values remain visible, lower confidence, and render as stale rather than unavailable.

- [ ] **Step 5: Verify GREEN**

Run the collector test and the existing independent-validation tests. Expected: all pass.

### Task 3: Generator, CLI, CI, And Docs Wiring

**Files:**
- Modify: `tools/oss_benchmark/core.py`
- Modify: `tools/oss_code_quality_benchmark.py`
- Modify: `.github/workflows/oss-code-quality-benchmark.yml`
- Modify: `docs/ci-cd.md`
- Modify: `docs/cicd/workflows.md`
- Modify: `tests/test_cicd_docs_contracts.py`
- Modify: `tests/test_oss_code_quality_benchmark.py`

- [ ] **Step 1: Write failing CLI/docs contracts**

Assert `--external-analyzer-timeout` parses, the workflow installs analyzer tools, and CI docs mention external analyzer execution and stale preservation.

- [ ] **Step 2: Verify RED**

Run:

```bash
uv run pytest tests/test_oss_code_quality_benchmark.py::test_cli_parses_external_analyzer_timeout tests/test_cicd_docs_contracts.py::test_oss_code_quality_benchmark_workflow_is_manual_and_self_service -q
```

Expected: fail on missing CLI/docs workflow fragments.

- [ ] **Step 3: Wire generator**

Call `collect_external_analyzer_results` before `build_independent_validation`, store `external_analyzer_results` in raw evidence, and pass timeout from CLI.

- [ ] **Step 4: Wire CI/docs**

Install `tokei`, `cloc`, `radon`, and `lizard` in the manual workflow; document that the benchmark now executes external analyzers and keeps stale analyzer values visible.

- [ ] **Step 5: Verify GREEN**

Run targeted CLI/docs tests. Expected: pass.

### Task 4: Regenerate And Verify

**Files:**
- Generated: `docs/benchmarks/oss-code-quality-benchmark-2026-06-12.md`
- Generated: `docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json`
- Generated: `docs/benchmarks/assets/oss-independent-validation.svg`
- Generated: `docs/benchmarks/assets/oss-analyzer-confidence.svg`
- Generated: `test_artifacts/oss-code-quality-benchmark/pr-comment.md`
- Generated: `docs/quality-metrics.md`

- [ ] **Step 1: Regenerate benchmark artifacts**

Run the local dpone refresh with `--allow-stale`, `--baseline-data`, `--pr-summary`, and `--external-analyzer-timeout 60`.

- [ ] **Step 2: Refresh dev metrics**

Run:

```bash
uv run dpone docs update-dev-metrics
```

- [ ] **Step 3: Full verification**

Run ruff, benchmark tests, docs contract tests, architecture fitness, metrics check, and strict MkDocs build.

- [ ] **Step 4: Report completion**

Report generated files, analyzer availability status, and all verification commands with outputs.
