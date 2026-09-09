# Benchmark Sling And MSSQL Hotspot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Sling to the OSS benchmark while reducing the current MSSQL source strategy architecture hotspot.

**Architecture:** Keep benchmark comparator configuration centralized in `tools/oss_benchmark/config.py` and freshness ordering in `tools/oss_benchmark/state.py`. Split `mssql_strategies.py` into a compatibility facade over focused support/full/incremental modules so public imports remain stable while implementation responsibility moves out.

**Tech Stack:** Python 3.11, pytest, ruff, MkDocs, dpone benchmark CLI, static import/LOC gates.

---

### Task 1: Add Failing Contracts

**Files:**
- Modify: `tests/test_architecture_fitness_gate.py`
- Modify: `tests/test_oss_code_quality_benchmark.py`

- [ ] Add an architecture contract that `runtime/sources/strategies/mssql/mssql_strategies.py` stays a small facade and does not define concrete strategy methods.
- [ ] Add benchmark contracts requiring Sling in the feature parity tool list, benchmark document, raw evidence, and pinned SHA list.
- [ ] Run:

```bash
uv run pytest tests/test_architecture_fitness_gate.py::test_mssql_source_strategy_facade_delegates_concrete_implementations tests/test_oss_code_quality_benchmark.py::test_feature_parity_matrix_scores_enterprise_capabilities tests/test_oss_code_quality_benchmark.py::test_benchmark_document_contract -q
```

Expected: fails before implementation because Sling is missing and the MSSQL source facade still owns concrete implementations.

### Task 2: Split MSSQL Source Strategy Implementation

**Files:**
- Create: `src/dpone/runtime/sources/strategies/mssql/mssql_schema_access.py`
- Create: `src/dpone/runtime/sources/strategies/mssql/mssql_base.py`
- Create: `src/dpone/runtime/sources/strategies/mssql/mssql_full.py`
- Create: `src/dpone/runtime/sources/strategies/mssql/mssql_incremental.py`
- Modify: `src/dpone/runtime/sources/strategies/mssql/mssql_strategies.py`
- Modify: `src/dpone/runtime/sources/strategies/mssql/__init__.py`

- [ ] Move database/schema/query/max-column compatibility helpers into `mssql_schema_access.py`.
- [ ] Move shared queryout helper wiring into `mssql_base.py`.
- [ ] Move full refresh extraction into `mssql_full.py`.
- [ ] Move incremental column extraction into `mssql_incremental.py`.
- [ ] Keep `mssql_strategies.py` as a tiny compatibility facade exporting `MSSQLFullExtractStrategy` and `MSSQLIncrementalExtractStrategy`.
- [ ] Run targeted MSSQL strategy tests and architecture test.

### Task 3: Add Sling Comparator

**Files:**
- Modify: `tools/oss_benchmark/config.py`
- Modify: `tools/oss_benchmark/state.py`
- Modify: `tools/oss_benchmark/feature_parity.py`
- Modify: `tests/test_oss_code_quality_benchmark.py`

- [ ] Add Sling project spec pinned to `slingdata-io/sling-cli main@6c4ca04c3328eb32da480780c9957bf17f80ffb5`.
- [ ] Add Sling ordering/stub metadata.
- [ ] Add Sling feature parity ratings using official GitHub/docs evidence.
- [ ] Update tests to require Sling in comparator lists.

### Task 4: Refresh Generated Evidence

**Files:**
- Update: `docs/benchmarks/oss-code-quality-benchmark-2026-06-12.md`
- Update: `docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json`
- Update: `docs/benchmarks/data/oss-code-quality-benchmark-history.json`
- Update: `docs/benchmarks/data/oss-benchmark-provenance.json`
- Update: `docs/benchmarks/assets/*.svg`
- Update: `docs/quality-metrics.md`

- [ ] Run the benchmark CLI with previous-data and stale preservation so any unavailable comparator retains prior evidence instead of being erased.
- [ ] Refresh developer quality metrics.
- [ ] Confirm the benchmark header reports the current dpone branch `dpone-mssql-odbc-options`, local SHA `31a08e0bb27125e13d2052385569983b12865674`, and version-era metrics for dpone `0.17.0`.

### Task 5: Verify Quality Path

**Files:** no new files expected.

- [ ] Run targeted tests for MSSQL, benchmark contracts, docs contracts, and architecture gates.
- [ ] Run ruff on changed benchmark/runtime files.
- [ ] Run `dpone docs check-import-rules`, layer metrics, module size, and dev metrics check.
- [ ] Run strict MkDocs if generated docs changed.
