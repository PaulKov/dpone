# CDC Compare Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a generic CDC compare and repair feature with `mssql -> clickhouse` first, comparing source rows against the current-state projection of a ClickHouse CDC log and optionally replaying repair events without mutating offsets.

**Architecture:** Keep compare and repair as focused runtime CDC services with injected readers and sink appliers. The ops layer composes local JSON readers or live MSSQL/ClickHouse readers, while CLI handlers only parse arguments and delegate. Repair actions write bounded CDC events through `CdcSinkApplier`; checkpoint ownership remains exclusively in `CdcRuntimeOrchestrator`.

**Tech Stack:** Python dataclasses, existing `CDCBatch`/`CDCChange`/`CdcRuntimeStream` contracts, `ClickHouseCdcSinkApplier`, `LocalCdcSinkApplier`, pytest, Docker-live MSSQL + ClickHouse integration.

---

### Task 1: Runtime Compare Contracts

**Files:**
- Create: `src/dpone/runtime/cdc/compare_models.py`
- Test: `tests/test_cdc_compare_repair.py`

- [ ] Write RED tests for `CdcCompareRow`, diff kinds, stable report JSON, and repair action rendering.
- [ ] Implement immutable compare rows, diff records, repair actions, repair plan, and compare report.
- [ ] Verify `uv run pytest tests/test_cdc_compare_repair.py -q`.

### Task 2: Generic Compare Service

**Files:**
- Create: `src/dpone/runtime/cdc/compare.py`
- Test: `tests/test_cdc_compare_repair.py`

- [ ] Write RED tests for missing target rows, extra target rows, value mismatches, delete mismatches, clean rows, `max_diffs`, and deterministic hashes.
- [ ] Implement `CdcRowHasher`, `InMemoryCdcCompareReader`, and `CdcCompareRepairService`.
- [ ] Verify compare writes `cdc_compare_repair.json`, `cdc_compare_repair.md`, and an embedded repair plan.

### Task 3: Repair Execution

**Files:**
- Create: `src/dpone/runtime/cdc/repair.py`
- Test: `tests/test_cdc_repair_execute.py`

- [ ] Write RED tests proving repair actions become `CDCChange` events and replay never commits offsets.
- [ ] Implement `CdcRepairExecutionService` and `CdcRepairExecutionReport`.
- [ ] Verify local sink receipt is durable and report schema is stable.

### Task 4: Live MSSQL and ClickHouse Compare Readers

**Files:**
- Create: `src/dpone/runtime/cdc/compare_readers.py`
- Test: `tests/test_cdc_compare_live_readers.py`
- Extend: `tests/integration/mssql/test_mssql_clickhouse_live_cdc_runtime_integration.py`

- [ ] Write RED tests for generated MSSQL source query and ClickHouse latest CDC log query.
- [ ] Implement `MssqlCdcCompareReader` and `ClickHouseCdcLogCompareReader`.
- [ ] Add Docker-live compare proof after MSSQL -> ClickHouse runtime apply: source rows equal latest ClickHouse CDC log projection.

### Task 5: Ops Facade and CLI

**Files:**
- Create: `src/dpone/ops/cdc/compare_repair.py`
- Create/extend: `src/dpone/commands/ops_parsers_cdc_compare_repair.py`
- Modify: `src/dpone/ops/catalog_cdc_recovery.py`, `src/dpone/ops/catalog_cdc.py`, `src/dpone/services/ops/command_handlers_cdc.py`, `src/dpone/services/ops/__init__.py`, `src/dpone/commands/ops_cmd.py`, `src/dpone/commands/registry_ops.py`
- Test: `tests/test_cli_cdc_compare_repair_commands.py`

- [ ] Write RED CLI delegation tests for `dpone ops cdc-compare-repair` and `dpone ops cdc-repair-execute`.
- [ ] Implement local JSON compare and local/live repair composition.
- [ ] Implement live `mssql -> clickhouse` compare composition with connector factories.
- [ ] Keep command handlers thin.

### Task 6: Docs and Public API

**Files:**
- Create: `docs/cdc-compare-repair.md`
- Create: `docs/developer-cdc-compare-repair.md`
- Modify: `docs/README.md`, `docs/architecture.md`, `docs/ci-cd.md`, `docs/developer-ci-cd.md`, `docs/ops-cli.md`, `docs/source-sink/mssql-to-clickhouse.md`, `docs/source-sink-matrix.md`, `mkdocs.yml`, `src/dpone/runtime/cdc/__init__.py`
- Test: `tests/test_cdc_compare_repair_docs_contract.py`

- [ ] Write RED docs-contract tests for user docs, developer docs, source-sink matrix, CLI examples, Docker-live instructions, and architecture boundaries.
- [ ] Add English self-service docs, runbooks, examples, and developer extension rules.
- [ ] Export public runtime CDC compare/repair classes through the lazy runtime facade.
- [ ] Regenerate CLI reference and quality metrics.

### Task 7: Verification

**Files:**
- All touched files.

- [ ] Run focused compare/repair tests.
- [ ] Run Docker-live MSSQL + ClickHouse CDC integration test with `DPONE_RUN_INTEGRATION=1`.
- [ ] Run full non-live test suite.
- [ ] Run ruff, format, mypy, generated docs checks, docs link check, import rules, layer metrics, module size, architecture fitness, MkDocs strict build, and package build.
