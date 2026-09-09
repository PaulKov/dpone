## OSS code quality benchmark refresh

- Last refresh: `2026-06-30T15:36:13+00:00`
- Runner: `codex-local`
- Source revision: `codex/release-v0.62.1@3f562b42ebc8db96a14361ce93353486faa23af4`
- Quality gates: **passed** (13 passed, 0 failed)
- Freshness: fresh `6`, stale `0`, unavailable `0`

### Quality gate snapshot

- Project evidence available: `available` == `available`
- Industrial Maintainability Index: `100` >= `85`
- Architecture risk score: `16` <= `24`
- Coverage confidence score: `100` >= `75`
- Largest production module LOC: `449` <= `600`
- Largest production module SLOC: `397` <= `400`
- Maximum fan-out: `26` <= `30`

### Regression summary

- Status: **unchanged**
- No dpone quality regression detected against previous evidence.

### PR regression gate

- Status: **warning** (0 blockers, 2 warnings, 0 improvements)
- warning Architecture runway score: warning (n/a)
- warning Connector slots before yellow: warning (n/a)

### Complexity & Boundary Discipline

- Status: **risk** (overall `59`, complexity `55`, boundary `60`, DI `65`)
- Violations: `4`, risks: `8`
- Top complexity hotspot: `src/dpone/commands/dag/list_edges_cmd.py` `cmd_dag_list_edges` complexity `77`
- Top risk: **P1** `airbyte-integrations/connectors/source-mssql/src/main/kotlin/io/airbyte/integrations/source/mssql/MsSqlServerDebeziumOperations.kt` - module complexity is 154.

### Semantic Maintainability Deep Scan

- Status: **strong** (overall `78`)
- SOLID/DI `80`, DRY/KISS `50`, boundary `79`
- god objects: `166` (modules `0`, classes `58`, functions `108`)
- Top semantic risk: **P1** `src/dpone/runtime/reconciliation/bigquery/store.py` - class `BigQueryReconciliationStore` is 420 LOC.

### Scoring Validity & Calibration

- dpone normalized score: `78` (raw `92`, profile `python-framework`)
- Sensitivity: `0` point swing, rank stability `stable`
- Anti-gaming guardrails: `1` warnings across `3` checks
- Strongest driver: Largest production module fits the normalized profile threshold.
- Next best action: Extract repeated branching into strategy contracts with one responsibility per class.

### Scale Readiness & Growth Simulation

- Architecture runway: `58` (constrained)
- Connector slots before yellow/red: `3` / `16`
- Growth ceiling: `246928` production SLOC
- Worst comparator-scale scenario: `pentaho-scale`
- Runway warning: Architecture runway score is below the scale-readiness warning threshold.

### Refactor ROI Roadmap

- Quality debt: `389` points, driver `complexity`, quick wins `3`
- #1 `src/dpone/commands/dag/subgraph_cmd.py`: ROI `93`, high-impact / low-effort; Command -> Application service -> Renderer - cmd_dag_subgraph complexity is 41.
- #2 `src/dpone/cli_render/manifest/explain.py`: ROI `91`, high-impact / low-effort; Renderer -> View model -> Formatter - render_manifest_explain_text complexity is 58.
- #3 `src/dpone/commands/dag/list_edges_cmd.py`: ROI `91`, high-impact / low-effort; Command -> Application service -> Renderer - cmd_dag_list_edges complexity is 77.

### Executable certification

- Status: **passed** (passed `5`, failed `0`, stale `0`, unavailable `0`)
- All required local executable scenarios are fresh and passing.

### Evidence Trust & Auditability

- Evidence confidence: **92/100** (`audit-ready`)
- Provenance ledger: `docs/benchmarks/data/oss-benchmark-provenance.json`
- Checksum manifest: `SHA-256` artifact checksums are written to `oss-benchmark-provenance.json`.
- Optional LOC/SLOC cross-check: `fresh` via `tokei`.

### Benchmark v3 Release Readiness

- Status: **release-ready**.
- Seal: `Benchmark v3 verified` with score `100/100`.
- Failed checks: `0`.
- Recommended action: Release the benchmark as v3 and use this evidence pack as the PR/release review summary.

### Public Evidence Integrity

- Status: **verified** with score `100/100`.
- claim coverage: `100%`.
- redaction violations: `0`.
- Public artifacts use `$WORKSPACE` / `$BENCHMARK_CACHE` tokens for machine-specific paths.

### Source Citation Verification

- Status: **verified**.
- Source health: `100/100`; claim traceability: `100%`.
- Sources: `48` verified, `0` stale, `0` unavailable.
- Stale source values keep prior content hashes and last successful update timestamps.

### Independent Analyzer Cross-Validation

- dpone confidence: **62/100** (`medium`)
- LOC/SLOC cross-check: `failed`.
- Complexity cross-check: `failed`.
- Stale analyzer values retained: `0`.
- Analyzer command ledger: `independent_validation.analyzer_commands` in raw benchmark evidence.

### Candidate quality delta

- Status: **passed**
- Baseline: `2026-06-30T15:30:34+00:00@3f562b42ebc8db96a14361ce93353486faa23af4`
- No candidate quality budget failed.
- Candidate module watchlist:
  - `src/dpone/commands/ops_parsers_artifacts.py` unchanged LOC 0, fan-out 0, risk `watch`
  - `src/dpone/commands/schema_contract_cmd.py` unchanged LOC 0, fan-out 0, risk `watch`
  - `src/dpone/commands/schema_migration_bundle_cmd.py` unchanged LOC 0, fan-out 0, risk `watch`
  - `src/dpone/commands/schema_migration_registry_cmd.py` unchanged LOC 0, fan-out 0, risk `watch`
  - `src/dpone/commands/schema_plan_cmd.py` unchanged LOC 0, fan-out 0, risk `watch`

### Score movement

| Project | Index | Delta | Risk | Coverage confidence | Freshness |
|---|---:|---:|---|---|---|
| dpone | 100 | 0 | low (16) | high (100) | fresh; updated 2026-06-30T15:36:13+00:00 |
| Airbyte | 73 | 0 | medium (48) | high (80) | fresh; updated 2026-06-30T15:36:13+00:00 |
| dlt | 66 | 0 | high (74) | high (100) | fresh; updated 2026-06-30T15:36:13+00:00 |
| Pentaho Kettle | 36 | 0 | critical (100) | medium (49) | fresh; updated 2026-06-30T15:36:13+00:00 |
| Apache Hop | 42 | 0 | critical (100) | high (86) | fresh; updated 2026-06-30T15:36:13+00:00 |
| Sling | 46 | 0 | high (50) | low (27) | fresh; updated 2026-06-30T15:36:13+00:00 |

### dpone architecture hotspots

- medium fan_out `dpone.runtime.sources.strategies.mssql.mssql_queryout_artifacts` (26 Ce)
- medium fan_in `dpone.readiness.migration_control` (69 Ca)

### Remediation backlog

- **P2** `dpone` Reduce fan_out pressure in dpone.runtime.sources.strategies.mssql.mssql_queryout_artifacts: Split responsibilities behind narrow contracts and keep public facades as compatibility shims.
- **P2** `dpone` Reduce fan_in pressure in dpone.readiness.migration_control: Split responsibilities behind narrow contracts and keep public facades as compatibility shims.
- **P2** `dpone` Lower fan-out in dpone.runtime.sources.strategies.mssql.mssql_queryout_artifacts: Introduce narrower ports or policy objects so this module depends on contracts rather than concrete peers.
- **P3** `dpone` Keep src/dpone/runtime/cdc/retention_models.py below the 600 LOC hard gate: Move the next new responsibility into a focused collaborator before this module crosses 600 LOC.
