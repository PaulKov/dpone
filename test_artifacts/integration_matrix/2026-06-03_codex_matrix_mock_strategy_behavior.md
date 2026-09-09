# Integration matrix mock strategy behavior artifact

- Date: 2026-06-03
- Runner: Codex in `<workspace>/dpone`
- Commit under test: local working tree after matrix behavior implementation
- Scope: source -> sink integration matrix, credential-free mock layers

## Commands

```bash
DPONE_RUN_INTEGRATION=1 \
DPONE_RUN_INTEGRATION_MATRIX=1 \
DPONE_MATRIX_RUN_MODE=mock_contract \
DPONE_MATRIX_ARTIFACT_DIR=test_artifacts/integration_matrix/mock_contract_latest \
uv run pytest -m integration_matrix tests/integration/matrix --tb=short
```

```bash
DPONE_RUN_INTEGRATION=1 \
DPONE_RUN_INTEGRATION_MATRIX=1 \
DPONE_MATRIX_RUN_MODE=mock_local \
DPONE_MATRIX_ARTIFACT_DIR=test_artifacts/integration_matrix/mock_local_latest \
uv run pytest -m integration_matrix_mock tests/integration/matrix --tb=short
```

## Result

| Layer | Result | Notes |
| --- | --- | --- |
| `mock_contract` | `230 passed` | 115 metadata/preflight cases + 115 strategy behavior cases. |
| `mock_local` | `184 passed, 46 skipped` | BigQuery target cases are documented-contract only in local/mock mode. |

## Strategy behavior coverage

| Strategy | Expected mock behavior |
| --- | --- |
| `full_refresh` | Target is replaced by the full source snapshot. |
| `incremental_append` | Source delta rows are appended and existing target rows are preserved. |
| `incremental_merge` | Rows are delete-aware upserted by `id`; physical delete keys are absent after reconciliation. |
| `replace` | Only the deterministic predicate window `business_date = 2026-06-03` is replaced through staging. |
| `xmin` | Postgres source transaction-id deltas are merged by `id` and paired with snapshot reconciliation for physical deletes. |
| `cdc` | Postgres/MSSQL insert, update, and delete events are applied or emitted as keyed Kafka events. |
| Kafka sink variants | Strategies emit keyed events instead of mutating target rows. |

## Fixtures and volumes

This run uses deterministic in-memory fixtures, not physical database tables. The default per-case volume profile is intentionally larger than a toy fixture:

| Field | Value | Config |
| --- | ---: | --- |
| Base source snapshot | 10,000 rows | `DPONE_MATRIX_MOCK_ROW_COUNT`, max 100,000 |
| Changed delta | 2,000 rows | `DPONE_MATRIX_CHANGE_RATIO=0.20` |
| Physical deletes | 500 rows | `DPONE_MATRIX_DELETE_RATIO=0.05` |
| Wide columns | 120 columns | fixed contract |

The default changed delta is split into 500 delete keys, 1,000 updates, and 500 inserts. Every sampled row carries 120 `wide_*` columns across numeric, temporal, text, binary, JSON/XML, array/range, spatial, network, system-ish, and ClickHouse-like families. Wide columns use deterministic null sparsity so dense, 25% sparse, 50% sparse, and 90% sparse columns are all represented.

Artifacts store compact samples plus full-volume counts, checksums, quality checks, and pass/fail state. They intentionally do not store all 10,000 rows per case.

## Artifact counts

| Artifact directory | Files |
| --- | ---: |
| `test_artifacts/integration_matrix/mock_contract_latest` | 230 |
| `test_artifacts/integration_matrix/mock_local_latest` | 184 |

## Why mock_local reports 46 skipped

`mock_local` skips only BigQuery target cases. There are 23 BigQuery target matrix cases after adding Postgres `xmin` and Postgres/MSSQL `cdc`; each selected case has a preflight test and a behavior test, so `23 x 2 = 46` skipped tests.

## Follow-up boundary

This artifact proves the credential-free matrix contract and mock strategy semantics. It does not prove vendor-managed BigQuery, real API credentials, managed Kafka, or production-size throughput. Those belong to `vendor_live` certification and long-running benchmark suites.
