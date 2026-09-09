# dpone Managed UX Live Gate Artifact

- Artifact date: 2026-06-02
- Executed by: Codex local agent
- Workspace: `<workspace>/dpone`
- Scope: Managed-like UX feature layer, post-implementation regression, fresh local Kafka/MSSQL/Postgres CDC live gates

## Result Summary

| Gate | Result | Notes |
| --- | --- | --- |
| `uv run ruff check .` | PASSED | All checks passed. |
| `uv run ruff format --check .` | PASSED | 726 files already formatted. |
| `uv run mypy --config-file mypy.ini` | PASSED | No issues found in 161 source files. |
| `uv run pytest -m "not integration_live" -q` | PASSED | Completed at 100%; expected live-gated skips only. |
| `uv sync --all-extras` | PASSED | Audited 95 packages before post-patch live rerun. |
| `uv build` | PASSED | Built `dist/dpone-0.1.0.tar.gz` and `dist/dpone-0.1.0-py3-none-any.whl`. |
| `uvx twine check dist/*` | PASSED | Source and wheel distributions passed metadata validation. |
| Fresh venv `dpone[full]` smoke | PASSED | CLI and managed UX imports available after wheel install. |
| Managed CLI smoke | PASSED | `doctor`, `init`, `plan`, `perf advise`, `state reset`, `connectors list`, `studio` returned valid JSON payloads. |
| `uv run pytest -m integration_kafka -q` | PASSED | 2/2 against local Redpanda Kafka + Schema Registry. |
| `uv run pytest -m integration_mssql -q` | PASSED | 4/4 against local SQL Server 2022, ODBC Driver 18, `bcp`. |
| `uv run pytest -m integration_postgres_cdc -q` | PASSED | 1/1 against local Postgres 16 with `wal_level=logical`. |
| `integration_clickhouse` pytest marker | NOT EXECUTABLE | Current tree contains CI metadata/docs refs, but no pytest tests marked `integration_clickhouse`. |

## Local Services Used

| Service | Container | Endpoint | Purpose |
| --- | --- | --- | --- |
| Redpanda Kafka | `dpone-it-redpanda` | `127.0.0.1:19092` | Kafka source/sink integration marker. |
| Redpanda Schema Registry | `dpone-it-redpanda` | `http://127.0.0.1:18081` | Kafka Schema Registry integration marker. |
| SQL Server 2022 | `dpone-it-mssql` | `127.0.0.1:11433`, db `dpone` | MSSQL CDC/Change Tracking/bcp integration marker. |
| Postgres 16 | `dpone-it-postgres-cdc` | `127.0.0.1:15435`, db `dpone` | Postgres logical CDC integration marker. |

## MSSQL CDC Harness Note

Root cause found during live testing: Docker SQL Server exposes CDC metadata/functions, but does not reliably run SQL Server Agent capture jobs in the background. The production `MSSQLCDCReader` remains unchanged. The integration harness now runs one bounded `sys.sp_cdc_scan @continuous = 0` inside the polling loop so local Docker CDC tests deterministically advance capture tables before reading through the production API.

## Managed UX Coverage

Validated core self-service surfaces:

- environment diagnostics: `dpone doctor --profile ci --format json|md`;
- manifest generation: `dpone init` interactive-service path and non-interactive CLI flags;
- dry-run planning: `dpone plan` with source/sink/strategy/schema evolution/state/quality/performance details;
- run artifacts: JSON, Markdown, HTML writer paths;
- quality contracts: `not_null`, `unique`, `accepted_values`, `min_rows`, `max_null_ratio`, `checksum`, and plan-only checks;
- state UX: inspect, reset preview/confirmed behavior, export, replay-from, compare;
- connector UX: list/certify/scaffold;
- performance advisor: Postgres->MSSQL bcp, MSSQL->ClickHouse direct TSV, partitioning, Kafka and ClickHouse recommendations;
- local Studio metadata endpoint via `dpone studio --format json`.

## Residual Gaps

- Add executable ClickHouse live pytest marker if we want a first-class local ClickHouse UX gate rather than CI metadata-only coverage.
- BigQuery live gates remain external-credential gated by design.
- Studio v1 is local service/CLI API scaffolding, not a polished frontend application.

## Post-Studio HTTP Smoke Update

After adding a direct Studio HTTP endpoint smoke contract, the following post-change gates were rerun successfully:

| Gate | Result | Notes |
| --- | --- | --- |
| `uv run pytest tests/test_managed_ux_contracts.py -q` | PASSED | 12/12, including real local `ThreadingHTTPServer` smoke for `/`, `/api/doctor`, `/api/connectors`. |
| `uv run ruff check .` | PASSED | All checks passed after Studio handler refactor. |
| `uv run ruff format --check .` | PASSED | 726 files already formatted. |
| `uv run mypy --config-file mypy.ini` | PASSED | No issues found in 161 source files. |
| `uv run pytest -m "not integration_live" -q` | PASSED | Completed at 100% after Studio HTTP smoke addition. |
| `uv build` | PASSED | Rebuilt source distribution and wheel after CLI module changes. |
| `uvx twine check dist/*` | PASSED | Rebuilt artifacts passed metadata validation. |
| Fresh venv `dpone[full]` smoke | PASSED | Verified CLI command registry and Studio handler import from rebuilt wheel. |
