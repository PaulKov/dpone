# Local MSSQL Mock Matrix Runbook

This runbook documents how MSSQL source/sink/state tests should run without external credentials.

## Goal

The `mock_local` layer should be able to start SQL Server in CI, create deterministic mock tables, load a wide dataset, and exercise all load strategies where MSSQL is a source or sink.

## Services

Docker Compose service:

```text
docker/docker-compose.integration.yml#mssql
```

Manual start:

```bash
docker compose -f docker/docker-compose.integration.yml up -d mssql
```

Default local settings:

| Setting | Value |
| --- | --- |
| Host | `127.0.0.1` |
| Port | `51433` |
| User | `sa` |
| Password | `Dp0ne.Strong.Pw.2026!` |
| Driver | `ODBC Driver 18 for SQL Server` |
| Trust server certificate | `yes` for local CI |

These are deterministic test-only values, not production credentials.

## Wide type dataset

The MSSQL mock dataset should include representative heavy and edge types:

- integer family: `bit`, `tinyint`, `smallint`, `int`, `bigint`;
- exact numeric: `decimal`, `numeric`, `money`, `smallmoney`;
- approximate numeric: `float`, `real`;
- temporal: `date`, `time`, `datetime`, `datetime2`, `datetimeoffset`, `smalldatetime`;
- text: `char`, `varchar`, `nchar`, `nvarchar`, `text`, `ntext` where compatibility testing requires deprecated types;
- binary: `binary`, `varbinary`, `image` where compatibility testing requires deprecated types;
- identifiers and system-ish types: `uniqueidentifier`, `rowversion`;
- semi-structured/spatial where available: `xml`, `geometry`, `geography`, `hierarchyid`, `sql_variant`.

When a type is unavailable in the local SQL Server image or requires optional assemblies, the test should record a skipped capability artifact instead of silently dropping the case.

## Strategies to exercise

For every MSSQL source or sink local case:

- `full_refresh` through staging/shadow commit;
- `incremental_append` with and without `only_new_rows` where supported;
- `incremental_merge` with `unique_key`;
- `replace` with a deterministic predicate;
- state write/read where the source strategy uses cursor, XMin-equivalent state, CDC, or Kafka offsets.

## Local command

```bash
DPONE_RUN_INTEGRATION=1 \
DPONE_RUN_INTEGRATION_MATRIX=1 \
DPONE_MATRIX_RUN_MODE=mock_local \
DPONE_MATRIX_SOURCE=mssql \
uv run pytest -m integration_matrix_mock tests/integration/matrix -q
```

MSSQL sink cases:

```bash
DPONE_RUN_INTEGRATION=1 \
DPONE_RUN_INTEGRATION_MATRIX=1 \
DPONE_MATRIX_RUN_MODE=mock_local \
DPONE_MATRIX_SINK=mssql \
uv run pytest -m integration_matrix_mock tests/integration/matrix -q
```

## Runbook: failure recovery

Failure: SQL Server container exits during startup.

- Verify the runner has enough memory; SQL Server containers are heavier than Postgres and ClickHouse.
- Check `docker logs dpone-it-mssql`.
- Verify `MSSQL_SA_PASSWORD` still satisfies complexity rules.

Failure: ODBC connection fails.

- Install `msodbcsql18`, `mssql-tools18`, and `unixodbc-dev`.
- Use `TrustServerCertificate=yes` for local CI.
- Verify `sqlcmd -S 127.0.0.1,51433 -U sa -P '<password>' -C -Q 'SELECT 1'`.

Failure: `bcp` fails.

- Verify `/opt/mssql-tools18/bin` is on `PATH`.
- Check field and row terminators in generated bulk files.
- Re-run the focused case with `DPONE_MATRIX_CASE_ID=<case>` and preserve `test_artifacts/integration_matrix`.

Failure: a wide type mismatch appears.

- Check the type mapping in [Type mapping matrix](../type-mapping-matrix.md).
- Add a dialect-specific cast in the staging load path if the target cannot preserve the source type natively.
- Record unsupported legacy/spatial/system types as explicit capability skips, not silent successes.

## Relationship to vendor_live

This runbook never requires external credentials. Managed SQL Server, cloud network paths, or production-like HA topologies belong to `vendor_live` or environment-specific certification gates.
