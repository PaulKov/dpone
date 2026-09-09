# Validate native BCP types in local Docker

Use this procedure after changing SQL Server native framing or ClickHouse binary
encoding. It creates isolated SQL Server and ClickHouse containers, exports
synthetic rows with the real Linux BCP executable, and compares queried target
values through Python RowBinary, Python Native and accelerated Native.

## Run the reproducible check

Docker must be running with enough memory for SQL Server (4 GiB) and ClickHouse
(2 GiB), plus engine overhead. The SQL Server image uses `linux/amd64`; on an ARM
host this is an emulated local test, not evidence for a native ARM SQL Server.
No existing databases are used and no host ports are published. Container
credentials are generated for the run and never written to the report.

From the repository root:

```bash
uv sync --frozen --extra clickhouse
uv pip install --no-deps -e packages/dpone-native-accel
.venv/bin/python tools/run_native_docker_corners.py \
  --output /tmp/dpone-native-corners-run1
```

Choose a new output directory for each run; an existing directory is rejected.
If Docker Desktop is installed on macOS but its executable is not in PATH, add
`--docker /Applications/Docker.app/Contents/Resources/bin/docker`.
The runner removes its containers and network on completion. Do not substitute
external services for this disposable stand.

The output contains `receipt.json` and a sanitized `pytest.log`. A successful
receipt requires actual passing tests, no skipped, deselected or expected-failure
outcomes, unchanged source/test
content during execution, and successful cleanup. It records image identities,
server and BCP versions, Git HEAD, and a content digest including uncommitted
source. It is a local verification receipt, not a release or published-package
certificate. Inherited `PYTEST_ADDOPTS` is cleared so local test filters cannot
silently reduce the matrix. Use `--sql-image` and `--ch-image` to verify another image pair;
results do not automatically extend to other exporter versions or platforms.

## Coverage and observed corrections

| Boundary | Verification |
|---|---|
| Physical layout | NOT NULL and nullable columns; adjacent integer sentinels; multiple rows; UUID byte order |
| Exact numerics | Integer extrema, decimal precision 1/9/10/19/20/28/29/38 at scale zero and full precision, money extrema, float(24)/(53) and IEEE extremes |
| Temporal values | Scales 0–7, midnight and final fractional tick, pre-epoch fractions, leap day, offsets ±14:00 crossing dates, legacy datetime rounding |
| Text and bytes | Legacy and UTF-8 collations, supplementary Unicode, embedded NUL/tab/newline, trailing spaces, fixed padding, empty versus NULL, all byte values |
| Payload length | varchar(8000), nvarchar(4000), MAX values of 65,535/65,536/65,537 bytes |
| Rejection and retry | Out-of-range target calendars/integers, truncated files, invalid prefixes, a valid row followed by truncation, empty export, clean retry |
| Replay | Reusing the same encoded payload produces the same queried rows after explicitly clearing the disposable target |

The actual exporter established rules that synthetic fixtures alone had missed:

- `bit NOT NULL` has a one-byte prefix on the tested Linux exporter.
- Modern temporal payloads use physical scale 7, including SQL columns declared
  at lower scales. Logical scale remains in `source_type`; the physical `scale`
  in the layout is 7. Previously generated lower-scale layouts must be re-exported.
- Native UTF-8 varchar bytes can exceed the source column's byte count. Bounded
  readers account for conversion expansion, and still reject malformed UTF-8.
- Raw `char NOT NULL` lacks a prefix; multibyte conversion makes its boundary
  ambiguous without additional exporter metadata. It is rejected before native
  export rather than guessed. Nullable char remains supported.
- ClickHouse can silently clamp out-of-range Date32/DateTime64 values. Both
  encoders reject them before sending a value to the server.

The optional accelerator must advertise `native_wire_revision: 2`. An older
provider causes an explicit Python fallback in `auto`; `required` fails before
file access. This revision is a provider capability and does not change the
serialized native-wire schema version. The provider's capability flag alone is
not a live certification receipt.

## Recovery and limits

For raw `char NOT NULL`, use a governed source projection with an accurately
reported schema, or select the ODBC row-stream transport:

```yaml
source:
  options:
    mssql_export_mode: row_stream
    native_transfer:
      wire:
        mode: typed_binary
        source_native_format: odbc_row_stream
        binary_format: native
```

Keep the usual connection, source/sink and staging configuration from the
[MSSQL connector guide](mssql.md). Changing a decoder length or hash manually is
not a recovery procedure. Re-export incompatible artifacts and verify the target
before retrying a previously published load.

These checks establish the tested codec/exporter/target combination. They do not
establish complete full-refresh publication atomicity, checkpoint recovery,
cancellation under production load, or every SQL Server/BCP/ClickHouse version.
Replay here uses an explicit disposable-table reset, not a production full-refresh
transaction. [Route certification](connector-certification.md) still governs
those separate contracts.
