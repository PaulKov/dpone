# Installation

Use this page when you want a clean local install and a quick smoke test. For connector-specific setup, start here and then follow the matching connector guide.

## Requirements

| Requirement | Supported default |
| --- | --- |
| Python | 3.11, 3.12 |
| Package manager | `pip`, `uv`, or any PEP 517 compatible installer |
| Operating system | Linux/macOS for local development; Linux recommended for CI and production |

## Base install

```bash
pip install dpone
dpone --version
dpone -v
dpone --help
```

## Install connector extras

Install only what your pipeline needs:

```bash
pip install "dpone[postgres]"
pip install "dpone[mssql]"
pip install "dpone[clickhouse]"
pip install "dpone[kafka]"
pip install "dpone[gcp]"
```

For a local sandbox or CI image that should include all public integrations:

```bash
pip install "dpone[full]"
```

## Container runtime

For MSSQL/ClickHouse native fast paths, the easiest repeatable runtime is the
Docker image described in [Runtime Docker image](../runtime-image.md). It bundles
`dpone`, ODBC Driver 18, `bcp`, `sqlcmd`, and `clickhouse-client` in one Linux
environment.

## Optional native tools

Some fast paths use native vendor utilities in addition to Python packages.

| Tool | Needed for | Check |
| --- | --- | --- |
| `bcp` | Fast MSSQL bulk import/export | `bcp -v` |
| `sqlcmd` | SQL Server bootstrap and diagnostics | `sqlcmd -?` |
| ODBC Driver 18 | MSSQL connectivity through `pyodbc` | `odbcinst -q -d` |
| `clickhouse-client` | Direct ClickHouse TSV import/export paths | `clickhouse-client --version` |
| Docker | Local integration stack | `docker info` |

## Smoke test

```bash
dpone --help
dpone --version
dpone -v
dpone doctor --profile local
```

`doctor` is intentionally diagnostic. A missing optional connector should be a warning until your manifest actually needs it.

## Next steps

- Run the [quickstart](quickstart.md).
- Configure credentials with [Credentials quickstart](credentials-quickstart.md).
- Pick a real pipeline from the [Examples gallery](examples-gallery.md).
- Read the full [Connections and credentials](../connections.md) guide when moving from local smoke tests to shared environments.
