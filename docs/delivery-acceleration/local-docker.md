# Verify delivery in a disposable local Docker environment

This runbook is for maintainers and platform engineers validating the real DDA
ClickHouse → MSSQL route. It uses the existing native runtime, native ClickHouse
source, BCP, SQL transaction authority and SQLite recovery journal. Return to
[delivery acceleration](index.md) for the feature scope and
[certification](certification.md) for evidence acceptance rules.

## Prepare the services

Explicitly approve the disposable environment before starting services or setting
live approval flags. Use separate ClickHouse and SQL Server containers and a Linux
runner with Python, Microsoft ODBC Driver 18 and BCP. Place them on a private
Docker network without published host ports. Preserve unrelated containers.

Create an `Atomic` ClickHouse database and a SQL Server database, both named
`dda_synthetic`. The local factory refuses other database names. SQL Server needs
catalog/identity-registry permissions and enough *allocated free log space* for
native admission; a maximum file size alone does not provide that headroom.
For this local synthetic experiment, SIMPLE recovery with a 256 MiB allocated
log avoids accumulation between runs. Do not apply this setting to working
or production databases. Retain service image digests, resource limits and the
actual server versions with the experiment evidence.

The September 2026 local experiment uses ClickHouse 24.8.14.39, SQL Server
16.0.4265.3, BCP 18.6 and Python 3.12. SQL Server is amd64 under ARM64 emulation.
These observations do not establish production x86-64 performance.

Supply credentials through process environment, without storing passwords in
manifests, command arguments, evidence or shell history:

| Environment variable | Meaning |
|---|---|
| `DPONE_IT_CH_HOST`, `DPONE_IT_CH_PORT` | Native ClickHouse endpoint |
| `DPONE_IT_CH_DATABASE` | `dda_synthetic` |
| `DPONE_IT_CH_USER`, `DPONE_IT_CH_PASSWORD` | Approved source credentials |
| `DPONE_IT_MSSQL_HOST`, `DPONE_IT_MSSQL_PORT` | SQL Server endpoint |
| `DPONE_IT_MSSQL_DATABASE` | `dda_synthetic` |
| `DPONE_IT_MSSQL_USER`, `DPONE_IT_MSSQL_PASSWORD` | Approved target credentials |
| `DPONE_IT_MSSQL_BCP_PATH` | Installed BCP executable |
| `DPONE_IT_MSSQL_TRUST_SERVER_CERTIFICATE` | Disposable self-signed TLS setting |
| `DPONE_DDA_SPOOL_ROOT` | Durable private directory outside both source checkouts |

Install the selected subject's native connector dependencies and keep the
factory/harness producer checkout separate. Explicitly set `PYTHONPATH` so the
selected subject's `src` precedes the producer root. Verify the actual imported
`dpone.__file__` and Git identity; an adapter label is not source identity.
When invoking repository pytest against a separate subject, set
`DPONE_TEST_USE_INSTALLED_PACKAGE=1` to prevent `tests/conftest.py` from prepending
the producer's `src`, and pass `-o pythonpath=/absolute/subject/src`. Despite that
legacy flag's name, record source imports honestly as source imports.

## Run and inspect

Prepare a JSON limits file containing all eight `NativeChunkLimits` fields:

```json
{
  "max_total_encoded_bytes": 100000000,
  "stage_allocated_bytes_stop_threshold": 100000000,
  "max_rows": 65536,
  "max_bytes": 16777216,
  "max_row_bytes": 1048576,
  "max_pending": 2,
  "max_staging_tables": 1024,
  "parallelism": 1
}
```

After environment approval, set `DPONE_RUN_INTEGRATION=1`,
`DPONE_RUN_INTEGRATION_LIVE=1` and `DPONE_DDA_DISPOSABLE_APPROVED=1`.
Invoke the existing harness with the local benchmark factory:

```bash
python tools/native_delivery_live_benchmark.py run \
  --factory tools.native_delivery_local.factory:create_benchmark_factory \
  --limits /absolute/evidence/limits.json \
  --profile narrow --rows 10000 --trials 3 \
  --strategy full_refresh --mode bounded_native \
  --output /absolute/evidence/candidate/run.json
```

Create the output parent directory before invoking the harness; it must already
exist and be private. Use a new output location for each run. Repeat with `partition_replace` and the
supported profiles `wide`, `unicode`, `decimal`, `null` and `skewed`. The binary
profile remains UNVERIFIED because the canonical planner does not provide an
authored ClickHouse String-to-varbinary mapping. Do not bypass that rejection.
The harness runs exact fidelity and recovery checks before timed trials, then
one warmup and at least three trials. Inspect with the harness `inspect` command.
A dirty producer or subject remains UNVERIFIED even when component checks pass.

## Recover and clean up

Keep the private spool directory and immutable invocation inventory. For harness
`recover` and `cleanup`, use `tools.native_delivery_local.factory:create_factory`
and the original invocation ID, strategy and limits. The maintenance factory
performs no benchmark preparation or source discovery. Recovery uses frozen
bindings and fails if it needs to reacquire the source after durable EOF.

Acknowledged rollback is a terminal fixture outcome: clean up and open a new
invocation. It is not a source-free replay case. Unknown commit outcomes block
replay and cleanup and retain resources for investigation. Never delete journals
or target receipts to force a fresh attempt.

Cleanup records deletion intent and progress under the current lease. It can
resume a partial cleanup while rejecting replacement objects. It removes owned
source and business tables; immutable SQL identity and transaction audit catalogs
remain. Native staging cleanup remains the runtime's responsibility.

The source DDL guard is a cooperative cross-process file lock taken by every
fixture DDL writer. It is suitable for this exclusively owned synthetic source;
exclusion of independent administrator DDL is UNVERIFIED. This factory does not
supply a production source-governance service.

## Interpret results

Retain raw failures, logs, limits, identities and dependency inventories. The
local comparison of baseline 0.76.0 and candidate source uses a shared 0.79.0
Airflow helper distribution needed by the import graph. This is a controlled
native source-component experiment, not certification of two standard installed
release environments. No Airflow/dbt workload runs in this experiment. Binary
support, production DDL exclusion and installed-release performance certification
must remain separate unresolved claims.
