# MSSQL Integration

`dpone` supports SQL Server as a first-class source, sink, and state backend.
Install the optional dependency with:

```bash
pip install "dpone[mssql]"
```

Runtime requirements for real bulk loads:

- Microsoft ODBC Driver 18 for SQL Server
- `mssql-tools18` or another installation that provides the `bcp` executable
- Network access from the runner to SQL Server port `1433`

Before a local MSSQL load, run:

```bash
dpone doctor --profile mssql --format json
```

The `mssql` profile requires both a loadable `pyodbc` module and `bcp` on
`PATH`. Module health is a dual import proof, not package discovery. A hook-free
`-I -S` child proves that the artifact loads directly from the captured runtime
paths; a second child reproduces the supported effective Python 3.11/3.12
startup projection. `PASS` requires two authenticated success receipts; it does
not certify mutable interpreter state outside that projection. Matching missing
or load-failure receipts retain that diagnosis; every disagreement is reported
as unsupported startup. A wheel whose unixODBC shared library is unavailable is
therefore a stable redacted load failure without loader paths or exception text.

Both attempts consume one immutable, bounded working-directory and `sys.path`
snapshot and share one five-second process-communication budget. Confirming
cleanup may then consume up to two additional seconds per started child; local
filesystem operations are not claimed to have a portable wall-clock bound.
Both children start in distinct private directories, so `python -c` cannot
introduce the captured cwd as a new
startup import root. After bootstrap, both restore the same target cwd, exact
path, selected target-visible environment, cached user-site state, warning
policy, allowlisted `-X` values, and pycache prefix.

Paths, policy framing and the receipt location travel over bounded stdin, never
process arguments. On Windows the parent writes that frame on a dedicated
bounded-lifecycle thread while the main thread owns the communication deadline;
pipe backpressure from a child that never reads stdin therefore triggers direct
child termination, wait and writer join within one cleanup deadline. A successful
attempt requires both the child and writer to be quiescent. The parent accepts
only the canonical CPython environment
proxy, snapshots at most 2,048 entries within both a 64 KiB encoded cap and the
32,767-unit Windows environment-block limit, then validates each final child
environment again after adding confined startup values. Replaced, concurrently
mutated, malformed or oversized environments fail as invalid policy before an
unbounded child launch. Windows' native leading-`=` drive-current-directory
entries are preserved exactly; later `=` characters and POSIX `=` keys remain
invalid. Before target import, each child authenticates the parent's
implementation, exact version, cache tag, ABI flags, executable/base-executable,
prefix family and platform-library directory. The effective leg additionally
requires exact immutable `sys.flags`; the hermetic leg requires its explicit
`-I -S` projection. Both attest canonical buffering, faulthandler state,
tracemalloc depth, frozen-`os` policy, debug-range mode and the effective integer
digit limit. A consumed macOS launcher or wrapper that redirects the runtime or
strips startup flags therefore produces unsupported startup, never `PASS`.
The effective child receives supported non-location startup controls before
interpreter initialization, including `PYTHONIOENCODING` and pycache policy;
warning options use their lossless effective projection.
Automatic-site location selectors (`PYTHONPATH`, `HOME`, `PYTHONUSERBASE`,
`APPDATA`, and `USERPROFILE`) remain confined for both children and are restored
only after bootstrap with the exact captured path and target environment. This
prevents a late selector from installing child-only hooks; path-only wheels and
editables still load from the authenticated `sys.path`. `PYTHONHASHSEED=0` is the only explicit
hash-seed state observable from the running interpreter. Explicit `random`, an
empty value, or a nonzero numeric seed is unsupported because a post-startup
environment value cannot authenticate the parent's effective seed. Visible
allocator (`PYTHONMALLOC`/`PYTHONMALLOCSTATS`), unauthenticated locale/case and
legacy Windows filesystem/console startup controls, and import/perf
instrumentation (`PYTHONPROFILEIMPORTTIME`, `PYTHONPERFSUPPORT`, `-X importtime`,
or `-X perf`) are unsupported unless `-E`/`-I` proves environment controls were
ignored. Invalid or contradictory startup-derived state is unsupported before
target classification.
Observable faulthandler enablement, tracemalloc enablement/depth and canonical
unbuffered-stdio mode are captured even if their environment control was
removed, while integer-digit, frozen-module and debug-range policy must agree
with authenticated child observations. Inspect-only and every
interactive parent are unsupported because `python -c` cannot reproduce their
exact flag and `PYTHONSTARTUP` semantics. Late `PYTHONSAFEPATH` and
`PYTHONNOUSERSITE` values must agree with the effective flags.

Ordinary wheels, direct user-site paths and path-only `.pth`/editable installs
remain supported. When automatic site loading is active, loaded or discoverable
`sitecustomize`/`usercustomize` (including a module that failed and was removed),
arbitrary executable `.pth` lines, patched
built-in import, and nonstandard meta/path/importer machinery are opaque
post-hook state and therefore fail closed as unsupported; they are never
replay-certified. Two executable `.pth` exceptions are authenticated narrowly:
the exact virtualenv scaffold and the stock setuptools
`distutils-precedence.pth` line when its finder is the loaded module's exact
`DISTUTILS_FINDER`. The finder-presence bit is receipt-bound in both attempts,
so late or removed `SETUPTOOLS_USE_DISTUTILS` state fails unsupported instead of
being certified. This recognizes a standard installed scaffold; it is not a
package-integrity or hostile-code attestation. With an effective `-S`, `.pth`
files that never participated in startup are not scanned, while already
loaded/discoverable customizers and current import machinery remain guarded.
Directory discovery is entry-capped; regular zip
roots are size-capped and use native zip semantics, including standard symlink
roots and Windows case behavior. Hook-dependent or flag-dependent disagreement
also fails closed as unsupported.
Automatic global/venv roots are derived from bounded `sys.prefix`/
`sys.exec_prefix` projections rather than mutable `site.PREFIXES`; observed
base-prefix roots are included when present on the captured import path. When
user site is not disabled by immutable flags, cached and canonical user-site
candidates are scanned independently of mutable `site.ENABLE_USER_SITE`.
Custom
`PYTHONHOME`, `PYTHONPLATLIBDIR`, `PYTHONEXECUTABLE`, and macOS
`__PYVENV_LAUNCHER__` startup state is rejected when it cannot be reproduced.
The requested module name must be an exact bounded string. A standard finder
miss is reported as not installed, while an already located top-level module or
executed package/module code that raises `ModuleNotFoundError` for its own name
is a load failure and receives load-failure remediation.

The probe intentionally does not clone mutable `sys.modules`, arbitrary runtime
`sys.meta_path`/`sys.path_hooks`/`sys.path_importer_cache`, mutated
`warnings.filters` or `sys.dont_write_bytecode`, tracing/audit hooks, threads,
stdio, allocator state, or the parent's exact hash seed. In particular, if a
fixed nonzero hash, allocator, or `PYTHONIOENCODING` control is removed after
interpreter startup, its provenance is not part of the
supported projection. Runtime mutations of general `sys.warnoptions` and
`sys._xoptions` are likewise outside it except for the observable effects
validated above. Tracemalloc history and allocations are not reproduced. It is
a loadability diagnostic, not a hostile-code sandbox. On
POSIX, each attempt owns a new session, kills the original inherited process
group, waits for the leader
and then proves that group disappeared within a fixed cleanup deadline;
deliberate process-group reassignment or session detachment is outside the
contract. On Windows, the runner bounds the stdin writer together with the
direct child, terminates and reaps that child on failure, and makes no
descendant-containment claim.
Every classification requires an exact receipt committed to a precreated
regular file. A pre-bootstrap exit or unconfirmed cleanup is unavailable, never
a target result. Fix hints distinguish missing, native load, path, policy,
unsupported startup, timeout and probe failures without changing the public v1
doctor/onboarding Python dataclasses or JSON projections.
The credential-free Windows 3.11 and 3.12 jobs are direct branch-protection
contexts; both hosted conclusions are required on the reviewed head.

## Recommended manifest

```yaml
source:
  type: postgres
  connection_type: vault
  connection_id: postgres-demo
  vault_path: postgres/demo-source
  table: {schema: public, name: orders}
  options:
    export_format: mssql-delimited
    batch_commit_mode: whole
    compress_export: false

sink:
  type: mssql
  connection_type: vault
  connection_id: mssql-dwh
  vault_path: mssql/demo-dwh
  table: {schema: landing, name: orders}
  strategy:
    mode: incremental_merge
    unique_key: [id]
    merge_policy: delete_insert
    duplicate_policy: fail
  options:
    bulk:
      mode: bcp
      bcp:
        field_terminator: "\t"
        row_terminator: "\n"
        batch_size: 100000
        packet_size: 16384

state:
  type: mssql
  connection_id: mssql-dwh
  connection_type: vault
  vault_path: mssql/demo-dwh
  table: {schema: etl_state, name: etl_xmin_state}
```

## Three-part table names

For SQL Server routes, dpone supports both the existing two-part
`schema.table` contract and first-class three-part `database.schema.table`
identities. Use the explicit form when one Airflow Connection points at a
server but a manifest needs a table in a different database:

```yaml
source:
  type: mssql
  connection_id: mssql_dwh
  connection_type: airflow
  table:
    database: analytics_staging
    schema: clickhouse
    name: v_dim_example
```

The rendered SQL Server object name is
`[analytics_staging].[clickhouse].[v_dim_example]`. If `database` is
omitted, dpone keeps the previous behavior and uses the database from the
connection.

Legacy compact manifests are accepted for compatibility:

```yaml
source:
  type: mssql
  table: {schema: analytics_staging.clickhouse, name: v_dim_example}
```

MSSQL staging defaults to the target database for cross-database loads unless
`sink.staging.database` is set explicitly. PostgreSQL and ClickHouse table
names remain two-part in this release.

## Credentials

Vault, Airflow, and environment providers share the same logical fields:

| Field | Default | Notes |
| --- | --- | --- |
| `host` | required | SQL Server host |
| `port` | `1433` | SQL Server TCP port |
| `database` | required | Database name |
| `username` / `password` | optional | Omit for trusted auth where supported |
| `driver` | `ODBC Driver 18 for SQL Server` | pyodbc driver name |
| `encrypt` | `yes` | ODBC encryption flag |
| `trust_server_certificate` | `no` | Set `yes` for local Docker only |
| `connect_timeout` | `10` | pyodbc login timeout, in seconds |
| `query_timeout` | `0` | pyodbc query timeout |
| `multi_subnet_failover` | unset | Set `yes` for SQL Server Always On availability group listeners |
| `application_intent` | unset | Set `ReadOnly` only when read-only routing is expected |
| `transparent_network_ip_resolution` | unset | Optional ODBC Driver for SQL Server network resolution flag |
| `bcp_path` | `bcp` | Absolute path if not on `PATH` |

For SQL Server Always On availability group listeners, Microsoft recommends
`MultiSubnetFailover=Yes`. In Airflow or runtime-only `AIRFLOW_CONN_*` URI
extras, configure it as `multi_subnet_failover=yes`; dpone maps it to the ODBC
connection string and maps `connect_timeout` to the pyodbc login timeout.

## Loading behavior

- `FULL_REFRESH`: bcp-load into staging, validate the native projection, then
  atomically `TRUNCATE` the existing target and use one table-locked
  `INSERT ... SELECT` in the receipt-backed target transaction. This preserves
  the target object, grants, indexes, constraints, and references.
- `INCREMENTAL_APPEND`: append all rows, or `only_new_rows: true` with `unique_key` for insert-not-exists.
- `INCREMENTAL_MERGE`: load into staging, fail on duplicate `unique_key`, then finalize with `merge_policy`.
- `REPLACE`: delete target rows matching `sink.strategy.custom_predicate`, then insert staged rows.
- `PARTITION_REPLACE`: load a complete partition slice into staging, then replace target rows whose partition column appears in staging. Exact native `date` columns use a distinct, sargable equality join; other types retain canonical, collation-independent identity matching.

MSSQL `merge_policy: auto` resolves to `delete_insert`.

## Physical design

Use [Physical design](physical-design.md) for MSSQL target DDL controls:

```yaml
sink:
  type: mssql
  options:
    physical_design:
      apply: online
      columns:
        amount:
          target_type:
            mssql: decimal(18,4)
      indexes:
        primary_key: [order_id]
      storage:
        mssql:
          compression: page
          clustered_columnstore: false
```

Planned SQL includes table creation, index DDL, and optional compression DDL.
Compression rebuilds and columnstore changes are considered blocking on existing
large tables, so `dpone` requires `safe_window` or `manual_approval` before
applying them outside new-table creation.

Runbook:

1. Use `dpone schema physical-plan --manifest ... --format md`.
2. If the plan contains `DATA_COMPRESSION`, schedule a maintenance window.
3. If exact decimal/string lengths matter, use `schema_contract` plus `physical_design.columns.<column>.target_type.mssql`.
4. Keep bcp staging tables simple; apply business constraints to final tables through governed DDL only.

```sql
BEGIN TRANSACTION;

DELETE t
FROM [landing].[orders] AS t
WHERE EXISTS (
    SELECT 1
    FROM [staging].[orders_stg] AS s
    WHERE s.[id] = t.[id]
);

INSERT INTO [landing].[orders] ([id], [amount], ...)
SELECT [id], [amount], ...
FROM [staging].[orders_stg];

COMMIT;
```

Use `merge_policy: shadow_swap` when reader stability is more important than storage/IO cost:

```sql
CREATE TABLE [landing].[orders__dpone_shadow_ab12cd34] (...);

INSERT INTO [landing].[orders__dpone_shadow_ab12cd34]
SELECT *
FROM [landing].[orders] AS t
WHERE NOT EXISTS (
    SELECT 1 FROM [staging].[orders_stg] AS s WHERE s.[id] = t.[id]
);

INSERT INTO [landing].[orders__dpone_shadow_ab12cd34]
SELECT * FROM [staging].[orders_stg];

EXEC sp_rename N'landing.orders', N'orders__dpone_backup_ab12cd34';
EXEC sp_rename N'landing.orders__dpone_shadow_ab12cd34', N'orders';
-- table-level GRANT/DENY from backup are copied onto the new target object before backup drop
DROP TABLE [landing].[orders__dpone_backup_ab12cd34];
```

Shadow swap preserves **table-level** SQL permissions (GRANT/DENY on the object) by copying them from the backup object onto the promoted shadow table before the backup is dropped. It does **not** recreate governed DDL that lives outside dpone-managed columns:

- foreign keys and inbound references
- non-clustered indexes not implied by the load schema
- triggers, check constraints, extended properties

For certification tables, keep governed DDL in schema migration / DBA-owned scripts and treat dpone `schema_evolution: additive` as column-level only. When reader stability matters less than permission-safe in-place mutation, use `merge_policy: delete_insert` instead of `shadow_swap`.

For the currently supported `partition_replace` fallback, opt out of native
metadata probes explicitly:

```yaml
sink:
  strategy:
    mode: partition_replace
    partition:
      column: business_date
      native_mode: fallback
      max_partitions_per_run: 32
```

`native_mode: fallback` normalizes to `native: false`; it does not query SQL
Server partition metadata or emit an unavailable-SWITCH warning. Native
`SWITCH` authoring hints and `native_mode: required` remain rejected before I/O
because that route is not live-certified. Do not add switch-out tables or
partition-function hints to bypass this gate.

When the validated target and native staging type for `partition.column` is
exactly `date`, the fallback finalizer deduplicates the staging identities and
keeps the target predicate sargable:

```sql
DELETE t
FROM [landing].[orders] AS t
INNER JOIN (
    SELECT DISTINCT s.[business_date]
    FROM [staging].[orders_stg] AS s
) AS p ON p.[business_date] = t.[business_date];
```

The target and staging physical types and collations are validated before this
statement. Other partition types continue to use the canonical
collation-independent identity expression. Runtime decision evidence records
`mssql.partition_replace.finalizer` with one of these selected values:

- `typed_sargable_distinct_join`;
- `canonical_identity_fallback`.

The replacement scope still comes from values present in staging. If an
expected partition becomes completely empty, staging contains no identity for
it, so this mode cannot delete the old target rows for that partition. Use an
explicit bounded replacement authority once the route supports expected
partition lists; do not treat `values_from_staging` as proof of an empty
partition.

Native `ALTER TABLE ... SWITCH PARTITION` can be certified only when all of
these prerequisites are proven together:

1. Target and staging are aligned to the same partition function.
2. The target has no unsupported secondary indexes for automatic switching.
3. Each `switch_out_table_template` table exists, is empty, and is aligned to the same partition metadata.
4. `max_partitions_per_run` caps the number of partitions switched in one run.
5. Route-live evidence proves atomicity, retry behavior, physical-design
   compatibility, and operational cleanup before the public authoring gate is
   widened.

MSSQL uses bcp character files by default. For high-throughput Postgres to MSSQL loads, set `source.options.export_format: mssql-delimited`, `source.options.batch_commit_mode: whole`, and `compress_export: false`. This path uses PostgreSQL `COPY TO STDOUT` to write a bcp-friendly UTF-8 file and avoids Python row-by-row serialization.

### Production-safe bcp text encoding

`bcp -c` is delimiter-based and does not implement PostgreSQL `COPY` NULL markers
or RFC-style CSV quoting. `dpone` therefore treats MSSQL bulk files as a native
transport with an explicit reversible text codec whenever the target is MSSQL:

| Source value | Bulk file representation | Target value after staging commit |
| --- | --- | --- |
| `NULL` | Empty bcp field | `NULL` |
| Empty string `""` | `\x1dE` marker | Empty string `""` |
| Tab | `\x1dT` | Tab |
| LF newline | `\x1dN` | LF newline |
| CR newline | `\x1dR` | CR newline |
| Codec marker prefix `\x1d` | `\x1dP` | `\x1d` |

This means `NULL` and `""` are never allowed to collapse into the same target
value on MSSQL loads. Python-generated streaming/in-memory bulk files apply the
codec before writing files. Native Postgres `COPY` and MSSQL `bcp queryout`
paths encode text columns in SQL before export, then MSSQL sink strategies decode
them with set-based `INSERT ... SELECT` expressions when committing from staging
to the final/shadow table.

The MSSQL-target codec is intentionally applied only when the sink is MSSQL.
MSSQL source exports that feed ClickHouse direct TSV use a separate
ClickHouse-compatible TabSeparated codec: `NULL` is emitted as `\N`, backslashes
are escaped, and tab/newline/carriage-return characters become `\t`, `\n`, and
`\r` before the file reaches `clickhouse-client` or HTTP streaming.

Do not rely on CSV quotes to protect tabs or newlines in bcp files. If a custom
file producer bypasses dpone's codec, it must either produce values that do not
contain the configured field/row terminators or use a compatible lossless
encoding layer before `bcp in`.

`dpone` also makes the `bcp in` null contract explicit by passing `-k`
(`KEEPNULLS`) for MSSQL imports. Target tables should still keep defaults out of
staging columns; defaults belong in final-table business logic, not in transport
tables.

Passwords are redacted in diagnostics. For SQL authentication, `dpone` avoids
putting the password in `bcp` argv by default and sends it through the bcp prompt
stdin. If your platform-specific `bcp` build cannot prompt on stdin, use trusted
authentication or set an explicit wrapper policy in your environment after
assessing process-list exposure.

Character bcp is disabled by default for MSSQL-only types that are not safe as
delimiter text: `binary`, `varbinary`, `image`, `rowversion`/`timestamp`,
`sql_variant`, `hierarchyid`, `geometry`, and `geography`. Use
`mssql_bcp_file_format: native` for MSSQL-native transfer artifacts, or set
`allow_unsafe_raw_mssql_bulk_files: true` only for a manually certified file
producer.

### MSSQL -> ClickHouse BCP native decoder

For high-throughput MSSQL -> ClickHouse transfers, use typed binary native wire
with SQL Server `bcp queryout -n`:

```yaml
source:
  type: mssql
  options:
    extract_mode: bcp_queryout
    bulk:
      mode: bcp
      bcp:
        file_format: native
    native_transfer:
      wire:
        mode: typed_binary
        source_native_format: bcp_native
        binary_format: native
        block_rows: 65536
        block_bytes: 64MiB
        acceleration:
          mode: auto
sink:
  type: clickhouse
  options:
    clickhouse_bulk:
      mode: http
      ingest_contract: typed_binary_staging
```

The route is `mssql_bcp_native_to_clickhouse_native`. MSSQL exports only the
projected columns and predicates; dpone decodes the native BCP artifact and
streams ClickHouse `Native` columnar blocks into staging. Runtime evidence
records the schema hash, query hash, native layout hash, sink binary evidence,
native acceleration evidence, rows, bytes, checksum, and fallback or blocker
codes without secrets or data values. Set `binary_format: rowbinary` to use the
v0.26 RowBinary route.

`acceleration.mode: auto` keeps weak-worker safety: it selects the certified
optional `dpone[accel]` fused provider for supported layouts, and otherwise
records a Python reference fallback in `dpone plan` and runtime evidence. Use
`acceleration.mode: required` when a release route must not run without the
native provider.

The first certified type set covers primitive numeric, decimal, date/time, text,
binary, and `uniqueidentifier` columns. `sql_variant`, XML, CLR/UDT, `hierarchyid`,
`geography`, `geometry`, and legacy `text`/`ntext`/`image` fail closed or fall
back to another certified route. Unicode-native `bcp -N` is intentionally
blocked on non-Windows runners; Linux/KPO releases use `-n` plus the native wire
layout contract.

For large `FULL_REFRESH` loads, `dpone` uses a staging-first shadow swap. Files are bulk-loaded into a staging table, copied into a shadow target table, and committed with a short metadata rename. Direct target `bcp` and target `TRUNCATE` are intentionally not used as the default production path.

For `INCREMENTAL_MERGE`, `merge_policy: delete_insert` is the default because it is the fastest set-based MSSQL upsert path for bounded deltas. Use `merge_policy: shadow_swap` when you need the old rebuild/swap behavior and can afford the extra storage and IO.

For `REPLACE`, dpone keeps the shadow-table finalization path for bounded predicate windows. Existing readers should point at the canonical table name.

For encrypted ODBC Driver 18 connections, keep `bulk.bcp.packet_size: 16384` unless you have tested a larger value against your driver/server combination. Larger packet sizes can fail with SSL packet-size errors on macOS/Linux clients.

## Schema evolution

MSSQL sink participates in automatic schema evolution. Before bcp/staging load,
dpone reads `INFORMATION_SCHEMA.COLUMNS`, applies safe `ALTER TABLE` statements,
and only then creates the staging table.

```yaml
sink:
  type: mssql
  options:
    schema_evolution:
      enabled: true
      mode: widening
      on_type_change: new_column
```

See [Schema evolution](schema-evolution.md) for schema drift behavior,
[Type mapping matrix](type-mapping-matrix.md) for type conversion policy, and
[Source -> sink matrix](source-sink-matrix.md) for pair-specific runbooks.

Most-used MSSQL paths:

- [PostgreSQL -> MSSQL](source-sink/postgres-to-mssql.md)
- [MSSQL -> MSSQL](source-sink/mssql-to-mssql.md)
- [MSSQL -> ClickHouse](source-sink/mssql-to-clickhouse.md)
- [MSSQL -> PostgreSQL](source-sink/mssql-to-postgres.md)
- [MSSQL -> Kafka](source-sink/mssql-to-kafka.md)
- [Kafka -> MSSQL](source-sink/kafka-to-mssql.md)
- [REST API -> MSSQL](source-sink/api-to-mssql.md)

## Physical deletes and reconciliation

`dpone` supports physical-delete propagation for SQL Server sources through snapshot reconciliation. On each reconciliation run, the runtime reads the configured `unique_key` columns from the source table, writes the key snapshot into the centralized reconciliation tech tables, compares it with the previous snapshot, and applies a soft delete in the target table for keys that disappeared from the source.

For `source.type: mssql`, the snapshot read uses `MSSQLConnector.build_select_query(...)` and streaming `pyodbc` reads. For `sink.type: mssql`, the delete application uses parameterized SQL Server `UPDATE` statements:

```sql
UPDATE [landing].[orders]
SET [__dpone__deleted_at] = SYSUTCDATETIME(),
    [__dpone__loaded_at] = SYSUTCDATETIME()
WHERE [__dpone__deleted_at] IS NULL
  AND [id] IN (?, ?, ?)
```

Composite keys are supported with parameterized `OR` groups. Large delete batches are chunked below SQL Server's 2100-parameter limit.

Recommended manifest shape:

```yaml
source:
  type: mssql
  connection_type: vault
  connection_id: mssql-source
  vault_path: mssql/demo-source
  table: {schema: dbo, name: orders}

sink:
  type: mssql
  connection_type: vault
  connection_id: mssql-dwh
  vault_path: mssql/demo-dwh
  table: {schema: landing, name: orders}
  strategy:
    mode: incremental_merge
    unique_key: [id]
  reconciliation:
    enabled: true

state:
  type: mssql
  connection_id: mssql-dwh
  connection_type: vault
  vault_path: mssql/demo-dwh
```

Operational notes:

- Reconciliation is snapshot-based, so it catches physical deletes even when the source has no CDC or tombstone column.
- The target table must contain `__dpone__deleted_at` and `__dpone__loaded_at`; normal dpone sink strategies manage these technical columns.
- Tech reconciliation snapshots are currently centralized in BigQuery. Use MSSQL CDC or Change Tracking readers when you need near-real-time delete events without full key snapshots.

## Local live gate and stress testing

Install the local Microsoft tools on macOS:

```bash
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
HOMEBREW_ACCEPT_EULA=Y brew install unixodbc msodbcsql18 mssql-tools18
```

Run the optional MSSQL integration test against a local SQL Server endpoint:

```bash
DPONE_IT_MSSQL_HOST=127.0.0.1 \
DPONE_IT_MSSQL_PORT=15433 \
DPONE_IT_MSSQL_DATABASE=dpone \
DPONE_IT_MSSQL_USER=sa \
DPONE_IT_MSSQL_PASSWORD='Dpone_Strong_12345!' \
DPONE_IT_MSSQL_TRUST_SERVER_CERTIFICATE=yes \
DPONE_IT_MSSQL_BCP_PATH=/opt/homebrew/bin/bcp \
uv run pytest -m integration_mssql -q
```

Run the reproducible local stress harness:

```bash
PYTHONUNBUFFERED=1 uv run python tools/mssql_stress.py \
  --rows 1000000 \
  --batch-size 100000 \
  --bcp-path /opt/homebrew/bin/bcp
```

The stress harness exercises the real runtime path:

- Postgres synthetic source generation.
- Postgres `COPY TO STDOUT` to `mssql-delimited` artifact.
- MSSQL staging-table `bcp in` followed by shadow-table commit for full refresh.
- MSSQL `bcp queryout`.
- ClickHouse native insert batches.

Observed local benchmark on Apple Silicon Docker Desktop with official SQL Server 2022 `linux/amd64` image:

| Flow | Rows | Time | Throughput |
| --- | ---: | ---: | ---: |
| Postgres synthetic source preparation | 15,000,000 | 26.946s | 556,667 rows/s |
| Postgres to MSSQL full refresh | 15,000,000 | 96.580s | 155,311 rows/s |
| MSSQL to ClickHouse full refresh | 15,000,000 | 124.653s | 120,333 rows/s |

The same workload should be benchmarked again on native x86_64 Linux before using these numbers as production capacity planning inputs.

## Performance roadmap

Parallel PostgreSQL extraction requires a shared MVCC snapshot coordinator
before it can be production-safe. Until that capability is implemented and
certified, PostgreSQL -> MSSQL uses one `COPY` statement. The future design may
use Spark JDBC-like range decomposition while preserving one snapshot:

- `partition_column`: monotonic numeric/date column used to split source scans.
- `lower_bound` / `upper_bound`: explicit scan bounds for reproducibility.
- `num_partitions`: number of parallel extract workers.
- `partition_stride`: optional fixed-width partition size.
- Per-partition artifacts loaded into independent staging tables, then finalized with a single set-based target operation.

Additional high-impact optimizations:

- Use native `bcp` into staging tables, then commit destructive strategies with shadow-table swaps.
- Keep direct target operations limited to short metadata swaps or append commits from staging.
- Add native ClickHouse file ingestion for MSSQL exported TSV/CSV to reduce Python row coercion overhead.
- Support `bcp queryout` partition fan-out for MSSQL sources.
- Add resumable partition manifests so failed large loads restart from the failed partition, not from zero.

### PostgreSQL -> MSSQL target tuning checklist

Benchmarks usually show PostgreSQL `COPY` is faster than SQL Server load and
finalization. When `postgres_to_mssql.target_load_finalize` is the bottleneck,
tune the target side first:

| Symptom | Likely bottleneck | Action |
| --- | --- | --- |
| `postgres_to_mssql.source_export` slow | PostgreSQL scan/COPY | Tune the bounded predicate and projection; check source indexes, source IO, and network throughput. |
| `postgres_to_mssql.target_load_finalize` slow | SQL Server bcp/finalizer/log | Tune `bulk.bcp.batch_size`, `bulk.bcp.table_lock`, log throughput, staging indexes and finalizer policy. |
| Lock waits during finalizer | Target mutation | Try `shadow_swap`, smaller partitions, or safe-window execution. |
| bcp rejects rows | File/type mismatch | Inspect the bcp error file and the Postgres -> MSSQL type mapping matrix. |

## Troubleshooting

- `pyodbc is required`: install `dpone[mssql]` and the Microsoft ODBC Driver.
- `bcp failed`: check `bcp_path`, network access, SQL login permissions, and whether the command works outside dpone.
- `Copy direction must be either 'in', 'out' or 'format'`: use dpone with the fixed bcp command order; raw bcp commands must follow `bcp <table-or-query> in|queryout <file> [options]`.
- `Text column data incomplete`: verify bcp terminators are escaped as `\t` and `\n`, not literal control characters in shell arguments.
- Empty strings become `NULL`: ensure the file was produced through dpone's MSSQL
  bulk text codec. Raw third-party TSV files cannot distinguish empty string
  from `NULL` in SQL Server bcp character mode.
- Tabs/newlines split rows or columns: use dpone-generated `mssql-delimited`
  artifacts or enable a compatible producer-side text codec before loading.
- `bcp` asks for a password and hangs: verify your installed `bcp` supports stdin
  prompt input. Prefer trusted auth where available, or use an environment-local
  wrapper after assessing process-list exposure.
- `MSSQL character bcp is unsafe`: switch MSSQL-to-MSSQL native transfer to
  `source.options.bulk.bcp.file_format: native`, or explicitly certify and
  allow your raw producer with `allow_unsafe_raw_mssql_bulk_files: true`.
- `SSL Provider: Packet size too large`: lower `bulk.bcp.packet_size` to `16384`.
- Decimal truncation: ensure Postgres numeric precision/scale are preserved, for example `numeric(18,2)`.
- Unicode issues: use ODBC Driver 18 and bcp code page `65001`; for older SQL Server/client combinations, prefer NVARCHAR columns.
- Local Docker: set `trust_server_certificate: yes` because the container certificate is self-signed.

## PostgreSQL -> MSSQL consistent snapshot mode

This route intentionally uses one PostgreSQL `COPY` statement. Independent
partition exports do not share an MVCC snapshot and are rejected before source
row I/O. Keep source partitions and export workers at one; tune native BCP and
the set-based finalizer from measured phase evidence. See
[Performance](performance.md) for safe SLO commands and benchmark profiles.

## MSSQL -> ClickHouse fast ingest

For MSSQL -> ClickHouse, use `clickhouse_bulk.mode: http` when the ClickHouse HTTP endpoint is reachable. This streams `bcp queryout` TSV files directly into ClickHouse with `INSERT ... FORMAT TabSeparated` and avoids Python row parsing.

```yaml
sink:
  type: clickhouse
  options:
    clickhouse_bulk:
      mode: http
      http:
        host: clickhouse.example.com
        port: 8123
```

See [Performance](performance.md) for local benchmark commands and verified 15M results.

For delimiter-safe high-throughput snapshots, v0.46 also supports an
Altinity-class pipe route:

```yaml
source:
  type: mssql
  options:
    native_transfer:
      wire:
        mode: typed_raw
        delimiter_profile: ascii_control
      snapshot:
        streaming:
          mode: required
          provider: bcp_pipe
          pipe_mode: fifo
          read_buffer_bytes: 4MiB
          cleanup_policy: eager
          delimiter_safety: advisory

sink:
  type: clickhouse
  options:
    clickhouse_bulk:
      mode: client
      ingest_contract: typed_raw_streaming_staging
      streaming:
        format: CustomSeparated
        async_insert: true
        wait_for_async_insert: true
```

This route starts one `bcp queryout -c`, streams the FIFO into
`clickhouse-client INSERT ... FORMAT CustomSeparated`, and keeps ClickHouse
staging/finalization governance. Use `delimiter_safety: certified_only` for
production unless a workload-level safety probe/certification exists; it blocks
uncertified streaming before source IO. Use `advisory` only for benchmarks or
explicitly accepted workloads with count/hash reconciliation.

`read_buffer_bytes` is the upper bound requested from the BCP FIFO on each
read. It defaults to `4MiB` and accepts `64KiB` through `16MiB`. It does not
create files, set ClickHouse block sizes, or replace the generic
`native_transfer.execution.transport.stream_buffer_bytes` budget. String
values use whole `KiB` or `MiB` units; integer values are bytes. The old
streaming-only name `target_chunk_bytes` remains accepted for one dedicated
deprecation release, keeps the historical effective 4 MiB buffer, and emits
`streaming_target_chunk_bytes_deprecated_use_read_buffer_bytes`. When both are
present, `read_buffer_bytes` wins.
Malformed legacy-only values fail with
`streaming_target_chunk_bytes_invalid`; a malformed legacy value is ignored
only when a valid canonical field is also present. FIFO consumers observe the
BCP process without a blocking open, so an early BCP failure cannot leave the
reader waiting indefinitely.

## Native queryout for MSSQL -> ClickHouse

For MSSQL -> ClickHouse, prefer `source.options.extract_mode: bcp_queryout`.
Use nested `source.options.partitioning` only when the boundary column has a
useful index/statistics path. dpone resolves `bounds: auto` with typed MSSQL
metadata, exports every safe partition through `bcp queryout`, and hands file
artifacts to the ClickHouse sink for staging-first loading.

Key MSSQL options:

```yaml
source:
  options:
    extract_mode: bcp_queryout
    bulk:
      mode: bcp
      bcp:
        batch_size: 100000
        packet_size: 65535
        timeout_seconds: 3600
    partitioning:
      strategy: auto
      column: order_id
      bounds: auto
      target_rows_per_partition: 1000000
      max_partitions: 64
      export_workers: 8
```

For heap/no-index tables or views, do not use range partitioning merely to make
files smaller. It can multiply full scans. Use one source scan with physical
chunks instead:

```yaml
source:
  options:
    extract_mode: bcp_queryout
    native_transfer:
      snapshot:
        scan:
          mode: auto
          heap_policy: single_scan_chunks
          require_index_for_range: true
        physical_chunking:
          mode: auto
          target_chunk_bytes: 64MiB
          max_chunk_bytes: 128MiB
          cleanup_policy: eager
```

Runbook: if the source is overloaded, reduce `export_workers`; if artifacts are
too small on an indexed route, increase `target_rows_per_partition`; if bounds
are skewed, choose a more uniformly distributed indexed partition column. If
`perf advise` emits `source_heap_range_parallelism_blocked`, keep
`single_scan_chunks` until a real boundary index exists.

For physical chunks, `target_chunk_bytes` is a soft target and
`max_chunk_bytes` is a hard per-file limit. dpone seals the current file before
the next complete row would exceed the maximum. A single row larger than the
maximum is not split: the run fails with
`physical_chunk_row_exceeds_max_bytes`, terminates BCP, and removes the FIFO and
incomplete file. Increase the maximum only after checking worker disk capacity
and the source row shape.

When the source query itself is expensive (for example a view-over-view or
procedure-prepared reporting result), materialize the projected query once into
an MSSQL work table and let the export optimizer read from that table:

```yaml
source:
  options:
    native_transfer:
      snapshot:
        materialization:
          mode: auto
          provider: mssql_work_table
          allow_source_writes: true
          work_connection_ref: mssql_dpone_snapshot_work
          cleanup_policy: eager
          cleanup:
            lock_timeout_ms: 5000
            defer_on_lock_timeout: true
          index:
            mode: auto
            boundary_columns: auto
```

`allow_source_writes` is required because dpone creates and drops a resolved
`[work_database].[work_schema].[__dpone_snapshot_<run_id>]`. Prefer
`work_connection_ref` for a promoted workload: the verified environment
registry supplies database and schema atomically, and dpone rejects a different
SQL Server endpoint. Explicit `work_database` and `work_schema` remain a
backward-compatible alternative and cannot be mixed with the logical ref.
Without either database selector, the existing two-part work-table name uses
the source connection's current database.
Use `benchmark_only` first when you need a DBA review of required grants,
estimated footprint and expected speedup without changing the runtime route.

The MSSQL work-table drop uses `SET LOCK_TIMEOUT` by default. A busy work
database should return deferred cleanup evidence such as
`mssql_cleanup_lock_timeout` instead of letting a `DROP TABLE` hold metadata
locks and block other DWH workloads. Increase `lock_timeout_ms` only when the
source DBA accepts the lock impact.
