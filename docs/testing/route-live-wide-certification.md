# Route live wide-type and full-strategy certification

This is the mandatory bar for claiming **Batch ETL supported** (or stronger) on
any source→sink route.

## Required evidence

1. **Wide typed fixture** covering the pair type profile’s round-tripable types
   (not a 3-column smoke table).
2. **Every load strategy** declared supported for that sink, each with semantic
   assertions (not only `COUNT(*)`).
3. Contract-required / unsupported types remain hermetic fail-closed tests and
   are listed as N/A for live with a reason.
4. SKIP/UNVERIFIED when environment or billing is missing — never report as PASS.

## Bounded execution and diagnostics

The Docker certification job retains its 60-minute hard budget. Pytest runs
under a 45-minute Linux process-tree supervisor, stops after the first failed
case, and leaves time for JUnit validation, disposable-service cleanup, and
artifact upload. The supervisor is a child subreaper and tracks immutable
`(pid, starttime)` identities through `/proc`; pidfds let it terminate and reap
ordinary descendants as well as BCP/process lanes that called `setsid()`.
Exit `124` means the deadline expired and quiescence was proved; exit `125`
means containment could not be proved and is always a failure. If pytest exits
while a descendant remains, the tree is reaped immediately and pytest's own
status is preserved. A green run still executes the complete selected matrix;
fail-fast applies only after certification is already invalid.

The supervisor invokes pytest through the exact checked-out virtual-environment
interpreter. Production-hydration helpers resolve the `dpone` console script
from that interpreter's install scheme instead of consulting ambient `PATH`,
verify its installed entry-point metadata, enforce a finite render timeout, and
require the exact catalog-version header. This preflight completes before any
vendor connection is opened or disposable database is created. A global,
missing, non-executable, stale, hung, or malformed CLI therefore cannot change
the proof.

CI writes `pytest-progress.jsonl` after every node boundary and test phase and
flushes each record immediately. This journal is diagnostic-only and every
record carries `release_ready: false`; it cannot replace JUnit or the complete
route authority written during successful session teardown. `pytest-exit.json`
is likewise marked diagnostic-only, carries the exact commit/run/attempt, and
cannot be accepted as release evidence. Together with `junit-gate.json` it
preserves the process and fail-closed gate outcomes even when JUnit is missing
or truncated. Long reviewed matrices may additionally fsync `subcase_started`
and `subcase_finished` records around each independently asserted cell. An
unmatched start identifies the exact cell active during a native crash, but it
is still diagnostic-only and never counts as partial certification. Pytest
faulthandler emits a stack for a
single phase that exceeds 60 seconds.

Disposable PostgreSQL objects and MSSQL databases use finite cleanup timeouts.
MSSQL cleanup closes workload sessions first, retries with fresh bounded admin
sessions, and requires a `DB_ID(...) IS NULL` catalog readback. An admin-session
close error also leaves quiescence unproven even when the database is absent.
A cleanup that cannot prove both absence and session closure fails the case; it
is never silently suppressed. The
workflow also bounds `docker compose down -v`, so a broken teardown cannot
consume the whole job without diagnostics.

## mysql→BigQuery reference implementation

| Asset | Path |
|---|---|
| Feature design | `docs/feature-design-route-live-wide-certification-v1.md` |
| Shared MySQL gates | `tests/integration/mysql/mysql_live_support.py` |
| Wide fixture | `tests/integration/mysql/mysql_wide_type_fixtures.py` |
| Strategy configs | `tests/integration/mysql/mysql_bigquery_strategy_configs.py` |
| Assertions | `tests/integration/mysql/mysql_bigquery_assertions.py` |
| Live suite | `tests/integration/mysql/test_mysql_to_bigquery_vendor_live_integration.py` |

Run:

```bash
docker compose -f docker/docker-compose.integration.yml up -d mysql
export DPONE_RUN_INTEGRATION=1 DPONE_RUN_INTEGRATION_LIVE=1
export BIGQUERY_DWH_PROJECT_ID=<project>
export BIGQUERY_DWH_SERVICE_ACCOUNT_KEY_FILE=/absolute/path/sa.json
uv run pytest tests/integration/mysql/test_mysql_to_bigquery_vendor_live_integration.py -q
```

## postgres→BigQuery reference implementation

| Asset | Path |
|---|---|
| Feature design | `docs/feature-design-postgres-route-live-wide-certification-v1.md` |
| Shared Postgres gates | `tests/integration/postgres/postgres_live_support.py` |
| Wide fixture | `tests/integration/postgres/postgres_bigquery_wide_fixtures.py` |
| Strategy configs | `tests/integration/postgres/postgres_bigquery_strategy_configs.py` |
| Assertions | `tests/integration/postgres/postgres_bigquery_assertions.py` |
| Live suite | `tests/integration/postgres/test_postgres_to_bigquery_vendor_live_integration.py` |
| Type profile | `postgres_to_bigquery_analytics_v1` |

Run:

```bash
docker compose -f docker/docker-compose.integration.yml up -d postgres
export DPONE_RUN_INTEGRATION=1 DPONE_RUN_INTEGRATION_LIVE=1
export BIGQUERY_DWH_PROJECT_ID=<project>
export BIGQUERY_DWH_SERVICE_ACCOUNT_KEY_FILE=/absolute/path/sa.json
export DPONE_IT_BQ_DATASET=dpone_it_postgres
uv run pytest tests/integration/postgres/test_postgres_to_bigquery_vendor_live_integration.py -q
```

## postgres→Postgres reference implementation

| Asset | Path |
|---|---|
| Feature design | `docs/feature-design-postgres-route-live-wide-certification-v1.md` |
| Shared Postgres gates | `tests/integration/postgres/postgres_live_support.py` |
| Wide fixture | `tests/integration/postgres/postgres_postgres_wide_fixtures.py` |
| Strategy configs | `tests/integration/postgres/postgres_postgres_strategy_configs.py` |
| Assertions | `tests/integration/postgres/postgres_postgres_assertions.py` |
| Live suite | `tests/integration/postgres/test_postgres_to_postgres_vendor_live_integration.py` |
| Type profile | `postgres_to_postgres_identity_v1` |

Run:

```bash
docker compose -f docker/docker-compose.integration.yml up -d postgres
export DPONE_RUN_INTEGRATION=1 DPONE_RUN_INTEGRATION_LIVE=1
uv run pytest tests/integration/postgres/test_postgres_to_postgres_vendor_live_integration.py -q
```

## postgres→MSSQL reference implementation

| Asset | Path |
|---|---|
| Feature design | `docs/feature-design-postgres-route-live-wide-certification-v1.md` |
| Shared Postgres gates | `tests/integration/postgres/postgres_live_support.py` |
| Wide fixture | `tests/integration/postgres/postgres_mssql_wide_fixtures.py` |
| Strategy configs | `tests/integration/postgres/postgres_mssql_strategy_configs.py` |
| Assertions | `tests/integration/postgres/postgres_mssql_assertions.py` |
| Live suite | `tests/integration/postgres/test_postgres_to_mssql_vendor_live_integration.py` |
| XMin + key reconciliation | `tests/integration/postgres/test_postgres_xmin_mssql_snapshot_reconciliation_live.py` |
| Schema-evolution matrix | `tests/integration/postgres/test_postgres_mssql_schema_evolution_matrix_live.py` |
| Physical-design matrix | `tests/integration/postgres/test_postgres_mssql_physical_design_matrix_live.py` |
| Target-behaviour matrix | `tests/integration/postgres/test_postgres_mssql_target_behavior_live.py` |
| Wide performance soak | `tests/integration/postgres/test_postgres_mssql_wide_performance_soak_live.py` |
| Type profile | `postgres_to_mssql_native_v2` |

Run:

```bash
docker compose -f docker/docker-compose.integration.yml up -d postgres mssql
export DPONE_RUN_INTEGRATION=1 DPONE_RUN_INTEGRATION_LIVE=1
uv run pytest \
  tests/integration/postgres/test_postgres_to_mssql_vendor_live_integration.py \
  tests/integration/postgres/test_postgres_xmin_mssql_snapshot_reconciliation_live.py \
  tests/integration/postgres/test_postgres_mssql_schema_evolution_matrix_live.py \
  tests/integration/postgres/test_postgres_mssql_physical_design_matrix_live.py \
  tests/integration/postgres/test_postgres_mssql_target_behavior_live.py \
  tests/integration/postgres/test_postgres_mssql_wide_performance_soak_live.py \
  -q -rs
```

`bytea`/`varbinary` uses hex character BCP + `CONVERT(varbinary, …, 2)`
(see `docs/feature-design-mssql-hex-binary-character-bcp-v1.md`).

The route fixture contains exactly 128 columns and covers every case in the
authoritative PostgreSQL→MSSQL type suite, including explicit array, enum and
range fallbacks. The batch capability boundary executes six supported MSSQL
strategies. Its `incremental_append` and legacy column-based
`incremental_merge` legs are explicit pre-I/O fail-closed cases because
target-derived `MAX + >` is not an atomic PostgreSQL boundary. Safe
incremental merge is certified by the XMin + same-snapshot key-reconciliation
suite. `cdc_apply` remains a separate fail-closed case because the MSSQL batch
sink has no CDC apply adapter.

Schema evolution runs the complete 4,608-case Cartesian product of the public
table/column/data-type/DDL/change-behaviour enums across nullable add,
generated compatibility column and type widen changes, including both
`apply_safe` values. A separate 96-case
matrix covers missing-target creation. Physical design runs every valid
compression/key/fillfactor combination, every policy-enum combination, and all
NONE/ROW/PAGE existing-table transitions with exact and invalid approvals.
Unsupported or non-apply combinations must preserve both `sys.*` catalog state
and business rows; skips do not count as evidence.

The target-behaviour matrix provisions real temporal, ledger,
memory-optimized, graph, trigger and inbound-FK objects. It certifies the five
key-preserving/ordinary combinations that may mutate and proves every other
combination is rejected before PostgreSQL COPY. FileTable is handled as an
explicit pinned-vendor capability boundary: the release image is SQL Server
2022 on Linux with `FilestreamEffectiveLevel = 0`, so a FileTable catalog
object cannot be provisioned there. The three reviewed FileTable cells prove
that platform fact directly; the runtime's hypothetical FileTable blocker is
additionally covered by catalog-unit tests and is not presented as a live
object observation.

The finite wide soak runs one warmup and seven separately observed mutations
for each of the eight MSSQL strategies. Every run uses 10,000 PostgreSQL rows
and the canonical 128-column catalog. It asserts the exact keyset, exact
physical type/length/precision/scale/nullability/collation catalog, all 128
values on both full boundary rows, zero retained staging objects and zero
retained transfer files. Interior rows remain deliberately sparse so the soak
measures route behavior instead of repeatedly decoding identical LOB
sentinels. The source is an explicit complete-relation snapshot test adapter;
this suite makes no production column-cursor or multi-worker backfill claim.

## postgres→ClickHouse reference implementation

| Asset | Path |
|---|---|
| Feature design | `docs/feature-design-postgres-route-live-wide-certification-v1.md` |
| Shared Postgres gates | `tests/integration/postgres/postgres_live_support.py` |
| Wide fixture | `tests/integration/postgres/postgres_clickhouse_wide_fixtures.py` |
| Strategy configs | `tests/integration/postgres/postgres_clickhouse_strategy_configs.py` |
| Assertions | `tests/integration/postgres/postgres_clickhouse_assertions.py` |
| Live suite | `tests/integration/postgres/test_postgres_to_clickhouse_vendor_live_integration.py` |
| Type profile | `postgres_to_clickhouse_analytics_v1` |

Run:

```bash
docker compose -f docker/docker-compose.integration.yml up -d postgres clickhouse
export DPONE_RUN_INTEGRATION=1 DPONE_RUN_INTEGRATION_LIVE=1
uv run pytest tests/integration/postgres/test_postgres_to_clickhouse_vendor_live_integration.py -q
```

`snapshot_diff` / `scd2` use staging-first ClickHouse finalizers
(`docs/feature-design-clickhouse-snapshot-diff-scd2-v1.md`).

## mssql→ClickHouse reference implementation

| Asset | Path |
|---|---|
| Feature design | `docs/feature-design-mssql-route-live-wide-certification-v1.md` |
| Shared MSSQL gates | `tests/integration/mssql/mssql_live_support.py` |
| Wide fixture | `tests/integration/mssql/mssql_clickhouse_wide_fixtures.py` |
| Strategy configs | `tests/integration/mssql/mssql_clickhouse_strategy_configs.py` |
| Assertions | `tests/integration/mssql/mssql_clickhouse_assertions.py` |
| Live suite | `tests/integration/mssql/test_mssql_to_clickhouse_vendor_live_integration.py` |
| Type profile | `mssql_to_clickhouse_lossless_v2` |

Run:

```bash
docker compose -f docker/docker-compose.integration.yml up -d mssql clickhouse
export DPONE_RUN_INTEGRATION=1 DPONE_RUN_INTEGRATION_LIVE=1
uv run pytest tests/integration/mssql/test_mssql_to_clickhouse_vendor_live_integration.py -q
```

`snapshot_diff` / `scd2` use staging-first ClickHouse finalizers
(`docs/feature-design-clickhouse-snapshot-diff-scd2-v1.md`). Phase A1 of
`docs/feature-design-mssql-route-live-wide-certification-v1.md`.

## mssql→Postgres reference implementation

| Asset | Path |
|---|---|
| Feature design | `docs/feature-design-mssql-route-live-wide-certification-v1.md` |
| Shared MSSQL gates | `tests/integration/mssql/mssql_live_support.py` |
| Wide fixture | `tests/integration/mssql/mssql_postgres_wide_fixtures.py` |
| Strategy configs | `tests/integration/mssql/mssql_postgres_strategy_configs.py` |
| Assertions | `tests/integration/mssql/mssql_postgres_assertions.py` |
| Live suite | `tests/integration/mssql/test_mssql_to_postgres_vendor_live_integration.py` |
| Type profile | `mssql_to_postgres_native_v1` |

Run:

```bash
docker compose -f docker/docker-compose.integration.yml up -d mssql postgres
export DPONE_RUN_INTEGRATION=1 DPONE_RUN_INTEGRATION_LIVE=1
uv run pytest tests/integration/mssql/test_mssql_to_postgres_vendor_live_integration.py -q
```

Postgres-safe CSV extract (not `mssql-delimited`); `varbinary` → `bytea` via
`\x` hex CSV. Phase A3 of
`docs/feature-design-mssql-route-live-wide-certification-v1.md`.

## mssql→Kafka reference implementation

| Asset | Path |
|---|---|
| Feature design | `docs/feature-design-mssql-route-live-wide-certification-v1.md` |
| Shared MSSQL gates | `tests/integration/mssql/mssql_live_support.py` |
| Wide fixture | `tests/integration/mssql/mssql_kafka_wide_fixtures.py` |
| Strategy configs | `tests/integration/mssql/mssql_kafka_strategy_configs.py` |
| Assertions | `tests/integration/mssql/mssql_kafka_assertions.py` |
| Live suite | `tests/integration/mssql/test_mssql_to_kafka_vendor_live_integration.py` |

Run:

```bash
docker compose -f docker/docker-compose.integration.yml up -d mssql kafka
export DPONE_RUN_INTEGRATION=1 DPONE_RUN_INTEGRATION_LIVE=1
uv sync --extra kafka --extra mssql
uv run pytest tests/integration/mssql/test_mssql_to_kafka_vendor_live_integration.py -q
```

`partition_replace` / `scd2` and backfill inner modes other than keyed upsert
replay are N/A for KafkaSink. Phase A4 of
`docs/feature-design-mssql-route-live-wide-certification-v1.md`.

## mssql→BigQuery reference implementation

| Asset | Path |
|---|---|
| Feature design | `docs/feature-design-mssql-route-live-wide-certification-v1.md` |
| Shared MSSQL gates | `tests/integration/mssql/mssql_live_support.py` |
| Wide fixture | `tests/integration/mssql/mssql_bigquery_wide_fixtures.py` |
| Strategy configs | `tests/integration/mssql/mssql_bigquery_strategy_configs.py` |
| Assertions | `tests/integration/mssql/mssql_bigquery_assertions.py` |
| Live suite | `tests/integration/mssql/test_mssql_to_bigquery_vendor_live_integration.py` |
| Type profile | `mssql_to_bigquery_analytics_v1` |

Run:

```bash
docker compose -f docker/docker-compose.integration.yml up -d mssql
export DPONE_RUN_INTEGRATION=1 DPONE_RUN_INTEGRATION_LIVE=1
export BIGQUERY_DWH_PROJECT_ID=<project>
export BIGQUERY_DWH_SERVICE_ACCOUNT_KEY_FILE=/absolute/path/sa.json
export DPONE_IT_BQ_DATASET=dpone_it_mssql
uv sync --extra mssql --extra gcp
uv run pytest tests/integration/mssql/test_mssql_to_bigquery_vendor_live_integration.py -q
```

CSV extract with base64 `varbinary`→BYTES; all eight BigQuery strategies.
Phase A5 of `docs/feature-design-mssql-route-live-wide-certification-v1.md`.

## mysql→Kafka reference implementation

| Asset | Path |
|---|---|
| Shared MySQL + Kafka gates | `tests/integration/mysql/mysql_live_support.py` |
| Wide fixture | `tests/integration/mysql/mysql_kafka_wide_fixtures.py` |
| Strategy configs | `tests/integration/mysql/mysql_kafka_strategy_configs.py` |
| Assertions | `tests/integration/mysql/mysql_kafka_assertions.py` |
| Live suite | `tests/integration/mysql/test_mysql_to_kafka_vendor_live_integration.py` |

Run:

```bash
docker compose -f docker/docker-compose.integration.yml up -d mysql kafka
export DPONE_RUN_INTEGRATION=1 DPONE_RUN_INTEGRATION_LIVE=1
uv run pytest tests/integration/mysql/test_mysql_to_kafka_vendor_live_integration.py -q
```

## postgres→Kafka reference implementation

| Asset | Path |
|---|---|
| Feature design | `docs/feature-design-postgres-route-live-wide-certification-v1.md` |
| Shared Postgres gates | `tests/integration/postgres/postgres_live_support.py` |
| Wide fixture | `tests/integration/postgres/postgres_kafka_wide_fixtures.py` |
| Strategy configs | `tests/integration/postgres/postgres_kafka_strategy_configs.py` |
| Assertions | `tests/integration/postgres/postgres_kafka_assertions.py` |
| Live suite | `tests/integration/postgres/test_postgres_to_kafka_vendor_live_integration.py` |

Run:

```bash
docker compose -f docker/docker-compose.integration.yml up -d postgres kafka
export DPONE_RUN_INTEGRATION=1 DPONE_RUN_INTEGRATION_LIVE=1
uv run pytest tests/integration/postgres/test_postgres_to_kafka_vendor_live_integration.py -q
```

`partition_replace` / `scd2` and backfill inner modes other than keyed upsert
replay are N/A for KafkaSink.

## Applying to other integrations

When adding or claiming a route:

1. Copy the helper split pattern (`*_live_support`, `*_wide_type_fixtures`,
   `*_strategy_configs`, `*_assertions`, thin `test_*`).
2. Map the pair type profile into a wide DDL + seed.
3. Cover every strategy in the sink factory (or document N/A).
4. Update matrix/guide only after live PASS evidence for that breadth.
5. Keep credentials out of git, logs, and artifacts.

Peer guides may already say Batch ETL from earlier narrow smoke evidence;
migrate those live suites to this wide/strategy bar before the next
certification claim bump. mysql→{bigquery,postgres,mssql,clickhouse,kafka},
postgres→{bigquery,postgres,mssql,clickhouse,kafka}, and
mssql→{clickhouse,mssql,postgres,kafka,bigquery} meet the wide bar
(mssql→CH/MSSQL binary columns use hex character BCP; mssql→Postgres/Kafka/BQ
use CSV; mssql→Postgres `varbinary` → `bytea` via `\x` hex; mssql→BigQuery
`varbinary` → BYTES via base64 CSV; ClickHouse includes `snapshot_diff`/`scd2`;
Kafka excludes `partition_replace`/`scd2` and backfill inner modes other than
keyed upsert replay as documented N/A). Phase A of
`docs/feature-design-postgres-route-live-wide-certification-v1.md` is complete;
Phase B1 closes the mssql binary character-BCP N/A; Phase B2 closes ClickHouse
`snapshot_diff`/`scd2`. **Phase A of
`docs/feature-design-mssql-route-live-wide-certification-v1.md` is complete**
(A1–A5: mssql→clickhouse, mssql→mssql, mssql→postgres, mssql→kafka,
mssql→bigquery). Phase C adds the self-service golden path and landing example
parity for mssql→sink guides
(`docs/feature-design-certified-route-recipe-golden-path-v1.md`; postgres/mysql
baseline: `docs/feature-design-certified-route-self-service-ux-v1.md`). Phase D
adds gated live CI (`docs/feature-design-route-live-wide-ci-v1.md`).

## CI (Phase D)

Workflow: `.github/workflows/route-live-wide-certification.yml`

| Trigger | Legs |
|---|---|
| `schedule` (nightly `15 3 * * *`) | Docker only: `{postgres,mysql,mssql}` × `{postgres,mssql,clickhouse,kafka}` |
| `workflow_dispatch` | Same Docker matrix (filterable via `source` / `sink`); optional BigQuery when `include_bigquery=true` |

Database ports for the PostgreSQL→MSSQL authority topology are not reserved as
fixed host numbers in CI. The workflow publishes PostgreSQL, PostGIS, both
authority nodes, and MSSQL as `127.0.0.1::CONTAINER_PORT`; Docker allocates and
binds each host port atomically. After the services are healthy,
`tools/ci/export_route_live_compose_ports.py` reads each mapping with
`docker compose port`, rejects non-IPv4-loopback, ambiguous, invalid, or
duplicate mappings, and writes the complete numeric projection to
`GITHUB_ENV`. A discovery failure leaves the environment unmodified and fails
the job before any certification test starts. Local Compose commands retain
their documented fixed defaults unless the caller explicitly overrides them.

Policy (**SKIP ≠ PASS**):

1. Preflight fail-closed (compose health + `DPONE_RUN_INTEGRATION=1`; BQ secrets
   required when BigQuery legs are requested).
2. After pytest, `tools/ci/assert_junit_executed.py` fails the job when junit is
   missing, empty, contains any skipped test, or `passed < 1`.
3. The PostgreSQL→MSSQL leg also requires parseable XMin, schema-evolution and
   physical-design evidence JSON files. An interrupted partial run cannot reuse
   stale evidence.
4. Default PR `ci.yml` still excludes `integration_live` — this workflow is
   opt-in / scheduled only.

Manual dispatch examples:

```bash
# One Docker leg
gh workflow run route-live-wide-certification.yml \
  -f source=postgres -f sink=mssql -f include_bigquery=false

# BigQuery legs (requires repo secrets BIGQUERY_DWH_PROJECT_ID +
# BIGQUERY_DWH_SERVICE_ACCOUNT_JSON)
gh workflow run route-live-wide-certification.yml \
  -f source=all -f sink=bigquery -f include_bigquery=true
```

Artifacts upload as `route-live-wide-{source}-{sink}` (junit under
`test_artifacts/route_live_wide/`). Keep credentials out of git, logs, and
artifacts.

## Strategy metadata on file exports

`snapshot_diff` and `scd2` require `__dpone__row_hash` (and SCD2 validity
columns) in staging. `StrategyMetadataEnricher` applies this for:

- `InMemoryRowsArtifact` / `StreamingRowsArtifact`
- delimited `FileExportArtifact` families (`csv`, `mssql-delimited`,
  `clickhouse-tsv`), including partitioned, batched, and physically chunked
  exports

Unsupported artifact/wire types fail closed — they must never silently skip
enrichment. Live IT should call the enricher (or `ETLProcessor`) before
`sink.load`, matching production.
