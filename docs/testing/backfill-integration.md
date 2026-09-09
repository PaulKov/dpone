# Backfill integration suite

End-to-end certification of the chunked backfill subsystem
([user guide](../backfill.md)) against real services: every case runs the full
runtime path — bounded extraction, staging, inner-strategy finalization per
chunk — and asserts row/checksum parity plus ledger invariants.

## Where it lives

| Location | Contents |
| --- | --- |
| `tests/integration/backfill/backfill_toolkit.py` | Shared seed spec, chunk-window builders, LoadConfig builder, parity/ledger assertions |
| `tests/integration/backfill/endpoints.py` | Engine endpoints (Postgres/ClickHouse/MSSQL/Kafka): DDL, deterministic seed, checksum dialects, source/sink factories |
| `tests/integration/backfill/conftest.py` | MSSQL and Kafka session fixtures with skip-first UX |
| `tests/integration/backfill/test_backfill_*_integration.py` | Route x inner-strategy matrix per route |
| `tests/integration/backfill/test_backfill_scenarios_integration.py` | Resume, idempotent re-run, parallel workers, verification bridge, invalid combinations |
| `tests/integration/backfill/test_postgres_mssql_shadow_initial_integration.py` | Mandatory 10-chunk/4-lane shadow initial smoke with target-commit/ledger-CAS crash and source-free resume |
| `tests/integration/airflow/test_airflow_backfill_interval_integration.py` | Interval-driven `dpone run` via the real CLI with Airflow-rendered `DPONE_*` env |
| `tests/test_backfill_orchestrator.py` | Runtime campaign lock, retry-failed selection, fail-closed retry policy, verification evidence |
| `tests/test_backfill_campaign_state.py` | Campaign locks, chunk leases, cancel, retryable chunks, config drift |
| `tests/test_backfill_diagnostic_connector.py` | CLI/API diagnostics resolve sink connectors through the standard runtime factory |
| `tests/test_backfill_audit_schema_state.py` | SQL audit-schema mirrors and read-side campaign recovery |
| `tests/test_backfill_audit_schema_service.py` | `status`/`doctor` service reads audit-schema state through injected sink connector |
| `tests/test_airflow_interval_env_rendering.py` | Real-Airflow Jinja rendering contract (runs where Airflow is importable) |

Marker: `integration_backfill` (declared in `pyproject.toml`).

## How to run locally

### Fast pre-deploy gate

Run this first for changes to PostgreSQL-to-MSSQL initial loading. It preserves
the production execution shape but caps the local plan at 10 chunks:

```bash
docker compose -f docker/docker-compose.integration.yml up -d --wait postgres mssql
docker compose -f docker/docker-compose.integration.yml run --rm mssql-init
DPONE_RUN_INTEGRATION=1 uv run pytest \
  tests/integration/backfill/test_postgres_mssql_shadow_initial_integration.py \
  -q
```

The test uses four fixed connections and real vendor engines. It deliberately
fails after the first target receipt but before ledger success, reconstructs a
new public runtime invocation, and requires the first chunk to replay without
source I/O or duplicate DML. The final live table must contain exactly 10 rows;
the complete shadow is published transactionally and the empty former live
table is retained as the rollback backup.

This smoke is the mandatory local-to-DEV promotion gate. A production plan of
hundreds or thousands of chunks is not repeated locally: keep the same
strategy, state, publication and concurrency contracts, reduce only the data
window to about 10 representative chunks, and run full volume in DEV after the
bounded gate passes.

### Full local matrix

1. Start the disposable stack (idempotent):

```bash
docker compose -f docker/docker-compose.integration.yml up -d --wait \
  postgres clickhouse mssql kafka schema-registry
```

2. Run the suite (defaults match the compose port forwards):

```bash
DPONE_RUN_INTEGRATION=1 uv run pytest -m integration_backfill \
  tests/integration/backfill tests/integration/airflow -rs -q
```

Overrides use the same `DPONE_IT_*` variables as the rest of the integration
layer (`DPONE_IT_PG_*`, `DPONE_IT_CH_*`, `DPONE_IT_MSSQL_*`,
`DPONE_IT_KAFKA_BOOTSTRAP`).

3. Optional: validate the scheduler-side rendering contract under a real
Airflow (mirrors `.github/workflows/airflow-pack-compat.yml`):

```bash
uv venv .venv-airflow --python 3.12
uv pip install --python .venv-airflow/bin/python \
  "apache-airflow==3.0.2" "apache-airflow-providers-cncf-kubernetes==10.4.2" \
  ./packages/dpone-airflow-pack pytest
.venv-airflow/bin/python -m pytest tests/test_airflow_interval_env_rendering.py -q
```

## Skip semantics

- Without `DPONE_RUN_INTEGRATION=1` every module skips.
- MSSQL cases skip with a clear reason when `ODBC Driver 18 for SQL Server`
  (plus `bcp` from mssql-tools18) is not installed or the service is
  unreachable; they always run in CI where the driver is provisioned.
- Kafka cases skip when `confluent-kafka` is missing or the broker is
  unreachable.

## Case matrix

Deterministic dataset per route: 120 rows, `id` 1..120,
`business_date` over 6 days, `bucket` 1..4, `amount = id * 7`.
Parity contract: `(count, sum(id), sum(amount))` source window vs target.

| Route | replace (date chunks) | incremental_merge (date chunks) | partition_replace (bucket chunks) | full_refresh (single chunk) |
| --- | --- | --- | --- | --- |
| postgres -> clickhouse | yes | yes | yes (native `REPLACE PARTITION`) | yes |
| mssql -> clickhouse | yes | yes | yes | covered by pg->ch (sink-side semantics) |
| postgres -> mssql | yes | yes | yes (delete+insert fallback) | covered by pg->ch |
| clickhouse -> mssql | yes | yes | yes (delete+insert fallback) | covered by pg->ch |
| postgres -> postgres | yes (pre-created target) | yes | yes | covered by pg->ch |
| postgres -> kafka | n/a (append-only log) | yes (keyed upsert replay, consumed back) | rejected by config validation | n/a |

The clickhouse -> mssql route additionally certifies the ClickHouse -> SQL
Server DDL type mapping (`MSSQLTypeMapper`): width-exact integer mappings
(`Int32 -> int`, `UInt64 -> decimal(20,0)`, ...), `Nullable(...)` /
`LowCardinality(...)` unwrapping, and `DateTime64 -> datetime2`, so repeated
chunks never trigger fake breaking schema-evolution changes
(unit contract: `tests/test_mssql_type_mapper_contracts.py`).

Scenario coverage on postgres -> clickhouse:

- empty/no-op chunks: a wider campaign window can include chunks before/after
  the available source data; those chunks still acquire a lease, execute,
  commit with `rows_extracted=0` / `rows_loaded=0`, and do not block later
  non-empty chunks;
- resume: chunk 2 fails once; the re-run skips the committed chunk (attempts
  stay at 1), retries the failed one, and reaches full parity;
- campaign lock: a second active runner fails before source IO instead of racing
  the same ledger;
- `retry-failed`: only chunks with status `failed` are retried; pending future
  chunks stay pending;
- unknown `retry_policy`: fails before source IO, so typos cannot silently widen
  a targeted retry;
- stale running chunk recovery: expired leases become retryable instead of
  blocking the campaign forever;
- cancellation: `cancel_requested` blocks new chunks and records the operator
  reason;
- idempotent re-run: a completed campaign re-run moves zero data and creates
  zero duplicates;
- `parallel_workers=2` over independent partition chunks with one connection
  per worker (pod-per-chunk topology);
- verification bridge: `<run_key>.execution.json` consumable by
  `dpone ops route-refresh-capture-snapshots` / `route-refresh-verify`;
- invalid combinations fail fast: multi-chunk `full_refresh`, unknown
  `inner_mode`, `incremental_merge` without `unique_key`, Kafka with a
  partitioned inner mode.

Airflow coverage: two data intervals rendered exactly as Airflow renders the
pack `env_vars`; each `dpone run` (real CLI subprocess, env-based credentials)
loads only its own window, a task-clear re-run of the same interval resumes
the same campaign without duplicates, and the XCom summary carries the
`interval` and bounded `backfill` sections.

## CI

`.github/workflows/backfill-integration.yml` runs the suite with disposable
compose services on path-filtered PRs, nightly, and on dispatch, and uploads
junit + coverage evidence. MSSQL ODBC/bcp tooling is installed on the runner,
so the MSSQL routes are always exercised in CI even when skipped locally.

For a fast pre-release proof of the production-shaped PostgreSQL-to-MSSQL
initial path, dispatch only the bounded ten-chunk/four-process profile from the
exact current `master` commit:

```bash
commit="$(git rev-parse origin/master)"
gh workflow run backfill-integration.yml \
  --ref master \
  -f profile=shadow_initial_smoke \
  -f expected_commit_sha="${commit}"
```

This profile starts only disposable PostgreSQL and MSSQL, runs
`test_postgres_mssql_shadow_initial_integration.py`, and fails closed on any
skip. The immutable commit assertion prevents a moving `master` from turning
an older smoke into release evidence. It is the hosted-runner equivalent of
the local command in [Backfill](../backfill.md#required-bounded-local-gate-before-dev)
when Docker or the x86 SQL Server image is unavailable on a developer Mac.

## Vendor-live backfill matrix

Managed/vendor systems are not exercised by the PR backfill workflow. They run
through the manual `vendor_live` profile in
`.github/workflows/live-certification.yml`:

```text
profile=vendor_live
run_vendor_live=true
vendor_backfill_source_filter=*
vendor_backfill_sink_filter=*
vendor_backfill_case_id_filter=*
```

The workflow does not start Docker services in this profile. It expects the CI
secret manager to provide the provider credentials. Before running managed
tests, it executes a redacted readiness gate:

```bash
uv run dpone ops managed-credentials-readiness \
  --profile vendor_live \
  --required-env APPSFLYER_API_TOKEN \
  --required-env GOOGLE_ADS_DEVELOPER_TOKEN \
  --output-dir test_artifacts/live_certification_vendor/managed-credentials \
  --format json
```

The readiness artifact contains only variable names and `present` / `missing`
status, never values. If this gate fails, the managed matrix does not start.
When it passes, the workflow runs the source/sink matrix with:

```bash
DPONE_RUN_INTEGRATION=1 \
DPONE_RUN_INTEGRATION_MATRIX=1 \
DPONE_MATRIX_RUN_MODE=vendor_live \
DPONE_MATRIX_STRATEGY=backfill \
DPONE_MATRIX_ARTIFACT_DIR=test_artifacts/live_certification_vendor/backfill_matrix \
uv run pytest -m integration_matrix tests/integration/matrix -q
```

Use the source/sink/case filters to constrain blast radius before running a
managed backfill certification. Keep `run_vendor_live=false` for routine PRs;
this layer is release-candidate or connector-change evidence only.

The generated matrix/load-step artifacts can be fed back into the advisor:

```bash
dpone backfill plan manifests/orders_backfill.yml \
  --advisor \
  --advisor-evidence test_artifacts/live_certification_vendor/backfill_matrix/certification_report.json \
  --advisor-evidence test_artifacts/acceptance/load_steps.json \
  --format json
```

This lets the advisor recommend `chunk.step`, `parallel_workers` and lease TTL
from real campaign timings instead of only static chunk-count heuristics.

If the vendor-live matrix is red, the advisor treats it as certification
evidence, not as a failed data chunk. The safe next action is to fix the matrix
blockers first; the advisor pins the recommendation to one worker and returns
`resolve_vendor_certification_failures`. Only a green matrix is allowed to
participate in performance tuning. Stage throughput from repeated load-step
records is also conservative: the slowest positive rows/sec per stage is used
for bottleneck detection.
