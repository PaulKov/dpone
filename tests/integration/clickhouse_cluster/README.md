# ClickHouse cluster publication acceptance

This opt-in fixture pins ClickHouse `24.8.14.39` and creates one shard with two
replicas plus one Keeper node. It exposes both an internally replicated cluster
and an `internal_replication=false` cluster for the external-publication route.
It is synthetic protocol evidence, not external deployment certification.

Run:

```bash
docker compose -f tests/integration/clickhouse_cluster/docker-compose.yml up -d --wait
DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION=1 uv run --extra clickhouse pytest \
  tests/integration/clickhouse_cluster -q
docker compose -f tests/integration/clickhouse_cluster/docker-compose.yml down -v
```

The test writes its machine-readable receipt beneath
`test_artifacts/clickhouse-cluster-publication/`. Generated receipts are not
source files and must not be hand-edited or committed.

The external-replication performance contract has a separate focused command:

```bash
DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION=1 uv run --extra clickhouse pytest \
  tests/integration/clickhouse_cluster/test_clickhouse_external_replication_performance_live.py::test_external_replication_performance_budget \
  -q
```

Its tracked budget is
`external_replication_performance_budget.json`. The fixture measures a
100,000-row canonical digest and a production-composed 10,000-row load fanned
out to both pinned members. A pass requires every measured trial to stay within
budget, exact per-member counts and content digests, `COMMITTED` publication,
and proven owned-candidate cleanup. Startup is excluded by the session
readiness probe and one unmeasured warmup. Do not rerun to select a faster
sample. The producer creates the canonical receipt with atomic no-clobber
semantics and fails if that path already exists. Preserve the prior receipt
byte-identically under a distinct evidence identity before starting a new
observation from a fresh checkout; fixture setup never deletes benchmark
evidence.

The fault profile deterministically exercises three recovery boundaries that a
normal happy-path run cannot create:

- a local one-replica cutover while the exact distributed-DDL entry remains
  active, followed by convergence of that same entry and generation mapping;
- a terminal distributed-DDL failure with mixed replica generations, proving a
  retry sends no second publication DDL and retains both generations;
- live `Active`/`Finished` success/failure queue rows, plus deterministic injected
  classifier cases for every remaining pinned status/exception normalization
  and missing, extra, or duplicate host evidence.

These are Docker protocol checks for the pinned server version, including the
real `ClickHouseSink` routing boundary. They support internal runtime admission
but do not certify any external deployment.

## MSSQL authority with real ClickHouse effects

`test_mssql_clickhouse_publication_live.py` combines the local SQL Server
authority fixture with the two-replica cluster above. Supply the existing local
SQL test environment (`DPONE_IT_MSSQL_HOST`, `DPONE_IT_MSSQL_PORT`,
`DPONE_IT_MSSQL_USER`, `DPONE_IT_MSSQL_PASSWORD`) only to the test process. The
host must be loopback or `host.docker.internal`; no external database is admitted.
The interpreter needs the MSSQL ODBC and ClickHouse HTTP drivers. For a container
runner, set `DPONE_IT_PUBLICATION_CH_HOST=host.docker.internal`; the fixed local
cluster port remains `18123`.

```bash
DPONE_RUN_MSSQL_CLICKHOUSE_PUBLICATION_LIVE=1 uv run pytest \
  tests/integration/clickhouse_cluster/test_mssql_clickhouse_publication_live.py -v
```

This separate opt-in does not reset the older cluster-publication receipts.
Each fixture creates unique SQL and ClickHouse databases and retains them for
inspection. It never drops or reuses an existing database. Keep the pytest
result with the exact source commit and container image identities; do not
selectively rerun failed cases to claim a passing original campaign.

The profile checks existing and absent targets, exact per-replica UUID/content
parity, durable SQL phases and single correlated publication/cleanup entries.
It injects lost SQL commit acknowledgements, unavailable SQL sessions, lost
ClickHouse responses, and abrupt control-flow loss after real publication or
cleanup. Fresh service/provider instances must reconcile the durable state
without a second effect. A committed intent with no submitted DDL must remain
blocked and preserve both generations. Replay must carry the original operation
and row-count result, not merely return without an exception.

These faults are injected at actual I/O boundaries, not an operating-system
process kill or a physical network partition. The profile does not exercise
source transport, full quality governance, native PREPARED recovery, a trusted
deployment freeze, or historical legacy-DDL coverage. Passing it is not a
retirement authorization or an external route certificate. The separate SQL
authority suite remains responsible for real concurrent CAS winners and
immutable history/admission checks.
