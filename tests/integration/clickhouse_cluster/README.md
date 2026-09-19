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
  tests/integration/clickhouse_cluster/test_clickhouse_external_replication_performance_live.py \
  -q
```

Its tracked budget is
`external_replication_performance_budget.json`. The fixture measures a
100,000-row canonical digest and a production-composed 10,000-row load fanned
out to both pinned members. A pass requires every measured trial to stay within
budget, exact per-member counts and content digests, `COMMITTED` publication,
and proven owned-candidate cleanup. Startup is excluded by the session
readiness probe and one unmeasured warmup. Do not rerun to select a faster
sample; preserve every exact-commit observation independently.

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
