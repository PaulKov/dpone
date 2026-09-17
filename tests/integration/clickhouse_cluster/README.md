# ClickHouse cluster publication acceptance

This opt-in fixture pins ClickHouse `24.8.14.39` and creates one shard with two
replicas plus one Keeper node. It is synthetic protocol evidence, not external
deployment certification.

Run:

```bash
docker compose -f tests/integration/clickhouse_cluster/docker-compose.yml up -d --wait
DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION=1 uv run pytest \
  tests/integration/clickhouse_cluster -q
docker compose -f tests/integration/clickhouse_cluster/docker-compose.yml down -v
```

The test writes its machine-readable receipt beneath
`test_artifacts/clickhouse-cluster-publication/`. Generated receipts are not
source files and must not be hand-edited or committed.

The fault profile deterministically exercises three recovery boundaries that a
normal happy-path run cannot create:

- a local one-replica cutover while the exact distributed-DDL entry remains
  active, followed by convergence of that same entry and generation mapping;
- a terminal distributed-DDL failure with mixed replica generations, proving a
  retry sends no second publication DDL and retains both generations;
- live `Active`/`Finished` success/failure queue rows, plus deterministic injected
  classifier cases for every remaining pinned status/exception normalization
  and missing, extra, or duplicate host evidence.

These are Docker protocol checks for the pinned server version. They do not
enable cluster admission or certify an external deployment.
