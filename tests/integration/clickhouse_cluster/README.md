# ClickHouse cluster publication acceptance

This opt-in fixture pins ClickHouse `24.8.14.39` and creates one shard with two
replicas plus one Keeper node. It is synthetic protocol evidence, not external
deployment certification.

Run:

```bash
docker compose -f tests/integration/clickhouse_cluster/docker-compose.yml up -d --wait
DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION=1 uv run pytest \
  tests/integration/clickhouse_cluster/test_clickhouse_cluster_publication_live.py -q
docker compose -f tests/integration/clickhouse_cluster/docker-compose.yml down -v
```

The test writes its machine-readable receipt beneath
`test_artifacts/clickhouse-cluster-publication/`. Generated receipts are not
source files and must not be hand-edited or committed.
