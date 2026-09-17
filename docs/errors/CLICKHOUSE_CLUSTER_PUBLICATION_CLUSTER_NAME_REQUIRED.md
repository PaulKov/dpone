# CLICKHOUSE_CLUSTER_PUBLICATION_CLUSTER_NAME_REQUIRED

Cluster DDL scope requires a non-empty ClickHouse cluster name. Configure
`physical_design.storage.clickhouse.cluster.name` or use the string shorthand.

Run `dpone check` to validate the static contract and `dpone plan` to inspect
the selected publication mode before execution.
