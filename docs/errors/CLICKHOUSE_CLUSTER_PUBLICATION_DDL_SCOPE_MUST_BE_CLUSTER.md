# CLICKHOUSE_CLUSTER_PUBLICATION_DDL_SCOPE_MUST_BE_CLUSTER

A requested replicated `full_refresh` must use cluster-wide DDL. Set
`physical_design.storage.clickhouse.cluster.ddl_scope: cluster`, or use the
cluster string shorthand. Local DDL cannot establish complete replica evidence.

The runtime fails closed instead of selecting the local publisher.
