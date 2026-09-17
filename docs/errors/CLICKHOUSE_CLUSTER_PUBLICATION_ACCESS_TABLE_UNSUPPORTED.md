# CLICKHOUSE_CLUSTER_PUBLICATION_ACCESS_TABLE_UNSUPPORTED

The first cluster publication protocol supports a direct replicated target,
not a separately managed `Distributed` access table. Remove
`physical_design.storage.clickhouse.access_table` from this workload and manage
any read facade independently.

No local publication fallback is attempted.
