# CLICKHOUSE_CLUSTER_PUBLICATION_ENGINE_MUST_BE_REPLICATED_MERGE_TREE

Cluster `full_refresh` requires a direct `Replicated*MergeTree` target. Set
`sink.options.physical_design.storage.clickhouse.engine` to a replicated
MergeTree-family engine, or remove cluster publication from this workload.

The runtime will not fall back to local publication. Rerun `dpone check` and
`dpone plan` after correcting the physical design.
