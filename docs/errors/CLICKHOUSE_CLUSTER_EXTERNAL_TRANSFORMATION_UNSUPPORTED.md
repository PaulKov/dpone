# CLICKHOUSE_CLUSTER_EXTERNAL_TRANSFORMATION_UNSUPPORTED

The selected external publication route received a lineage projection or an
unknown decoder-dependent artifact that cannot be replayed identically on every
member.

Disable lineage for this route and use the documented versioned MSSQL
TabSeparated codec or a replayable row artifact. Dpone aborts an unused
`LOCKED` authority before returning this error; retry with a new run identity
only after correcting the manifest or artifact producer.

See [ClickHouse cluster publication](../clickhouse-cluster-publication.md) and
the [operator runbook](../clickhouse-cluster-publication-runbook.md).
