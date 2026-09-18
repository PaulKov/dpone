# CLICKHOUSE_CLUSTER_EXTERNAL_ENGINE_UNSUPPORTED

External replication requires direct, independent, non-replicated
MergeTree-family targets. A `Replicated*MergeTree`, `Distributed`, Shared, or
otherwise unsupported engine cannot use per-member external staging safely.

Use external mode only for the admitted non-replicated layout. Use internal mode
for a compatible `Replicated*MergeTree` topology whose complete inventory
reports `internal_replication=true`.

No engine is substituted and no local publication fallback is attempted. See
the [mode matrix](../clickhouse-cluster-publication-reference.md#mode-matrix).
