# CLICKHOUSE_CLUSTER_EXTERNAL_INVENTORY_INCOMPLETE

The runtime could not prove the exact one-shard member set required for external
publication. A member may be missing, unreachable, duplicated, unexpected, or
ambiguous.

Restore catalog access and direct-member connectivity, then retry the same
operation. Partial inventory and `skip_unavailable_shards=1` are not completion
evidence.

Do not publish or clean up while inventory is incomplete. Follow the
[read-only evidence procedure](../clickhouse-cluster-publication-runbook.md#2-collect-read-only-evidence).
