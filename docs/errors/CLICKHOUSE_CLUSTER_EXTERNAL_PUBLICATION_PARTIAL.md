# CLICKHOUSE_CLUSTER_EXTERNAL_PUBLICATION_PARTIAL

The bound publication operation did not converge to the desired generation on
every required member. The original queue entry may still be active, or it may
have terminated with mixed member generations.

If the exact entry is active, retry read-only reconciliation of that entry. If
it is terminal and mixed, retain both generations and escalate. Do not dispatch
another exchange or rename; it can revert members that already committed.

See [Terminal partial publication](../clickhouse-cluster-publication-runbook.md#terminal-partial-publication).
