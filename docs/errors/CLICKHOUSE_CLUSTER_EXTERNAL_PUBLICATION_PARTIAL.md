# CLICKHOUSE_CLUSTER_EXTERNAL_PUBLICATION_PARTIAL

The bound publication operation reached a terminal queue result without
converging to the desired generation on every required member. Physical truth
may be mixed, or every member may still retain the predecessor.

Retain every observed generation and escalate. Do not dispatch another exchange
or rename; the original operation is terminal, and a second dispatch can revert
members that already committed.

See [Terminal partial publication](../clickhouse-cluster-publication-runbook.md#terminal-partial-publication).
