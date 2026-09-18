# CLICKHOUSE_CLUSTER_EXTERNAL_MODE_MISMATCH

The declared cluster replication mode does not match the complete runtime
inventory. External mode requires every member to report
`internal_replication=false`; internal mode requires every member to report
`internal_replication=true`.

Inspect the complete topology through approved read-only diagnostics. Correct
the manifest or cluster configuration through change control, then rerun
`dpone check` and `dpone plan`. Mixed flags are unsupported.

The runtime stops before source extraction and never falls back to local
publication. See the
[cluster publication runbook](../clickhouse-cluster-publication-runbook.md#mode-or-inventory-mismatch).
