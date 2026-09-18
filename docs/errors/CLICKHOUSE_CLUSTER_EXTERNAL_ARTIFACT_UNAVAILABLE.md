# CLICKHOUSE_CLUSTER_EXTERNAL_ARTIFACT_UNAVAILABLE

A same-operation retry reached `STAGING`, but the exact retained artifact could
not be reopened from the configured durable store.

Do not re-extract the source, delete authority, or create another candidate.
Restore the exact content-addressed object identified by the authority receipt,
then retry with the same run identity. If it cannot be restored, retain all
objects and escalate for controlled recovery.

See [ClickHouse cluster publication](../clickhouse-cluster-publication.md) and
the [operator runbook](../clickhouse-cluster-publication-runbook.md).
