# CLICKHOUSE_CLUSTER_EXTERNAL_ARTIFACT_UNSUPPORTED

External per-member staging requires one immutable replayable artifact with a
SHA-256 digest, schema identity, byte size, and row count. The current artifact
is one-shot, mutable, incomplete, over budget, or uses an unsupported canonical
type profile.

Before candidate mutation, the owning runtime may perform exact cleanup and
record `ABORTED`. Produce a new bounded operation with a supported artifact;
do not materialize or replace data manually during recovery.

See [Unsupported artifact recovery](../clickhouse-cluster-publication-runbook.md#unsupported-artifact).
