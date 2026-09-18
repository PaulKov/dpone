# CLICKHOUSE_CLUSTER_EXTERNAL_STAGING_DIVERGED

At least one member candidate does not match the authority-bound UUID, schema,
row count, or canonical content digest for the desired logical generation.

Stop publication and preserve the authority, sealed artifact, and every
candidate. Same-operation recovery may replace only an exact owned unpublished
partial candidate. A foreign UUID or different complete digest requires
operator escalation.

Never append another copy of the artifact. Follow
[Staging divergence](../clickhouse-cluster-publication-runbook.md#staging-divergence).
