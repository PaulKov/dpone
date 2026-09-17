# CLICKHOUSE_CLUSTER_PUBLICATION_STAGING_DATABASE_MUST_MATCH_TARGET

Cluster `full_refresh` exchanges exact replicated generations and therefore
requires every managed staging/candidate table in the target database. Remove
the separate ClickHouse `sink.staging.schema` or set it to the target schema.

The runtime will not copy or rename across databases and will not fall back to
local publication.
