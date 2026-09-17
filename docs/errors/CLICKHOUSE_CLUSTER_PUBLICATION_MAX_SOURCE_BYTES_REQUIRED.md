# CLICKHOUSE_CLUSTER_PUBLICATION_MAX_SOURCE_BYTES_REQUIRED

Cluster `full_refresh` must be bounded. Configure a positive
`sink.strategy.max_source_bytes` authorized by the platform profile. The limit
counts the complete emitted source payload before ClickHouse storage
compression.

Missing, zero, boolean, or negative limits block before source extraction.
