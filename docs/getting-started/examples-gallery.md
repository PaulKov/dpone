# Examples gallery

Start from the combination closest to your real pipeline. Each guide includes supported strategies, schema evolution behavior, reconciliation notes, type mapping caveats, and runbooks.

Start from your route (source → sink): the Flow column below is the same route
vocabulary as the [source → sink matrix](../source-sink-matrix.md). For Airflow,
scaffold that route with a built-in recipe on
[First Airflow DAG](first-airflow-dag.md).

**Evidence** values:

- `wide vendor-live` — Docker vendor-live suite covers the sink strategy set
  documented in the guide ([certification bar](../testing/route-live-wide-certification.md));
- `Batch ETL` — supported path; see the guide for the current evidence breadth.

## Most common database flows

| Flow | Evidence | Start here |
| --- | --- | --- |
| PostgreSQL -> MSSQL | wide vendor-live | [Postgres -> MSSQL](../source-sink/postgres-to-mssql.md) |
| PostgreSQL -> PostgreSQL | wide vendor-live | [Postgres -> Postgres](../source-sink/postgres-to-postgres.md) |
| PostgreSQL -> ClickHouse | wide vendor-live | [Postgres -> ClickHouse](../source-sink/postgres-to-clickhouse.md) |
| MySQL -> MSSQL | wide vendor-live | [MySQL -> MSSQL](../source-sink/mysql-to-mssql.md) |
| MySQL -> Postgres | wide vendor-live | [MySQL -> Postgres](../source-sink/mysql-to-postgres.md) |
| MySQL -> ClickHouse | wide vendor-live | [MySQL -> ClickHouse](../source-sink/mysql-to-clickhouse.md) |
| MSSQL -> MSSQL | wide vendor-live | [MSSQL -> MSSQL](../source-sink/mssql-to-mssql.md) |
| MSSQL -> ClickHouse | wide vendor-live | [MSSQL -> ClickHouse](../source-sink/mssql-to-clickhouse.md) |

## MySQL wide-certified flows

| Flow | Evidence | Start here |
| --- | --- | --- |
| MySQL -> BigQuery | wide vendor-live | [MySQL -> BigQuery](../source-sink/mysql-to-bigquery.md) |
| MySQL -> Postgres | wide vendor-live | [MySQL -> Postgres](../source-sink/mysql-to-postgres.md) |
| MySQL -> MSSQL | wide vendor-live | [MySQL -> MSSQL](../source-sink/mysql-to-mssql.md) |
| MySQL -> ClickHouse | wide vendor-live | [MySQL -> ClickHouse](../source-sink/mysql-to-clickhouse.md) |
| MySQL -> Kafka | wide vendor-live | [MySQL -> Kafka](../source-sink/mysql-to-kafka.md) |

## MSSQL wide-certified flows

| Flow | Evidence | Start here |
| --- | --- | --- |
| MSSQL -> BigQuery | wide vendor-live | [MSSQL -> BigQuery](../source-sink/mssql-to-bigquery.md) |
| MSSQL -> Postgres | wide vendor-live | [MSSQL -> Postgres](../source-sink/mssql-to-postgres.md) |
| MSSQL -> MSSQL | wide vendor-live | [MSSQL -> MSSQL](../source-sink/mssql-to-mssql.md) |
| MSSQL -> ClickHouse | wide vendor-live | [MSSQL -> ClickHouse](../source-sink/mssql-to-clickhouse.md) |
| MSSQL -> Kafka | wide vendor-live | [MSSQL -> Kafka](../source-sink/mssql-to-kafka.md) |

## API and Kafka flows

| Flow | Evidence | Start here |
| --- | --- | --- |
| REST API -> MSSQL | Batch ETL | [REST API -> MSSQL](../source-sink/api-to-mssql.md) |
| REST API -> ClickHouse | Batch ETL | [REST API -> ClickHouse](../source-sink/api-to-clickhouse.md) |
| Kafka -> MSSQL | Batch ETL | [Kafka -> MSSQL](../source-sink/kafka-to-mssql.md) |
| PostgreSQL -> Kafka | wide vendor-live | [Postgres -> Kafka](../source-sink/postgres-to-kafka.md) |
| MySQL -> Kafka | wide vendor-live | [MySQL -> Kafka](../source-sink/mysql-to-kafka.md) |
| MSSQL -> Kafka | wide vendor-live | [MSSQL -> Kafka](../source-sink/mssql-to-kafka.md) |

## Warehousing flows

| Flow | Evidence | Start here |
| --- | --- | --- |
| PostgreSQL -> BigQuery | wide vendor-live | [Postgres -> BigQuery](../source-sink/postgres-to-bigquery.md) |
| MySQL -> BigQuery | wide vendor-live | [MySQL -> BigQuery](../source-sink/mysql-to-bigquery.md) |
| MSSQL -> BigQuery | wide vendor-live | [MSSQL -> BigQuery](../source-sink/mssql-to-bigquery.md) |
| ClickHouse -> BigQuery | Batch ETL | [ClickHouse -> BigQuery](../source-sink/clickhouse-to-bigquery.md) |
| REST API -> BigQuery | Batch ETL | [REST API -> BigQuery](../source-sink/api-to-bigquery.md) |
| Kafka -> BigQuery | Batch ETL | [Kafka -> BigQuery](../source-sink/kafka-to-bigquery.md) |

## Full matrix

Use the [Source -> sink matrix](../source-sink-matrix.md) when you need every supported pair and strategy in one place.

Use the [Type mapping matrix](../type-mapping-matrix.md) when you need to understand how source types map into target systems.

Use [Route live wide certification](../testing/route-live-wide-certification.md) when you need the evidence bar behind `wide vendor-live` claims.
