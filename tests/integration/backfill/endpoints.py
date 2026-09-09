"""Route endpoints for backfill integration tests.

Each endpoint encapsulates one engine's DDL/seed/checksum dialect and the
construction of real dpone source/sink runtime objects, so matrix tests
compose endpoints instead of duplicating SQL per route.
"""

from __future__ import annotations

from typing import Any

from backfill_toolkit import Checksum, SeedSpec

from dpone.runtime.process_logging import create_etl_logger

_CHECKSUM_QUERY = (
    "SELECT COUNT(*) AS row_count, COALESCE(SUM(id), 0) AS id_sum, "
    "COALESCE(SUM(amount), 0) AS amount_sum FROM {table}{where}"
)


def _where(predicate: str | None) -> str:
    return f" WHERE {predicate}" if predicate else ""


def _checksum_row(row: tuple[Any, ...]) -> Checksum:
    return (int(row[0]), int(row[1]), int(row[2]))


class PostgresEndpoint:
    """PostgreSQL as backfill source and/or target."""

    source_type = "postgres"
    sink_type = "postgres"

    def __init__(self, connector: Any) -> None:
        self.connector = connector

    def reset_session(self) -> None:
        """Recover a shared session from an aborted transaction left by
        a previous (intentionally failing) scenario."""

        try:
            self.connector.rollback()
        except Exception:  # noqa: BLE001 - the session may already be clean
            pass

    def qualified(self, schema: str, table: str) -> str:
        return f'"{schema}"."{table}"'

    def create_empty_table(self, schema: str, table: str, *, with_technical_columns: bool = False) -> None:
        """Empty target with the canonical dataset DDL (replace mode needs an
        existing target because its predicate delete has no bootstrap path).

        ``with_technical_columns`` mirrors the sink contract: predicate-replace
        inserts populate ``__dpone__loaded_at``/``__dpone__deleted_at``.
        """

        technical = (
            ', "__dpone__loaded_at" timestamp NULL, "__dpone__deleted_at" timestamp NULL'
            if with_technical_columns
            else ""
        )
        self.connector.execute_query(
            f"CREATE TABLE {self.qualified(schema, table)} ("
            "id integer NOT NULL, business_date date NOT NULL, bucket integer NOT NULL, amount integer NOT NULL"
            f"{technical})"
        )

    def seed(self, schema: str, table: str, spec: SeedSpec) -> None:
        target = self.qualified(schema, table)
        self.create_empty_table(schema, table)
        self.connector.execute_query(
            f"INSERT INTO {target} (id, business_date, bucket, amount) "
            f"SELECT gs, DATE '{spec.start_date.isoformat()}' + ((gs - 1) % {spec.days}), "
            f"((gs - 1) % {spec.buckets}) + 1, gs * 7 "
            f"FROM generate_series(1, {spec.rows}) AS gs"
        )

    def checksum(self, schema: str, table: str, *, predicate: str | None = None) -> Checksum:
        query = _CHECKSUM_QUERY.format(table=self.qualified(schema, table), where=_where(predicate))
        return _checksum_row(self.connector.get_records(query)[0])

    def drop(self, schema: str, table: str) -> None:
        self.connector.execute_query(f"DROP TABLE IF EXISTS {self.qualified(schema, table)}")

    def create_source(self) -> Any:
        from dpone.runtime.sources.postgres import PostgresSource

        return PostgresSource(self.connector, state_storage=None, logger=create_etl_logger())

    def create_sink(self) -> Any:
        from dpone.runtime.sinks.postgres import PostgresSink

        return PostgresSink(self.connector, state_storage=None)


class ClickHouseEndpoint:
    """ClickHouse as backfill source and/or target."""

    source_type = "clickhouse"
    sink_type = "clickhouse"

    def __init__(self, connector: Any, *, database: str) -> None:
        self.connector = connector
        self.database = database

    def qualified(self, schema: str, table: str) -> str:
        return f"`{schema}`.`{table}`"

    def seed(self, schema: str, table: str, spec: SeedSpec) -> None:
        self.connector.execute_query(
            f"CREATE TABLE {self.qualified(schema, table)} ("
            "id Int32, business_date Date, bucket Int32, amount Int32) "
            "ENGINE = MergeTree ORDER BY id"
        )
        self.connector.execute_query(
            f"INSERT INTO {self.qualified(schema, table)} (id, business_date, bucket, amount) "
            f"SELECT number + 1, toDate('{spec.start_date.isoformat()}') + (number % {spec.days}), "
            f"(number % {spec.buckets}) + 1, (number + 1) * 7 "
            f"FROM numbers({spec.rows})"
        )

    def create_source(self) -> Any:
        from dpone.runtime.sources.clickhouse import ClickHouseSource

        return ClickHouseSource(self.connector, logger=create_etl_logger())

    def create_partitioned_target(self, schema: str, table: str, *, partition_column: str) -> None:
        """Partitioned MergeTree target required by native REPLACE PARTITION."""

        self.connector.execute_query(
            f"CREATE TABLE {self.qualified(schema, table)} ("
            "id Int32, business_date Date, bucket Int32, amount Int32) "
            f"ENGINE = MergeTree PARTITION BY {partition_column} ORDER BY id"
        )

    def checksum(self, schema: str, table: str, *, predicate: str | None = None) -> Checksum:
        query = _CHECKSUM_QUERY.format(table=self.qualified(schema, table), where=_where(predicate))
        return _checksum_row(self.connector.get_records(query)[0])

    def duplicate_id_count(self, schema: str, table: str) -> int:
        rows = self.connector.get_records(
            f"SELECT COUNT(*) FROM (SELECT id FROM {self.qualified(schema, table)} GROUP BY id HAVING count() > 1)"
        )
        return int(rows[0][0])

    def drop(self, schema: str, table: str) -> None:
        self.connector.execute_query(f"DROP TABLE IF EXISTS {self.qualified(schema, table)}")

    def create_sink(self) -> Any:
        from dpone.runtime.sinks.clickhouse import ClickHouseSink

        return ClickHouseSink(self.connector)


class MSSQLEndpoint:
    """SQL Server as backfill source and/or target."""

    source_type = "mssql"
    sink_type = "mssql"

    def __init__(self, connector: Any) -> None:
        self.connector = connector

    def qualified(self, schema: str, table: str) -> str:
        return f"[{schema}].[{table}]"

    def ensure_schema(self, schema: str) -> None:
        self.connector.execute_query(f"IF SCHEMA_ID('{schema}') IS NULL EXEC('CREATE SCHEMA [{schema}]')")

    def seed(self, schema: str, table: str, spec: SeedSpec) -> None:
        self.ensure_schema(schema)
        target = self.qualified(schema, table)
        self.connector.execute_query(f"DROP TABLE IF EXISTS {target}")
        self.connector.execute_query(
            f"CREATE TABLE {target} ("
            "[id] int NOT NULL, [business_date] date NOT NULL, [bucket] int NOT NULL, [amount] int NOT NULL)"
        )
        self.connector.execute_query(
            "WITH numbers AS (SELECT TOP (?) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS gs "
            "FROM sys.all_objects a CROSS JOIN sys.all_objects b) "
            f"INSERT INTO {target} ([id], [business_date], [bucket], [amount]) "
            f"SELECT gs, DATEADD(day, (gs - 1) % {spec.days}, ?), ((gs - 1) % {spec.buckets}) + 1, gs * 7 "
            "FROM numbers",
            params=(spec.rows, spec.start_date.isoformat()),
        )

    def checksum(self, schema: str, table: str, *, predicate: str | None = None) -> Checksum:
        query = _CHECKSUM_QUERY.format(table=self.qualified(schema, table), where=_where(predicate))
        return _checksum_row(self.connector.get_records(query)[0])

    def drop(self, schema: str, table: str) -> None:
        self.connector.execute_query(f"DROP TABLE IF EXISTS {self.qualified(schema, table)}")

    def create_source(self) -> Any:
        from dpone.runtime.sources.mssql import MSSQLSource

        return MSSQLSource(self.connector, logger=create_etl_logger())

    def create_sink(self) -> Any:
        from dpone.runtime.sinks.mssql import MSSQLSink

        return MSSQLSink(self.connector)


class KafkaTargetEndpoint:
    """Kafka topic as backfill target (keyed upsert replay)."""

    sink_type = "kafka"

    def __init__(self, connector: Any) -> None:
        self.connector = connector

    def create_sink(self) -> Any:
        from dpone.runtime.sinks.kafka import KafkaSink

        return KafkaSink(self.connector)

    def consume_ids(self, topic: str, *, expected: int, group_id: str) -> set[int]:
        """Read produced JSON events back and return the distinct id set."""

        import json

        consumer = self.connector.create_consumer(
            group_id=group_id,
            options={"auto.offset.reset": "earliest", "enable.partition.eof": True},
        )
        consumer.subscribe([topic])
        ids: set[int] = set()
        empty_polls = 0
        while len(ids) < expected and empty_polls < 40:
            message = consumer.poll(0.25)
            if message is None or message.error():
                empty_polls += 1
                continue
            empty_polls = 0
            value = json.loads(message.value())
            payload = value.get("data") if isinstance(value.get("data"), dict) else value
            ids.add(int(payload["id"]))
        consumer.close()
        return ids


__all__ = [
    "ClickHouseEndpoint",
    "KafkaTargetEndpoint",
    "MSSQLEndpoint",
    "PostgresEndpoint",
]
