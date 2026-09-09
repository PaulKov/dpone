"""Hermetic contracts for postgres_to_clickhouse_analytics_v1."""

from __future__ import annotations

from dpone.type_system.source_sink.postgres_clickhouse import PostgresClickHouseTypeMapper


def test_postgres_clickhouse_core_types() -> None:
    mapper = PostgresClickHouseTypeMapper()
    assert mapper.resolve("integer").target_type == "Int32"
    assert mapper.resolve("boolean").target_type == "Bool"
    assert mapper.resolve("jsonb").target_type == "String"
    assert mapper.resolve("uuid").target_type == "UUID"
    assert mapper.resolve("bytea").target_type == "String"
    assert mapper.resolve("numeric(18,4)").target_type == "Decimal(18,4)"
    assert mapper.resolve("timestamp with time zone").target_type == "DateTime64(6, 'UTC')"
