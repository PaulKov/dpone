from __future__ import annotations

from dpone.runtime.connectors.mssql_bulk import is_mssql_character_bulk_unsafe_type


def test_mssql_character_bulk_allows_temporal_source_types() -> None:
    assert not is_mssql_character_bulk_unsafe_type("timestamp without time zone")
    assert not is_mssql_character_bulk_unsafe_type("timestamp with time zone")
    assert not is_mssql_character_bulk_unsafe_type("datetime2")
    assert not is_mssql_character_bulk_unsafe_type("date")


def test_mssql_character_bulk_rejects_rowversion_and_binary_types() -> None:
    assert is_mssql_character_bulk_unsafe_type("timestamp")
    assert is_mssql_character_bulk_unsafe_type("rowversion")
    assert is_mssql_character_bulk_unsafe_type("varbinary(max)")
