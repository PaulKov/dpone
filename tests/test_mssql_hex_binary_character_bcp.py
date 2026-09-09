"""Hermetic contracts for MSSQL hex-binary character BCP wire."""

from __future__ import annotations

from dpone.config.load_config import LoadConfig
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.connectors.bulk_text_codec import BulkTextCodec
from dpone.runtime.connectors.mssql_bulk import is_mssql_character_bulk_unsafe_type
from dpone.runtime.sinks.staging_managers.mssql import MSSQLStagingManager
from dpone.runtime.sinks.strategies.mssql.mssql_load_strategies import MSSQLFullRefreshStrategy
from dpone.runtime.support.mssql_bcp_values import format_mssql_bcp_scalar
from dpone.runtime.support.mssql_hex_binary import (
    build_mssql_staging_select_expression,
    is_mssql_hex_binary_wire_type,
    plan_mssql_character_staging_types,
)


class FakeMSSQLConnector:
    def __init__(self) -> None:
        self.queries: list[tuple] = []

    def quote_identifier(self, name: str) -> str:
        return f"[{name}]"

    def execute_query(self, query: str, params=None) -> int:
        self.queries.append((query, params))
        return 1

    def get_records(self, query: str, params=None, *, as_dict: bool = False):
        """Expose the exact counted-DML connector capability used in production."""

        self.queries.append((query, params))
        assert as_dict is True
        return [{"__dpone__affected_rows": 1}]

    def begin(self) -> None:
        return None

    def commit_transaction(self) -> None:
        return None

    def rollback(self) -> None:
        return None


def test_hex_binary_wire_type_detects_varbinary_not_rowversion() -> None:
    assert is_mssql_hex_binary_wire_type("varbinary(max)")
    assert is_mssql_hex_binary_wire_type("binary(16)")
    assert is_mssql_hex_binary_wire_type("image")
    assert not is_mssql_hex_binary_wire_type("rowversion")
    assert not is_mssql_hex_binary_wire_type("timestamp")
    assert not is_mssql_hex_binary_wire_type("nvarchar(max)")


def test_plan_mssql_character_staging_rewrites_binary_to_nvarchar() -> None:
    column_types, hex_cols, targets = plan_mssql_character_staging_types(
        [("id", "int"), ("payload", "varbinary(max)"), ("note", "nvarchar(max)")]
    )
    assert column_types == {
        "id": "nvarchar(max)",
        "payload": "nvarchar(max)",
        "note": "nvarchar(max)",
    }
    assert hex_cols == frozenset({"payload"})
    assert targets["payload"] == "varbinary(max)"


def test_bulk_text_codec_never_decodes_dpone_internal_digest_columns() -> None:
    expression = build_mssql_staging_select_expression(
        column="__dpone__delta_hash",
        alias="r",
        quote_identifier=lambda value: f"[{value}]",
        column_types={"__dpone__delta_hash": "char(64)"},
        hex_binary_columns=frozenset(),
        target_column_types={"__dpone__delta_hash": "char(64)"},
        bulk_text_codec=BulkTextCodec(),
    )

    assert expression == "r.[__dpone__delta_hash]"
    assert "CASE WHEN" not in expression


def test_mssql_staging_create_uses_nvarchar_for_hex_binary() -> None:
    connector = FakeMSSQLConnector()
    manager = MSSQLStagingManager(connector)
    load_config = LoadConfig(
        source_conn_id="src",
        target_conn_id="dst",
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        staging_schema="staging",
    )
    artifact = manager.create(load_config, [("id", "int"), ("payload", "varbinary(max)")])
    create_sql = connector.queries[-1][0]
    assert "nvarchar(max)" in create_sql
    assert "payload" in create_sql
    assert artifact.hex_binary_columns == frozenset({"payload"})
    assert artifact.target_column_types["payload"] == "varbinary(max)"
    assert artifact.column_types["payload"] == "nvarchar(max)"
    # Character safety still rejects raw varbinary staging metadata.
    assert is_mssql_character_bulk_unsafe_type("varbinary(max)")
    assert not is_mssql_character_bulk_unsafe_type(artifact.column_types["payload"])


def test_mssql_staging_commit_converts_hex_binary_columns() -> None:
    connector = FakeMSSQLConnector()
    strategy = MSSQLFullRefreshStrategy(connector, logger=None, staging_manager=None)
    staging = StagingTableArtifact(
        schema="staging",
        table="orders_stg",
        columns=["id", "payload"],
        staging_manager=None,
        column_types={"id": "int", "payload": "nvarchar(max)"},
        target_column_types={"id": "int", "payload": "varbinary(max)"},
        hex_binary_columns=frozenset({"payload"}),
        bulk_text_codec=BulkTextCodec(),
    )
    load_config = LoadConfig(
        source_conn_id="src",
        target_conn_id="dst",
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
    )
    strategy._insert_from_staging_to_table(load_config, staging, "orders_shadow")
    query = connector.queries[-1][0]
    assert "CONVERT(varbinary(max)," in query
    assert ", 2) AS [payload]" in query


def test_format_mssql_bcp_scalar_hex_encodes_bytes() -> None:
    assert format_mssql_bcp_scalar(b"\x01\x02\x03", mssql_type="nvarchar(max)") == "010203"
    assert format_mssql_bcp_scalar(b"\xab\xcd", mssql_type="varbinary(max)") == "abcd"
