from __future__ import annotations

import argparse
import json
import logging

from dpone.cli.parser import build_parser
from dpone.commands.schema_plan_cmd import cmd_schema_type_matrix
from dpone.services.schema_type_matrix import PairTypeMatrixService


def test_pair_type_matrix_explains_mssql_clickhouse_expected_compatibility() -> None:
    matrix = PairTypeMatrixService().build(
        source="mssql",
        sink="clickhouse",
        source_types=(
            "doc_movement_id:int nullable",
            "dm_base_zone_name:nvarchar(510) nullable",
            "datetime",
            "datetime2(7) nullable",
        ),
    )

    by_source = {entry["source_type"]: entry for entry in matrix["entries"]}

    assert by_source["int nullable"]["target_type"] == "Nullable(Int32)"
    assert by_source["int nullable"]["column"] == "doc_movement_id"
    assert by_source["int nullable"]["schema_evolution_compatible"] is True
    assert by_source["nvarchar(510) nullable"]["target_type"] == "Nullable(String)"
    assert by_source["nvarchar(510) nullable"]["column"] == "dm_base_zone_name"
    assert by_source["datetime"]["target_type"] == "DateTime64(3)"
    assert by_source["datetime"]["native_transport"] == "epoch_or_text DateTime64 wire format"
    assert by_source["datetime2(7) nullable"]["target_type"] == "Nullable(DateTime64(7))"
    assert matrix["runbook"].endswith("source-sink/mssql-to-clickhouse.md#runbook-false-schema-evolution-type-changes")


def test_pair_type_matrix_reuses_postgres_mssql_profile() -> None:
    matrix = PairTypeMatrixService().build(
        source="postgres",
        sink="mssql",
        source_types=("integer", "jsonb", "my_enum"),
    )

    by_source = {entry["source_type"]: entry for entry in matrix["entries"]}

    assert by_source["integer"]["target_type"] == "int"
    assert by_source["jsonb"]["target_type"] == "nvarchar(max)"
    assert by_source["my_enum"]["requires_explicit_contract"] is True


def test_pair_type_matrix_certifies_clickhouse_mssql_landing_profile() -> None:
    matrix = PairTypeMatrixService().build(
        source="clickhouse",
        sink="mssql",
        source_types=(
            "user_id:UInt64",
            "event_at:DateTime64(3, 'Europe/Moscow')",
            "payload:Nullable(String)",
            "amount:Decimal(18,4)",
            "tags:Array(String)",
            "too_precise:DateTime64(9)",
            "huge_decimal:Decimal(76,4)",
        ),
    )

    by_column = {entry["column"]: entry for entry in matrix["entries"]}

    assert matrix["profile"] == "clickhouse_to_mssql_landing_v1"
    assert by_column["user_id"]["target_type"] == "decimal(20,0)"
    assert by_column["event_at"]["target_type"] == "datetime2(3)"
    assert by_column["payload"]["target_type"] == "nvarchar(max)"
    assert by_column["amount"]["target_type"] == "decimal(18,4)"
    assert by_column["tags"]["target_type"] == "nvarchar(max)"
    assert by_column["tags"]["requires_explicit_contract"] is True
    assert by_column["tags"]["schema_evolution_compatible"] is False
    assert by_column["too_precise"]["target_type"] == "datetime2(7)"
    assert by_column["too_precise"]["lossless"] is False
    assert by_column["too_precise"]["requires_explicit_contract"] is True
    assert by_column["huge_decimal"]["target_type"] == "nvarchar(max)"
    assert by_column["huge_decimal"]["requires_explicit_contract"] is True


def test_pair_type_matrix_lists_priority_profiles() -> None:
    pairs = PairTypeMatrixService().available_pairs()

    assert ("mssql", "clickhouse") in pairs
    assert ("postgres", "mssql") in pairs
    assert ("mssql", "mssql") in pairs
    assert ("postgres", "postgres") in pairs
    assert ("postgres", "clickhouse") in pairs
    assert ("clickhouse", "mssql") in pairs


def test_pair_type_matrix_explains_postgres_clickhouse_profile() -> None:
    matrix = PairTypeMatrixService().build(
        source="postgres",
        sink="clickhouse",
        source_types=("id:integer", "payload:jsonb nullable", "updated_at:timestamp with time zone"),
    )

    by_column = {entry["column"]: entry for entry in matrix["entries"]}

    assert matrix["profile"] == "postgres_to_clickhouse_analytics_v1"
    assert by_column["id"]["target_type"] == "Int32"
    assert by_column["id"]["schema_evolution_compatible"] is True
    assert by_column["payload"]["target_type"] == "Nullable(String)"
    # jsonb→String is an intentional text landing that must stay contract-aware.
    assert by_column["payload"]["requires_explicit_contract"] is True
    assert by_column["updated_at"]["target_type"] == "DateTime64(6, 'UTC')"


def test_pair_type_matrix_explains_identity_profiles() -> None:
    mssql = PairTypeMatrixService().build(
        source="mssql",
        sink="mssql",
        source_types=("doc_movement_id:int nullable", "dm_base_zone_name:nvarchar(510) nullable"),
    )
    postgres = PairTypeMatrixService().build(
        source="postgres",
        sink="postgres",
        source_types=("id:integer", "payload:jsonb nullable"),
    )

    mssql_by_column = {entry["column"]: entry for entry in mssql["entries"]}
    postgres_by_column = {entry["column"]: entry for entry in postgres["entries"]}

    assert mssql_by_column["doc_movement_id"]["target_type"] == "int"
    assert mssql_by_column["dm_base_zone_name"]["target_type"] == "nvarchar(510)"
    assert all(entry["schema_evolution_compatible"] for entry in mssql["entries"])
    assert postgres_by_column["id"]["target_type"] == "integer"
    assert postgres_by_column["payload"]["target_type"] == "jsonb"


def test_schema_type_matrix_cli_renders_json(capsys) -> None:
    args = argparse.Namespace(
        source="mssql",
        sink="clickhouse",
        source_type=["int nullable", "nvarchar(510) nullable", "datetime"],
        type_fidelity_json=None,
        format="json",
    )

    exit_code = cmd_schema_type_matrix(args, ctx=object(), logger=logging.getLogger(__name__))

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["source"] == "mssql"
    assert payload["sink"] == "clickhouse"
    assert payload["entries"][0]["target_type"] == "Nullable(Int32)"


def test_schema_type_matrix_cli_reports_unsupported_pair_without_traceback(capsys) -> None:
    args = argparse.Namespace(
        source="clickhouse",
        sink="kafka",
        source_type=None,
        type_fidelity_json=None,
        format="text",
    )

    exit_code = cmd_schema_type_matrix(args, ctx=object(), logger=logging.getLogger(__name__))

    assert exit_code == 2
    assert "type matrix is not available for clickhouse -> kafka" in capsys.readouterr().out


def test_schema_type_matrix_cli_is_registered() -> None:
    parser = build_parser()

    parsed = parser.parse_args(["schema", "type-matrix", "--source", "mssql", "--sink", "clickhouse"])

    assert parsed.schema_cmd == "type-matrix"


def test_schema_type_matrix_cli_accepts_priority_profiles() -> None:
    parser = build_parser()

    parsed = parser.parse_args(["schema", "type-matrix", "--source", "postgres", "--sink", "clickhouse"])

    assert parsed.source == "postgres"
    assert parsed.sink == "clickhouse"
