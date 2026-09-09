from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from dpone.cli.parser import build_parser
from dpone.commands.schema_plan_cmd import cmd_schema_explain
from dpone.readiness.schema_evolution import ColumnDef
from dpone.services.schema_evolution_explain import SchemaEvolutionExplainService


def test_schema_evolution_explain_marks_expected_mssql_clickhouse_types_compatible() -> None:
    payload = SchemaEvolutionExplainService().explain(
        source_system="mssql",
        sink_system="clickhouse",
        source=[
            ColumnDef("doc_movement_id", "int", nullable=True),
            ColumnDef("dm_base_zone_name", "nvarchar(510) nullable", nullable=True),
        ],
        target=[
            ColumnDef("doc_movement_id", "Nullable(Int32)", nullable=True),
            ColumnDef("dm_base_zone_name", "Nullable(String)", nullable=True),
        ],
    )

    by_column = {item["column"]: item for item in payload["type_decisions"]}

    assert payload["has_breaking_changes"] is False
    assert payload["schema_plan"]["changes"] == []
    assert by_column["doc_movement_id"]["matrix_expected_target_type"] == "Nullable(Int32)"
    assert by_column["doc_movement_id"]["matrix_matches_target"] is True
    assert by_column["dm_base_zone_name"]["matrix_matches_target"] is True


def test_schema_evolution_explain_cli_outputs_json(tmp_path: Path, capsys) -> None:
    source = tmp_path / "source.json"
    target = tmp_path / "target.json"
    source.write_text(json.dumps([{"name": "id", "dtype": "integer", "nullable": False}]), encoding="utf-8")
    target.write_text(json.dumps([{"name": "id", "dtype": "Int32", "nullable": False}]), encoding="utf-8")
    args = argparse.Namespace(
        source=str(source),
        target=str(target),
        source_system="postgres",
        sink_system="clickhouse",
        mode="widening",
        allow_drop=False,
        on_type_change="fail",
        format="json",
    )

    exit_code = cmd_schema_explain(args, ctx=object(), logger=logging.getLogger(__name__))

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["source_system"] == "postgres"
    assert payload["sink_system"] == "clickhouse"
    assert payload["type_decisions"][0]["matrix_matches_target"] is True


def test_schema_explain_cli_is_registered() -> None:
    parser = build_parser()

    parsed = parser.parse_args(
        [
            "schema",
            "explain",
            "--source",
            "source.json",
            "--target",
            "target.json",
            "--source-system",
            "mssql",
            "--sink-system",
            "clickhouse",
        ]
    )

    assert parsed.schema_cmd == "explain"
