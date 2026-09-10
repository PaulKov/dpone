from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from dpone.readiness.managed import ExecutionPlanService
from dpone.runtime.bulk_wire import BulkWirePlanner
from dpone.runtime.connectors.mssql_bulk import BcpOptions, BcpRunner
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.strategy_intelligence.advisor import StrategyAdvisor, StrategyContext

ROOT = Path(__file__).resolve().parents[1]


def _clickhouse_bulk_schema(schema: dict, relative: str) -> dict:
    if relative.endswith("etl-config.schema.json"):
        return schema["properties"]["sink"]["properties"]["options"]["properties"]["clickhouse_bulk"]
    options = schema["definitions"]["process_fragment"]["properties"]["sink"]["properties"]["options"]["properties"]
    return options["clickhouse_bulk"]


def test_public_schemas_expose_typed_binary_native_wire_policy() -> None:
    for relative in ("src/dpone/schema/etl-config.schema.json", "src/dpone/schema/etl-batch-manifest.schema.json"):
        schema = json.loads((ROOT / relative).read_text())
        policy = schema["definitions"]["native_transfer_wire_policy"]["properties"]

        assert "typed_binary" in policy["mode"]["enum"]
        assert policy["source_native_format"]["enum"] == ["auto", "odbc_row_stream", "bcp_native"]
        assert policy["binary_format"]["enum"] == ["rowbinary", "native", "mssql_native"]
        assert "block_rows" in policy
        assert "block_bytes" in policy
        clickhouse_bulk = _clickhouse_bulk_schema(schema, relative)
        assert "typed_binary_staging" in clickhouse_bulk["properties"]["ingest_contract"]["enum"]


def test_bcp_unicode_native_is_blocked_on_non_windows() -> None:
    runner = BcpRunner(
        credentials=object(),
        options=BcpOptions(file_format="wide-native"),
    )

    if sys.platform == "win32":
        assert runner._file_format_flag() == "-N"
    else:
        with pytest.raises(ValueError, match="bcp_unicode_native_requires_windows"):
            runner._file_format_flag()


def test_uppercase_n_file_format_is_treated_as_unicode_native_flag() -> None:
    runner = BcpRunner(
        credentials=object(),
        options=BcpOptions(file_format="N"),
    )

    if sys.platform == "win32":
        assert runner._file_format_flag() == "-N"
    else:
        with pytest.raises(ValueError, match="bcp_unicode_native_requires_windows"):
            runner._file_format_flag()


def test_unsupported_native_wire_type_blocks_before_export() -> None:
    contract = build_mssql_bcp_native_contract(
        schema=[("shape", "geography nullable")],
        query="SELECT [shape] FROM [dbo].[geo]",
        bcp_version="test-bcp",
    )

    assert contract.blockers == ("mssql_bcp_native_unsupported_type:geography nullable",)


def test_typed_binary_auto_keeps_existing_odbc_row_stream_default() -> None:
    contract = BulkWirePlanner().plan(
        source_type="mssql",
        sink_type="clickhouse",
        schema=[("id", "int")],
        source_options={"native_transfer": {"wire": {"mode": "typed_binary"}}},
        sink_options={"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"}},
    )

    assert contract.selected_route == "typed_binary_row_stream"


def test_execution_plan_reports_bcp_native_rowbinary_bulk_path(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text(
        """
name: orders
source:
  type: mssql
  connection_id: mssql
  table:
    schema: dbo
    name: orders
  options:
    extract_mode: bcp_queryout
    native_transfer:
      wire:
        mode: typed_binary
        source_native_format: bcp_native
sink:
  type: clickhouse
  connection_id: clickhouse
  table:
    schema: analytics
    name: orders
  strategy:
    mode: full_refresh
  options:
    clickhouse_bulk:
      mode: http
      ingest_contract: typed_binary_staging
""".strip()
        + "\n",
        encoding="utf-8",
    )

    plan = ExecutionPlanService().plan_manifest(manifest)

    assert plan["bulk_path"] == "mssql_bcp_native_to_clickhouse_rowbinary"
    assert plan["native_transfer_bulk_wire"]["selected_route"] == "typed_binary_bcp_native"


def test_execution_plan_reports_bcp_native_clickhouse_native_bulk_path(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text(
        """
name: orders
source:
  type: mssql
  connection_id: mssql
  table:
    schema: dbo
    name: orders
  options:
    extract_mode: bcp_queryout
    native_transfer:
      wire:
        mode: typed_binary
        source_native_format: bcp_native
        binary_format: native
sink:
  type: clickhouse
  connection_id: clickhouse
  table:
    schema: analytics
    name: orders
  strategy:
    mode: full_refresh
  options:
    clickhouse_bulk:
      mode: http
      ingest_contract: typed_binary_staging
""".strip()
        + "\n",
        encoding="utf-8",
    )

    plan = ExecutionPlanService().plan_manifest(manifest)

    assert plan["bulk_path"] == "mssql_bcp_native_to_clickhouse_native"
    assert plan["native_transfer_bulk_wire"]["input_format"] == "Native"


def test_strategy_advisor_reports_specific_bcp_native_fast_path() -> None:
    decision = StrategyAdvisor().advise(
        StrategyContext(
            source_type="mssql",
            sink_type="clickhouse",
            requested_mode="full_refresh",
            source_options={
                "native_transfer": {"wire": {"mode": "typed_binary", "source_native_format": "bcp_native"}}
            },
            sink_options={"clickhouse_bulk": {"mode": "http", "ingest_contract": "typed_binary_staging"}},
        )
    )

    assert decision.native_fast_path == "mssql_bcp_native_to_clickhouse_rowbinary"
