from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.readiness.managed import ExecutionPlanService
from dpone.runtime.source_impact import SourceImpactInspector
from dpone.runtime.sources.strategies.mssql import MSSQLFullExtractStrategy


def test_manifest_schemas_expose_runtime_storage_and_native_transfer_options() -> None:
    config_schema = json.loads(Path("src/dpone/schema/etl-config.schema.json").read_text(encoding="utf-8"))
    batch_schema = json.loads(Path("src/dpone/schema/etl-batch-manifest.schema.json").read_text(encoding="utf-8"))

    config_options = config_schema["properties"]["source"]["properties"]["options"]["properties"]
    batch_options = batch_schema["definitions"]["process_fragment"]["properties"]["source"]["properties"]["options"][
        "properties"
    ]

    assert "runtime" in config_schema["properties"]
    assert "runtime" in batch_schema["properties"]
    assert "transfer_store" in config_schema["definitions"]["runtime_storage"]["properties"]
    assert "transfer_store" in batch_schema["definitions"]["runtime_storage"]["properties"]
    assert "columns" in config_options
    assert "native_transfer" in config_options
    transport = config_schema["definitions"]["native_transfer_transport_policy"]
    batch_transport = batch_schema["definitions"]["native_transfer_transport_policy"]
    assert set(transport["properties"]["mode"]["enum"]) == {"auto", "file", "object", "stream"}
    assert "stream_buffer_bytes" in transport["properties"]
    assert set(batch_transport["properties"]["mode"]["enum"]) == {"auto", "file", "object", "stream"}
    certification = config_schema["definitions"]["native_transfer_route_certification_policy"]
    assert set(certification["properties"]["mode"]["enum"]) == {"auto", "advisory", "certified_only"}
    wire = config_schema["definitions"]["native_transfer_wire_policy"]
    batch_wire = batch_schema["definitions"]["native_transfer_wire_policy"]
    assert "typed_binary" in wire["properties"]["mode"]["enum"]
    assert wire["properties"]["source_native_format"]["enum"] == ["auto", "odbc_row_stream", "bcp_native"]
    assert wire["properties"]["binary_format"]["enum"] == ["rowbinary", "native"]
    assert "block_rows" in wire["properties"]
    assert "block_bytes" in wire["properties"]
    assert set(batch_wire["properties"]["null_policy"]["enum"]) == {"sidecar", "source_marker", "not_nullable_only"}
    assert batch_wire["properties"]["source_native_format"]["enum"] == ["auto", "odbc_row_stream", "bcp_native"]
    assert "columns" in batch_options
    assert "native_transfer" in batch_options
    clickhouse_bulk = config_schema["properties"]["sink"]["properties"]["options"]["properties"]["clickhouse_bulk"]
    assert "typed_binary_staging" in clickhouse_bulk["properties"]["ingest_contract"]["enum"]


def test_source_impact_inspector_warns_on_select_star_and_missing_boundary_index() -> None:
    diagnostics = SourceImpactInspector().inspect(
        source_type="mssql",
        base_query="SELECT * FROM dbo.orders_view WHERE CAST(id AS varchar(20)) >= '10'",
        partition_column="id",
        indexed_columns=("created_at",),
        source_kind="view",
    )

    codes = {item.code for item in diagnostics}
    assert "source_select_star" in codes
    assert "source_boundary_index_unknown" in codes
    assert "source_non_sargable_boundary" in codes
    assert "source_view_over_view_risk" in codes


def test_plan_includes_runtime_storage_and_native_execution_policy(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text(
        """
name: orders
runtime:
  storage:
    work_dir: /mnt/dpone-work
    min_free_bytes: 2GiB
source:
  type: mssql
  connection_id: mssql
  table:
    schema: dbo
    name: orders
  options:
    columns: [id, name]
    partitioning:
      column: id
      bounds: auto
      target_rows_per_partition: 100000
    native_transfer:
      execution:
        profile: safe_worker
sink:
  type: clickhouse
  connection_id: clickhouse
  table:
    schema: analytics
    name: orders
  strategy:
    mode: full_refresh
""".strip()
        + "\n",
        encoding="utf-8",
    )

    plan = ExecutionPlanService().plan_manifest(manifest, explain_strategy=True)

    assert plan["runtime_storage"]["work_dir"] == "/mnt/dpone-work"
    assert plan["runtime_storage"]["min_free_bytes"] == 2 * 1024 * 1024 * 1024
    assert plan["native_transfer_execution"]["profile"] == "safe_worker"
    assert plan["native_transfer_execution"]["transport"]["mode"] == "auto"
    assert plan["native_transfer_execution"]["transport"]["fallback_to_file"] is True
    assert plan["native_transfer_execution"]["resource_policy"]["max_active_files"] == 1
    assert plan["source"]["columns"] == ["id", "name"]


def test_plan_includes_object_backed_transfer_store_contract(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text(
        """
name: orders
runtime:
  storage:
    profile: object_backed
    work_dir: /mnt/dpone-work
    transfer_store:
      type: s3
      uri: s3://dpone-stage/native-transfer
      connection_id: dpone_transfer_store
      connection_type: env
source:
  type: mssql
  connection_id: mssql
  table:
    schema: dbo
    name: orders
  options:
    native_transfer:
      execution:
        resume_policy: object_store_if_verified
sink:
  type: clickhouse
  connection_id: clickhouse
  table:
    schema: analytics
    name: orders
  strategy:
    mode: full_refresh
""".strip()
        + "\n",
        encoding="utf-8",
    )

    plan = ExecutionPlanService().plan_manifest(manifest, explain_strategy=True)

    assert plan["runtime_storage"]["profile"] == "object_backed"
    assert plan["runtime_storage"]["transfer_store"]["uri"] == "s3://dpone-stage/native-transfer"
    assert plan["native_transfer_execution"]["resume_policy"] == "object_store_if_verified"


def test_plan_resolves_postgres_clickhouse_http_as_stream_transport(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text(
        """
name: orders
source:
  type: postgres
  connection_id: postgres
  table:
    schema: public
    name: orders
  options:
    extract_mode: copy_to_stdout
    native_transfer:
      execution:
        transport:
          mode: auto
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
""".strip()
        + "\n",
        encoding="utf-8",
    )

    plan = ExecutionPlanService().plan_manifest(manifest)

    assert plan["native_transfer_transport"]["transport"] == "stream"
    assert plan["native_transfer_transport"]["eligibility"] == {
        "source": True,
        "sink": True,
        "codec": True,
        "reasons": [],
    }
    assert plan["native_transfer_route_decision"]["selected_transport"] == "stream"
    assert plan["native_transfer_route_decision"]["certification_mode"] == "advisory"
    assert plan["native_transfer_route_decision"]["release_gate"] == "warning"


def test_plan_certified_only_blocks_missing_route_certification_artifact(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text(
        """
name: orders
source:
  type: postgres
  connection_id: postgres
  table:
    schema: public
    name: orders
  options:
    extract_mode: copy_to_stdout
    native_transfer:
      execution:
        certification:
          mode: certified_only
          artifact: .dpone/certification/native_transfer_route_certification.json
        transport:
          mode: auto
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
""".strip()
        + "\n",
        encoding="utf-8",
    )

    plan = ExecutionPlanService().plan_manifest(manifest)

    assert plan["native_transfer_route_decision"]["selected_transport"] is None
    assert plan["native_transfer_route_decision"]["certification_mode"] == "certified_only"
    assert "native_transfer_route_certification.missing" in plan["native_transfer_route_decision"]["blockers"]


def test_plan_resolves_mssql_bcp_as_file_transport_with_explicit_reason(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text(
        """
name: counterparty
source:
  type: mssql
  connection_id: mssql
  table:
    schema: dbo
    name: v_counterparty
  options:
    extract_mode: bcp_queryout
    native_transfer:
      execution:
        transport:
          mode: auto
sink:
  type: clickhouse
  connection_id: clickhouse
  table:
    schema: analytics
    name: counterparty
  strategy:
    mode: full_refresh
  options:
    clickhouse_bulk:
      mode: http
""".strip()
        + "\n",
        encoding="utf-8",
    )

    plan = ExecutionPlanService().plan_manifest(manifest)

    assert plan["native_transfer_transport"]["transport"] == "file"
    assert plan["native_transfer_transport"]["fallback_reason"] == "native_transfer_stream_fallback_file_only_source"
    assert "bcp_queryout_is_file_transport" in plan["native_transfer_transport"]["eligibility"]["reasons"]


def test_plan_includes_typed_bulk_wire_for_mssql_clickhouse(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text(
        """
name: counterparty
source:
  type: mssql
  connection_id: mssql
  table:
    schema: dbo
    name: counterparty
    options:
      columns:
      - name: id
        type: int
      - name: name
        type: nvarchar(100)
    extract_mode: bcp_queryout
    native_transfer:
      wire:
        mode: typed_raw
        delimiter_profile: ascii_control
sink:
  type: clickhouse
  connection_id: clickhouse
  table:
    schema: analytics
    name: counterparty
  strategy:
    mode: full_refresh
  options:
    clickhouse_bulk:
      mode: http
      ingest_contract: typed_staging
""".strip()
        + "\n",
        encoding="utf-8",
    )

    plan = ExecutionPlanService().plan_manifest(manifest)

    assert plan["bulk_path"] == "mssql_bcp_queryout_to_clickhouse_typed_wire"
    assert plan["native_transfer_bulk_wire"]["selected_route"] == "typed_raw_direct"
    assert plan["native_transfer_bulk_wire"]["input_format"] == "CustomSeparated"
    assert plan["native_transfer_bulk_wire"]["delimiter_profile"]["field_delimiter"] == "\x1f"
    assert plan["native_transfer_bulk_wire"]["mssql_source_escaping"] is False


def test_mssql_extract_prunes_configured_columns(tmp_path: Path) -> None:
    class FakeConnector:
        bcp_path = "bcp"
        trust_server_certificate = "yes"

        def __init__(self) -> None:
            self.columns: list[str] = []

        def fetch_schema(self, schema: str, table: str):
            assert (schema, table) == ("dbo", "orders")
            return [("id", "int"), ("name", "varchar(20)"), ("payload", "nvarchar(max)")]

        def build_select_query(self, schema: str, table: str, columns: list[str]) -> str:
            self.columns = columns
            return f"SELECT {', '.join(columns)} FROM {schema}.{table}"

        def quote_identifier(self, name: str) -> str:
            return f"[{name}]"

        def bcp_queryout(self, query: str, output_path: str, *, options=None) -> int:
            del query, options
            Path(output_path).write_text("1\talpha\n", encoding="utf-8")
            return 1

    config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="analytics",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={"columns": ["id", "name"], "runtime_storage": {"work_dir": str(tmp_path)}},
    )

    result = MSSQLFullExtractStrategy(FakeConnector(), _Logger()).extract(config, None)

    assert result.schema == [("id", "int"), ("name", "varchar(20)")]
    assert Path(result.artifact.file_path).parent == tmp_path


def test_mssql_extract_rejects_unknown_pruned_column() -> None:
    class FakeConnector:
        def fetch_schema(self, schema: str, table: str):
            return [("id", "int")]

    config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="analytics",
        target_table="orders",
        options={"columns": ["missing"]},
    )

    with pytest.raises(ValueError, match="Unknown source columns"):
        MSSQLFullExtractStrategy(FakeConnector(), _Logger()).extract(config, None)


class _Logger:
    def log_etl_progress(self, event: str, payload: dict[str, object]) -> None:
        del event, payload
