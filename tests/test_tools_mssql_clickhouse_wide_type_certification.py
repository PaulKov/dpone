from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


def _load_tool_module():
    path = Path("tools/mssql_clickhouse_wide_type_certification.py")
    spec = importlib.util.spec_from_file_location("dpone_tools_mssql_clickhouse_wide_type_certification", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_wide_fixture_contains_required_mssql_edge_types() -> None:
    module = _load_tool_module()

    columns = module.build_wide_columns(120)
    by_name = {column.name: column for column in columns}

    assert len(columns) == 120
    assert by_name["payload_bin"].mssql_type == "varbinary(16)"
    assert by_name["fixed_payload"].mssql_type == "binary(4)"
    assert by_name["row_version"].mssql_type == "rowversion"
    assert by_name["business_time"].mssql_type == "time(7)"
    assert by_name["offset_at"].mssql_type == "datetimeoffset(7)"
    assert by_name["empty_string"].mssql_type == "nvarchar(20)"
    assert all(column.insert_expression for column in columns if column.name != "row_version")


def test_load_config_options_enable_lossless_binary_and_time_policy() -> None:
    module = _load_tool_module()
    config = module.WideTypeCertificationConfig(
        rows=10000,
        column_count=120,
        source_schema="wide_src",
        source_table="orders",
        target_database="analytics",
        target_table="orders_ch",
        output_dir=Path("out"),
        mssql_params={"host": "127.0.0.1", "password": "secret", "bcp_path": "bcp"},
        clickhouse_params={"host": "127.0.0.1", "port": 9000, "http_port": 8123, "password": "secret"},
        target_rows_per_partition=2500,
        export_workers=2,
        load_workers=2,
        batch_size=5000,
    )

    load_config = module.build_load_config(config)

    assert load_config.options["type_fidelity"] == {
        "binary_encoding": "hex",
        "time_encoding": "seconds_since_midnight",
        "temporal": {"offset_timestamp": {"mode": "utc_instant", "timezone": "UTC"}},
    }
    assert load_config.options["mssql_queryout_projection"] == "view"
    assert load_config.options["partitioning"]["load_workers"] == 2
    assert load_config.options["clickhouse_bulk"]["mode"] == "http"


def test_mssql_typed_hash_projection_casts_datetimeoffset_for_odbc() -> None:
    module = _load_tool_module()

    expression = module._mssql_hash_select_expression("offset_at", "datetimeoffset(7)")

    assert "SWITCHOFFSET" in expression
    assert "datetime2(7)" in expression
    assert "[offset_at]" in expression
    assert "AS [offset_at]" in expression


def test_typed_hash_projections_preserve_datetime2_seventh_fractional_digit() -> None:
    module = _load_tool_module()

    source = module._mssql_hash_select_expression("created_at", "datetime2(7) nullable")
    target = module._clickhouse_hash_select_expression("created_at", "datetime2(7) nullable")

    assert source == "CONVERT(VARCHAR(33), [created_at], 121) AS [created_at]"
    assert target == "toString(`created_at`) AS `created_at`"


def test_clickhouse_typed_hash_projection_preserves_datetimeoffset_seventh_fractional_digit() -> None:
    module = _load_tool_module()

    target = module._clickhouse_hash_select_expression("offset_at", "datetimeoffset(7) nullable")

    assert target == "toString(`offset_at`) AS `offset_at`"


def test_clickhouse_typed_hash_projection_hex_encodes_binary_without_driver_text_decoding() -> None:
    module = _load_tool_module()

    expression = module._clickhouse_hash_select_expression(
        "payload_bin",
        "varbinary(16) nullable",
        binary_is_raw=True,
    )

    assert expression == "hex(`payload_bin`) AS `payload_bin`"
    assert module._clickhouse_hash_select_expression("payload_bin", "varbinary(16) nullable") == "`payload_bin`"
    assert module._clickhouse_hash_select_expression("order_id", "bigint") == "`order_id`"


def test_evidence_writer_persists_json_and_markdown(tmp_path: Path) -> None:
    module = _load_tool_module()
    result = module.WideTypeCertificationResult(
        rows=10000,
        column_count=120,
        source_count=10000,
        target_count=10000,
        duplicate_count=0,
        typed_hash_passed=True,
        typed_hash_source="sha256:abc",
        typed_hash_target="sha256:abc",
        elapsed_seconds=1.25,
        prepare_seconds=0.25,
        export_seconds=0.5,
        load_seconds=0.75,
        artifact_bytes=123456,
        passed=True,
    )

    written = module.WideTypeEvidenceWriter(tmp_path).write(result)

    payload = json.loads(written.json_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "dpone.mssql_clickhouse.wide_type_certification.v1"
    assert payload["passed"] is True
    markdown = written.markdown_path.read_text(encoding="utf-8")
    assert markdown.startswith("# MSSQL -> ClickHouse wide type certification")
    assert "Prepare seconds" in markdown
