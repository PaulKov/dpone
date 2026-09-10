"""Offline plan must describe authored native intent without promising live admission."""

from pathlib import Path

import pytest

from dpone.commands.plan_cmd import _render_md, _render_text
from dpone.readiness.managed import ExecutionPlanService

SAMPLE = Path("examples/native/clickhouse-to-mssql-native.yaml")


def test_native_example_plan_is_bounded_and_requires_composition() -> None:
    plan = ExecutionPlanService().plan_manifest(SAMPLE, explain_strategy=True)
    native = plan["mssql_native"]
    assert native["status"] == "composition_required"
    assert native["source_query_count"] == 1
    assert native["offset_pagination"] is False
    assert native["limits"]["parallelism"] == 2
    assert native["limits"]["max_total_encoded_bytes"] == 1073741824
    assert native["spool_payload_bound"] == 5 * 16777216
    assert native["publication_scope"]["column"] == "observed_at"
    assert plan["native_transfer_bulk_wire"]["binary_format"] == "mssql_native"
    assert plan["native_transfer_bulk_wire"]["input_format"] == "mssql_native"
    assert plan["native_transfer_route_decision"]["selected_transport"] == "bounded_native_files"
    assert plan["native_transfer_snapshot_optimization"] == {}
    assert not any(item["code"] == "source_select_star" for item in plan["source_impact"])
    assert plan["type_matrix"]["ready"] is False
    assert plan["physical_design"]["ddl"] == []
    assert plan["strategy_intelligence"]["decision"]["native_transfer_plan"]["export_method"] == "one_clickhouse_query"


@pytest.mark.parametrize("render", [_render_md, _render_text])
def test_native_plan_render_reports_limits_and_no_generic_snapshot(render) -> None:
    output = render(ExecutionPlanService().plan_manifest(SAMPLE))
    for token in (
        "composition_required",
        "max_row_bytes",
        "max_total_encoded_bytes",
        "stage_allocated_bytes_stop_threshold",
        "spool_payload_bound",
        "source_query_count",
        "offset_pagination",
        "observed_stop_threshold",
    ):
        assert token in output
    for token in (
        "source_select_star",
        "RowBinary",
        "partition_planner",
        "max_parallel_exports",
        "fallback_file_only_source",
        "stream_buffer_bytes",
    ):
        assert token not in output


def test_legacy_route_keeps_generic_plan(tmp_path: Path) -> None:
    import yaml

    raw = yaml.safe_load(SAMPLE.read_text())
    raw["defaults"]["source"]["options"] = {}
    raw["defaults"]["sink"]["strategy"] = {"mode": "full_refresh"}
    path = tmp_path / "legacy.yaml"
    path.write_text(yaml.safe_dump(raw))
    plan = ExecutionPlanService().plan_manifest(path)
    assert "mssql_native" not in plan
    assert plan["bulk_path"] == "mssql_bcp_import"
    assert plan["native_transfer_snapshot_optimization"]["partition_planner"] == "statistics"
    assert any(item["code"] == "source_select_star" for item in plan["source_impact"])


@pytest.mark.parametrize("missing", ["wire", "chunking"])
def test_partial_optin_does_not_relabel_legacy_plan(missing: str) -> None:
    from copy import deepcopy
    from types import SimpleNamespace

    from dpone.readiness.mssql_native_planning import project_mssql_native

    native = {
        "wire": {"mode": "typed_binary", "binary_format": "mssql_native"},
        "execution": {"chunking": {"mode": "bounded_stream"}},
    }
    if missing == "wire":
        native.pop("wire")
    else:
        native["execution"].pop("chunking")
    plan = {"source": {"type": "clickhouse"}, "sink": {"type": "mssql"}, "bulk_path": "legacy"}
    original = deepcopy(plan)
    project_mssql_native(plan, SimpleNamespace(options={"native_transfer": native}))
    assert plan == original


def test_native_full_refresh_plan_uses_validated_limit_defaults(tmp_path: Path) -> None:
    import yaml

    raw = yaml.safe_load(SAMPLE.read_text())
    raw["defaults"]["sink"]["strategy"] = {"mode": "full_refresh"}
    chunks = raw["defaults"]["source"]["options"]["native_transfer"]["execution"]["native_chunks"]
    chunks.pop("max_row_bytes")
    path = tmp_path / "native-full.yaml"
    path.write_text(yaml.safe_dump(raw))
    plan = ExecutionPlanService().plan_manifest(path)
    assert plan["mssql_native"]["publication_scope"] == {"mode": "full_refresh"}
    assert plan["mssql_native"]["limits"]["max_row_bytes"] == 1048576
