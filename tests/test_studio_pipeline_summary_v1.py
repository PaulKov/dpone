from __future__ import annotations

import copy
import json
from pathlib import Path

import jsonschema
import yaml

from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service
from dpone.readiness.studio_composition import build_studio_api_service
from dpone.readiness.studio_http_models import StudioApiError
from dpone.readiness.studio_openapi import studio_openapi_document


def _project(tmp_path: Path) -> None:
    self_service = build_airflow_self_service_service(root=tmp_path)
    assert self_service.init_project(airflow=True).passed
    assert self_service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
    ).passed


def test_pipeline_list_uses_public_summary_schema_and_preserves_process_order(
    tmp_path: Path,
) -> None:
    _project(tmp_path)
    service = build_studio_api_service(root=tmp_path)

    result = service.pipeline_list()

    assert result["schema"] == "dpone.pipeline-summary-list.v1"
    assert result["count"] == 1
    summary = result["items"][0]
    schema = json.loads(Path("src/dpone/schema/pipeline-summary.schema.json").read_text(encoding="utf-8"))
    jsonschema.validate(summary, schema)
    assert summary["id"] == "orders_daily"
    assert [process["name"] for process in summary["processes"]] == ["orders_daily"]
    assert summary["processes"][0]["source"] == "mssql"
    assert summary["processes"][0]["sink"] == "clickhouse"
    assert summary["support"]["status"] == "supported"
    assert summary["certification"]["evidence_status"] == "UNVERIFIED"
    assert summary["next_actions"][0]["argv"] == [
        "dpone",
        "airflow",
        "preview",
        "orders_daily",
    ]
    openapi = studio_openapi_document()
    jsonschema.validate(
        result,
        {
            "$ref": "#/components/schemas/PipelineSummaryListResponse",
            "components": openapi["components"],
        },
    )


def test_pipeline_list_rejects_invalid_pagination_with_structured_error(
    tmp_path: Path,
) -> None:
    _project(tmp_path)
    service = build_studio_api_service(root=tmp_path)

    try:
        service.pipeline_list(limit=201)
    except StudioApiError as exc:
        assert exc.code == "DPONE_STUDIO_PAGE_LIMIT_INVALID"
    else:
        raise AssertionError("invalid Studio pagination must fail closed")


def test_pipeline_explain_is_scoped_to_requested_pipeline(
    tmp_path: Path,
) -> None:
    _project(tmp_path)
    service = build_studio_api_service(root=tmp_path)

    result = service.pipeline_explain("orders_daily")

    serialized = json.dumps(result, sort_keys=True)
    assert "orders_daily" in serialized
    assert "another_pipeline" not in serialized
    assert result["next_actions"][0]["argv"][-1] == "orders_daily"
    openapi = studio_openapi_document()
    jsonschema.validate(
        result,
        {
            "$ref": "#/components/schemas/PipelineExplainResponse",
            "components": openapi["components"],
        },
    )


def test_pipeline_summary_blocks_an_unknown_runtime_route(
    tmp_path: Path,
) -> None:
    _project(tmp_path)
    source_path = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    payload = yaml.safe_load(source_path.read_text(encoding="utf-8"))
    payload["processes"][0]["source"]["type"] = "unknown_source"
    source_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    summary = build_studio_api_service(root=tmp_path).pipeline_list()["items"][0]

    assert summary["valid"] is False
    assert summary["operational_status"] == "blocked"
    assert summary["support"] is None
    assert summary["errors"][0]["code"] == "DPONE_ROUTE_NOT_SUPPORTED"
    assert summary["errors"][0]["entity"]["id"] == ("unknown_source:clickhouse:incremental_merge")
    assert summary["next_actions"][0]["id"] == "check_pipeline"
    assert summary["next_actions"][0]["argv"] == [
        "dpone",
        "check",
        "pipelines/orders_daily",
    ]


def test_pipeline_summary_blocks_a_mixed_process_pipeline_when_one_route_is_unknown(
    tmp_path: Path,
) -> None:
    _project(tmp_path)
    source_path = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    payload = yaml.safe_load(source_path.read_text(encoding="utf-8"))
    unsupported = copy.deepcopy(payload["processes"][0])
    unsupported["name"] = "unsupported_copy"
    unsupported["source"]["type"] = "unknown_source"
    payload["processes"].append(unsupported)
    source_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    summary = build_studio_api_service(root=tmp_path).pipeline_list()["items"][0]

    assert summary["valid"] is False
    assert summary["operational_status"] == "blocked"
    assert [item["name"] for item in summary["processes"]] == [
        "orders_daily",
        "unsupported_copy",
    ]
    assert summary["errors"][0]["code"] == "DPONE_ROUTE_NOT_SUPPORTED"
    assert summary["next_actions"][0]["id"] == "check_pipeline"
