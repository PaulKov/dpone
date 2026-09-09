from __future__ import annotations

from pathlib import Path

import jsonschema

from dpone.gitops.schema_airflow_preflight_contracts import airflow_explain_contract
from dpone.readiness.airflow_self_service import AirflowSelfServiceService
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service


def _initialized_service(root: Path) -> AirflowSelfServiceService:
    service = build_airflow_self_service_service(root=root)
    assert service.init_project(airflow=True).passed
    assert service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
    ).passed
    return service


def test_preview_deployment_is_create_or_compare_and_never_overwritten(tmp_path: Path) -> None:
    service = _initialized_service(tmp_path)
    first = service.preview("orders_daily")
    assert first.passed

    deployment = first.details["deployment"]
    assert isinstance(deployment, dict)
    deployment_id = str(deployment["deployment_id"]).replace(":", "-")
    index_path = tmp_path / ".dpone-cache" / "deployments" / "local-preview" / deployment_id / "airflow-index.json"
    index_path.write_bytes(b'{"corrupt":true}\n')

    repeated = service.preview("orders_daily")

    assert not repeated.passed
    assert repeated.exit_code == 4
    assert repeated.errors[0]["code"] == "DPONE_DEPLOYMENT_ALREADY_EXISTS"
    assert index_path.read_bytes() == b'{"corrupt":true}\n'


def test_repeated_identical_preview_is_idempotent(tmp_path: Path) -> None:
    service = _initialized_service(tmp_path)

    first = service.preview("orders_daily")
    repeated = service.preview("orders_daily")

    assert first.passed
    assert repeated.passed
    assert repeated.details["release"]["release_id"] == first.details["release"]["release_id"]
    assert repeated.details["deployment"]["deployment_id"] == first.details["deployment"]["deployment_id"]


def test_explain_reports_canonical_deployment_identity_and_valid_states(tmp_path: Path) -> None:
    service = _initialized_service(tmp_path)
    preview = service.preview("orders_daily")
    assert preview.passed

    result = service.explain("orders_daily")

    assert result.passed
    artifact_state = result.details["artifact_state"]
    deployment_id = preview.details["deployment"]["deployment_id"]
    assert artifact_state["published_deployment_id"] == deployment_id
    assert artifact_state["published_generation"] == deployment_id
    jsonschema.Draft202012Validator(airflow_explain_contract().schema).validate(result.to_dict())
