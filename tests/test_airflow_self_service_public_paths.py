from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from dpone.commands.airflow_self_service_output import _public_target
from dpone.commands.run_safe_sample_cmd import build_safe_sample_result
from dpone.readiness.airflow_authoring_check_service import AirflowAuthoringCheckService
from dpone.readiness.airflow_live_preflight import build_live_preflight_report
from dpone.readiness.airflow_local_safe_sample_deployment import (
    ensure_local_safe_sample_deployment,
)
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service
from tests.mssql_asset_registry_fixtures import write_mssql_connection_registry


def test_output_target_is_project_relative_for_absolute_in_root_input(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    monkeypatch.chdir(tmp_path)

    assert _public_target(source.as_posix()) == "pipelines/orders_daily/pipeline.yaml"


def test_missing_absolute_source_error_does_not_expose_workstation_root(tmp_path: Path) -> None:
    source = tmp_path / "pipelines/orders_daily/pipeline.yaml"

    result = AirflowAuthoringCheckService(root=tmp_path).inspect(source)

    assert result.result.passed is False
    assert result.result.errors[0]["entity"]["id"] == "pipelines/orders_daily/pipeline.yaml"
    assert tmp_path.as_posix() not in str(result.result.to_dict())


def test_preview_release_provenance_uses_project_relative_source(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
    )
    write_mssql_connection_registry(tmp_path, include=("mssql_dev",))

    result = service.preview("orders_daily")

    assert result.passed is True
    release = result.details["release"]
    assert release["provenance"]["source"] == "pipelines/orders_daily/pipeline.yaml"
    assert tmp_path.as_posix() not in str(result.to_dict())


def test_live_preflight_errors_use_project_relative_source_label(tmp_path: Path) -> None:
    source = tmp_path / "pipelines/orders_daily/pipeline.yaml"

    report = build_live_preflight_report(
        connection_details={"connection_refs": [], "resolved_connection_refs": []},
        environment="dev",
        source_path=source,
        source_label="pipelines/orders_daily/pipeline.yaml",
    )

    assert report["errors"][0]["path"] == "pipelines/orders_daily/pipeline.yaml"
    assert tmp_path.as_posix() not in str(report)


def test_local_safe_sample_receipt_uses_project_relative_paths(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    service.init_project(airflow=True)
    service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
    )

    result = ensure_local_safe_sample_deployment(
        root=tmp_path,
        pipeline_source_path="orders_daily",
        policy_environment="dev",
    )

    assert result.status == "promoted"
    assert result.deployment_dir is not None
    assert not Path(result.deployment_dir).is_absolute()
    assert tmp_path.as_posix() not in str(result.to_dict())


def test_safe_sample_argument_error_does_not_echo_absolute_manifest_path(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    source = tmp_path / "external" / "pipeline.yaml"
    monkeypatch.chdir(project)

    result = build_safe_sample_result(
        SimpleNamespace(
            path=source.as_posix(),
            sample=None,
            target="temporary",
            selector=None,
            run_id="",
            format="json",
        )
    )

    assert result.payload["manifest"] == "pipeline.yaml"
    assert tmp_path.as_posix() not in str(result.payload)
