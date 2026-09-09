"""Contract tests for domain-first self-service authoring."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest
import yaml

from dpone.app.cli_reference_source import build_root_parser
from dpone.cli import main as cli_main
from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.manifest import project_discovery_scan as project_discovery_module
from dpone.manifest.project_config import ProjectConfigError, resolve_project_layout
from dpone.manifest.project_discovery import ProjectDiscoveryService
from dpone.readiness.airflow_authoring_check_service import AirflowAuthoringCheckService
from dpone.readiness.airflow_preview_service import AirflowPreviewService
from dpone.readiness.airflow_self_service import AirflowSelfServiceService
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service
from dpone.readiness.project_selection_loader import ProjectSelectionLoader
from dpone.readiness.project_selection_preview import ProjectSelectionPreviewService
from dpone.services.workload_discovery_projection import build_change_impact_report
from dpone.services.workload_index_contract import compare_workload_indexes, workload_index_from_snapshot


def _run_cli(
    args: list[str],
    capsys: pytest.CaptureFixture[str],
) -> tuple[int, str, str]:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(args)
    captured = capsys.readouterr()
    return int(exc.value.code or 0), captured.out, captured.err


def _init_domain(service: AirflowSelfServiceService, domain: str) -> None:
    assert service.init_domain(
        domain=domain,
        owner_team=f"data-{domain}",
        owner_contact=f"{domain}@example.com",
        approver_team="data-platform",
    ).passed


def test_domain_first_service_builds_one_authoring_source_and_project_graph(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)

    assert service.init_project(airflow=True, layout="domain_first").passed
    _init_domain(service, "crm")
    result = service.init_pipeline(
        pipeline_id="orders_daily",
        domain="crm",
        route="mssql:clickhouse:incremental_merge",
        from_locator="mssql_dev:dbo.orders",
        to_locator="clickhouse_dev:analytics.orders",
        unique_key="order_id",
        airflow=None,
    )

    assert result.passed, result.errors
    source_path = tmp_path / "workloads/crm/pipelines/orders_daily/pipeline.yaml"
    assert source_path.is_file()
    assert not (tmp_path / "domains").exists()
    payload = yaml.safe_load(source_path.read_text(encoding="utf-8"))
    assert payload["authoring"]["source"] == "workloads/crm/pipelines/orders_daily/pipeline.yaml"
    assert payload["metadata"]["domain"] == "crm"
    process = payload["processes"][0]
    assert process["source"]["connection_ref"] == "mssql_dev"
    assert process["source"]["table"] == {"schema": "dbo", "name": "orders"}
    assert process["sink"]["connection_ref"] == "clickhouse_dev"
    assert process["sink"]["table"] == {"schema": "analytics", "name": "orders"}
    assert process["sink"]["strategy"]["unique_key"] == "order_id"

    selection = ProjectSelectionLoader(root=tmp_path).load(tmp_path)
    assert tuple(selection.checked_sources) == ("orders_daily",)
    assert selection.graph.by_id()["orders_daily"].domain == "crm"
    assert tuple(item.declaration.dag_id for item in selection.dags) == ("orders_daily",)
    assert service.check("crm/orders_daily").passed
    preview = service.preview("orders_daily")
    assert preview.passed, preview.errors
    assert preview.details["pipeline_id"] == "orders_daily"
    project_preview = ProjectSelectionPreviewService(root=tmp_path).preview(
        target=tmp_path,
        select=(),
        exclude=(),
        state_path=None,
        selectors_path="selectors.yaml",
        max_selected=100,
    )
    assert project_preview.passed, project_preview.errors
    assert project_preview.details["release"]["artifacts"]["workload_packs"][0]["id"] == "orders_daily"


def test_domain_first_pipeline_requires_domain_ownership_before_writes(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="domain_first").passed

    result = service.init_pipeline(
        pipeline_id="orders_daily",
        domain="crm",
        recipe="mssql-to-clickhouse-incremental",
        airflow=None,
    )

    assert not result.passed
    assert result.errors[0]["code"] == "DPONE_DOMAIN_OWNERSHIP_MISSING"
    assert not (tmp_path / "workloads/crm/pipelines/orders_daily").exists()


def test_domain_first_rejects_duplicate_pipeline_id_before_writes(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="domain_first").passed
    _init_domain(service, "crm")
    _init_domain(service, "finance")
    assert service.init_pipeline(
        pipeline_id="orders_daily",
        domain="crm",
        recipe="mssql-to-clickhouse-incremental",
        airflow=None,
    ).passed

    duplicate = service.init_pipeline(
        pipeline_id="orders_daily",
        domain="finance",
        recipe="mssql-to-clickhouse-incremental",
        airflow=None,
    )

    assert not duplicate.passed
    assert duplicate.errors[0]["code"] == "DPONE_PIPELINE_ID_DUPLICATE"
    assert not (tmp_path / "workloads/finance/pipelines/orders_daily").exists()


@pytest.mark.parametrize(
    ("layout", "reason"),
    [
        ({}, "layout_mode_invalid"),
        ({"mode": "unknown"}, "layout_mode_invalid"),
        ({"mode": "domain_first", "root": "../outside"}, "layout_root_invalid"),
        ({"mode": "domain_first", "root": "/tmp/outside"}, "layout_root_invalid"),
        ({"mode": "domain_first", "root": " workloads"}, "layout_root_invalid"),
        ({"mode": "domain_first", "root": "workloads "}, "layout_root_invalid"),
        ({"mode": "domain_first", "pipeline_id_scope": "domain"}, "pipeline_id_scope_invalid"),
    ],
)
def test_project_layout_fails_closed(layout: dict[str, object], reason: str, tmp_path: Path) -> None:
    (tmp_path / "dpone.yaml").write_text(
        yaml.safe_dump({"schema": "dpone.project.v1", "layout": layout}, sort_keys=False),
        encoding="utf-8",
    )

    with pytest.raises(ProjectConfigError) as exc:
        resolve_project_layout(tmp_path)

    assert exc.value.reason == reason


def test_domain_first_cli_keeps_route_and_domain_options_target_scoped(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    code, output, error = _run_cli(["init", "project", "--airflow", "--layout", "domain-first"], capsys)
    assert code == 0, error
    assert "dpone init domain sales" in output
    code, output, error = _run_cli(
        [
            "init",
            "domain",
            "crm",
            "--owner-team",
            "data-crm",
            "--owner-contact",
            "crm@example.com",
            "--approver-team",
            "data-platform",
        ],
        capsys,
    )
    assert code == 0, error
    assert "dpone init pipeline orders_daily --domain crm" in output
    code, _, error = _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--domain",
            "crm",
            "--route",
            "mssql:clickhouse:incremental_merge",
            "--from",
            "mssql_dev:dbo.orders",
            "--to",
            "clickhouse_dev:analytics.orders",
            "--key",
            "order_id",
        ],
        capsys,
    )
    assert code == 0, error

    code, _, error = _run_cli(["init", "project", "--domain", "crm"], capsys)
    assert code == 2
    assert "project does not accept: --domain" in error


def test_pipeline_key_remains_visible_to_public_contract_introspection() -> None:
    parser = build_root_parser()
    root_subcommands = next(action for action in parser._actions if isinstance(action, argparse._SubParsersAction))
    init_parser = root_subcommands.choices["init"]
    init_targets = next(action for action in init_parser._actions if isinstance(action, argparse._SubParsersAction))
    pipeline_parser = init_targets.choices["pipeline"]

    key_action = next(action for action in pipeline_parser._actions if "--key" in action.option_strings)

    assert key_action.dest == "unique_key"


def test_invalid_route_locator_is_pipeline_owned_and_fails_before_writes(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="domain_first").passed
    _init_domain(service, "crm")

    result = service.init_pipeline(
        pipeline_id="orders_daily",
        domain="crm",
        recipe="mssql-to-clickhouse-incremental",
        airflow=None,
        from_locator="not-a-locator",
        to_locator="clickhouse_dev:analytics.orders",
        unique_key="order_id",
    )

    assert not result.passed
    assert result.errors[0]["code"] == "DPONE_PIPELINE_LOCATOR_INVALID"
    assert result.errors[0]["entity"] == {"kind": "pipeline", "id": "orders_daily"}
    assert not (tmp_path / "workloads/crm/pipelines/orders_daily").exists()


def test_flat_builtin_domain_override_is_validated_before_writes(tmp_path: Path) -> None:
    result = build_airflow_self_service_service(root=tmp_path).init_pipeline(
        pipeline_id="orders_daily",
        domain="INVALID DOMAIN",
        recipe="mssql-to-clickhouse-incremental",
        airflow=False,
    )

    assert not result.passed
    assert result.exit_code == 2
    assert result.errors[0]["code"] == "DPONE_DOMAIN_ID_INVALID"
    assert result.errors[0]["entity"] == {"kind": "pipeline", "id": "orders_daily"}
    assert result.errors[0]["fixes"][0]["command"] == "dpone init pipeline --help"
    assert not (tmp_path / "pipelines").exists()
    assert not (tmp_path / "domains").exists()


def test_flat_builtin_answers_domain_is_validated_before_writes(tmp_path: Path) -> None:
    answers = tmp_path / "answers.yaml"
    answers.write_text("domain: INVALID\n", encoding="utf-8")

    result = build_airflow_self_service_service(root=tmp_path).init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=False,
        answers=answers,
    )

    assert not result.passed
    assert result.errors[0]["code"] == "DPONE_DOMAIN_ID_INVALID"
    assert result.errors[0]["entity"] == {"kind": "pipeline", "id": "orders_daily"}
    assert not (tmp_path / "pipelines").exists()
    assert not (tmp_path / "domains").exists()


def test_flat_layout_remains_default_and_keeps_domain_catalog(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True).passed
    assert resolve_project_layout(tmp_path).mode == "flat"

    result = service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=None,
    )

    assert result.passed
    assert (tmp_path / "pipelines/orders_daily/pipeline.yaml").is_file()
    assert (tmp_path / "domains/sales.yaml").is_file()


def test_domain_first_no_airflow_is_durable_and_emits_no_dag(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="domain_first").passed
    _init_domain(service, "crm")

    result = service.init_pipeline(
        pipeline_id="orders_daily",
        domain="crm",
        recipe="mssql-to-clickhouse-incremental",
        airflow=False,
    )

    assert result.passed
    source_path = tmp_path / "workloads/crm/pipelines/orders_daily/pipeline.yaml"
    payload = yaml.safe_load(source_path.read_text(encoding="utf-8"))
    assert payload["metadata"]["airflow"] is False
    assert "airflow" not in payload["metadata"]["tags"]
    discovery = ProjectDiscoveryService(tmp_path).discover()
    assert discovery.ok
    assert workload_index_from_snapshot(discovery)["workloads"][0]["airflow"]["enabled"] is False
    loaded = ProjectSelectionLoader(root=tmp_path).load(tmp_path)
    assert tuple(loaded.checked_sources) == ("orders_daily",)
    assert loaded.dags == ()
    preview = AirflowPreviewService(
        root=tmp_path,
        authoring_check=AirflowAuthoringCheckService(root=tmp_path),
    ).preview("orders_daily")
    assert not preview.passed
    assert preview.errors[0]["code"] == "DPONE_AIRFLOW_DISABLED"
    assert not (tmp_path / ".dpone-cache").exists()
    selection_preview = ProjectSelectionPreviewService(root=tmp_path).preview(
        target=tmp_path,
        select=(),
        exclude=(),
        state_path=None,
        selectors_path="selectors.yml",
        max_selected=100,
    )
    assert not selection_preview.passed
    assert selection_preview.errors[0]["code"] == "DPONE_AIRFLOW_DISABLED"
    assert not (tmp_path / ".dpone-cache").exists()


def test_disabling_airflow_retires_an_existing_single_workload_preview(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="domain-first").passed
    _init_domain(service, "crm")
    assert service.init_pipeline(
        pipeline_id="orders_daily",
        domain="crm",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
    ).passed
    enabled = service.preview("orders_daily")
    assert enabled.passed
    source = tmp_path / "workloads/crm/pipelines/orders_daily/pipeline.yaml"
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    payload["metadata"]["airflow"] = False
    source.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    disabled = service.preview("orders_daily")

    assert not disabled.passed
    assert disabled.errors[0]["code"] == "DPONE_AIRFLOW_DISABLED"
    assert disabled.details["artifact_state"] == "retired"
    assert disabled.details["retired_deployment_id"] == enabled.details["deployment"]["deployment_id"]
    index = json.loads((tmp_path / ".dpone-cache/current/airflow-index.json").read_text(encoding="utf-8"))
    assert index["deployment_id"] == disabled.details["deployment_id"]
    assert index["dag_specs"] == []
    assert index["workload_packs"] == []


def test_disabling_airflow_preserves_multi_workload_preview_and_requires_refresh(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="domain-first").passed
    _init_domain(service, "crm")
    for pipeline_id in ("orders_daily", "customers_daily"):
        assert service.init_pipeline(
            pipeline_id=pipeline_id,
            domain="crm",
            recipe="mssql-to-clickhouse-incremental",
            airflow=True,
        ).passed
    project_preview = ProjectSelectionPreviewService(root=tmp_path).preview(
        target=tmp_path,
        select=(),
        exclude=(),
        state_path=None,
        selectors_path="selectors.yml",
        max_selected=100,
    )
    assert project_preview.passed
    index_path = tmp_path / ".dpone-cache/current/airflow-index.json"
    current_before = index_path.read_bytes()
    source = tmp_path / "workloads/crm/pipelines/orders_daily/pipeline.yaml"
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    payload["metadata"]["airflow"] = False
    source.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    disabled = service.preview("orders_daily")

    assert not disabled.passed
    assert disabled.errors[0]["code"] == "DPONE_AIRFLOW_DISABLED"
    assert disabled.details["artifact_state"] == "project_preview_refresh_required"
    assert index_path.read_bytes() == current_before
    assert disabled.errors[0]["fixes"][0]["command"] == ("dpone airflow preview . --exclude id:orders_daily")


def test_domain_first_discovery_rejects_non_boolean_airflow_policy(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="domain_first").passed
    _init_domain(service, "crm")
    assert service.init_pipeline(
        pipeline_id="orders_daily",
        domain="crm",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
    ).passed
    source_path = tmp_path / "workloads/crm/pipelines/orders_daily/pipeline.yaml"
    payload = yaml.safe_load(source_path.read_text(encoding="utf-8"))
    payload["metadata"]["airflow"] = "false"
    source_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    snapshot = ProjectDiscoveryService(tmp_path).discover()

    assert not snapshot.ok
    assert snapshot.issues[0].code == "DPONE_PIPELINE_SOURCE_INVALID"


def test_folder_dependency_changes_semantic_workload_identity(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="domain_first").passed
    _init_domain(service, "crm")
    assert service.init_pipeline(
        pipeline_id="orders_daily",
        domain="crm",
        recipe="mssql-to-clickhouse-incremental",
        authoring_mode="folder",
        airflow=None,
    ).passed
    discovery = ProjectDiscoveryService(tmp_path)
    baseline = discovery.discover()
    assert baseline.ok

    fragment_path = tmp_path / "workloads/crm/pipelines/orders_daily/steps/load.yaml"
    fragment = yaml.safe_load(fragment_path.read_text(encoding="utf-8"))
    fragment["processes"][0]["source"]["table"]["name"] = "orders_v2"
    fragment_path.write_text(yaml.safe_dump(fragment, sort_keys=False), encoding="utf-8")

    current = discovery.discover()
    impact = compare_workload_indexes(baseline, current)
    assert current.ok
    assert impact.modified == ("orders_daily",)
    item = workload_index_from_snapshot(current)["workloads"][0]
    assert item["source_sha256"] == workload_index_from_snapshot(baseline)["workloads"][0]["source_sha256"]
    assert (
        item["semantic_fingerprint"] != workload_index_from_snapshot(baseline)["workloads"][0]["semantic_fingerprint"]
    )
    assert item["dependencies"]


def test_discovery_rejects_symlink_and_incomplete_direct_pipeline_directory(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="domain_first").passed
    _init_domain(service, "crm")
    pipelines = tmp_path / "workloads/crm/pipelines"
    (pipelines / "incomplete").mkdir(parents=True)
    outside = tmp_path.parent / f"{tmp_path.name}-outside"
    outside.mkdir()
    (pipelines / "escaped").symlink_to(outside, target_is_directory=True)

    snapshot = ProjectDiscoveryService(tmp_path).discover()

    assert not snapshot.ok
    assert {issue.code for issue in snapshot.issues} == {
        "DPONE_DISCOVERY_PATH_INVALID",
        "DPONE_PIPELINE_SOURCE_NOT_FOUND",
    }


def test_discovery_does_not_treat_unreadable_root_as_empty(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="domain_first").passed
    _init_domain(service, "crm")
    monkeypatch.setattr(project_discovery_module, "observe_namespace", lambda *args, **kwargs: None)

    snapshot = ProjectDiscoveryService(tmp_path).discover()

    assert not snapshot.ok
    assert snapshot.issues[0].code == "DPONE_DISCOVERY_PATH_INVALID"


def test_workload_index_contains_self_verifiable_authority_and_layout_identity(
    tmp_path: Path,
) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="domain_first").passed
    _init_domain(service, "crm")
    assert service.init_pipeline(
        pipeline_id="orders_daily",
        domain="crm",
        recipe="mssql-to-clickhouse-incremental",
        airflow=None,
    ).passed

    payload = workload_index_from_snapshot(ProjectDiscoveryService(tmp_path).discover())

    assert payload["layout_root"] == "workloads"
    assert payload["pipeline_id_scope"] == "project"
    assert payload["workloads"][0]["ownership_fingerprint"].startswith("sha256:")


def test_discovery_does_not_count_unreadable_input_as_zero_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="domain_first").passed
    _init_domain(service, "crm")
    monkeypatch.setattr(project_discovery_module, "confined_size", lambda root, path: None)

    snapshot = ProjectDiscoveryService(tmp_path).discover()

    assert not snapshot.ok
    assert snapshot.issues[0].code == "DPONE_SELECTION_STATE_CHANGED"


def test_builtin_answers_are_confined_and_forbid_unknown_fields(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="domain_first").passed
    _init_domain(service, "crm")
    outside = tmp_path.parent / f"{tmp_path.name}-answers.yaml"
    outside.write_text("source_table: stolen\n", encoding="utf-8")

    escaped = service.init_pipeline(
        pipeline_id="orders_daily",
        domain="crm",
        recipe="mssql-to-clickhouse-incremental",
        answers=outside,
        airflow=None,
    )
    assert not escaped.passed
    assert escaped.errors[0]["code"] == "DPONE_RECIPE_ANSWERS_INVALID"
    assert not (tmp_path / "workloads/crm/pipelines/orders_daily").exists()

    answers = tmp_path / "answers.yaml"
    answers.write_text("password: do-not-accept\n", encoding="utf-8")
    forbidden = service.init_pipeline(
        pipeline_id="orders_daily",
        domain="crm",
        recipe="mssql-to-clickhouse-incremental",
        answers=answers,
        airflow=None,
    )
    assert not forbidden.passed
    assert forbidden.errors[0]["code"] == "DPONE_RECIPE_ANSWERS_FIELD_FORBIDDEN"
    assert "do-not-accept" not in str(forbidden.to_dict())


def test_domain_first_public_schema_producers_are_registered(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="domain_first").passed
    _init_domain(service, "crm")
    assert service.init_pipeline(
        pipeline_id="orders_daily",
        domain="crm",
        recipe="mssql-to-clickhouse-incremental",
        airflow=None,
    ).passed
    snapshot = ProjectDiscoveryService(tmp_path).discover()
    validator = GitOpsSchemaValidator()

    assert not validator.validate(workload_index_from_snapshot(snapshot), expected_kind="dpone.workload-index.v1")
    ownership = yaml.safe_load((tmp_path / "workloads/crm/ownership.yaml").read_text(encoding="utf-8"))
    assert not validator.validate(ownership, expected_kind="dpone.domain-ownership.v1")
    project = yaml.safe_load((tmp_path / "dpone.yaml").read_text(encoding="utf-8"))
    assert not validator.validate(project, expected_kind="dpone.project.v1")
    report = build_change_impact_report(tmp_path, baseline_index=workload_index_from_snapshot(snapshot))
    assert not validator.validate(report, expected_kind="dpone.workload-change-impact.v1")


def test_domain_first_generated_test_runs_by_pipeline_id_and_project(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="domain_first").passed
    _init_domain(service, "crm")
    assert service.init_pipeline(
        pipeline_id="orders_daily",
        domain="crm",
        recipe="mssql-to-clickhouse-incremental",
        airflow=None,
    ).passed
    monkeypatch.chdir(tmp_path)

    by_id, stdout, stderr = _run_cli(["test", "orders_daily"], capsys)
    assert by_id == 0, stderr
    assert "dpone test: PASS" in stdout
    project, stdout, stderr = _run_cli(["test", "."], capsys)
    assert project == 0, stderr
    assert "dpone test: PASS" in stdout
