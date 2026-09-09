"""Public Python and canonical workload-index contracts."""

from __future__ import annotations

import copy
from pathlib import Path

import pytest

import dpone
from dpone.manifest.project_discovery import (
    ProjectDiscoveryProjectionError,
    ProjectDiscoveryService,
)
from dpone.ports.project_authoring_lock import ProjectAuthoringLockError
from dpone.readiness.airflow_self_service import AirflowSelfServiceService
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service
from dpone.services.workload_index_contract import validate_workload_index, workload_index_from_snapshot


def _domain(service: AirflowSelfServiceService, domain: str) -> None:
    result = service.init_domain(
        domain=domain,
        owner_team=f"data-{domain}",
        owner_contact=f"{domain}@example.com",
        approver_team="data-platform",
    )
    assert result.passed


def _pipeline(service: AirflowSelfServiceService, pipeline_id: str, domain: str) -> None:
    result = service.init_pipeline(
        pipeline_id=pipeline_id,
        domain=domain,
        recipe="mssql-to-clickhouse-incremental",
        airflow=None,
    )
    assert result.passed


def test_root_package_lazily_exports_domain_first_application_api() -> None:
    from dpone import (
        AirflowSelfServiceService as ExportedSelfService,
    )
    from dpone import (
        LoadedProjectSelectionGraph,
        ProjectDiscoveryService,
        ProjectDiscoverySnapshot,
        ProjectSelectionLoader,
        WorkloadIndexChangeImpact,
        compare_workload_indexes,
    )
    from dpone import (
        build_airflow_self_service_service as ExportedBuilder,
    )
    from dpone import (
        workload_index_from_snapshot as exported_workload_index_from_snapshot,
    )

    assert issubclass(ExportedSelfService, AirflowSelfServiceService)
    assert ExportedBuilder is build_airflow_self_service_service
    assert all(
        item is not None
        for item in (
            LoadedProjectSelectionGraph,
            ProjectDiscoveryService,
            ProjectDiscoverySnapshot,
            ProjectSelectionLoader,
            WorkloadIndexChangeImpact,
            compare_workload_indexes,
            exported_workload_index_from_snapshot,
        )
    )
    assert {
        "AirflowSelfServiceService",
        "LoadedProjectSelectionGraph",
        "ProjectDiscoveryService",
        "ProjectDiscoverySnapshot",
        "ProjectSelectionLoader",
        "WorkloadIndexChangeImpact",
        "build_airflow_self_service_service",
        "compare_workload_indexes",
        "workload_index_from_snapshot",
    }.issubset(dpone.__all__)


def test_root_export_preserves_self_service_constructor_and_string_roots(tmp_path: Path) -> None:
    from dpone import AirflowSelfServiceService as ExportedSelfService
    from dpone import ProjectSelectionLoader

    service = ExportedSelfService(root=str(tmp_path))
    assert service.init_project(airflow=True, layout="domain_first").passed
    _domain(service, "crm")
    _pipeline(service, "orders_daily", "crm")

    loaded = ProjectSelectionLoader(root=str(tmp_path)).load(".")

    assert tuple(node.node_id for node in loaded.graph.nodes) == ("orders_daily",)


def test_self_service_rejects_a_symbolic_link_project_root(tmp_path: Path) -> None:
    actual = tmp_path / "actual"
    actual.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(actual, target_is_directory=True)

    with pytest.raises(ProjectAuthoringLockError, match="symbolic link"):
        AirflowSelfServiceService(root=alias)


def test_python_api_recipe_route_conflict_has_recovery_contract(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)

    result = service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        route="mssql:clickhouse:incremental_merge",
        airflow=None,
    )

    assert result.passed is False
    assert result.exit_code == 2
    error = result.errors[0]
    assert error["code"] == "DPONE_RECIPE_ROUTE_CONFLICT"
    assert error["docs_url"] == "docs/errors/DPONE_RECIPE_ROUTE_CONFLICT.md"
    assert error["fixes"] == [
        {
            "id": "choose_recipe_or_route",
            "safety": "manual",
            "command": "dpone init pipeline --help",
        }
    ]
    assert not (tmp_path / "dpone.yaml").exists()


def test_workload_index_uses_and_enforces_pipeline_id_order(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="domain_first").passed
    _domain(service, "crm")
    _domain(service, "finance")
    _pipeline(service, "zeta_daily", "crm")
    _pipeline(service, "alpha_daily", "finance")

    snapshot = ProjectDiscoveryService(tmp_path).discover()
    payload = workload_index_from_snapshot(snapshot)

    assert [item["pipeline_id"] for item in payload["workloads"]] == ["alpha_daily", "zeta_daily"]

    reordered = copy.deepcopy(payload)
    reordered["workloads"].reverse()
    with pytest.raises(ProjectDiscoveryProjectionError, match="canonical pipeline-id order"):
        validate_workload_index(reordered)


def test_workload_index_is_independent_of_creation_order(tmp_path: Path) -> None:
    projects = (tmp_path / "forward", tmp_path / "reverse")
    for root, domains, pipelines in (
        (projects[0], ("crm", "finance"), (("zeta_daily", "crm"), ("alpha_daily", "finance"))),
        (projects[1], ("finance", "crm"), (("alpha_daily", "finance"), ("zeta_daily", "crm"))),
    ):
        root.mkdir()
        service = build_airflow_self_service_service(root=root)
        assert service.init_project(airflow=True, layout="domain_first").passed
        for domain in domains:
            _domain(service, domain)
        for pipeline_id, domain in pipelines:
            _pipeline(service, pipeline_id, domain)

    forward = workload_index_from_snapshot(ProjectDiscoveryService(projects[0]).discover())
    reverse = workload_index_from_snapshot(ProjectDiscoveryService(projects[1]).discover())

    assert forward == reverse


def test_workload_index_rejects_noncanonical_nested_order_and_paths(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="domain_first").passed
    _domain(service, "crm")
    _pipeline(service, "orders_daily", "crm")
    payload = workload_index_from_snapshot(ProjectDiscoveryService(tmp_path).discover())
    workload = payload["workloads"][0]

    unordered_refs = copy.deepcopy(payload)
    unordered_refs["workloads"][0]["connection_refs"] = ["mssql_dev", "clickhouse_dev"]
    with pytest.raises(ProjectDiscoveryProjectionError, match="connection refs"):
        validate_workload_index(unordered_refs)

    unsafe_source = copy.deepcopy(payload)
    unsafe_source["workloads"][0]["authoring_source"] = "../outside.yaml"
    with pytest.raises(ProjectDiscoveryProjectionError, match="authoring source"):
        validate_workload_index(unsafe_source)

    noncanonical_source = copy.deepcopy(payload)
    noncanonical_source["workloads"][0]["authoring_source"] = "workloads/crm/pipelines/another_pipeline/pipeline.yaml"
    with pytest.raises(ProjectDiscoveryProjectionError, match="not canonical"):
        validate_workload_index(noncanonical_source)

    noncanonical_dag = copy.deepcopy(payload)
    noncanonical_dag["workloads"][0]["airflow"]["dag_id"] = "another_dag"
    with pytest.raises(ProjectDiscoveryProjectionError, match="DAG id is not canonical"):
        validate_workload_index(noncanonical_dag)

    unsafe_layout_root = copy.deepcopy(payload)
    unsafe_layout_root["layout_root"] = "../outside"
    with pytest.raises(ProjectDiscoveryProjectionError, match="layout root"):
        validate_workload_index(unsafe_layout_root)

    dependency = {
        "kind": "sql_file",
        "path": "workloads/crm/pipelines/orders_daily/query.sql",
        "sha256": "sha256:" + "0" * 64,
    }
    unordered_dependencies = copy.deepcopy(payload)
    unordered_dependencies["workloads"][0]["dependencies"] = [
        dependency,
        {**dependency, "path": "workloads/crm/pipelines/orders_daily/a.sql"},
    ]
    with pytest.raises(ProjectDiscoveryProjectionError, match="dependencies"):
        validate_workload_index(unordered_dependencies)

    assert workload["connection_refs"] == ["clickhouse_dev", "mssql_dev"]


def test_flat_snapshot_cannot_be_misrepresented_as_an_empty_workload_index(tmp_path: Path) -> None:
    service = AirflowSelfServiceService(root=tmp_path)
    assert service.init_project(airflow=True).passed
    assert service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=None,
    ).passed

    snapshot = ProjectDiscoveryService(tmp_path).discover()

    assert snapshot.layout.mode == "flat"
    with pytest.raises(ProjectDiscoveryProjectionError, match="domain-first"):
        workload_index_from_snapshot(snapshot)
