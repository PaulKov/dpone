"""CLI init dag and reconcile IR wiring for colocated domain DAGs."""

from __future__ import annotations

import json
import logging
from argparse import Namespace
from pathlib import Path

import pytest
import yaml

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.cli import main as cli_main
from dpone.commands.gitops.airflow_pack_cmd import cmd_gitops_airflow_reconcile
from dpone.gitops.airflow_dag_spec_builder import AirflowDagSpecBuilder
from dpone.manifest.project_discovery import ProjectDiscoveryService
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service
from tests.airflow_dag_spec_repo import dag_declaration, manifest_ref, write_domain, write_manifest, write_workload_set


def _ctx(tmp_path: Path) -> AppContext:
    settings = Settings(repo_root=tmp_path, project_dir=tmp_path, manifest_dir=tmp_path, sources_registry_paths=())
    return AppContext(settings=settings, logger=logging.getLogger("test"), fs=LocalFileSystem(), yaml=PyYamlCodec())


def _run_cli(args: list[str], capsys: pytest.CaptureFixture[str]) -> tuple[int, str, str]:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(args)
    captured = capsys.readouterr()
    return int(exc.value.code or 0), captured.out, captured.err


def _seed(tmp_path: Path, *, dual_read: bool = False) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="domain_first").passed
    assert service.init_domain(
        domain="crm",
        owner_team="data-crm",
        owner_contact="crm@example.com",
        approver_team="data-platform",
    ).passed
    assert service.init_pipeline(
        pipeline_id="orders_daily",
        domain="crm",
        route="mssql:clickhouse:incremental_merge",
        from_locator="mssql_dev:dbo.orders",
        to_locator="clickhouse_dev:analytics.orders",
        unique_key="order_id",
        airflow=None,
    ).passed
    if not dual_read:
        config_path = tmp_path / "dpone.yaml"
        payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        payload["layout"]["dual_read_legacy_catalogs"] = False
        config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def test_init_dag_writes_colocated_domain_dag_yaml(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    _seed(tmp_path, dual_read=False)

    code, stdout, stderr = _run_cli(
        [
            "init",
            "dag",
            "DAG__crm__orders__refresh",
            "--domain",
            "crm",
            "--schedule",
            "0 6 * * *",
            "--pipeline",
            "orders_daily",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 0, (stdout, stderr)
    path = tmp_path / "workloads/crm/dags/DAG__crm__orders__refresh.yaml"
    assert path.is_file()
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert payload["schema"] == "dpone.domain-dag.v1"
    assert payload["pipelines"] == ["orders_daily"]
    snapshot = ProjectDiscoveryService(tmp_path).discover()
    assert snapshot.ok, snapshot.issues
    assert snapshot.domain_dags[0].dag_id == "DAG__crm__orders__refresh"


def test_builder_uses_colocated_discovery_ir(tmp_path: Path) -> None:
    _seed(tmp_path, dual_read=False)
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_dag(
        dag_id="DAG__crm__orders__refresh",
        domain="crm",
        schedule="0 6 * * *",
        pipelines=("orders_daily",),
    ).passed

    # Minimal workload-set so catalog membership can resolve the pipeline pack path.
    manifest = "workloads/crm/pipelines/orders_daily/pipeline.yaml"
    write_domain(
        tmp_path,
        domain="crm",
        workloads={"orders_daily": manifest_ref(manifest)},
        dags={},
    )
    workload_set = write_workload_set(tmp_path)
    report = AirflowDagSpecBuilder(repo_root=tmp_path).build(workload_set=str(workload_set), env="dev")
    assert not report.blockers, report.blockers
    assert [spec.dag_id for spec in report.specs] == ["DAG__crm__orders__refresh"]
    assert report.specs[0].source_path == "workloads/crm/dags/DAG__crm__orders__refresh.yaml"


def test_reconcile_writes_dag_spec_from_colocated_ir(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _seed(tmp_path, dual_read=False)
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_dag(
        dag_id="DAG__crm__orders__refresh",
        domain="crm",
        schedule="0 6 * * *",
        pipelines=("orders_daily",),
    ).passed
    manifest = "workloads/crm/pipelines/orders_daily/pipeline.yaml"
    write_domain(
        tmp_path,
        domain="crm",
        workloads={"orders_daily": manifest_ref(manifest)},
        dags={},
    )
    workload_set = write_workload_set(tmp_path)
    changed = tmp_path / "changed.txt"
    changed.write_text("workloads/crm/dags/DAG__crm__orders__refresh.yaml\n", encoding="utf-8")

    code = cmd_gitops_airflow_reconcile(
        Namespace(
            workload_set=str(workload_set),
            changed_files=[],
            changed_files_file=changed.relative_to(tmp_path).as_posix(),
            env="dev",
            output_dir=".dpone/gitops",
            output=None,
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)
    assert code == 0, payload
    assert payload["dag_specs"] == [".dpone/gitops/airflow/_dags/DAG__crm__orders__refresh.dag-spec.json"]
    assert (tmp_path / payload["dag_specs"][0]).is_file()


def test_empty_discovery_ir_falls_back_to_workload_set_catalog(tmp_path: Path) -> None:
    _seed(tmp_path, dual_read=True)
    manifest = "workloads/crm/pipelines/orders_daily/pipeline.yaml"
    write_domain(
        tmp_path,
        domain="crm",
        workloads={"orders_daily": manifest_ref(manifest)},
        dags={
            "DAG__crm__orders__refresh": dag_declaration(
                workloads=["orders_daily"],
                wiring={"mode": "waves", "max_parallel_workloads": 2},
            )
        },
    )
    workload_set = write_workload_set(tmp_path)
    report = AirflowDagSpecBuilder(repo_root=tmp_path).build(workload_set=str(workload_set), env="dev")
    assert not report.blockers, report.blockers
    assert [spec.dag_id for spec in report.specs] == ["DAG__crm__orders__refresh"]
    assert report.specs[0].source_path.endswith("dpone_workloads/gitops/domains/crm.yaml")


def test_catalog_only_reconcile_still_works_without_domain_first(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    manifest = write_manifest(tmp_path, "account_sales")
    write_domain(
        tmp_path,
        domain="sales",
        workloads={"work-item_account_sales": manifest_ref(manifest)},
        dags={
            "DAG__sales__account_activity__refresh": dag_declaration(
                workloads=["work-item_account_sales"],
                wiring={"mode": "explicit", "dependencies": {}},
            )
        },
    )
    workload_set = write_workload_set(tmp_path)
    changed = tmp_path / "changed.txt"
    changed.write_text("dpone_workloads/gitops/domains/sales.yaml\n", encoding="utf-8")

    code = cmd_gitops_airflow_reconcile(
        Namespace(
            workload_set=str(workload_set),
            changed_files=[],
            changed_files_file=changed.relative_to(tmp_path).as_posix(),
            env="dev",
            output_dir=".dpone/gitops",
            output=None,
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)
    assert code == 0, payload
    assert payload["dag_specs"] == [".dpone/gitops/airflow/_dags/DAG__sales__account_activity__refresh.dag-spec.json"]
