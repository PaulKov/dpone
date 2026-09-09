"""Focused tests for domain-colocated DAG discovery and dual-read merge."""

from __future__ import annotations

from pathlib import Path

import yaml

from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.manifest.domain_dag_loader import load_domain_dag_file
from dpone.manifest.legacy_catalog_adapter import (
    adapt_legacy_domain_catalogs,
    merge_discovered_domain_dags,
)
from dpone.manifest.project_config import resolve_project_layout
from dpone.manifest.project_discovery import ProjectDiscoveryService
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _seed_domain_first(tmp_path: Path, *, dual_read: bool = True) -> None:
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
        payload["layout"]["system_root"] = ".dpone"
        config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _domain_dag_payload(*, dag_id: str = "DAG__crm__orders__refresh") -> dict:
    return {
        "schema": "dpone.domain-dag.v1",
        "dag_id": dag_id,
        "domain": "crm",
        "description": "CRM orders refresh",
        "start_date": "2026-01-01",
        "timezone": "UTC",
        "schedule": "0 6 * * *",
        "catchup": False,
        "pipelines": ["orders_daily"],
        "wiring": {"mode": "waves", "max_parallel_workloads": 2},
    }


def test_domain_first_layout_defaults_system_root_and_dual_read(tmp_path: Path) -> None:
    _seed_domain_first(tmp_path)
    layout = resolve_project_layout(tmp_path)
    assert layout.mode == "domain_first"
    assert layout.system_root == ".dpone"
    assert layout.dual_read_legacy_catalogs is True


def test_domain_dag_schema_contract_accepts_authored_file() -> None:
    issues = GitOpsSchemaValidator().validate(
        _domain_dag_payload(),
        expected_kind="dpone.domain-dag.v1",
    )
    assert issues == ()


def test_discovery_accepts_colocated_dags_directory(tmp_path: Path) -> None:
    _seed_domain_first(tmp_path, dual_read=False)
    dag_path = tmp_path / "workloads/crm/dags/DAG__crm__orders__refresh.yaml"
    _write(dag_path, _domain_dag_payload())

    snapshot = ProjectDiscoveryService(tmp_path).discover()
    assert snapshot.ok, snapshot.issues
    assert len(snapshot.domain_dags) == 1
    dag = snapshot.domain_dags[0]
    assert dag.dag_id == "DAG__crm__orders__refresh"
    assert dag.domain == "crm"
    assert dag.source == "colocated"
    assert dag.pipelines == ("orders_daily",)
    assert dag.path == "workloads/crm/dags/DAG__crm__orders__refresh.yaml"


def test_discovery_rejects_cross_domain_pipeline_ref(tmp_path: Path) -> None:
    _seed_domain_first(tmp_path, dual_read=False)
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_domain(
        domain="finance",
        owner_team="data-finance",
        owner_contact="finance@example.com",
        approver_team="data-platform",
    ).passed
    assert service.init_pipeline(
        pipeline_id="ledger_daily",
        domain="finance",
        route="mssql:clickhouse:incremental_merge",
        from_locator="mssql_dev:dbo.ledger",
        to_locator="clickhouse_dev:analytics.ledger",
        unique_key="ledger_id",
        airflow=None,
    ).passed
    payload = _domain_dag_payload()
    payload["pipelines"] = ["ledger_daily"]
    _write(tmp_path / "workloads/crm/dags/DAG__crm__orders__refresh.yaml", payload)

    snapshot = ProjectDiscoveryService(tmp_path).discover()
    assert not snapshot.ok
    assert any(item.code == "DPONE_DOMAIN_DAG_CROSS_DOMAIN_PIPELINE" for item in snapshot.issues)


def test_domain_dag_filename_must_match_dag_id(tmp_path: Path) -> None:
    _seed_domain_first(tmp_path, dual_read=False)
    path = "workloads/crm/dags/wrong_name.yaml"
    _write(tmp_path / path, _domain_dag_payload())
    discovered, issues = load_domain_dag_file(tmp_path, path, expected_domain="crm")
    assert discovered is None
    assert any(item.code == "DPONE_DOMAIN_DAG_ID_PATH_MISMATCH" for item in issues)


def test_dual_read_equal_fingerprint_prefers_colocated_with_warning(tmp_path: Path) -> None:
    _seed_domain_first(tmp_path, dual_read=True)
    colocated = _domain_dag_payload()
    _write(tmp_path / "workloads/crm/dags/DAG__crm__orders__refresh.yaml", colocated)
    _write(
        tmp_path / "dpone_workloads/gitops/domains/crm.yaml",
        {
            "domain": "crm",
            "dags": {
                "DAG__crm__orders__refresh": {
                    "description": "CRM orders refresh",
                    "start_date": "2026-01-01",
                    "timezone": "UTC",
                    "schedule": "0 6 * * *",
                    "catchup": False,
                    "workloads": ["orders_daily"],
                    "wiring": {"mode": "waves", "max_parallel_workloads": 2},
                }
            },
        },
    )

    snapshot = ProjectDiscoveryService(tmp_path).discover()
    assert snapshot.ok, snapshot.issues
    assert len(snapshot.domain_dags) == 1
    assert snapshot.domain_dags[0].source == "colocated"
    assert any(item.code == "DPONE_DOMAIN_DAG_DUAL_READ_WARNING" for item in snapshot.warnings)


def test_dual_read_fingerprint_mismatch_is_blocker(tmp_path: Path) -> None:
    _seed_domain_first(tmp_path, dual_read=True)
    _write(tmp_path / "workloads/crm/dags/DAG__crm__orders__refresh.yaml", _domain_dag_payload())
    _write(
        tmp_path / "dpone_workloads/gitops/domains/crm.yaml",
        {
            "domain": "crm",
            "dags": {
                "DAG__crm__orders__refresh": {
                    "description": "different schedule",
                    "start_date": "2026-01-01",
                    "timezone": "UTC",
                    "schedule": "0 7 * * *",
                    "catchup": False,
                    "workloads": ["orders_daily"],
                    "wiring": {"mode": "waves", "max_parallel_workloads": 2},
                }
            },
        },
    )

    snapshot = ProjectDiscoveryService(tmp_path).discover()
    assert not snapshot.ok
    assert any(item.code == "DPONE_DOMAIN_DAG_DUAL_READ_CONFLICT" for item in snapshot.issues)
    assert "DAG__crm__orders__refresh" not in {item.dag_id for item in snapshot.domain_dags}


def test_discovery_allows_optional_domain_airflow_subtree(tmp_path: Path) -> None:
    _seed_domain_first(tmp_path, dual_read=False)
    (tmp_path / "workloads/crm/airflow/native").mkdir(parents=True)
    (tmp_path / "workloads/crm/airflow/native/.keep").write_text("", encoding="utf-8")
    snapshot = ProjectDiscoveryService(tmp_path).discover()
    assert snapshot.ok, snapshot.issues
    assert not any(item.code == "DPONE_DISCOVERY_PATH_INVALID" for item in snapshot.issues)


def test_legacy_cross_domain_pipeline_refs_are_allowed_during_dual_read(tmp_path: Path) -> None:
    _seed_domain_first(tmp_path, dual_read=True)
    _write(
        tmp_path / "dpone_workloads/gitops/domains/interchange.yaml",
        {
            "domain": "interchange",
            "dags": {
                "DAG__crm__orders__refresh": {
                    "start_date": "2026-01-01",
                    "schedule": "0 6 * * *",
                    "workloads": ["orders_daily"],
                }
            },
        },
    )
    snapshot = ProjectDiscoveryService(tmp_path).discover()
    assert snapshot.ok, snapshot.issues
    assert len(snapshot.domain_dags) == 1
    assert snapshot.domain_dags[0].source == "legacy_catalog"
    assert snapshot.domain_dags[0].domain == "interchange"


def test_legacy_adapter_loads_catalog_dags(tmp_path: Path) -> None:
    _write(
        tmp_path / "dpone_workloads/gitops/domains/crm.yaml",
        {
            "domain": "crm",
            "dags": {
                "DAG__crm__orders__refresh": {
                    "start_date": "2026-01-01",
                    "schedule": "0 6 * * *",
                    "workloads": ["orders_daily"],
                }
            },
        },
    )
    dags, issues, consumed = adapt_legacy_domain_catalogs(tmp_path)
    assert not issues
    assert len(dags) == 1
    assert dags[0].source == "legacy_catalog"
    assert "dpone_workloads/gitops/domains/crm.yaml" in consumed

    merged, blockers, warnings = merge_discovered_domain_dags([], dags)
    assert not blockers
    assert not warnings
    assert merged[0].dag_id == "DAG__crm__orders__refresh"
