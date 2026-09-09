"""Confinement contracts for domain-first content dependencies."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from dpone.manifest.authoring import default_authoring_compiler
from dpone.manifest.project_discovery import ProjectDiscoveryService
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service


def _pipeline_source(root: Path) -> Path:
    service = build_airflow_self_service_service(root=root)
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
        recipe="mssql-to-clickhouse-incremental",
        airflow=None,
    ).passed
    source = root / "workloads/crm/pipelines/orders_daily/pipeline.yaml"
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    payload["processes"][0]["source"]["query"] = {"sql_file": "query.sql"}
    source.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return source


def test_discovery_rejects_sql_dependency_symlink_even_inside_project(tmp_path: Path) -> None:
    source = _pipeline_source(tmp_path)
    target = source.parent / "target.sql"
    target.write_text("SELECT 1\n", encoding="utf-8")
    (source.parent / "query.sql").symlink_to(target.name)

    snapshot = ProjectDiscoveryService(tmp_path).discover()

    assert not snapshot.ok
    assert snapshot.issues[0].code == "DPONE_PIPELINE_COMPILATION_FAILED"


def test_discovery_detects_sql_dependency_replaced_by_symlink_after_compile(tmp_path: Path) -> None:
    source = _pipeline_source(tmp_path)
    query = source.parent / "query.sql"
    query.write_text("SELECT 1\n", encoding="utf-8")
    replacement = source.parent / "replacement.sql"
    replacement.write_text("SELECT 2\n", encoding="utf-8")
    compiler = default_authoring_compiler()

    class RetargetingCompiler:
        def compile(self, *args: Any, **kwargs: Any):
            compilation = compiler.compile(*args, **kwargs)
            query.unlink()
            query.symlink_to(replacement.name)
            return compilation

    snapshot = ProjectDiscoveryService(tmp_path, compiler=RetargetingCompiler()).discover()

    assert not snapshot.ok
    assert any(issue.code == "DPONE_SELECTION_STATE_CHANGED" for issue in snapshot.issues)
