from __future__ import annotations

import json
import logging
import subprocess
from argparse import Namespace
from pathlib import Path
from textwrap import dedent

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.services.gitops.affected_service import GitOpsAffectedService


def _ctx(tmp_path: Path) -> AppContext:
    settings = Settings(repo_root=tmp_path, project_dir=tmp_path, manifest_dir=tmp_path, sources_registry_paths=())
    return AppContext(settings=settings, logger=logging.getLogger("test"), fs=LocalFileSystem(), yaml=PyYamlCodec())


def _args(**overrides: object) -> Namespace:
    data = {
        "changed_files": [],
        "changed_files_file": None,
        "from_ref": None,
        "to_ref": None,
        "workload_root": None,
        "manifest_glob": "manifests/**/*.yaml",
        "include_global_overrides": False,
        "include_env_overrides": [],
        "include_registry": False,
        "registry": [],
        "support_path": [],
        "runner": "generic",
        "emit_plans": False,
        "output_dir": ".dpone/gitops/affected",
        "output": None,
        "format": "json",
    }
    data.update(overrides)
    return Namespace(**data)


def _write(path: Path, text: str = "source: {}\nsink: {}\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dedent(text).strip() + "\n", encoding="utf-8")
    return path


def _workload(tmp_path: Path) -> Path:
    workload = tmp_path / "dpone_workloads"
    _write(
        workload / "manifests" / "mssql" / "orders.yaml",
        """
        convention: conventions/custom.yaml
        depends_on:
          - path: shared/bootstrap.yaml
        source: {}
        sink: {}
        """,
    )
    _write(workload / "manifests" / "mssql" / "conventions" / "custom.yaml", "vars: {}")
    _write(workload / "manifests" / "mssql" / "shared" / "bootstrap.yaml", "source: {}\nsink: {}")
    _write(workload / "manifests" / "mssql" / "customers.yaml", "source: {}\nsink: {}\n")
    return workload


def test_gitops_affected_service_reports_impacted_manifest_reasons(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _workload(tmp_path)

    view = GitOpsAffectedService(ctx=_ctx(tmp_path)).build_view(
        _args(
            changed_files=[
                "dpone_workloads/manifests/mssql/conventions/custom.yaml",
                "dpone_workloads/manifests/mssql/shared/bootstrap.yaml",
                "README.md",
            ]
        )
    )

    payload = view.report.to_jsonable()

    assert view.exit_code == 0
    assert payload["kind"] == "gitops.affected"
    assert payload["changed_files"] == [
        "dpone_workloads/manifests/mssql/conventions/custom.yaml",
        "dpone_workloads/manifests/mssql/shared/bootstrap.yaml",
        "README.md",
    ]
    assert [item["manifest"] for item in payload["impacted_manifests"]] == [
        "dpone_workloads/manifests/mssql/orders.yaml"
    ]
    reasons = payload["impacted_manifests"][0]["reasons"]
    assert {reason["matched_path"] for reason in reasons} == {
        "dpone_workloads/manifests/mssql/conventions/custom.yaml",
        "dpone_workloads/manifests/mssql/shared/bootstrap.yaml",
    }
    assert payload["impacted_manifests"][0]["suggested_commands"] == [
        "dpone gitops plan dpone_workloads/manifests/mssql/orders.yaml"
    ]
    assert payload["blockers"] == []
    assert str(tmp_path) not in json.dumps(payload)


def test_gitops_affected_service_blocks_unsafe_changed_paths(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _workload(tmp_path)

    view = GitOpsAffectedService(ctx=_ctx(tmp_path)).build_view(_args(changed_files=["../escape.yaml"]))
    payload = view.report.to_jsonable()

    assert view.exit_code == 2
    assert payload["blockers"][0]["code"] == "invalid_changed_path"
    assert str(tmp_path) not in json.dumps(payload)


def test_gitops_affected_service_can_emit_impacted_plans(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _workload(tmp_path)

    view = GitOpsAffectedService(ctx=_ctx(tmp_path)).build_view(
        _args(
            changed_files=["dpone_workloads/manifests/mssql/shared/bootstrap.yaml"],
            emit_plans=True,
            output_dir=".dpone/gitops/affected",
        )
    )
    payload = view.report.to_jsonable()

    emitted_plan = payload["impacted_manifests"][0]["emitted_plan"]
    plan_payload = json.loads((tmp_path / emitted_plan).read_text(encoding="utf-8"))

    assert view.exit_code == 0
    assert emitted_plan.endswith("gitops_plan.json")
    assert plan_payload["kind"] == "gitops.plan"
    assert plan_payload["manifest"] == "dpone_workloads/manifests/mssql/orders.yaml"


def test_gitops_affected_service_reads_changed_files_file(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _workload(tmp_path)
    changes_file = _write(
        tmp_path / "changed.txt",
        """
        dpone_workloads/manifests/mssql/shared/bootstrap.yaml
        dpone_workloads/manifests/mssql/shared/bootstrap.yaml

        README.md
        """,
    )

    view = GitOpsAffectedService(ctx=_ctx(tmp_path)).build_view(_args(changed_files_file="changed.txt"))
    payload = view.report.to_jsonable()

    assert view.exit_code == 0
    assert payload["changed_files"] == [
        "dpone_workloads/manifests/mssql/shared/bootstrap.yaml",
        "README.md",
    ]
    assert payload["impacted_manifests"][0]["manifest"] == "dpone_workloads/manifests/mssql/orders.yaml"
    assert str(changes_file.resolve()) not in json.dumps(payload)


def test_gitops_affected_service_reads_git_diff_refs(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _workload(tmp_path)
    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True, text=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-m", "base"], cwd=tmp_path, check=True, capture_output=True, text=True)
    base = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=tmp_path, check=True, capture_output=True, text=True
    ).stdout.strip()
    _write(
        tmp_path / "dpone_workloads" / "manifests" / "mssql" / "shared" / "bootstrap.yaml",
        "source: {changed: true}\nsink: {}",
    )
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True)
    subprocess.run(
        ["git", "commit", "-m", "change bootstrap"], cwd=tmp_path, check=True, capture_output=True, text=True
    )

    view = GitOpsAffectedService(ctx=_ctx(tmp_path)).build_view(_args(from_ref=base, to_ref="HEAD"))
    payload = view.report.to_jsonable()

    assert view.exit_code == 0
    assert payload["changed_files"] == ["dpone_workloads/manifests/mssql/shared/bootstrap.yaml"]
    assert payload["impacted_manifests"][0]["manifest"] == "dpone_workloads/manifests/mssql/orders.yaml"


def test_gitops_affected_service_blocks_missing_changed_file_source(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _workload(tmp_path)

    view = GitOpsAffectedService(ctx=_ctx(tmp_path)).build_view(_args())
    payload = view.report.to_jsonable()

    assert view.exit_code == 2
    assert payload["blockers"][0]["code"] == "changed_files_missing"
