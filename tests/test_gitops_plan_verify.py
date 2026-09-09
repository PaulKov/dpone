from __future__ import annotations

import json
import logging
from argparse import Namespace
from hashlib import sha256
from pathlib import Path
from textwrap import dedent

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.services.gitops.plan_service import GitOpsPlanService
from dpone.services.gitops.verify_service import GitOpsVerifyService


def _ctx(tmp_path: Path) -> AppContext:
    settings = Settings(repo_root=tmp_path, project_dir=tmp_path, manifest_dir=tmp_path, sources_registry_paths=())
    return AppContext(settings=settings, logger=logging.getLogger("test"), fs=LocalFileSystem(), yaml=PyYamlCodec())


def _write(path: Path, text: str = "source: {}\nsink: {}\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dedent(text).strip() + "\n", encoding="utf-8")
    return path


def _plan_args(path: str, **overrides: object) -> Namespace:
    data = {
        "path": path,
        "workload_root": None,
        "include_global_overrides": False,
        "include_env_overrides": [],
        "include_registry": False,
        "registry": [],
        "support_path": [],
        "runner": "kubernetes_pod_operator",
        "run_command": None,
        "output": None,
        "format": "json",
    }
    data.update(overrides)
    return Namespace(**data)


def _verify_args(plan: str, **overrides: object) -> Namespace:
    data = {
        "plan": plan,
        "worktree": ".",
        "verify_lock": False,
        "output": None,
        "format": "json",
    }
    data.update(overrides)
    return Namespace(**data)


def test_gitops_plan_service_builds_runner_contract_from_sparse_paths(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    workload = tmp_path / "dpone_workloads"
    _write(
        workload / "manifests" / "mssql" / "orders.yaml",
        """
        convention: conventions/custom.yaml
        registry: registry/sources.yaml
        depends_on:
          - path: shared/bootstrap.yaml#public.bootstrap
          - group: landing_shared
        source: {}
        sink: {}
        """,
    )
    _write(workload / "manifests" / "mssql" / "conventions" / "custom.yaml", "vars: {}")
    _write(workload / "manifests" / "mssql" / "registry" / "sources.yaml", "entries: []")
    _write(workload / "manifests" / "mssql" / "shared" / "bootstrap.yaml", "source: {}\nsink: {}")
    (workload / "sql" / "orders").mkdir(parents=True)

    view = GitOpsPlanService(ctx=_ctx(tmp_path)).build_view(
        _plan_args(
            "dpone_workloads/manifests/mssql/orders.yaml",
            include_env_overrides=["dev"],
            include_registry=True,
            support_path=["dpone_workloads/sql/orders/"],
            run_command="dpone run dpone_workloads/manifests/mssql/orders.yaml",
        )
    )

    payload = view.report.to_jsonable()

    assert view.exit_code == 0
    assert payload["kind"] == "gitops.plan"
    assert payload["manifest"] == "dpone_workloads/manifests/mssql/orders.yaml"
    assert payload["workload_root"] == "dpone_workloads"
    assert payload["runner"]["kind"] == "kubernetes_pod_operator"
    assert payload["command"]["text"] == "dpone run dpone_workloads/manifests/mssql/orders.yaml"
    assert [entry["path"] for entry in payload["sparse_paths"]] == [
        "dpone_workloads/manifests/mssql/orders.yaml",
        "dpone_workloads/manifests/mssql/conventions/custom.yaml",
        "dpone_workloads/manifests/mssql/registry/sources.yaml",
        "dpone_workloads/manifests/mssql/shared/bootstrap.yaml",
        "dpone_workloads/overrides/dev.yaml",
        "dpone_workloads/registry/",
        "dpone_workloads/sql/orders/",
    ]
    assert any(warning["code"] == "group_dependency_unresolved" for warning in payload["warnings"])
    assert payload["blockers"] == []
    assert payload["provenance"]["schema_version"] == "1"
    assert payload["lock"]["schema_version"] == "1"
    lock_by_path = {entry["path"]: entry for entry in payload["lock"]["entries"]}
    assert (
        lock_by_path["dpone_workloads/manifests/mssql/orders.yaml"]["sha256"]
        == sha256((workload / "manifests" / "mssql" / "orders.yaml").read_bytes()).hexdigest()
    )
    assert lock_by_path["dpone_workloads/sql/orders/"]["sha256"] is None
    assert not any(str(tmp_path) in json.dumps(section) for section in (payload["sparse_paths"], payload["warnings"]))


def test_gitops_verify_service_blocks_missing_required_sparse_paths(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "dpone_workloads" / "manifests" / "orders.yaml")
    plan_view = GitOpsPlanService(ctx=_ctx(tmp_path)).build_view(
        _plan_args(
            "dpone_workloads/manifests/orders.yaml",
            support_path=["dpone_workloads/sql/orders/"],
        )
    )
    plan_path = tmp_path / "gitops_plan.json"
    plan_path.write_text(plan_view.report.to_json(), encoding="utf-8")

    verify_view = GitOpsVerifyService(ctx=_ctx(tmp_path)).build_view(_verify_args("gitops_plan.json"))
    payload = verify_view.report.to_jsonable()

    assert verify_view.exit_code == 2
    assert payload["kind"] == "gitops.verify"
    assert payload["plan"] == "gitops_plan.json"
    assert payload["worktree"] == "."
    assert payload["checked_paths"][0]["path"] == "dpone_workloads/manifests/orders.yaml"
    assert any(blocker["code"] == "missing_required_path" for blocker in payload["blockers"])
    assert str(tmp_path) not in json.dumps(payload)


def test_gitops_verify_service_accepts_complete_sparse_worktree(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "dpone_workloads" / "manifests" / "orders.yaml")
    (tmp_path / "dpone_workloads" / "sql" / "orders").mkdir(parents=True)
    plan_view = GitOpsPlanService(ctx=_ctx(tmp_path)).build_view(
        _plan_args(
            "dpone_workloads/manifests/orders.yaml",
            support_path=["dpone_workloads/sql/orders/"],
        )
    )
    plan_path = tmp_path / "gitops_plan.json"
    plan_path.write_text(plan_view.report.to_json(), encoding="utf-8")

    verify_view = GitOpsVerifyService(ctx=_ctx(tmp_path)).build_view(_verify_args("gitops_plan.json"))

    assert verify_view.exit_code == 0
    assert verify_view.report.to_jsonable()["blockers"] == []


def test_gitops_verify_service_blocks_lock_digest_drift(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    manifest = _write(tmp_path / "dpone_workloads" / "manifests" / "orders.yaml")
    plan_view = GitOpsPlanService(ctx=_ctx(tmp_path)).build_view(_plan_args("dpone_workloads/manifests/orders.yaml"))
    plan_path = tmp_path / "gitops_plan.json"
    plan_path.write_text(plan_view.report.to_json(), encoding="utf-8")
    manifest.write_text("source: {changed: true}\nsink: {}\n", encoding="utf-8")

    verify_view = GitOpsVerifyService(ctx=_ctx(tmp_path)).build_view(_verify_args("gitops_plan.json", verify_lock=True))
    payload = verify_view.report.to_jsonable()

    assert verify_view.exit_code == 2
    assert payload["lock_checks"][0]["path"] == "dpone_workloads/manifests/orders.yaml"
    assert payload["lock_checks"][0]["passed"] is False
    assert payload["blockers"][0]["code"] == "lock_digest_mismatch"
    assert str(tmp_path) not in json.dumps(payload)


def test_gitops_verify_service_accepts_matching_lock(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "dpone_workloads" / "manifests" / "orders.yaml")
    plan_view = GitOpsPlanService(ctx=_ctx(tmp_path)).build_view(_plan_args("dpone_workloads/manifests/orders.yaml"))
    plan_path = tmp_path / "gitops_plan.json"
    plan_path.write_text(plan_view.report.to_json(), encoding="utf-8")

    verify_view = GitOpsVerifyService(ctx=_ctx(tmp_path)).build_view(_verify_args("gitops_plan.json", verify_lock=True))
    payload = verify_view.report.to_jsonable()

    assert verify_view.exit_code == 0
    assert payload["blockers"] == []
    assert payload["lock_checks"] == [
        {
            "path": "dpone_workloads/manifests/orders.yaml",
            "exists": True,
            "is_dir": False,
            "expected_sha256": sha256(
                (tmp_path / "dpone_workloads" / "manifests" / "orders.yaml").read_bytes()
            ).hexdigest(),
            "actual_sha256": sha256(
                (tmp_path / "dpone_workloads" / "manifests" / "orders.yaml").read_bytes()
            ).hexdigest(),
            "passed": True,
        }
    ]
