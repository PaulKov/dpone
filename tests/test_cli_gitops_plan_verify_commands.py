from __future__ import annotations

import json
import logging
from argparse import Namespace
from pathlib import Path

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.commands.gitops.plan_cmd import cmd_gitops_plan
from dpone.commands.gitops.verify_cmd import cmd_gitops_verify


def _ctx(tmp_path: Path) -> AppContext:
    settings = Settings(repo_root=tmp_path, project_dir=tmp_path, manifest_dir=tmp_path, sources_registry_paths=())
    return AppContext(settings=settings, logger=logging.getLogger("test"), fs=LocalFileSystem(), yaml=PyYamlCodec())


def _write(path: Path, text: str = "source: {}\nsink: {}\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
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
        "runner": "generic",
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


def test_gitops_plan_cli_prints_json_and_writes_output_file(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "dpone_workloads" / "manifests" / "orders.yaml")
    output = tmp_path / "artifacts" / "gitops_plan.json"

    code = cmd_gitops_plan(
        _plan_args(
            "dpone_workloads/manifests/orders.yaml",
            output="artifacts/gitops_plan.json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )

    stdout_payload = json.loads(capsys.readouterr().out)
    file_payload = json.loads(output.read_text(encoding="utf-8"))

    assert code == 0
    assert stdout_payload["kind"] == "gitops.plan"
    assert file_payload == stdout_payload


def test_gitops_plan_cli_markdown_output_contains_sparse_paths(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "dpone_workloads" / "manifests" / "orders.yaml")

    code = cmd_gitops_plan(
        _plan_args("dpone_workloads/manifests/orders.yaml", format="markdown"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )

    output = capsys.readouterr().out

    assert code == 0
    assert "# GitOps plan" in output
    assert "dpone_workloads/manifests/orders.yaml" in output


def test_gitops_verify_cli_returns_blocker_json_for_missing_paths(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "dpone_workloads" / "manifests" / "orders.yaml")
    plan_code = cmd_gitops_plan(
        _plan_args(
            "dpone_workloads/manifests/orders.yaml",
            support_path=["dpone_workloads/sql/orders/"],
            output="gitops_plan.json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    capsys.readouterr()

    verify_code = cmd_gitops_verify(
        _verify_args("gitops_plan.json"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(capsys.readouterr().out)

    assert plan_code == 0
    assert verify_code == 2
    assert payload["kind"] == "gitops.verify"
    assert payload["blockers"][0]["code"] == "missing_required_path"


def test_gitops_verify_cli_markdown_output_contains_status(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "dpone_workloads" / "manifests" / "orders.yaml")
    cmd_gitops_plan(
        _plan_args("dpone_workloads/manifests/orders.yaml", output="gitops_plan.json"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    capsys.readouterr()

    code = cmd_gitops_verify(
        _verify_args("gitops_plan.json", format="markdown"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )

    output = capsys.readouterr().out

    assert code == 0
    assert "# GitOps verify" in output
    assert "passed" in output


def test_gitops_verify_cli_verify_lock_blocks_digest_drift(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    manifest = _write(tmp_path / "dpone_workloads" / "manifests" / "orders.yaml")
    cmd_gitops_plan(
        _plan_args("dpone_workloads/manifests/orders.yaml", output="gitops_plan.json"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    capsys.readouterr()
    manifest.write_text("source: {changed: true}\nsink: {}\n", encoding="utf-8")

    code = cmd_gitops_verify(
        _verify_args("gitops_plan.json", verify_lock=True),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert payload["blockers"][0]["code"] == "lock_digest_mismatch"
    assert payload["lock_checks"][0]["passed"] is False
    assert str(tmp_path) not in json.dumps(payload)
