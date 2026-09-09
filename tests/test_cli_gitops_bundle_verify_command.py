from __future__ import annotations

import json
import logging
import subprocess
from argparse import Namespace
from pathlib import Path

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.commands.gitops.bundle_cmd import cmd_gitops_bundle
from dpone.services.gitops.bundle_service import GitOpsBundleService


def _ctx(tmp_path: Path) -> AppContext:
    settings = Settings(repo_root=tmp_path, project_dir=tmp_path, manifest_dir=tmp_path, sources_registry_paths=())
    return AppContext(settings=settings, logger=logging.getLogger("test"), fs=LocalFileSystem(), yaml=PyYamlCodec())


def _bundle_args(**overrides: object) -> Namespace:
    data = {
        "changed_files": ["dpone_workloads/manifests/seed.yaml"],
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
        "worktree": ".",
        "verify_lock": False,
        "fail_on_empty_impact": False,
        "fail_on_warnings": False,
        "require_lock": False,
        "policy_profile": "release",
        "attest": True,
        "output_dir": ".dpone/gitops/bundle",
        "output": None,
        "format": "json",
    }
    data.update(overrides)
    return Namespace(**data)


def _verify_args(**overrides: object) -> Namespace:
    data = {
        "bundle_action": "verify",
        "bundle_path": ".dpone/gitops/bundle/bundle.json",
        "require_attestation": True,
        "output": None,
        "format": "json",
    }
    data.update(overrides)
    return Namespace(**data)


def _write(path: Path, text: str = "source: {}\nsink: {}\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _build_attested_bundle(tmp_path: Path) -> None:
    workload = tmp_path / "dpone_workloads"
    _write(workload / "manifests" / "orders.yaml", "depends_on:\n  - path: seed.yaml\nsource: {}\nsink: {}\n")
    _write(workload / "manifests" / "seed.yaml")
    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True, text=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True, capture_output=True, text=True)
    subprocess.run(["git", "commit", "-m", "baseline"], cwd=tmp_path, check=True, capture_output=True, text=True)
    view = GitOpsBundleService(ctx=_ctx(tmp_path)).build_view(_bundle_args())
    assert view.exit_code == 0


def test_gitops_bundle_verify_cli_prints_json(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)

    code = cmd_gitops_bundle(_verify_args(), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["kind"] == "gitops.bundle_verify"
    assert payload["attestation_check"]["passed"] is True


def test_gitops_bundle_verify_cli_prints_markdown(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)

    code = cmd_gitops_bundle(_verify_args(format="markdown"), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    output = capsys.readouterr().out

    assert code == 0
    assert "# GitOps bundle verify" in output
    assert ".dpone/gitops/bundle/bundle.json" in output


def test_gitops_bundle_verify_cli_blocks_missing_path(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)

    code = cmd_gitops_bundle(_verify_args(bundle_path=None), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert payload["blockers"][0]["code"] == "bundle_path_missing"
