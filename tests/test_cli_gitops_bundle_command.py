from __future__ import annotations

import json
import logging
from argparse import Namespace
from pathlib import Path

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.commands.gitops.bundle_cmd import cmd_gitops_bundle


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
        "worktree": ".",
        "verify_lock": False,
        "fail_on_empty_impact": False,
        "fail_on_warnings": False,
        "require_lock": False,
        "policy_profile": "custom",
        "attest": False,
        "output_dir": ".dpone/gitops/bundle",
        "output": None,
        "format": "json",
    }
    data.update(overrides)
    return Namespace(**data)


def _write(path: Path, text: str = "source: {}\nsink: {}\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_gitops_bundle_cli_prints_json_and_writes_output(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    workload = tmp_path / "dpone_workloads"
    _write(workload / "manifests" / "orders.yaml", "depends_on:\n  - path: seed.yaml\n")
    _write(workload / "manifests" / "seed.yaml")

    code = cmd_gitops_bundle(
        _args(
            changed_files=["dpone_workloads/manifests/seed.yaml"],
            output=".dpone/gitops/bundle-output.json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )

    stdout_payload = json.loads(capsys.readouterr().out)
    file_payload = json.loads((tmp_path / ".dpone/gitops/bundle-output.json").read_text(encoding="utf-8"))

    assert code == 0
    assert stdout_payload["kind"] == "gitops.bundle"
    assert file_payload == stdout_payload


def test_gitops_bundle_cli_markdown_output_contains_bundle_summary(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    workload = tmp_path / "dpone_workloads"
    _write(workload / "manifests" / "orders.yaml", "depends_on:\n  - path: seed.yaml\n")
    _write(workload / "manifests" / "seed.yaml")

    code = cmd_gitops_bundle(
        _args(changed_files=["dpone_workloads/manifests/seed.yaml"], format="markdown"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )

    output = capsys.readouterr().out

    assert code == 0
    assert "# GitOps bundle" in output
    assert "dpone_workloads/manifests/orders.yaml" in output


def test_gitops_bundle_cli_supports_release_profile_and_attestation(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    workload = tmp_path / "dpone_workloads"
    _write(workload / "manifests" / "orders.yaml", "depends_on:\n  - path: seed.yaml\n")
    _write(workload / "manifests" / "seed.yaml")

    code = cmd_gitops_bundle(
        _args(
            changed_files=["dpone_workloads/manifests/seed.yaml"],
            policy_profile="release",
            attest=True,
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["policy"]["profile"] == "release"
    assert payload["policy"]["require_lock"] is True
    assert payload["policy"]["verify_lock"] is True
    assert payload["attestation"]["bundle_digest"]
