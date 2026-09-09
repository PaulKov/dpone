from __future__ import annotations

import json
import logging
from argparse import Namespace
from pathlib import Path

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.commands.manifest.sparse_paths_cmd import cmd_manifest_sparse_paths


def _ctx(tmp_path: Path) -> AppContext:
    settings = Settings(repo_root=tmp_path, project_dir=tmp_path, manifest_dir=tmp_path, sources_registry_paths=())
    return AppContext(settings=settings, logger=logging.getLogger("test"), fs=LocalFileSystem(), yaml=PyYamlCodec())


def _args(path: str, **overrides: object) -> Namespace:
    data = {
        "path": path,
        "workload_root": None,
        "include_global_overrides": False,
        "include_env_overrides": [],
        "include_registry": False,
        "registry": [],
        "support_path": [],
        "format": "sparse",
    }
    data.update(overrides)
    return Namespace(**data)


def _write(path: Path, text: str = "source: {}\nsink: {}\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_manifest_sparse_paths_cli_prints_sparse_checkout_allowlist(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    manifest = _write(tmp_path / "dpone_workloads" / "manifests" / "mssql" / "orders.yaml")
    (tmp_path / "dpone_workloads" / "registry").mkdir(parents=True)
    (tmp_path / "dpone_workloads" / "sql" / "orders").mkdir(parents=True)

    code = cmd_manifest_sparse_paths(
        _args(
            "dpone_workloads/manifests/mssql/orders.yaml",
            include_global_overrides=True,
            include_env_overrides=["dev"],
            include_registry=True,
            support_path=["dpone_workloads/sql/orders/"],
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )

    assert code == 0
    assert capsys.readouterr().out.splitlines() == [
        "dpone_workloads/manifests/mssql/orders.yaml",
        "dpone_workloads/overrides/global.yaml",
        "dpone_workloads/overrides/dev.yaml",
        "dpone_workloads/registry/",
        "dpone_workloads/sql/orders/",
    ]
    assert manifest.exists()


def test_manifest_sparse_paths_cli_json_exposes_entries_warnings_and_blockers(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write(
        tmp_path / "dpone_workloads" / "manifests" / "orders.yaml",
        "depends_on:\n  - group: landing_shared\nsource: {}\nsink: {}\n",
    )

    code = cmd_manifest_sparse_paths(
        _args(
            "dpone_workloads/manifests/orders.yaml",
            include_env_overrides=["dev"],
            support_path=["dpone_workloads/sql/orders/"],
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["kind"] == "manifest.sparse_paths"
    assert payload["manifest"] == "dpone_workloads/manifests/orders.yaml"
    assert payload["workload_root"] == "dpone_workloads"
    assert payload["entries"][0]["path"] == "dpone_workloads/manifests/orders.yaml"
    assert any(
        entry["path"] == "dpone_workloads/overrides/dev.yaml" and entry["exists"] is False
        for entry in payload["entries"]
    )
    assert any(warning["code"] == "group_dependency_unresolved" for warning in payload["warnings"])
    assert payload["blockers"] == []


def test_manifest_sparse_paths_cli_returns_blocker_for_invalid_paths(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "dpone_workloads" / "manifests" / "orders.yaml")

    code = cmd_manifest_sparse_paths(
        _args(
            "dpone_workloads/manifests/orders.yaml",
            support_path=["../escape"],
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert payload["entries"][0]["path"] == "dpone_workloads/manifests/orders.yaml"
    assert payload["blockers"][0]["code"] == "invalid_path"


def test_manifest_sparse_paths_cli_blockers_do_not_expose_local_absolute_paths(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "dpone_workloads" / "manifests" / "orders.yaml")

    code = cmd_manifest_sparse_paths(
        _args(
            "dpone_workloads/manifests/orders.yaml",
            support_path=["README.md"],
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert payload["blockers"][0]["code"] == "invalid_path"
    assert str(tmp_path) not in payload["blockers"][0]["message"]


def test_manifest_sparse_paths_cli_returns_blocker_for_invalid_workload_root(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "dpone_workloads" / "manifests" / "orders.yaml")

    code = cmd_manifest_sparse_paths(
        _args(
            "dpone_workloads/manifests/orders.yaml",
            workload_root="../outside",
            format="json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert payload["blockers"][0]["code"] == "invalid_path"
    assert payload["blockers"][0]["source"] == "--workload-root"
