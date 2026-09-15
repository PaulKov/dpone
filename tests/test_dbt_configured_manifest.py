"""Singleton manifest selection uses bounded literal project configuration."""

from __future__ import annotations

import argparse
from pathlib import Path

import pytest

from dpone.commands.dbt_publish_cli_support import manifest_path
from dpone.commands.dbt_publish_project_inputs import DbtProjectRootError


@pytest.mark.parametrize("config, target", [("", "target"), ("target-path: build/dbt\n", "build/dbt")])
def test_default_manifest_uses_project_target(tmp_path: Path, config: str, target: str) -> None:
    (tmp_path / "dbt_project.yml").write_text("name: demo\n" + config)
    assert manifest_path(argparse.Namespace(project_dir=str(tmp_path))) == tmp_path / target / "manifest.json"


@pytest.mark.parametrize(
    "config",
    [
        "target-path: ../outside\n",
        "target-path: /tmp/outside\n",
        "target-path: .\n",
        "target-path: \"{{ env_var('TARGET') }}\"\n",
        "target-path: ${TARGET}\n",
        "target-path: null\n",
        "target-path: [build]\n",
        "target-path: false\n",
        "target-path: build\ntarget-path: target\n",
        "target-path: [\n",
        "target-path: &target build\n",
        "[]\n",
        pytest.param("#" + "x" * (1024 * 1024), id="oversized"),
    ],
)
def test_invalid_config_never_falls_back_to_default(tmp_path: Path, config: str) -> None:
    (tmp_path / "dbt_project.yml").write_text(config)
    (tmp_path / "target").mkdir()
    (tmp_path / "target" / "manifest.json").write_text("{}")
    with pytest.raises(DbtProjectRootError):
        manifest_path(argparse.Namespace(project_dir=str(tmp_path)))


def test_explicit_manifest_does_not_interpret_project_config(tmp_path: Path) -> None:
    (tmp_path / "dbt_project.yml").write_text("target-path: [\n")
    args = argparse.Namespace(project_dir=str(tmp_path), manifest="chosen/manifest.json")
    assert manifest_path(args) == Path("chosen/manifest.json")


def test_project_config_symlink_is_rejected(tmp_path: Path) -> None:
    source = tmp_path / "other.yml"
    source.write_text("name: demo\n")
    (tmp_path / "dbt_project.yml").symlink_to(source)
    with pytest.raises(DbtProjectRootError):
        manifest_path(argparse.Namespace(project_dir=str(tmp_path)))
