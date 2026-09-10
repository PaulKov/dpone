from __future__ import annotations

from pathlib import Path

from dpone.services.ci.snapshot_version import (
    build_snapshot_version,
    build_snapshot_version_from_pyproject,
    read_project_version,
)

ROOT = Path(__file__).resolve().parents[1]


def test_read_project_version() -> None:
    assert read_project_version(ROOT / "pyproject.toml") == "0.76.0"


def test_build_snapshot_version_from_release_base() -> None:
    assert build_snapshot_version("1.4.3", 8123) == "1.4.3.dev8123"
    assert build_snapshot_version("1.4.3rc1", 8123) == "1.4.3.dev8123"
    assert build_snapshot_version("garbage", 8123) == "0.0.0.dev8123"


def test_build_snapshot_version_from_pyproject() -> None:
    assert build_snapshot_version_from_pyproject(ROOT / "pyproject.toml", pipeline_iid=77) == "0.76.0.dev77"
