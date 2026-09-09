from __future__ import annotations

import importlib.metadata
import json
import subprocess
import sys
from pathlib import Path


def _wheel_directory(tmp_path: Path) -> Path:
    dist = tmp_path / "wheels"
    dist.mkdir()
    version = importlib.metadata.version("dpone")
    for name, content in {
        f"dpone-{version}-py3-none-any.whl": b"dpone",
        f"dpone_airflow_pack-{version}-py3-none-any.whl": b"pack",
        f"apache_airflow_providers_dpone-{version}-py3-none-any.whl": b"provider",
    }.items():
        (dist / name).write_bytes(content)
    return dist


def test_cli_creates_silent_manifest(tmp_path: Path) -> None:
    dist = _wheel_directory(tmp_path)
    output = tmp_path / "candidate.json"
    result = subprocess.run(
        [sys.executable, "tools/ci/build_candidate_manifest.py", "--dist", str(dist), "--output", str(output)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == result.stderr == ""
    assert json.loads(output.read_text(encoding="utf-8"))["schema_version"] == "dpone.compatibility-candidate.v1"


def test_cli_never_replaces_existing_output(tmp_path: Path) -> None:
    dist = _wheel_directory(tmp_path)
    output = tmp_path / "candidate.json"
    output.write_text("retained", encoding="utf-8")
    result = subprocess.run(
        [sys.executable, "tools/ci/build_candidate_manifest.py", "--dist", str(dist), "--output", str(output)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr.startswith("CANDIDATE_MANIFEST_UNVERIFIED:")
    assert output.read_text(encoding="utf-8") == "retained"
