from __future__ import annotations

import os
import subprocess
import sys
import tomllib
from importlib.metadata import version
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MATERIAL_CONSTRAINT = "mkdocs-material>=9.7,<10"
PYMDOWN_CONSTRAINT = "pymdown-extensions>=11.0.1,<12"
MKDOCS_BUILD_TIMEOUT_SECONDS = 300


def _version_tuple(value: str) -> tuple[int, int, int]:
    parts = value.split(".")[:3]
    return tuple(int(part) for part in [*parts, *["0"] * (3 - len(parts))])


def test_docs_toolchain_uses_the_security_patched_release_line() -> None:
    assert (9, 7, 0) <= _version_tuple(version("mkdocs-material")) < (10, 0, 0)
    assert (11, 0, 1) <= _version_tuple(version("pymdown-extensions")) < (12, 0, 0)


def test_docs_dependency_constraints_keep_the_toolchain_synchronized() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    dev_dependencies = set(pyproject["dependency-groups"]["dev"])
    docs_requirements = (ROOT / "docs" / "requirements.txt").read_text(encoding="utf-8").splitlines()
    benchmark_workflow = (ROOT / ".github" / "workflows" / "oss-code-quality-benchmark.yml").read_text(encoding="utf-8")

    assert MATERIAL_CONSTRAINT in dev_dependencies
    assert MATERIAL_CONSTRAINT in docs_requirements
    assert PYMDOWN_CONSTRAINT in dev_dependencies
    assert PYMDOWN_CONSTRAINT in docs_requirements
    assert "--with mkdocs-material" not in benchmark_workflow


def test_strict_mkdocs_build_succeeds_with_the_patched_toolchain(tmp_path: Path) -> None:
    env = os.environ.copy()
    env.pop("NO_MKDOCS_2_WARNING", None)

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "mkdocs",
            "build",
            "--strict",
            "--site-dir",
            str(tmp_path / "site"),
        ],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=MKDOCS_BUILD_TIMEOUT_SECONDS,
    )
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
