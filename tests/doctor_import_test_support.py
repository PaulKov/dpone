from __future__ import annotations

import os
import subprocess
import sys
import venv
from collections.abc import Mapping
from pathlib import Path

from dpone.readiness.python_import_probe_child import RECEIPT_PREFIX

_COVERAGE_AUTOSTART_ENVIRONMENT = frozenset(
    {
        "COVERAGE_PROCESS_CONFIG",
        "COVERAGE_PROCESS_START",
    }
)


def _outer_probe_environment(
    environment: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Copy an outer-interpreter environment without coverage auto-start controls."""

    source = os.environ if environment is None else environment
    return {
        key: value
        for key, value in source.items()
        if key not in _COVERAGE_AUTOSTART_ENVIRONMENT and not key.startswith("COV_CORE_")
    }


def _run_outer_probe(
    python: Path,
    script: str,
    *arguments: str,
    environment: dict[str, str] | None = None,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        (str(python), "-c", script, *arguments),
        cwd=cwd,
        env=_outer_probe_environment(environment),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )


def _receipt_from_probe_input(payload: bytes, outcome: int = 0) -> bytes:
    receipt_size_end = payload.index(b"\n")
    receipt_size = int(payload[:receipt_size_end])
    token_frame = receipt_size_end + 1 + receipt_size
    token_size_end = payload.index(b"\n", token_frame)
    token_size = int(payload[token_frame:token_size_end])
    token_start = token_size_end + 1
    token = payload[token_start : token_start + token_size]
    return RECEIPT_PREFIX + token.hex().encode("ascii") + f":{outcome}\n".encode("ascii")


def _write_probe_receipt(
    kwargs: dict[str, object],
    receipt: bytes | None = None,
    *,
    outcome: int = 0,
) -> None:
    payload = kwargs["input"]
    assert isinstance(payload, bytes)
    path_size_end = payload.index(b"\n")
    path_size = int(payload[:path_size_end])
    path_start = path_size_end + 1
    receipt_path = payload[path_start : path_start + path_size].decode("utf-8", "surrogatepass")
    observed = _receipt_from_probe_input(payload, outcome) if receipt is None else receipt
    Path(receipt_path).write_bytes(observed)


def _probe_venv_python(root: Path, sitecustomize: str) -> Path:
    python = _probe_clean_venv_python(root)
    discovered = subprocess.run(
        (str(python), "-c", "import site; print(site.getsitepackages()[0])"),
        env=_outer_probe_environment(),
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    site_packages = Path(discovered.stdout.strip())
    (site_packages / "sitecustomize.py").write_text(sitecustomize, encoding="utf-8")
    return python


def _probe_clean_venv_python(root: Path) -> Path:
    """Build a hook-free interpreter with path-only access to the test tree."""

    environment = root / "venv"
    venv.EnvBuilder(
        with_pip=False,
        symlinks=os.name != "nt",
        system_site_packages=False,
    ).create(environment)
    python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    discovered = subprocess.run(
        (str(python), "-c", "import site; print(site.getsitepackages()[0])"),
        env=_outer_probe_environment(),
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    site_packages = Path(discovered.stdout.strip())
    repository_source = Path(__file__).resolve().parents[1] / "src"
    inherited_paths = [
        entry
        for entry in sys.path
        if entry and Path(entry).is_dir() and Path(entry).resolve() != repository_source.resolve()
    ]
    (site_packages / "dpone-probe-source.pth").write_text(
        "\n".join((str(repository_source), *inherited_paths)) + "\n",
        encoding="utf-8",
    )
    return python
