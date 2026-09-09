from __future__ import annotations

from pathlib import Path

from tests.gitlab_ci import load_gitlab_ci_config, load_gitlab_ci_text

ROOT = Path(__file__).resolve().parents[1]


def test_ci_has_runtime_install_smoke_job() -> None:
    data = load_gitlab_ci_config()
    assert "snapshot_runtime_install_smoke" in data


def test_ci_runtime_install_smoke_uses_runtime_exec() -> None:
    text = load_gitlab_ci_text()
    assert "DPONE_INSTALL_MODE=snapshot" in text
    assert "DPONE_PACKAGE_TARGET=/tmp/dpone-runtime-site" in text
    assert "dpone-runtime-exec python tools/package_smoke.py" in text
