"""The Docker evidence producer never upgrades skips or changed source into a pass."""

from __future__ import annotations

import json
import subprocess
from types import SimpleNamespace

import pytest
from tools import run_native_docker_corners as runner


@pytest.mark.parametrize(
    "summary,exit_code,changed,expected",
    [
        ("171 passed in 1s", 0, False, "PASS"),
        ("170 passed, 1 skipped in 1s", 0, False, "FAIL"),
        ("171 skipped in 1s", 0, False, "FAIL"),
        ("1 failed, 170 passed in 1s", 1, False, "FAIL"),
        ("3 passed, 168 deselected in 1s", 0, False, "FAIL"),
        ("170 passed, 1 xfailed in 1s", 0, False, "FAIL"),
        ("170 passed, 1 xpassed in 1s", 0, False, "FAIL"),
        ("no tests ran", 0, False, "FAIL"),
        ("171 passed in 1s", 0, True, "FAIL"),
    ],
)
@pytest.mark.parametrize("cleanup_failure", [False, True])
def test_receipt_requires_actual_passes_unchanged_source_and_cleanup(
    tmp_path, monkeypatch, summary, exit_code, changed, expected, cleanup_failure
):
    monkeypatch.setenv("PYTEST_ADDOPTS", "-k test_native_docker_empty_corrupt_and_retry")
    operations = []
    identities = iter(["before", "after" if changed else "before"])
    monkeypatch.setattr(runner, "source_digest", lambda: next(identities))

    def require(arguments, **kwargs):
        if arguments[:2] == ["git", "rev-parse"]:
            return "a" * 40
        if "image" in arguments and "inspect" in arguments:
            return '[{"Id":"sha256:synthetic","Architecture":"amd64","Os":"linux"}]'
        return "synthetic"

    def command(arguments, **kwargs):
        operations.append(arguments)
        if "pytest" in arguments:
            assert "PYTEST_ADDOPTS" not in kwargs["env"]
            return subprocess.CompletedProcess(arguments, exit_code, summary, "")
        if cleanup_failure and arguments[:3] == ["docker", "rm", "-f"]:
            return subprocess.CompletedProcess(arguments, 1, "", "cleanup failed")
        return subprocess.CompletedProcess(arguments, 0, "synthetic", "")

    monkeypatch.setattr(runner, "require", require)
    monkeypatch.setattr(runner, "command", command)
    output = tmp_path / "receipt"
    status = runner.run(SimpleNamespace(output=output, docker="docker", sql_image="sql", ch_image="ch"))
    receipt = json.loads((output / "receipt.json").read_text())
    if cleanup_failure:
        expected = "FAIL"
    assert receipt["status"] == expected
    assert status == (0 if expected == "PASS" else 1)
    assert receipt["cleanup"] == ("FAIL" if cleanup_failure else "PASS")
    assert len([op for op in operations if op[:3] == ["docker", "rm", "-f"]]) == 2
    assert operations[-1][:3] == ["docker", "network", "rm"]
    assert "MSSQL_SA_PASSWORD" not in (output / "receipt.json").read_text()
