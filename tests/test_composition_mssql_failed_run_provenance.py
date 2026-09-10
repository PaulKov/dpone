"""Failed captures retain source observations without becoming passing evidence."""

import json

import pytest
from tools import composition_mssql_synthetic as runner

from tests.test_composition_mssql_synthetic_runner import harness as harness


@pytest.mark.parametrize("changed", [False, True])
def test_failed_component_retains_after_source_and_original_failure(harness, changed):
    calls, control, output = harness
    control.update(pytest_code=1, changed=changed)
    assert runner.run(output) == 1
    report = json.loads((output / "summary.json").read_text())
    assert report["status"] == "FAIL" and report["reason"] == "component_pytest_failed"
    assert report["source_commit_after"] == ("b" if changed else "a") * 40
    assert report["source_clean_after"] is True
    assert report["source_verification"] == ("FAIL" if changed else "PASS")
    assert report["cleanup"] == "PASS"
    assert any(args[:3] == ["docker", "rm", "-f"] for args, _ in calls)


@pytest.mark.parametrize("failed_component", [False, True])
def test_unavailable_after_source_preserves_cleanup_and_never_certifies(harness, monkeypatch, failed_component):
    _, control, output = harness
    control["pytest_code"] = int(failed_component)
    original = runner.source_identity
    reads = []

    def observe():
        reads.append(True)
        if len(reads) == 2:
            raise RuntimeError("sensitive source readback failure")
        return original()

    monkeypatch.setattr(runner, "source_identity", observe)
    assert runner.run(output) == 1
    report = json.loads((output / "summary.json").read_text())
    assert report["status"] == "FAIL" and report["cleanup"] == "PASS"
    assert report["source_verification"] == "UNVERIFIED"
    assert report["reason"] == ("component_pytest_failed" if failed_component else "source_readback_unavailable")
    assert "source_commit_after" not in report
    assert "sensitive" not in (output / "summary.json").read_text()
