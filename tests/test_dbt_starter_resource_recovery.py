"""Recovery remains a read-only surface independent of writer import order."""

import json
import subprocess
import sys

import pytest


def sidecar_payload(root, directory):
    return {
        "schema": "dpone.starter-resource-recovery.v1",
        "operation": directory.name,
        "root": {"device": root.stat().st_dev, "inode": root.stat().st_ino},
        "directory": {"device": directory.stat().st_dev, "inode": directory.stat().st_ino},
        "rollback": {
            "path": directory.relative_to(root).as_posix() + "/events.jsonl",
            "device": 1,
            "inode": 2,
            "directories": [],
            "removed": False,
            "preserved": True,
            "recovery_path": directory.relative_to(root).as_posix() + "/.dpone-rollback-" + "d" * 32,
            "directory_recovery_paths": [],
        },
    }


@pytest.mark.parametrize("change", [None, "truncated", "root", "directory", "operation", "extra"])
def test_orphan_sidecar_is_read_only_and_bound_to_operation(tmp_path, change):
    from tools.dbt_self_service.starter_resource_recovery import recovery_report

    directory = tmp_path / ".dpone-starter-resource-transactions" / "12345678-1234-1234-1234-123456789abc"
    directory.mkdir(parents=True)
    payload = sidecar_payload(tmp_path, directory)
    if change in {"root", "directory"}:
        payload[change]["inode"] += 1
    elif change == "operation":
        payload["operation"] = "00000000-0000-0000-0000-000000000000"
    elif change == "extra":
        payload["unexpected"] = True
    content = json.dumps(payload).encode()
    if change == "truncated":
        content = content[:-1]
    sidecar = directory / "recovery.json"
    sidecar.write_bytes(content)
    report = recovery_report(tmp_path)
    assert report.pending and report.discovery_required
    assert sidecar.read_bytes() == content
    if change is None:
        assert report.status == "RECOVERY_REQUIRED"
        assert payload["rollback"]["recovery_path"] in report.paths
    else:
        assert report.status == "INVALID"


@pytest.mark.parametrize("order", ["recovery,journal,files", "journal,files,recovery", "files,recovery,journal"])
def test_fresh_import_and_report_perform_no_writes(tmp_path, order):
    script = """
import importlib, os, sys
from pathlib import Path
sys.dont_write_bytecode = True
def audit(event, args):
    if event in {"os.mkdir", "os.remove", "os.rename", "os.rmdir", "os.chmod", "os.link", "os.symlink"}:
        raise AssertionError("Unexpected mutation")
    if event == "open" and isinstance(args[2], int) and args[2] & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC):
        raise AssertionError("Unexpected writable open")
sys.addaudithook(audit)
for name in sys.argv[2].split(","):
    importlib.import_module("tools.dbt_self_service.starter_resource_" + name)
from tools.dbt_self_service import starter_resource_journal as journal, starter_resource_recovery as recovery
assert journal.RecoveryReport is recovery.RecoveryReport
assert journal.recovery_report is recovery.recovery_report
assert journal.leaf_recovery_paths is recovery.leaf_recovery_paths
assert not recovery.recovery_report(Path(sys.argv[1])).pending
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(tmp_path), order], capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stderr
    assert list(tmp_path.iterdir()) == []
