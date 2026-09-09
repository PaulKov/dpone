from __future__ import annotations

from pathlib import Path

from tests.agent_policy.workflow_privilege_fixtures import finding_codes, write_repository
from tools.agent_policy.workflow_privilege_contracts import V1_LIMITS
from tools.agent_policy.workflow_privilege_snapshot import SnapshotReader


def test_snapshot_ignores_unrelated_root_coverage_artifact(tmp_path: Path) -> None:
    root = write_repository(
        tmp_path / "repo",
        {"current.yml": b"name: Current\non: {push: null}\npermissions: {}\njobs: {}\n"},
        policy=b"schema_version: 1\n",
    )
    lease = SnapshotReader(limits=V1_LIMITS).acquire(root)

    (root / ".coverage.gw0").write_bytes(b"")

    finalized = lease.finalize(policy_schema_version=1)
    assert finalized.snapshot.complete is finalized.reference.complete is True
    assert finalized.snapshot.manifest_sha256 == finalized.reference.manifest_sha256
    assert finding_codes(finalized.snapshot) == ()
