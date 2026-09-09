from __future__ import annotations

from pathlib import Path

from tests.agent_policy.workflow_privilege_fixtures import write_repository
from tools.agent_policy.workflow_privilege_contracts import V1_LIMITS
from tools.agent_policy.workflow_privilege_policy_selection import V2_POLICY_PATH, V3_POLICY_PATH, VersionedPolicyReader
from tools.agent_policy.workflow_privilege_service import scan_repository


def test_absent_v2_keeps_v1_as_the_only_active_policy(tmp_path: Path) -> None:
    root = write_repository(
        tmp_path / "v1", {"current.yml": b"name: Current\non: {push: null}\npermissions: {}\njobs: {}\n"}
    )

    lease = VersionedPolicyReader(limits=V1_LIMITS).acquire(root)

    assert lease.version == 1
    assert lease.historical_v1 is None
    assert lease.snapshot.policy is not None
    assert lease.snapshot.policy.path == ".agents/policy/workflow-security-privileged.yml"
    lease.close()


def test_present_v2_keeps_v1_open_for_later_binding_validation(tmp_path: Path) -> None:
    root = write_repository(
        tmp_path / "v2", {"current.yml": b"name: Current\non: {push: null}\npermissions: {}\njobs: {}\n"}
    )
    candidate = root / V2_POLICY_PATH
    candidate.write_bytes(b"schema_version: 2\n")

    lease = VersionedPolicyReader(limits=V1_LIMITS).acquire(root)

    assert lease.version == 2
    assert lease.historical_v1 is not None
    assert lease.snapshot.policy is not None
    assert lease.snapshot.policy.path == V2_POLICY_PATH
    lease.finalize(policy_schema_version=2)


def test_invalid_v2_never_falls_back_to_v1(tmp_path: Path) -> None:
    root = write_repository(
        tmp_path / "invalid-v2", {"current.yml": b"name: Current\non: {push: null}\npermissions: {}\njobs: {}\n"}
    )
    (root / V2_POLICY_PATH).write_text("schema_version: 2\n", encoding="utf-8")

    report = scan_repository(root)

    assert report["status"] == "UNVERIFIED"
    assert report["policy"]["path"] == V2_POLICY_PATH
    assert {finding["code"] for finding in report["findings"]} == {"PRIVILEGE_INVALID_POLICY"}


def test_present_v3_retains_both_predecessors_for_fail_closed_binding_validation(tmp_path: Path) -> None:
    root = write_repository(
        tmp_path / "v3", {"current.yml": b"name: Current\non: {push: null}\npermissions: {}\njobs: {}\n"}
    )
    (root / V2_POLICY_PATH).write_text("schema_version: 2\n", encoding="utf-8")
    (root / V3_POLICY_PATH).write_text("schema_version: 3\n", encoding="utf-8")

    lease = VersionedPolicyReader(limits=V1_LIMITS).acquire(root)

    assert lease.version == 3
    assert lease.historical_v1 is not None
    assert lease.historical_v2 is not None
    assert lease.snapshot.policy is not None
    assert lease.snapshot.policy.path == V3_POLICY_PATH
    lease.finalize(policy_schema_version=3)
