"""Snapshot observation boundaries and interruption-safe resource ownership."""

from __future__ import annotations

from pathlib import Path

import pytest
from tests.agent_policy.workflow_privilege_fixtures import (
    WORKFLOW_DIRECTORY,
    finding_codes,
    track_open_descriptors,
    write_repository,
)
from tests.agent_policy.workflow_privilege_fixtures import (
    assert_descriptors_closed as _assert_closed,
)
from tools.agent_policy import workflow_privilege_snapshot as snapshot_module
from tools.agent_policy.workflow_privilege_contracts import V1_LIMITS
from tools.agent_policy.workflow_privilege_snapshot import SnapshotReader


def _minimal_repository(path: Path) -> Path:
    return write_repository(
        path,
        {"current.yml": b"name: Current\non: {push: null}\npermissions: {}\njobs: {}\n"},
        policy=b"schema_version: 1\n",
    )


def test_notification_evidence_rejects_restored_state_even_when_metadata_matches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _minimal_repository(tmp_path / "repo")
    lease = SnapshotReader(limits=V1_LIMITS).acquire(root)
    workflow = root / WORKFLOW_DIRECTORY / "current.yml"
    original = workflow.read_bytes()
    workflow.write_bytes(b"temporary")
    workflow.write_bytes(original)
    monkeypatch.setattr(snapshot_module, "_revalidation_phase", lambda _state: True)
    monkeypatch.setattr(snapshot_module, "_tree_revalidates", lambda _state: True)
    result = lease.finalize(policy_schema_version=1)
    assert result.snapshot.complete is result.reference.complete is False
    assert result.snapshot.manifest_sha256 is result.reference.manifest_sha256 is None
    assert finding_codes(result.snapshot) == ("PRIVILEGE_CONCURRENT_MUTATION",)


def test_finalization_interruption_closes_observer_and_all_descriptors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _minimal_repository(tmp_path / "repo")
    opened = track_open_descriptors(monkeypatch, snapshot_module)
    lease = SnapshotReader(limits=V1_LIMITS).acquire(root)
    assert lease._state is not None
    observer = lease._state.observer

    def interrupt(_state: object) -> bool:
        raise KeyboardInterrupt("injected finalization interruption")

    monkeypatch.setattr(snapshot_module, "_revalidation_phase", interrupt)
    with pytest.raises(KeyboardInterrupt):
        lease.finalize(policy_schema_version=1)
    _assert_closed(opened)
    assert not observer.unchanged()


def test_observer_close_failure_still_releases_snapshot_descriptors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _minimal_repository(tmp_path / "repo")
    opened = track_open_descriptors(monkeypatch, snapshot_module)
    lease = SnapshotReader(limits=V1_LIMITS).acquire(root)
    assert lease._state is not None
    observer = lease._state.observer
    real_close = observer.close

    def fail_close() -> None:
        real_close()
        raise OSError("injected close failure")

    monkeypatch.setattr(observer, "close", fail_close)
    with pytest.raises(OSError, match="injected close failure"):
        lease.close()
    _assert_closed(opened)
    lease.close()
