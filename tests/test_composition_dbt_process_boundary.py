"""Local allocation invariants with simulated root; no live certification."""

import os
from pathlib import Path

import pytest

from dpone.adapters import composition_dbt_process_boundary as boundary
from dpone.adapters.composition_child_identity_allocator import (
    CompositionChildIdentity,
    CompositionChildIdentityAllocator,
    CompositionChildIdentityRecord,
)
from dpone.contracts.composition_dbt_outcome import DbtCaptureError
from tests.composition_mssql_gate_helpers import attempt
from tests.composition_supervisor_simulation import supervisor_simulation

UID_START = 1_000_000_000
GID_START = 1_100_000_000


class StubIdentityAllocator:
    """Return one caller-supplied identity without touching the mount."""

    def __init__(self, identity):
        self.identity = identity
        self.calls = 0

    def allocate(self, attempt):
        self.calls += 1
        return self.identity


def identities_for(tmp_path):
    root = tmp_path / "composition"
    root.mkdir(mode=0o755, exist_ok=True)
    return CompositionChildIdentityAllocator(root, uid_start=UID_START, gid_start=GID_START, count=1024)


def boundary_for(tmp_path, identities):
    # macOS pytest roots have user-owned ancestry: only ownership is simulated.
    root = tmp_path / "output"
    profiles = tmp_path / "profiles"
    root.mkdir(mode=0o755, exist_ok=True)
    profiles.mkdir(mode=0o755, exist_ok=True)
    return boundary.LinuxDbtProcessBoundary(root, profiles, identities=identities)


@pytest.fixture
def simulation(monkeypatch):
    supervisor_simulation(monkeypatch)
    monkeypatch.setattr(boundary, "_require_supervisor", lambda: None)
    monkeypatch.setattr(boundary, "_require_identity_quiescent", lambda uid, gid: None)
    monkeypatch.setattr(boundary, "_require_tmpfs", lambda fd: None)


@pytest.fixture
def identities(tmp_path, simulation):
    return identities_for(tmp_path)


@pytest.fixture
def supervisor(tmp_path, identities):
    return boundary_for(tmp_path, identities)


def test_fresh_layout_and_replay_rejection(supervisor):
    allocated = supervisor.allocate(attempt(), runtime_attempt_id="a" * 32)
    assert allocated.output_directory == allocated.run_output_root / "attempts" / ("a" * 32)
    assert (allocated.output_directory.stat().st_mode & 0o777) == 0o711
    assert (allocated.target.stat().st_mode & 0o777) == 0o700
    assert (allocated.preflight_target.stat().st_mode & 0o777) == 0o700
    with pytest.raises(DbtCaptureError, match="child_identity_replay"):
        supervisor.allocate(attempt(), runtime_attempt_id="a" * 32)


def test_layout_uses_the_persistently_reserved_identity(supervisor, identities):
    allocated = supervisor.allocate(attempt(), runtime_attempt_id="a" * 32)
    reserved = sorted(path.name for path in supervisor.output_root.iterdir() if path.name[:4] in {"uid-", "gid-"})
    uid = int(next(name for name in reserved if name.startswith("uid-")).removeprefix("uid-"))
    gid = int(next(name for name in reserved if name.startswith("gid-")).removeprefix("gid-"))
    assert UID_START <= uid < UID_START + 1024 and GID_START <= gid < GID_START + 1024
    assert identities.read(uid) == CompositionChildIdentityRecord(attempt().attempt_sha256, uid, gid)
    assert allocated.profile_store.profile_path.parent.name.startswith("attempt-")


def test_reserved_identity_is_never_reused_by_another_attempt(supervisor, identities):
    first = supervisor.allocate(attempt(1), runtime_attempt_id="a" * 32)
    second = supervisor.allocate(attempt(2), runtime_attempt_id="b" * 32)
    assert first.run_output_root != second.run_output_root
    reserved = [path.name for path in supervisor.output_root.iterdir() if path.name.startswith("uid-")]
    assert len(set(reserved)) == 2


@pytest.mark.parametrize("target", ["../escape", "/target", "logs", "preflight/target", "x/../target", "target/", "."])
def test_bad_targets_reject_before_mutation(supervisor, identities, target):
    with pytest.raises(DbtCaptureError):
        supervisor.allocate(attempt(), runtime_attempt_id="a" * 32, target_path=target)
    assert list(supervisor.output_root.iterdir()) == []
    assert not (identities.root / "attempts").exists()


def test_symlink_root_is_never_followed(supervisor, identities, tmp_path):
    link = tmp_path / "link"
    link.symlink_to(supervisor.output_root, target_is_directory=True)
    unsafe = boundary.LinuxDbtProcessBoundary(link, supervisor.profile_tmpfs_root, identities=identities)
    with pytest.raises(DbtCaptureError):
        unsafe.allocate(attempt(), runtime_attempt_id="a" * 32)
    assert list(supervisor.output_root.iterdir()) == []


def test_writable_root_rejected_without_chmod(supervisor):
    supervisor.output_root.chmod(0o777)
    with pytest.raises(DbtCaptureError):
        supervisor.allocate(attempt(), runtime_attempt_id="a" * 32)
    assert supervisor.output_root.stat().st_mode & 0o777 == 0o777


def test_profile_rejects_plain_credentials_and_cleans_up(supervisor):
    allocated = supervisor.allocate(attempt(), runtime_attempt_id="a" * 32)
    with pytest.raises(DbtCaptureError):
        with allocated.profile_store.materialize(b"p: {password: secret}"):
            pytest.fail("plaintext profile exposed")
    raw = b"""p:
  outputs:
    x:
      user: "{{ env_var('ATTEMPT_USER') }}"
      password: "{{ env_var('ATTEMPT_PASSWORD') }}"
"""
    with allocated.profile_store.materialize(raw) as path:
        assert path.read_bytes() == raw
        assert path.stat().st_mode & 0o777 == 0o440
        assert path.parent.stat().st_mode & 0o777 == 0o710
    assert not path.exists()
    with pytest.raises(DbtCaptureError):
        with allocated.profile_store.materialize(raw):
            pytest.fail("profile reused")


@pytest.mark.parametrize("uid,gid", [(0, GID_START), (UID_START, 0), (-1, GID_START), (True, GID_START)])
def test_reject_privileged_or_invalid_identity(tmp_path, simulation, uid, gid):
    stub = StubIdentityAllocator(CompositionChildIdentity(uid, gid))
    supervisor = boundary_for(tmp_path, stub)
    with pytest.raises(DbtCaptureError, match="child_identity"):
        supervisor.allocate(attempt(), runtime_attempt_id="a" * 32)
    assert stub.calls == 1
    assert list(supervisor.output_root.iterdir()) == []


@pytest.mark.parametrize(
    "field,value", [("Uid", "23001 23001 23001 23001"), ("Gid", "23001 23001 23001 23001"), ("Groups", "9 23001")]
)
def test_existing_uid_gid_or_supplementary_group_rejected(tmp_path, monkeypatch, field, value):
    process = tmp_path / "123"
    process.mkdir()
    fields = {"Uid": "44 44 44 44", "Gid": "55 55 55 55", "Groups": "55"}
    fields[field] = value
    (process / "status").write_text("\n".join(f"{key}: {values}" for key, values in fields.items()))
    monkeypatch.setattr(boundary, "Path", lambda value: tmp_path if value == "/proc" else Path(value))
    with pytest.raises(DbtCaptureError, match="not_quiescent"):
        boundary._require_identity_quiescent(23001, 23001)


def test_incomplete_process_visibility_rejected(tmp_path, monkeypatch):
    process = tmp_path / "123"
    process.mkdir()
    (process / "status").write_text("Uid: 44 44 44 44")
    monkeypatch.setattr(boundary, "Path", lambda value: tmp_path if value == "/proc" else Path(value))
    with pytest.raises(DbtCaptureError, match="visibility"):
        boundary._require_identity_quiescent(23001, 23001)


def test_non_linux_or_nonroot_rejected(tmp_path):
    import sys

    if sys.platform == "linux" and os.geteuid() == 0 and os.getegid() == 0:
        pytest.skip("This case exercises unsupported supervisor environments")
    stub = StubIdentityAllocator(CompositionChildIdentity(UID_START, GID_START))
    unsupported = boundary.LinuxDbtProcessBoundary(tmp_path, tmp_path, identities=stub)
    with pytest.raises(DbtCaptureError, match="supervisor_boundary"):
        unsupported.allocate(attempt(), runtime_attempt_id="a" * 32)
    assert stub.calls == 0
    assert list(tmp_path.iterdir()) == []


def test_existing_profile_is_preserved_on_exclusive_create_failure(supervisor):
    allocated = supervisor.allocate(attempt(), runtime_attempt_id="b" * 32)
    directory = next(supervisor.profile_tmpfs_root.iterdir())
    profile = directory / "profiles.yml"
    profile.write_bytes(b"existing-original")
    raw = b"user: \"{{ env_var('U') }}\"\npassword: \"{{ env_var('P') }}\""
    with pytest.raises(DbtCaptureError):
        with allocated.profile_store.materialize(raw):
            pytest.fail("reused file")
    assert profile.read_bytes() == b"existing-original"


def test_non_tmpfs_profile_root_rejected_before_reservation(supervisor, monkeypatch):
    def reject(descriptor):
        raise DbtCaptureError("capture_profile_not_tmpfs")

    monkeypatch.setattr(boundary, "_require_tmpfs", reject)
    with pytest.raises(DbtCaptureError, match="not_tmpfs"):
        supervisor.allocate(attempt(), runtime_attempt_id="a" * 32)
    assert list(supervisor.output_root.iterdir()) == []


def test_busy_identity_rejected_before_reservation(supervisor, monkeypatch):
    def reject(uid, gid):
        raise DbtCaptureError("capture_child_not_quiescent")

    monkeypatch.setattr(boundary, "_require_identity_quiescent", reject)
    with pytest.raises(DbtCaptureError, match="not_quiescent"):
        supervisor.allocate(attempt(), runtime_attempt_id="a" * 32)
    assert list(supervisor.output_root.iterdir()) == []


def test_nested_target_has_supervisor_owned_traversal(supervisor):
    allocated = supervisor.allocate(attempt(), runtime_attempt_id="a" * 32, target_path="dbt/target")
    assert allocated.target == allocated.output_directory / "dbt" / "target"
    assert allocated.target.parent.stat().st_mode & 0o777 == 0o711
    assert allocated.target.stat().st_mode & 0o777 == 0o700


def test_duplicate_credential_key_rejected_before_file_write(supervisor):
    allocated = supervisor.allocate(attempt(), runtime_attempt_id="c" * 32)
    raw = b"user: \"{{ env_var('U') }}\"\npassword: plaintext-secret\npassword: \"{{ env_var('P') }}\""
    with pytest.raises(DbtCaptureError, match="profile_credentials"):
        with allocated.profile_store.materialize(raw):
            pytest.fail("duplicate profile was materialized")
    assert not allocated.profile_store.profile_path.exists()
