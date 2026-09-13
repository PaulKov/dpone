"""Offline boundary doubles; these tests do not certify a Linux deployment."""

import json
import os
from hashlib import sha256
from pathlib import Path

import pytest

from dpone.adapters import composition_native_profile as profile
from dpone.contracts.composition_dbt_outcome import DbtCaptureError
from dpone.contracts.composition_supervisor import CompositionSupervisorProjection


@pytest.fixture
def observed(tmp_path, monkeypatch):
    root = tmp_path / "supervisor"
    root.mkdir()
    (root / "run").mkdir()
    profiles = tmp_path / "profiles"
    profiles.mkdir()
    opened = []

    def open_directory(path, **_kwargs):
        fd = os.open(path, os.O_RDONLY)
        opened.append(fd)
        return fd

    monkeypatch.setattr(profile, "open_protected", open_directory)
    monkeypatch.setattr(profile, "require_supervisor", lambda: None)
    monkeypatch.setattr(profile, "filesystem_type", lambda fd: 0x01021994)
    monkeypatch.setattr(profile.time, "monotonic", lambda: 1.0)
    kwargs = dict(
        supervisor=CompositionSupervisorProjection("stand-pvc", 1000000, 2000000, 1000000),
        supervisor_root=root,
        profiles_root=profiles,
        io_deadline=lambda: 20.0,
    )
    return kwargs, opened


def assert_closed(opened):
    for fd in opened:
        with pytest.raises(OSError):
            os.fstat(fd)


def test_exact_original_no_effects(observed):
    kwargs, opened = observed
    before = list(kwargs["supervisor_root"].iterdir())
    raw = profile.observe_native_execution_profile(**kwargs)
    body = json.loads(raw)
    assert set(body) == {"schema", "supervisor", "supervisor_sha256", "roots"}
    assert body["schema"] == "dpone.composition-native-profile-observation.v1"

    def canonical(value):
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()

    assert raw == canonical(body)
    assert body["supervisor_sha256"] == "sha256:" + sha256(canonical(body["supervisor"])).hexdigest()
    assert set(body["roots"]) == {"supervisor", "run", "profiles"}
    for facts in body["roots"].values():
        info = Path(facts["path"]).stat()
        assert facts == dict(
            path=facts["path"],
            device=info.st_dev,
            inode=info.st_ino,
            uid=info.st_uid,
            gid=info.st_gid,
            mode=info.st_mode & 0o7777,
            filesystem_type=0x01021994,
        )
    assert list(kwargs["supervisor_root"].iterdir()) == before
    assert list(kwargs["profiles_root"].iterdir()) == []
    assert_closed(opened)


@pytest.mark.parametrize("deadline", [True, float("nan"), float("inf"), "20", 0.0])
def test_invalid_deadline_before_open(observed, deadline):
    kwargs, opened = observed
    kwargs["io_deadline"] = lambda: deadline
    with pytest.raises(DbtCaptureError):
        profile.observe_native_execution_profile(**kwargs)
    assert not opened


def test_deadline_not_renewed_and_expiry_closes(observed, monkeypatch):
    kwargs, opened = observed
    calls = []
    kwargs["io_deadline"] = lambda: calls.append(1) or 20.0
    original = profile.filesystem_type

    def expire(fd):
        monkeypatch.setattr(profile.time, "monotonic", lambda: 21.0)
        return original(fd)

    monkeypatch.setattr(profile, "filesystem_type", expire)
    with pytest.raises(DbtCaptureError, match="capture_profile_deadline"):
        profile.observe_native_execution_profile(**kwargs)
    assert calls == [1]
    assert_closed(opened)


def test_rotation_rejected(observed, monkeypatch):
    kwargs, opened = observed
    original = profile.open_protected
    calls = []

    def rotate(path, **_kwargs):
        calls.append(path)
        if len(calls) == 4:
            path.rename(path.with_name("old-supervisor"))
            path.mkdir()
        return original(path)

    monkeypatch.setattr(profile, "open_protected", rotate)
    with pytest.raises(DbtCaptureError, match="capture_profile_changed"):
        profile.observe_native_execution_profile(**kwargs)
    assert_closed(opened)


def test_wrong_filesystem_closes(observed, monkeypatch):
    kwargs, opened = observed
    monkeypatch.setattr(profile, "filesystem_type", lambda fd: 123)
    with pytest.raises(DbtCaptureError, match="capture_profile_not_tmpfs"):
        profile.observe_native_execution_profile(**kwargs)
    assert_closed(opened)


def test_partial_acquisition_closes(observed, monkeypatch):
    kwargs, opened = observed
    original = profile.open_protected

    def fail(path, **_kwargs):
        if opened:
            raise OSError("secret path must not escape")
        return original(path)

    monkeypatch.setattr(profile, "open_protected", fail)
    with pytest.raises(DbtCaptureError, match="capture_profile_unavailable"):
        profile.observe_native_execution_profile(**kwargs)
    assert_closed(opened)


@pytest.mark.parametrize(
    "field,value", [("supervisor_root", Path("relative")), ("profiles_root", Path("/a/../b")), ("supervisor", None)]
)
def test_malformed_inputs_no_open(observed, field, value):
    kwargs, opened = observed
    kwargs[field] = value
    with pytest.raises(DbtCaptureError):
        profile.observe_native_execution_profile(**kwargs)
    assert not opened


def test_privilege_drift_rejected(observed, monkeypatch):
    kwargs, opened = observed
    calls = []

    def require():
        calls.append(1)
        if len(calls) == 2:
            raise DbtCaptureError("capture_supervisor_boundary")

    monkeypatch.setattr(profile, "require_supervisor", require)
    with pytest.raises(DbtCaptureError, match="capture_supervisor_boundary"):
        profile.observe_native_execution_profile(**kwargs)
    assert_closed(opened)


def test_linux_provisioned_actual_tmpfs():
    """Opt-in actual root/mount observation; provisioning belongs to the caller."""
    import sys
    import time

    root = os.environ.get("DPONE_NATIVE_PROFILE_TEST_ROOT")
    profiles = os.environ.get("DPONE_NATIVE_PROFILE_TEST_PROFILES")
    if not root or not profiles:
        pytest.skip("requires explicitly provisioned native profile roots")
    assert sys.platform == "linux" and os.geteuid() == os.getegid() == 0
    original = profile.observe_native_execution_profile(
        supervisor=CompositionSupervisorProjection("stand-pvc", 1000000, 2000000, 1000000),
        supervisor_root=Path(root),
        profiles_root=Path(profiles),
        io_deadline=lambda: time.monotonic() + 5,
    )
    body = json.loads(original)
    assert body["roots"]["profiles"]["filesystem_type"] == 0x01021994
    assert body["roots"]["supervisor"]["inode"] == Path(root).stat().st_ino


def test_protected_open_late_initial_fd_is_closed(monkeypatch):
    from dpone.adapters import composition_supervisor_filesystem as filesystem

    closed = []
    checks = []
    monkeypatch.setattr(filesystem.os, "open", lambda *args, **kwargs: 927)
    monkeypatch.setattr(filesystem.os, "close", closed.append)

    def check():
        checks.append(1)
        if len(checks) == 2:
            raise DbtCaptureError("capture_profile_deadline")

    with pytest.raises(DbtCaptureError, match="capture_profile_deadline"):
        filesystem.open_protected(Path("/example"), require_current=check)
    assert closed == [927]


def test_shared_tmpfs_alias_preserved(monkeypatch):
    from dpone.adapters import composition_dbt_process_boundary as boundary
    from dpone.adapters import composition_supervisor_filesystem as filesystem

    assert boundary._require_tmpfs is filesystem.require_tmpfs
    monkeypatch.setattr(filesystem, "filesystem_type", lambda fd: 0x01021994)
    boundary._require_tmpfs(123)
    monkeypatch.setattr(filesystem, "filesystem_type", lambda fd: 123)
    with pytest.raises(DbtCaptureError, match="capture_profile_not_tmpfs"):
        boundary._require_tmpfs(123)


@pytest.mark.parametrize("extra", [{"unexpected": True}, {"child_identity_count": float("nan")}])
def test_untrusted_projection_rejected_before_open(observed, extra):
    kwargs, opened = observed
    original = kwargs["supervisor"]

    class UntrustedProjection:
        def require_valid(self):
            pass

        def to_dict(self):
            return original.to_dict() | extra

    kwargs["supervisor"] = UntrustedProjection()
    with pytest.raises(DbtCaptureError):
        profile.observe_native_execution_profile(**kwargs)
    assert not opened


def test_projection_subclass_cannot_override_validation(observed):
    kwargs, opened = observed

    class UntrustedProjection(CompositionSupervisorProjection):
        def require_valid(self):
            pass

        def to_dict(self):
            return super().to_dict() | {"unexpected": True}

    kwargs["supervisor"] = UntrustedProjection("stand-pvc", 1000000, 2000000, 1000000)
    with pytest.raises(DbtCaptureError, match="capture_profile_supervisor"):
        profile.observe_native_execution_profile(**kwargs)
    assert not opened


def test_invalid_canonical_projection_rejected_before_open(observed):
    kwargs, opened = observed
    kwargs["supervisor"] = CompositionSupervisorProjection("stand-pvc", 1000000, 2000000, True)
    with pytest.raises(DbtCaptureError):
        profile.observe_native_execution_profile(**kwargs)
    assert not opened
