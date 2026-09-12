"""Offline protected-dispatch boundaries; no Linux/dbt certification."""

from dataclasses import replace
from hashlib import sha256

import pytest

from dpone.adapters.composition_dbt_capture import ProtectedDbtCapture
from dpone.contracts.composition_dbt_outcome import DbtCaptureError, DbtDispatchIntent
from tests.composition_mssql_gate_helpers import attempt
from tests.test_composition_activation_contract import digest


def intent(tmp_path):
    return DbtDispatchIntent(
        attempt(),
        ("/opt/venv/bin/dbt", "build"),
        digest("toolchain"),
        "mssql-sid:" + "61" * 16,
        str(tmp_path),
        1000,
        1001,
        1001,
        (
            ("preflight_manifest", "preflight/manifest.json"),
            ("build_manifest", "target/manifest.json"),
            ("run_results", "target/run_results.json"),
            ("execution_evidence", "evidence.json"),
        ),
        "sha256:" + sha256(b"preflight").hexdigest(),
        working_directory="/project",
        timeout_seconds=20,
    )


class Store:
    def __init__(self, value):
        self.value = value
        self.calls = []
        self.lost_ack = False

    def load_intent(self, value):
        assert value == self.value.attempt
        return self.value

    def record_dispatch_once(self, value, preflight):
        self.calls.append("intent")
        if self.lost_ack or self.calls.count("intent") > 1:
            raise DbtCaptureError("dispatch_unknown")

    def capture_once(self, record):
        self.calls.append("capture")


def test_lost_intent_ack_never_launches(tmp_path, monkeypatch):
    value = intent(tmp_path)
    store = Store(value)
    store.lost_ack = True
    launches = []
    capture = ProtectedDbtCapture(store, lambda value: launches.append(value))
    from dpone.contracts.composition_dbt_outcome import DbtArtifactOriginal

    original = DbtArtifactOriginal("preflight_manifest", "preflight/manifest.json", b"preflight")
    monkeypatch.setattr(capture, "_require_boundary", lambda value: None)
    monkeypatch.setattr("dpone.adapters.composition_dbt_capture._require_uid_quiescent", lambda uid: None)
    monkeypatch.setattr(capture, "_read_original", lambda intent, role: original)
    with pytest.raises(DbtCaptureError):
        capture.dispatch(value.attempt)
    assert store.calls == ["intent"] and launches == []


def test_same_uid_rejected_even_with_private_local_directory(tmp_path):
    import os

    value = replace(intent(tmp_path), supervisor_uid=os.geteuid(), child_uid=os.geteuid())
    store = Store(value)
    capture = ProtectedDbtCapture(store, lambda value: pytest.fail("launched"))
    with pytest.raises(DbtCaptureError):
        capture.dispatch(value.attempt)
    assert store.calls == []


@pytest.mark.parametrize("fault", ["symlink", "parent_symlink", "hardlink", "fifo", "oversize", "missing"])
def test_nofollow_bounded_capture_rejects_unsafe_files(tmp_path, fault):
    import os

    from dpone.contracts.composition_dbt_outcome import MAX_ARTIFACT_BYTES

    value = replace(intent(tmp_path), supervisor_uid=os.geteuid())
    directory = tmp_path / "preflight"
    directory.mkdir()
    path = directory / "manifest.json"
    if fault == "symlink":
        (tmp_path / "other.json").write_bytes(b"{}")
        path.symlink_to(tmp_path / "other.json")
    elif fault == "parent_symlink":
        directory.rmdir()
        (tmp_path / "other").mkdir()
        (tmp_path / "other/manifest.json").write_bytes(b"{}")
        directory.symlink_to(tmp_path / "other", target_is_directory=True)
    elif fault == "hardlink":
        path.write_bytes(b"{}")
        os.link(path, tmp_path / "alias.json")
    elif fault == "fifo":
        os.mkfifo(path)
    elif fault == "oversize":
        with path.open("wb") as handle:
            handle.truncate(MAX_ARTIFACT_BYTES + 1)
    with pytest.raises(DbtCaptureError):
        ProtectedDbtCapture._read_original(value, "preflight_manifest")


def test_original_is_detached_exact_bytes(tmp_path):
    import os

    value = replace(intent(tmp_path), supervisor_uid=os.geteuid())
    (tmp_path / "preflight").mkdir()
    path = tmp_path / "preflight/manifest.json"
    path.write_bytes(b'{"exact":"original"}')
    observed = ProtectedDbtCapture._read_original(value, "preflight_manifest")
    path.write_bytes(b"changed")
    assert observed.content == b'{"exact":"original"}'


def test_journal_replay_is_not_a_new_executor(tmp_path, monkeypatch):
    from dpone.contracts.composition_dbt_outcome import DbtArtifactOriginal, DbtChildExit

    value = intent(tmp_path)
    store = Store(value)
    exits = []
    store.record_exit_once = lambda record: exits.append(record)
    capture = ProtectedDbtCapture(store, lambda value: DbtChildExit(1, 2, 0))
    monkeypatch.setattr(capture, "_require_boundary", lambda value: None)
    monkeypatch.setattr("dpone.adapters.composition_dbt_capture._require_uid_quiescent", lambda uid: None)
    monkeypatch.setattr(
        capture,
        "_read_original",
        lambda value, role: DbtArtifactOriginal(role, "preflight/manifest.json", b"preflight"),
    )
    monkeypatch.setattr(capture, "_observe_quiescence", lambda *args: b"offline")
    capture.dispatch(value.attempt)
    with pytest.raises(DbtCaptureError):
        capture.dispatch(value.attempt)
    assert len(exits) == 1


def test_proc_uid_scan_includes_escaped_process_groups(monkeypatch):
    from pathlib import Path

    from dpone.adapters.composition_dbt_capture import _require_uid_quiescent

    monkeypatch.setattr(Path, "iterdir", lambda path: iter([Path("/proc/123")]))
    monkeypatch.setattr("dpone.adapters.composition_dbt_capture._read_process", lambda pid: (1001, 999, 10))
    with pytest.raises(DbtCaptureError, match="capture_child_not_quiescent"):
        _require_uid_quiescent(1001)


@pytest.mark.parametrize("changed", [False, True])
def test_capture_reopens_protected_exit_and_exact_preflight(tmp_path, monkeypatch, changed):
    import os

    from dpone.contracts.composition_dbt_outcome import DbtChildExit, DbtExitRecord

    value = replace(intent(tmp_path), supervisor_uid=os.geteuid())
    for role, relative in value.artifact_paths:
        path = tmp_path / relative
        path.parent.mkdir(exist_ok=True)
        path.write_bytes(b"preflight" if role == "preflight_manifest" else b"{}")
    original = ProtectedDbtCapture._read_original(value, "preflight_manifest")
    store = Store(value)
    store.read_exit = lambda attempt: DbtExitRecord(value, DbtChildExit(123, 100, 0), b"offline", original)
    capture = ProtectedDbtCapture(store, lambda value: pytest.fail("capture launched build"))
    monkeypatch.setattr(capture, "_require_boundary", lambda value: None)
    monkeypatch.setattr(capture, "_observe_quiescence", lambda *args: b"offline")
    if changed:
        (tmp_path / "preflight/manifest.json").write_bytes(b"changed")
        with pytest.raises(DbtCaptureError, match="capture_preflight_changed"):
            capture.capture(value.attempt)
        assert store.calls == []
    else:
        observed = capture.capture(value.attempt)
        assert len(observed.originals) == 4 and store.calls == ["capture"]


def test_proc_read_permission_failure_is_not_quiescence(monkeypatch):
    from pathlib import Path

    from dpone.adapters.composition_dbt_capture import _read_process

    def unavailable(path):
        raise PermissionError("unavailable")

    monkeypatch.setattr(Path, "read_text", unavailable)
    with pytest.raises(DbtCaptureError, match="capture_process_visibility"):
        _read_process(123)


def test_linux_runner_drops_uid_gid_and_supplementary_groups(tmp_path, monkeypatch):
    from dpone.adapters.composition_dbt_capture import LinuxDbtBuildRunner

    value = replace(intent(tmp_path), supervisor_uid=0)
    calls = []

    class Process:
        pid = 123

        def wait(self, **kwargs):
            assert kwargs == {"timeout": value.timeout_seconds}
            return 0

    def spawn(argv, **kwargs):
        calls.append((argv, kwargs))
        return Process()

    monkeypatch.setattr("dpone.adapters.composition_dbt_capture._require_linux_supervisor", lambda value: None)
    monkeypatch.setattr("dpone.adapters.composition_dbt_capture._require_uid_quiescent", lambda uid: None)
    monkeypatch.setattr("dpone.adapters.composition_dbt_capture._read_process", lambda pid: (1001, 123, 100))
    monkeypatch.setattr("dpone.adapters.composition_dbt_capture.subprocess.Popen", spawn)
    result = LinuxDbtBuildRunner(lambda attempt: {})(value)
    assert result.pid == 123 and result.start_ticks == 100
    assert calls[0][0] == value.argv
    assert calls[0][1]["cwd"] == value.working_directory
    assert value.working_directory != value.output_directory
    assert {key: calls[0][1][key] for key in ("user", "group", "extra_groups", "start_new_session")} == {
        "user": 1001,
        "group": 1001,
        "extra_groups": [],
        "start_new_session": True,
    }
