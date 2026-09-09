from __future__ import annotations

import importlib
import subprocess

import pytest

import dpone.readiness.python_import_environment as probe_environment
import dpone.readiness.python_import_probe_protocol as probe_protocol
from dpone.readiness.python_import_environment import (
    MAX_ENVIRONMENT_ENTRIES,
    MAX_ENVIRONMENT_UTF16_UNITS,
    _copy_bounded_environment,
    capture_bounded_environment,
)
from dpone.readiness.python_import_health import probe_python_import

pytestmark = pytest.mark.usefixtures("replayable_doctor_import_surface")


def test_environment_snapshot_rejects_replaced_mapping_before_iteration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class HostileEnvironment(dict[str, str]):
        def items(self):
            raise AssertionError("noncanonical environment was iterated")

    monkeypatch.setattr(probe_environment.os, "environ", HostileEnvironment())

    assert capture_bounded_environment() is None


def test_environment_type_binding_cannot_be_poisoned_before_module_reload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class HostileEnvironment(dict[str, str]):
        pass

    with monkeypatch.context() as context:
        context.setattr(probe_environment.os, "environ", HostileEnvironment())
        reloaded = importlib.reload(probe_environment)
        assert reloaded._CANONICAL_ENVIRON_TYPE is getattr(reloaded.os, "_Environ")
        assert reloaded.capture_bounded_environment() is None
    importlib.reload(probe_environment)


def test_environment_snapshot_stops_at_entry_cap_without_trusting_length() -> None:
    class LyingEnvironment(dict[str, str]):
        def __len__(self) -> int:
            return 0

        def items(self):
            for index in range(MAX_ENVIRONMENT_ENTRIES + 1):
                yield (f"DPONE_ENV_{index}", "x")
            raise AssertionError("environment was consumed beyond cap plus one")

    source = LyingEnvironment()

    assert _copy_bounded_environment(source, LyingEnvironment) is None


def test_environment_snapshot_fails_closed_on_concurrent_mutation() -> None:
    class MutatingEnvironment(dict[str, str]):
        def items(self):
            yield ("DPONE_ENV_PRESENT", "x")
            raise RuntimeError("mapping changed size during iteration")

    source = MutatingEnvironment()

    assert _copy_bounded_environment(source, MutatingEnvironment) is None


def test_environment_snapshot_rejects_oversize_before_filesystem_encoding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = {"DPONE_ENV_PADDING": "x" * MAX_ENVIRONMENT_UTF16_UNITS}
    calls = 0

    def unexpected_encoding(_value: object) -> bytes:
        nonlocal calls
        calls += 1
        raise AssertionError("oversized environment reached filesystem encoding")

    monkeypatch.setattr(probe_environment.os, "fsencode", unexpected_encoding)

    assert _copy_bounded_environment(source, dict) is None
    assert calls == 0


def test_environment_snapshot_enforces_windows_utf16_block_limit() -> None:
    source = {"DPONE_ENV_PADDING": "\U0001f642" * (MAX_ENVIRONMENT_UTF16_UNITS // 2)}

    assert _copy_bounded_environment(source, dict) is None


def test_environment_snapshot_preserves_exact_values_within_portable_limit() -> None:
    source = {"DPONE_ENV_KEY": "value\udcff"}

    assert _copy_bounded_environment(source, dict) == source


def test_hidden_drive_environment_keys_follow_platform_process_semantics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hidden = {"=C:": "C:\\workspace"}

    if probe_environment.os.name != "nt":
        assert _copy_bounded_environment(hidden, dict) is None
    with monkeypatch.context() as context:
        context.setattr(probe_environment.os, "name", "nt")
        assert _copy_bounded_environment(hidden, dict) == hidden
        assert _copy_bounded_environment({"==C:": "C:\\workspace"}, dict) is None
        assert _copy_bounded_environment({"=": "C:\\workspace"}, dict) is None


@pytest.mark.skipif(probe_environment.os.name != "nt", reason="requires Windows environment-block semantics")
def test_windows_final_child_environment_preserves_hidden_drive_cwd_entry() -> None:
    hidden = {"=C:": "C:\\workspace", "SYSTEMROOT": "C:\\Windows"}

    assert probe_environment.validate_child_environment(hidden) == hidden


def test_invalid_environment_snapshot_is_policy_invalid_before_child_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    monkeypatch.setattr(probe_protocol, "capture_bounded_environment", lambda: None)

    def runner(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess((), 0)

    result = probe_python_import("json", process_runner=runner)

    assert result.reason_code == "python_import_policy_invalid"
    assert calls == 0
