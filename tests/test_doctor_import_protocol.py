from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

import dpone.readiness.python_import_health as import_health
import dpone.readiness.python_import_user_site as import_user_site
from dpone.readiness.python_import_health import (
    PythonImportHealth,
    probe_python_import,
)
from dpone.readiness.python_import_probe_child import (
    EFFECTIVE_STARTUP_ENVIRONMENT,
    MAX_PATH_ENTRIES,
    MAX_WARN_OPTION_COUNT,
    SANITIZED_STARTUP_ENVIRONMENT,
)
from dpone.readiness.python_import_probe_snapshots import ProbePayloadError, capture_probe_path
from tests.doctor_import_test_support import _outer_probe_environment, _write_probe_receipt

pytestmark = pytest.mark.usefixtures("replayable_doctor_import_surface")


def test_outer_probe_environment_strips_only_coverage_autostart_controls() -> None:
    environment = {
        "COVERAGE_PROCESS_CONFIG": "serialized config",
        "COVERAGE_PROCESS_START": "/private/coverage.ini",
        "COV_CORE_CONFIG": ":memory:",
        "COV_CORE_SOURCE": "dpone",
        "COV_COREX": "preserved",
        "COVERAGE_FILE": "/private/.coverage",
        "COVERAGE_RCFILE": "/private/coverage.ini",
        "DPONE_EXPLICIT_TEST": "preserved",
        "PATH": "/usr/bin",
    }

    assert _outer_probe_environment(environment) == {
        "COV_COREX": "preserved",
        "COVERAGE_FILE": "/private/.coverage",
        "COVERAGE_RCFILE": "/private/coverage.ini",
        "DPONE_EXPLICIT_TEST": "preserved",
        "PATH": "/usr/bin",
    }


def test_import_probe_captures_one_path_snapshot_for_both_children(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_path = "/private/original-import-path"
    changed_path = "/private/concurrently-mutated-path"
    getcwd_calls = 0
    payloads: list[bytes] = []
    startup_cwds: list[str] = []
    runtime_path = list(sys.path)
    monkeypatch.setattr(sys, "path", [original_path, *runtime_path])

    def getcwd() -> str:
        nonlocal getcwd_calls
        getcwd_calls += 1
        return "/private/original-cwd" if getcwd_calls == 1 else "/private/changed-cwd"

    monkeypatch.setattr(import_health.os, "getcwd", getcwd)

    def runner(
        command: tuple[str, ...],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[bytes]:
        payload = kwargs["input"]
        cwd = kwargs["cwd"]
        assert isinstance(payload, bytes)
        assert isinstance(cwd, str)
        payloads.append(payload)
        startup_cwds.append(cwd)
        sys.path[:] = [changed_path, *runtime_path]
        _write_probe_receipt(kwargs)
        return subprocess.CompletedProcess(command, 0)

    result = probe_python_import("json", process_runner=runner)

    assert result.passed is True
    assert getcwd_calls == 1
    assert len(set(startup_cwds)) == 2
    assert "/private/original-cwd" not in startup_cwds
    assert all(not Path(cwd).exists() for cwd in startup_cwds)
    assert all(original_path.encode() in payload for payload in payloads)
    assert all(changed_path.encode() not in payload for payload in payloads)


def test_path_snapshot_rejects_oversized_sequence_before_iteration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class OversizedPath:
        def __len__(self) -> int:
            return MAX_PATH_ENTRIES + 1

        def __iter__(self):
            raise AssertionError("oversized path was materialized")

    monkeypatch.setattr(sys, "path", OversizedPath())

    with pytest.raises(ProbePayloadError, match="path"):
        capture_probe_path()


def test_interpreter_containers_require_exact_builtins_before_iteration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class HostileList(list):
        def __iter__(self):
            raise AssertionError("custom interpreter container was iterated")

    monkeypatch.setattr(sys, "warnoptions", HostileList())

    result = probe_python_import("json")

    assert result.reason_code == "python_import_startup_unsupported"


def test_import_probe_uses_safe_child_and_passes_paths_only_over_stdin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[tuple[str, ...]] = []
    captured_input: list[bytes] = []
    captured_environment: list[dict[str, str]] = []
    captured_workspaces: list[str] = []
    sensitive_path = "/private/project-path"
    monkeypatch.setattr(sys, "path", [sensitive_path, *sys.path])
    monkeypatch.setattr(sys, "pycache_prefix", sensitive_path)
    monkeypatch.setattr(sys, "_xoptions", {"pycache_prefix": sensitive_path})
    sensitive_warning = "error:private-token:UserWarning:private.module"
    monkeypatch.setattr(sys, "warnoptions", [sensitive_warning])
    monkeypatch.setenv("PYTHONWARNINGS", sensitive_warning)
    monkeypatch.setenv("PYTHONPATH", sensitive_path)

    def successful_runner(
        command: tuple[str, ...],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[bytes]:
        captured.append(command)
        payload = kwargs["input"]
        assert isinstance(payload, bytes)
        captured_input.append(payload)
        environment = kwargs["env"]
        assert isinstance(environment, dict)
        captured_environment.append(environment)
        cwd = kwargs["cwd"]
        assert isinstance(cwd, str)
        captured_workspaces.append(cwd)
        assert kwargs["stdout"] == subprocess.DEVNULL
        assert kwargs["stderr"] == subprocess.DEVNULL
        _write_probe_receipt(kwargs)
        return subprocess.CompletedProcess(command, 0)

    assert probe_python_import("json", process_runner=successful_runner).passed is True
    assert len(captured) == 2
    assert captured[0][:3] == (sys.executable, "-I", "-S")
    assert captured[1][0] == sys.executable
    assert ("-P" in captured[1]) is bool(sys.flags.safe_path)
    assert "-I" not in captured[1]
    assert all(sensitive_path not in repr(command) for command in captured)
    assert all(sensitive_warning not in repr(command) for command in captured)
    assert all(sensitive_path.encode("utf-8") in payload for payload in captured_input)
    assert "PYTHONWARNINGS" not in captured_environment[0]
    assert "PYTHONPATH" not in captured_environment[0]
    assert captured_environment[1]["PYTHONWARNINGS"] == sensitive_warning
    assert "PYTHONPATH" not in captured_environment[1]
    assert len(set(captured_workspaces)) == 2
    assert all(not Path(workspace).exists() for workspace in captured_workspaces)
    assert os.getcwd() not in captured_workspaces


def test_import_probe_blocks_late_home_user_site_startup_injection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    late_home = tmp_path / "late-home"
    late_home.mkdir()
    monkeypatch.setattr(import_user_site.site, "ENABLE_USER_SITE", False)
    for name in ("APPDATA", "PYTHONUSERBASE", "USERPROFILE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("HOME", str(late_home))
    captured_environments: list[dict[str, str]] = []
    captured_inputs: list[bytes] = []

    def runner(command: tuple[str, ...], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        environment = kwargs["env"]
        payload = kwargs["input"]
        cwd = kwargs["cwd"]
        assert isinstance(environment, dict)
        assert isinstance(payload, bytes)
        assert isinstance(cwd, str)
        captured_environments.append(environment)
        captured_inputs.append(payload)
        assert environment["HOME"] == cwd
        assert environment["PYTHONUSERBASE"] == cwd
        assert environment["APPDATA"] == cwd
        assert environment["USERPROFILE"] == cwd
        _write_probe_receipt(kwargs, outcome=3)
        return subprocess.CompletedProcess(command, 3)

    result = probe_python_import("_dpone_missing_after_late_home", process_runner=runner)

    assert result.reason_code == "python_import_not_installed"
    assert result.passed is False
    assert all(str(late_home).encode() in payload for payload in captured_inputs)
    assert all(str(late_home) not in environment.values() for environment in captured_environments)


def test_import_probe_rejects_unhashable_user_site_enable_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(import_user_site.site, "ENABLE_USER_SITE", [])

    assert import_user_site.capture_user_site_snapshot() is None
    assert probe_python_import("json").reason_code == "python_import_policy_invalid"


def test_startup_sanitizer_covers_supported_cpython_help_env_contract() -> None:
    documented = {
        "PYTHONBREAKPOINT",
        "PYTHONCASEOK",
        "PYTHONCOERCECLOCALE",
        "PYTHONDEBUG",
        "PYTHONDEVMODE",
        "PYTHONDONTWRITEBYTECODE",
        "PYTHONFAULTHANDLER",
        "PYTHONHASHSEED",
        "PYTHONHOME",
        "PYTHONINSPECT",
        "PYTHONINTMAXSTRDIGITS",
        "PYTHONIOENCODING",
        "PYTHONLEGACYWINDOWSFSENCODING",
        "PYTHONLEGACYWINDOWSSTDIO",
        "PYTHONMALLOC",
        "PYTHONMALLOCSTATS",
        "PYTHONNODEBUGRANGES",
        "PYTHONNOUSERSITE",
        "PYTHONOPTIMIZE",
        "PYTHONPATH",
        "PYTHONPERFSUPPORT",
        "PYTHONPLATLIBDIR",
        "PYTHONPROFILEIMPORTTIME",
        "PYTHONPYCACHEPREFIX",
        "PYTHONSAFEPATH",
        "PYTHONSTARTUP",
        "PYTHONTRACEMALLOC",
        "PYTHONUNBUFFERED",
        "PYTHONUTF8",
        "PYTHONVERBOSE",
        "PYTHONWARNDEFAULTENCODING",
        "PYTHONWARNINGS",
    }

    assert documented <= SANITIZED_STARTUP_ENVIRONMENT
    assert EFFECTIVE_STARTUP_ENVIRONMENT == SANITIZED_STARTUP_ENVIRONMENT - {
        "APPDATA",
        "HOME",
        "PYTHONPATH",
        "PYTHONUSERBASE",
        "PYTHONWARNINGS",
        "USERPROFILE",
    }


def test_import_probe_restores_sanitized_environment_for_target_import(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_name = "_dpone_target_environment_probe"
    expected = "ignore:private-value"
    expected_home = str(tmp_path / "target-visible-home")
    (tmp_path / f"{module_name}.py").write_text(
        "import os\n"
        f"assert os.environ.get('PYTHONWARNINGS') == {expected!r}\n"
        f"assert os.environ.get('HOME') == {expected_home!r}\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("PYTHONWARNINGS", expected)
    monkeypatch.setenv("HOME", expected_home)

    result = probe_python_import(module_name)

    assert result.passed is True


def test_import_probe_does_not_accept_target_os_exit_zero_without_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_name = "_dpone_early_exit_probe"
    (tmp_path / f"{module_name}.py").write_text(
        "import os\nos._exit(0)\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    result = probe_python_import(module_name)

    assert result.reason_code == "python_import_probe_unavailable"
    assert result.passed is False


@pytest.mark.parametrize("malformed_receipt", (b"", b"not-a-receipt\n", b"x" * 256))
def test_import_probe_requires_an_exact_positive_completion_receipt(
    malformed_receipt: bytes,
) -> None:
    def runner(
        command: tuple[str, ...],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[bytes]:
        _write_probe_receipt(kwargs, malformed_receipt)
        return subprocess.CompletedProcess(command, 0)

    result = probe_python_import("json", process_runner=runner)

    assert result == PythonImportHealth(
        passed=False,
        reason_code="python_import_probe_unavailable",
        summary="module import probe did not confirm completion",
    )


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX umask semantics")
def test_import_probe_workspace_permissions_do_not_depend_on_ambient_umask() -> None:
    previous_umask = os.umask(0o777)
    try:
        result = probe_python_import("json")
    finally:
        os.umask(previous_umask)

    assert result == PythonImportHealth(True, None, "module import succeeded")


def test_import_probe_preserves_a_surrogateescaped_effective_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "path", ["/tmp/invalid-byte-\udcff", *sys.path])

    result = probe_python_import("json")

    assert result.passed is True


@pytest.mark.parametrize(
    "invalid_path",
    (
        ["/safe", object()],
        ["/safe\x00shadow"],
        ["x" * (64 * 1024)],
    ),
)
def test_import_probe_fails_closed_for_an_invalid_effective_path(
    invalid_path: list[object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "path", invalid_path)
    calls = 0

    def runner(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess((), 0, b"", b"")

    result = probe_python_import("json", process_runner=runner)

    assert result.reason_code == "python_import_path_invalid"
    assert result.passed is False
    assert calls == 0


@pytest.mark.parametrize("surface", ("path", "warning"))
def test_import_probe_rejects_oversized_text_before_encoding_or_child_start(
    surface: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class EncodeMustNotRun(str):
        def encode(self, *_args: object, **_kwargs: object) -> bytes:
            raise AssertionError("oversized input reached UTF-8 encoding")

    oversized = EncodeMustNotRun("x" * (64 * 1024 + 1))
    if surface == "path":
        monkeypatch.setattr(sys, "path", [oversized])
    else:
        monkeypatch.setattr(sys, "warnoptions", [oversized])
    calls = 0

    def runner(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess((), 0)

    result = probe_python_import("json", process_runner=runner)

    expected_reason = "python_import_path_invalid" if surface == "path" else "python_import_policy_invalid"
    assert result.reason_code == expected_reason
    assert result.passed is False
    assert calls == 0


@pytest.mark.parametrize(
    "warnoptions",
    (
        ["ignore"] * (MAX_WARN_OPTION_COUNT + 1),
        ["x" * 33_000, "y" * 33_000],
    ),
)
def test_startup_warning_frame_is_bounded_before_filesystem_encoding(
    warnoptions: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "warnoptions", warnoptions)
    encoding_calls = 0

    def unexpected_encoding(_value: object) -> bytes:
        nonlocal encoding_calls
        encoding_calls += 1
        raise AssertionError("oversized warnings reached filesystem encoding")

    monkeypatch.setattr(import_health.os, "fsencode", unexpected_encoding)

    replayable, rendered = import_health._startup_warning_environment(warnoptions)

    assert replayable is False
    assert rendered is None
    assert encoding_calls == 0


def test_path_frame_rejects_cumulative_overflow_before_late_encoding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class EncodeMustNotRun(str):
        def encode(self, *_args: object, **_kwargs: object) -> bytes:
            raise AssertionError("cumulative overflow reached late UTF-8 encoding")

    monkeypatch.setattr(
        sys,
        "path",
        ["x" * 40_000, EncodeMustNotRun("y" * 40_000)],
    )
    calls = 0

    def runner(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess((), 0)

    result = probe_python_import("json", process_runner=runner)

    assert result.reason_code == "python_import_path_invalid"
    assert result.passed is False
    assert calls == 0
