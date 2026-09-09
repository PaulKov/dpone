from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

import dpone.readiness.python_import_probe_execution as import_execution
import dpone.readiness.python_import_user_site as import_user_site
from dpone.readiness.python_import_health import (
    PythonImportHealth,
    probe_python_import,
)
from dpone.readiness.python_import_probe_runner import ContainedProcessResult
from tests.doctor_import_test_support import _outer_probe_environment, _write_probe_receipt

pytestmark = pytest.mark.usefixtures("replayable_doctor_import_surface")


def test_import_probe_executes_the_real_module_import() -> None:
    result = probe_python_import("json")

    assert result == PythonImportHealth(
        passed=True,
        reason_code=None,
        summary="module import succeeded",
    )


def test_import_probe_does_not_trust_an_in_memory_specless_module(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_name = "_dpone_specless_probe"
    loaded_stub = ModuleType(module_name)
    loaded_stub.__spec__ = None
    monkeypatch.setitem(sys.modules, module_name, loaded_stub)
    calls: list[tuple[object, ...]] = []

    def failed_runner(*args: object, **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        calls.append(args)
        _write_probe_receipt(kwargs, outcome=1)
        return subprocess.CompletedProcess((), 1, b"", b"secret /private/path")

    result = probe_python_import(module_name, process_runner=failed_runner)

    assert result == PythonImportHealth(
        passed=False,
        reason_code="python_import_failed",
        summary="module is installed but cannot be loaded",
    )
    assert calls
    assert "secret" not in result.summary
    assert "/private/path" not in result.summary


def test_import_probe_reports_timeout_with_a_stable_reason() -> None:
    def timed_out(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        raise subprocess.TimeoutExpired(("python",), 5)

    result = probe_python_import("pyodbc", process_runner=timed_out)

    assert result.reason_code == "python_import_timed_out"
    assert result.passed is False


def test_import_probe_does_not_trust_an_unauthenticated_failure_exit() -> None:
    def pre_bootstrap_failure(
        command: tuple[str, ...],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[bytes]:
        return subprocess.CompletedProcess(command, 1)

    result = probe_python_import("json", process_runner=pre_bootstrap_failure)

    assert result == PythonImportHealth(
        False,
        "python_import_probe_unavailable",
        "module import probe did not confirm completion",
    )


def test_import_probe_requires_an_authenticated_matching_interpreter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "hexversion", sys.hexversion + 1)

    result = probe_python_import("json")

    assert result == PythonImportHealth(
        False,
        "python_import_startup_unsupported",
        "effective Python startup environment cannot be reproduced safely",
    )


def test_import_probe_preserves_effective_project_import_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / "_dpone_project_local_probe.py").write_text(
        "import os\nassert os.getcwd() == " + repr(str(project_root)) + "\n",
        encoding="utf-8",
    )
    editable_root = tmp_path / "editable-src"
    editable_root.mkdir()
    (editable_root / "_dpone_editable_path_probe.py").write_text(
        "VALUE = 'editable-path'\n",
        encoding="utf-8",
    )

    original_path = list(sys.path)
    monkeypatch.chdir(project_root)
    monkeypatch.setattr(sys, "path", ["", *original_path])
    assert probe_python_import("_dpone_project_local_probe").passed is True

    monkeypatch.setattr(sys, "path", [str(editable_root), *original_path])
    assert probe_python_import("_dpone_editable_path_probe").passed is True


def test_import_probe_rejects_an_implicit_console_script_cwd_shadow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_name = "_dpone_cwd_shadow_probe"
    (tmp_path / f"{module_name}.py").write_text(
        "VALUE = 'must not be imported'\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "path",
        [entry for entry in sys.path if entry not in {"", str(tmp_path)}],
    )

    result = probe_python_import(module_name)

    assert result == PythonImportHealth(
        passed=False,
        reason_code="python_import_not_installed",
        summary="module is not installed",
    )


def test_import_probe_distinguishes_missing_target_from_broken_dependency(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing = probe_python_import("_dpone_target_that_is_not_installed")
    assert missing == PythonImportHealth(
        passed=False,
        reason_code="python_import_not_installed",
        summary="module is not installed",
    )

    (tmp_path / "_dpone_broken_dependency_probe.py").write_text(
        "import _dpone_dependency_that_is_not_installed\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    broken = probe_python_import("_dpone_broken_dependency_probe")
    assert broken == PythonImportHealth(
        passed=False,
        reason_code="python_import_failed",
        summary="module is installed but cannot be loaded",
    )


def test_import_probe_does_not_misclassify_self_raised_module_not_found(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = "_dpone_self_missing_target"
    (tmp_path / f"{target}.py").write_text(
        f"raise ModuleNotFoundError('self failure', name={target!r})\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    result = probe_python_import(target)

    assert result == PythonImportHealth(
        passed=False,
        reason_code="python_import_failed",
        summary="module is installed but cannot be loaded",
    )


@pytest.mark.parametrize("namespace", (False, True))
def test_import_probe_keeps_genuine_dotted_finder_miss_as_not_installed(
    namespace: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package = tmp_path / ("dpone_namespace" if namespace else "dpone_package")
    package.mkdir()
    if not namespace:
        (package / "__init__.py").write_text("", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))

    result = probe_python_import(f"{package.name}.missing")

    assert result == PythonImportHealth(
        passed=False,
        reason_code="python_import_not_installed",
        summary="module is not installed",
    )


def test_import_probe_treats_dotted_package_code_failure_as_load_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package = tmp_path / "dpone_package_self_missing"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    target = f"{package.name}.target"
    (package / "target.py").write_text(
        f"raise ModuleNotFoundError('self failure', name={target!r})\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    result = probe_python_import(target)

    assert result.reason_code == "python_import_failed"


@pytest.mark.parametrize(
    ("outcomes", "reason_code", "passed"),
    (
        ((0, 0), None, True),
        ((3, 3), "python_import_not_installed", False),
        ((1, 1), "python_import_failed", False),
        ((0, 1), "python_import_startup_unsupported", False),
        ((1, 0), "python_import_startup_unsupported", False),
        ((3, 0), "python_import_startup_unsupported", False),
    ),
)
def test_import_probe_requires_dual_authenticated_agreement(
    outcomes: tuple[int, int],
    reason_code: str | None,
    passed: bool,
) -> None:
    calls = 0

    def runner(
        command: tuple[str, ...],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[bytes]:
        nonlocal calls
        outcome = outcomes[calls]
        calls += 1
        _write_probe_receipt(kwargs, outcome=outcome)
        return subprocess.CompletedProcess(command, outcome)

    result = probe_python_import("json", process_runner=runner)

    assert result.passed is passed
    assert result.reason_code == reason_code
    assert calls == 2


def test_import_probe_shares_only_process_communication_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = iter((100.0, 104.0, 200.0, 203.0))
    timeouts: list[float] = []
    monkeypatch.setattr(import_execution.time, "monotonic", lambda: next(clock))

    def runner(
        command: tuple[str, ...],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[bytes]:
        timeout = kwargs["timeout"]
        assert isinstance(timeout, float)
        timeouts.append(timeout)
        _write_probe_receipt(kwargs)
        return ContainedProcessResult(
            command,
            0,
            communication_elapsed_seconds=1.0,
        )

    result = probe_python_import("json", process_runner=runner)

    assert result.passed is True
    assert timeouts == [5.0, 4.0]


def test_import_probe_uses_runner_call_duration_without_trusted_accounting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = iter((100.0, 101.5, 200.0, 201.0))
    timeouts: list[float] = []
    monkeypatch.setattr(import_execution.time, "monotonic", lambda: next(clock))

    def runner(
        command: tuple[str, ...],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[bytes]:
        timeout = kwargs["timeout"]
        assert isinstance(timeout, float)
        timeouts.append(timeout)
        _write_probe_receipt(kwargs)
        return subprocess.CompletedProcess(command, 0)

    result = probe_python_import("json", process_runner=runner)

    assert result.passed is True
    assert timeouts == [5.0, 3.5]


def test_import_probe_does_not_misclassify_custom_user_site_editables_as_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    user_site = tmp_path / "custom-user-site"
    user_site.mkdir()
    (user_site / "editable-hook.pth").write_text(
        "import __editable___dpone_probe\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(import_user_site.site, "ENABLE_USER_SITE", True)
    monkeypatch.setattr(import_user_site.site, "USER_BASE", str(tmp_path))
    monkeypatch.setattr(import_user_site.site, "USER_SITE", str(user_site))
    monkeypatch.setenv("PYTHONUSERBASE", str(tmp_path))

    def runner(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        nonlocal calls
        calls += 1
        outcome = 3 if calls == 1 else 0
        _write_probe_receipt(_kwargs, outcome=outcome)
        return subprocess.CompletedProcess((), outcome)

    result = probe_python_import("_dpone_pep660_editable", process_runner=runner)

    assert result.reason_code == "python_import_startup_unsupported"
    assert result.summary == "effective Python startup environment cannot be reproduced safely"
    assert result.passed is False
    assert calls == 2


def test_import_probe_ignores_custom_user_base_when_user_site_is_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(import_user_site.site, "ENABLE_USER_SITE", False)
    monkeypatch.setenv("PYTHONUSERBASE", "/private/inactive-user-site")

    def runner(
        command: tuple[str, ...],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[bytes]:
        _write_probe_receipt(kwargs)
        return subprocess.CompletedProcess(command, 0)

    result = probe_python_import("json", process_runner=runner)

    assert result == PythonImportHealth(True, None, "module import succeeded")


def test_import_probe_supports_user_site_paths_when_no_hook_is_needed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user_site = tmp_path / "user-site"
    user_site.mkdir()
    (user_site / "path-only.pth").write_text(str(tmp_path) + "\n", encoding="utf-8")
    module_name = "_dpone_user_site_path_probe"
    (tmp_path / f"{module_name}.py").write_text(
        "import site\n"
        f"assert site.USER_BASE == {str(tmp_path)!r}\n"
        f"assert site.USER_SITE == {str(user_site)!r}\n"
        "assert site.ENABLE_USER_SITE is True\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(import_user_site.site, "ENABLE_USER_SITE", True)
    monkeypatch.setattr(import_user_site.site, "USER_BASE", str(tmp_path))
    monkeypatch.setattr(import_user_site.site, "USER_SITE", str(user_site))
    monkeypatch.syspath_prepend(str(tmp_path))

    result = probe_python_import(module_name)

    assert result == PythonImportHealth(True, None, "module import succeeded")


@pytest.mark.parametrize(
    ("name", "invalid_value"),
    (
        ("PYTHONHASHSEED", "invalid"),
        ("PYTHONMALLOC", "invalid"),
        ("PYTHONTRACEMALLOC", "invalid"),
        ("PYTHONIOENCODING", "invalid-codec"),
    ),
)
def test_import_probe_rejects_unreplayable_cpython_startup_controls(
    name: str,
    invalid_value: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(name, invalid_value)

    result = probe_python_import("json")

    assert result == PythonImportHealth(
        False,
        "python_import_startup_unsupported",
        "effective Python startup environment cannot be reproduced safely",
    )


@pytest.mark.parametrize(
    ("name", "value", "assertion"),
    (
        (
            "PYTHONFAULTHANDLER",
            "1",
            "import faulthandler\nassert faulthandler.is_enabled()\nassert 'faulthandler' not in sys._xoptions\n",
        ),
        (
            "PYTHONTRACEMALLOC",
            "5",
            "import tracemalloc\n"
            "assert tracemalloc.is_tracing()\n"
            "assert tracemalloc.get_traceback_limit() == 5\n"
            "assert 'tracemalloc' not in sys._xoptions\n",
        ),
    ),
)
def test_import_probe_replays_effective_env_derived_runtime_state(
    name: str,
    value: str,
    assertion: str,
    tmp_path: Path,
    clean_doctor_probe_python: Path,
) -> None:
    module_name = "_dpone_env_derived_runtime_state"
    (tmp_path / f"{module_name}.py").write_text(
        "import os, sys\n" + f"assert os.environ[{name!r}] == {value!r}\n" + assertion,
        encoding="utf-8",
    )
    script = (
        "import os, sys; "
        "sys.path.insert(0, sys.argv[1]); "
        "from dpone.readiness.python_import_health import probe_python_import; "
        f"os._exit(0 if probe_python_import({module_name!r}).passed else 1)"
    )
    environment = dict(os.environ)
    environment[name] = value

    completed = subprocess.run(
        (str(clean_doctor_probe_python), "-c", script, str(tmp_path)),
        check=False,
        env=_outer_probe_environment(environment),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=20,
    )

    assert completed.returncode == 0


@pytest.mark.parametrize(
    "module_body",
    (
        "import atexit, time\natexit.register(time.sleep, 60)\n",
        ("import threading, time\nthreading.Thread(target=time.sleep, args=(60,), daemon=False).start()\n"),
    ),
)
def test_import_probe_commits_success_without_waiting_for_target_shutdown_hooks(
    module_body: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_name = "_dpone_hanging_shutdown_probe"
    (tmp_path / f"{module_name}.py").write_text(module_body, encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))

    result = probe_python_import(module_name)

    assert result == PythonImportHealth(True, None, "module import succeeded")
