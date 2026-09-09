from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.readiness.python_import_health import (
    PythonImportHealth,
    _interpreter_probe_options,
    probe_python_import,
)
from dpone.readiness.python_import_probe_protocol import normalize_xoptions
from dpone.readiness.python_import_probe_runner import run_contained_import_process
from tests.doctor_import_test_support import _outer_probe_environment

pytestmark = pytest.mark.usefixtures("replayable_doctor_import_surface")


@pytest.mark.parametrize("module_name", (object(), "x" * 1_025))
def test_import_probe_rejects_noncanonical_or_oversized_module_name_before_child_start(
    module_name: object,
) -> None:
    class HostileName:
        def __str__(self) -> str:
            raise AssertionError("module name conversion must not run")

    candidate = HostileName() if type(module_name) is object else module_name
    calls = 0

    def runner(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess((), 0)

    result = probe_python_import(candidate, process_runner=runner)  # type: ignore[arg-type]

    assert result.reason_code == "python_import_name_invalid"
    assert calls == 0


@pytest.mark.parametrize(
    "startup_control",
    ("PYTHONHOME", "PYTHONPLATLIBDIR", "PYTHONEXECUTABLE", "__PYVENV_LAUNCHER__"),
)
def test_import_probe_fails_closed_for_unreplayable_startup_derived_state(
    startup_control: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    monkeypatch.setenv(startup_control, "/private/effective-python-root")

    def runner(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess((), 0)

    result = probe_python_import("json", process_runner=runner)

    assert result == PythonImportHealth(
        passed=False,
        reason_code="python_import_startup_unsupported",
        summary="effective Python startup environment cannot be reproduced safely",
    )
    assert calls == 0


def test_import_probe_mirrors_parent_import_policy_flags() -> None:
    flags = SimpleNamespace(
        isolated=1,
        ignore_environment=1,
        no_user_site=1,
        no_site=1,
        optimize=2,
        dont_write_bytecode=1,
        bytes_warning=2,
        dev_mode=1,
        utf8_mode=1,
        warn_default_encoding=1,
        safe_path=1,
        int_max_str_digits=4_300,
    )

    assert _interpreter_probe_options(flags, ("error::UserWarning",)) == (
        "-P",
        "-I",
        "-S",
        "-OO",
        "-B",
        "-bb",
        "-X",
        "dev",
        "-X",
        "utf8=1",
        "-X",
        "warn_default_encoding",
        "-X",
        "int_max_str_digits=4300",
    )
    sensitive_value = "private-xoption-value"
    valued_options = _interpreter_probe_options(
        flags,
        (),
        {"faulthandler": sensitive_value},
    )
    assert valued_options is not None
    assert ("-X", "faulthandler") == valued_options[-2:]
    assert sensitive_value not in repr(valued_options)

    flags.debug = 2
    flags.inspect = 1
    flags.interactive = 1
    flags.verbose = 2
    flags.quiet = 1
    flags.hash_randomization = 0
    complete_flags = _interpreter_probe_options(flags, ())
    assert complete_flags is not None
    assert "-dd" in complete_flags
    assert "-i" in complete_flags
    assert "-vv" in complete_flags
    assert "-q" in complete_flags


@pytest.mark.parametrize("safe_path", (False, True))
def test_import_probe_preserves_target_visible_safe_path(
    safe_path: bool,
    tmp_path: Path,
    clean_doctor_probe_python: Path,
) -> None:
    module_name = f"_dpone_safe_path_{int(safe_path)}"
    (tmp_path / f"{module_name}.py").write_text(
        "import sys\nassert bool(sys.flags.safe_path) is " + repr(safe_path) + "\n",
        encoding="utf-8",
    )
    script = (
        "import sys; "
        "sys.path.insert(0, sys.argv[1]); "
        "from dpone.readiness.python_import_health import probe_python_import; "
        f"raise SystemExit(0 if probe_python_import({module_name!r}).passed else 1)"
    )
    options = ("-P",) if safe_path else ()

    completed = subprocess.run(
        (str(clean_doctor_probe_python), *options, "-c", script, str(tmp_path)),
        env=_outer_probe_environment(),
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=15,
    )

    assert completed.returncode == (0 if safe_path else 1)


@pytest.mark.parametrize(
    "guard",
    (
        "if sys.flags.isolated: raise RuntimeError('effective runtime only')\n",
        "if not sys.flags.isolated: raise RuntimeError('hermetic runtime only')\n",
    ),
)
def test_import_probe_never_certifies_a_sys_flags_dependent_disagreement(
    guard: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_name = "_dpone_flags_dependent_probe"
    (tmp_path / f"{module_name}.py").write_text("import sys\n" + guard, encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))

    result = probe_python_import(module_name)

    assert result == PythonImportHealth(
        False,
        "python_import_startup_unsupported",
        "effective Python startup environment cannot be reproduced safely",
    )


@pytest.mark.parametrize(
    "xoption",
    ("frozen_modules", "frozen_modules=off", "dev=1", "no_debug_ranges"),
)
def test_import_probe_preserves_import_relevant_xoptions(
    xoption: str,
    tmp_path: Path,
    clean_doctor_probe_python: Path,
) -> None:
    module_name = "_dpone_xoption_probe_" + xoption.replace("=", "_")
    if xoption == "frozen_modules":
        assertion = "import importlib.util\nassert importlib.util.find_spec('__hello__').origin == 'frozen'\n"
    elif xoption == "frozen_modules=off":
        assertion = "import importlib.util\nassert importlib.util.find_spec('__hello__').origin != 'frozen'\n"
    elif xoption == "dev=1":
        assertion = "import sys\nassert sys.flags.dev_mode\nassert sys._xoptions['dev'] == '1'\n"
    else:
        assertion = (
            "def marker():\n    return None\n"
            "assert all(a is None and b is None for _, _, a, b in marker.__code__.co_positions())\n"
        )
    (tmp_path / f"{module_name}.py").write_text(assertion, encoding="utf-8")
    script = (
        "import sys; "
        "sys.path.insert(0, sys.argv[1]); "
        "from dpone.readiness.python_import_health import probe_python_import; "
        f"raise SystemExit(0 if probe_python_import({module_name!r}).passed else 1)"
    )

    completed = subprocess.run(
        (str(clean_doctor_probe_python), "-X", xoption, "-c", script, str(tmp_path)),
        env=_outer_probe_environment(),
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=15,
    )

    assert completed.returncode == 0


def test_import_probe_transports_pycache_prefix_outside_process_arguments(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prefix = str(tmp_path / "private-pycache-prefix")
    module_name = "_dpone_pycache_prefix_probe"
    (tmp_path / f"{module_name}.py").write_text(
        "import sys\n"
        f"assert sys.pycache_prefix == {prefix!r}\n"
        f"assert sys._xoptions.get('pycache_prefix') == {prefix!r}\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setattr(sys, "pycache_prefix", prefix)
    monkeypatch.setattr(sys, "_xoptions", {"pycache_prefix": prefix})

    result = probe_python_import(module_name)

    assert result.passed is True


def test_import_probe_rejects_unknown_xoptions_before_child_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "_xoptions", {"future_private_option": "secret"})
    calls = 0

    def runner(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess((), 0)

    result = probe_python_import("json", process_runner=runner)

    assert result.reason_code == "python_import_policy_invalid"
    assert result.passed is False
    assert calls == 0


@pytest.mark.parametrize("key", ("tracemalloc", "int_max_str_digits"))
def test_import_probe_rejects_oversized_decimal_xoption_without_conversion(
    key: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "_xoptions", {key: "9" * 5_000})
    calls = 0

    def runner(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess((), 0)

    result = probe_python_import("json", process_runner=runner)

    assert result.reason_code == "python_import_policy_invalid"
    assert result.passed is False
    assert calls == 0


def test_xoptions_require_an_exact_builtin_dict_before_iteration() -> None:
    class HostileDict(dict):
        def items(self):
            raise AssertionError("custom xoptions mapping was iterated")

    assert normalize_xoptions(HostileDict()) is None


def test_import_probe_mirrors_warning_errors_before_target_import(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_name = "_dpone_warning_probe"
    (tmp_path / f"{module_name}.py").write_text(
        "import warnings\nwarnings.warn('must fail import', UserWarning)\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setattr(sys, "warnoptions", ["error::UserWarning"])

    result = probe_python_import(module_name)

    assert result == PythonImportHealth(
        passed=False,
        reason_code="python_import_failed",
        summary="module is installed but cannot be loaded",
    )


def test_import_probe_mirrors_default_encoding_warning_errors(
    tmp_path: Path,
    clean_doctor_probe_python: Path,
) -> None:
    module_name = "_dpone_default_encoding_warning_probe"
    (tmp_path / f"{module_name}.py").write_text(
        "with open(__file__) as stream:\n    stream.read()\n",
        encoding="utf-8",
    )
    script = (
        "import os, sys; "
        "sys.path.insert(0, sys.argv[1]); "
        "from dpone.readiness.python_import_health import probe_python_import; "
        f"result = probe_python_import({module_name!r}); "
        "os._exit(0 if result.reason_code == 'python_import_failed' else 1)"
    )
    completed = subprocess.run(
        (
            str(clean_doctor_probe_python),
            "-X",
            "warn_default_encoding",
            "-W",
            "error::EncodingWarning",
            "-c",
            script,
            str(tmp_path),
        ),
        env=_outer_probe_environment(),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.parametrize(
    ("flag_name", "invalid_value"),
    (
        ("utf8_mode", 2),
        ("dev_mode", -1),
        ("int_max_str_digits", 100),
    ),
)
def test_import_probe_rejects_invalid_interpreter_policy_before_child_start(
    flag_name: str,
    invalid_value: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values = {
        "isolated": 0,
        "ignore_environment": 0,
        "no_user_site": 0,
        "no_site": 0,
        "optimize": 0,
        "dont_write_bytecode": 0,
        "bytes_warning": 0,
        "dev_mode": 0,
        "utf8_mode": 0,
        "warn_default_encoding": 0,
        "safe_path": 0,
        "int_max_str_digits": 4_300,
    }
    values[flag_name] = invalid_value
    monkeypatch.setattr(sys, "flags", SimpleNamespace(**values))
    calls = 0

    def runner(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess((), 0, b"", b"")

    result = probe_python_import("json", process_runner=runner)

    assert result.reason_code == "python_import_policy_invalid"
    assert result.passed is False
    assert calls == 0


def test_import_probe_rejects_an_unsafe_warning_policy_before_child_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "warnoptions", ["error\x00ignored"])
    calls = 0

    def runner(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess((), 0, b"", b"")

    result = probe_python_import("json", process_runner=runner)

    assert result.reason_code == "python_import_policy_invalid"
    assert result.passed is False
    assert calls == 0


def test_import_probe_mirrors_cpython_invalid_warning_option_semantics(
    clean_doctor_probe_python: Path,
) -> None:
    script = (
        "from dpone.readiness.python_import_health import probe_python_import; "
        "raise SystemExit(0 if probe_python_import('json').passed else 1)"
    )

    completed = subprocess.run(
        (str(clean_doctor_probe_python), "-W", "definitely_invalid_action", "-c", script),
        env=_outer_probe_environment(),
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=15,
    )

    assert completed.returncode == 0


def test_import_probe_rejects_unreplayable_startup_warning_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sensitive_warning = "ignore:\ud800"
    monkeypatch.setattr(sys, "warnoptions", [sensitive_warning])
    commands: list[tuple[str, ...]] = []

    def runner(
        command: tuple[str, ...],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[bytes]:
        commands.append(command)
        return run_contained_import_process(command, **kwargs)

    result = probe_python_import("json", process_runner=runner)

    assert result.reason_code == "python_import_startup_unsupported"
    assert result.passed is False
    assert commands == []
