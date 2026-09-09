from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from dpone.readiness.python_import_health import probe_python_import
from dpone.readiness.python_import_runtime_identity import capture_interpreter_fingerprint
from tests.doctor_import_test_support import _outer_probe_environment

_UNSUPPORTED = "python_import_startup_unsupported"
_OUTER_AUTHENTICATION_TIMEOUT_SECONDS = 60


def test_stock_windows_absent_abiflags_has_an_exact_absent_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delattr(sys, "abiflags", raising=False)

    fingerprint = capture_interpreter_fingerprint()

    assert fingerprint is not None
    assert fingerprint.abiflags is None


def test_present_non_string_abiflags_is_not_conflated_with_absence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "abiflags", None, raising=False)

    assert capture_interpreter_fingerprint() is None


@pytest.mark.skipif(not hasattr(sys, "abiflags"), reason="requires a configured-build abiflags attribute")
@pytest.mark.usefixtures("replayable_doctor_import_surface")
def test_mutated_abiflags_presence_cannot_match_the_fresh_interpreter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delattr(sys, "abiflags")

    result = probe_python_import("json")

    assert result.reason_code == _UNSUPPORTED


@pytest.mark.usefixtures("replayable_doctor_import_surface")
def test_oversized_hexversion_fails_closed_before_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "hexversion", 10**5000)

    result = probe_python_import("json")

    assert result.passed is False
    assert result.reason_code == "python_import_policy_invalid"


@pytest.mark.skipif(sys.platform != "darwin", reason="requires macOS launcher semantics")
def test_same_version_consumed_launcher_cannot_change_target_interpreter_identity(
    tmp_path: Path,
    clean_doctor_probe_python: Path,
) -> None:
    wrapper = clean_doctor_probe_python.parent / "python-wrapper"
    wrapper.write_text(
        "#!/bin/sh\nexec " + repr(str(clean_doctor_probe_python)) + ' "$@"\n',
        encoding="utf-8",
    )
    wrapper.chmod(0o700)
    target = "_dpone_same_version_launcher_target"
    (tmp_path / f"{target}.py").write_text(
        "import os, sys\n"
        "if sys.executable == os.environ['DPONE_WRAPPER']: "
        "raise ImportError('parent executable identity')\n",
        encoding="utf-8",
    )
    environment = dict(os.environ)
    environment["DPONE_WRAPPER"] = str(wrapper)
    environment["__PYVENV_LAUNCHER__"] = str(wrapper)
    repository_source = Path(__file__).resolve().parents[1] / "src"
    runtime_paths = [
        str(tmp_path),
        str(repository_source),
        *(entry for entry in sys.path if entry and Path(entry).is_dir()),
    ]
    script = (
        "import os, sys\n"
        "sys.path[:0] = sys.argv[1:]\n"
        "direct_failed = False\n"
        f"try: __import__({target!r})\n"
        "except ImportError: direct_failed = True\n"
        "from dpone.readiness.python_import_health import probe_python_import\n"
        f"result = probe_python_import({target!r})\n"
        f"os._exit(0 if direct_failed and result.reason_code == {_UNSUPPORTED!r} else 1)\n"
    )

    completed = subprocess.run(
        (
            str(clean_doctor_probe_python),
            "-E",
            "-S",
            "-c",
            script,
            *runtime_paths,
        ),
        check=False,
        env=_outer_probe_environment(environment),
        capture_output=True,
        text=True,
        # The nested production probes retain their five-second budgets.  This
        # outer deadline covers Windows process creation and interpreter
        # startup under a loaded hosted runner without weakening the invariant.
        timeout=_OUTER_AUTHENTICATION_TIMEOUT_SECONDS,
    )

    assert completed.returncode == 0, (completed.stdout, completed.stderr)


@pytest.mark.parametrize(
    ("startup_arguments", "strip_kind", "strip_value", "target_source"),
    (
        (("-O",), "argument", "-O", "import sys\nif sys.flags.optimize: raise ImportError\n"),
        (("-u",), "argument", "-u", "import sys\nif sys.stdout.write_through: raise ImportError\n"),
        (
            ("-X", "faulthandler"),
            "xoption",
            "faulthandler",
            "import faulthandler\nif faulthandler.is_enabled(): raise ImportError\n",
        ),
        (
            ("-X", "tracemalloc=3"),
            "xoption",
            "tracemalloc",
            "import tracemalloc\nif tracemalloc.get_traceback_limit() == 3: raise ImportError\n",
        ),
        (
            ("-X", "frozen_modules=off"),
            "xoption",
            "frozen_modules",
            "import _imp\nif not _imp.is_frozen('os'): raise ImportError\n",
        ),
        (
            ("-X", "no_debug_ranges"),
            "xoption",
            "no_debug_ranges",
            "def marker(): pass\n"
            "positions = tuple(marker.__code__.co_positions())\n"
            "if positions and all(a is None and b is None for _, _, a, b in positions): raise ImportError\n",
        ),
        (
            ("-X", "int_max_str_digits=640"),
            "xoption",
            "int_max_str_digits",
            "import sys\nif sys.get_int_max_str_digits() == 640: raise ImportError\n",
        ),
    ),
)
def test_child_must_authenticate_immutable_flags_and_runtime_state(
    startup_arguments: tuple[str, ...],
    strip_kind: str,
    strip_value: str,
    target_source: str,
    tmp_path: Path,
    clean_doctor_probe_python: Path,
) -> None:
    target = "_dpone_stripped_startup_policy_target"
    (tmp_path / f"{target}.py").write_text(target_source, encoding="utf-8")
    script = (
        "import os, sys\n"
        "from dpone.readiness.python_import_health import probe_python_import\n"
        "from dpone.readiness.python_import_probe_runner import run_contained_import_process\n"
        "sys.path.insert(0, sys.argv[1])\n"
        "direct_failed = False\n"
        f"try: __import__({target!r})\n"
        "except ImportError: direct_failed = True\n"
        "def runner(command, **kwargs):\n"
        "    filtered = []\n"
        "    index = 0\n"
        "    while index < len(command):\n"
        "        if sys.argv[2] == 'argument' and command[index] == sys.argv[3]:\n"
        "            index += 1\n"
        "        elif (sys.argv[2] == 'xoption' and command[index] == '-X' "
        "and index + 1 < len(command) and command[index + 1].split('=', 1)[0] == sys.argv[3]):\n"
        "            index += 2\n"
        "        else:\n"
        "            filtered.append(command[index])\n"
        "            index += 1\n"
        "    return run_contained_import_process(tuple(filtered), **kwargs)\n"
        f"result = probe_python_import({target!r}, process_runner=runner)\n"
        f"os._exit(0 if direct_failed and result.reason_code == {_UNSUPPORTED!r} else 1)\n"
    )

    completed = subprocess.run(
        (
            str(clean_doctor_probe_python),
            *startup_arguments,
            "-c",
            script,
            str(tmp_path),
            strip_kind,
            strip_value,
        ),
        env=_outer_probe_environment(),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, (completed.stdout, completed.stderr)
