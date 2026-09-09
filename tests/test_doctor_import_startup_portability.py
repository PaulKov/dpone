from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.doctor_import_test_support import _outer_probe_environment, _run_outer_probe

_UNSUPPORTED = "python_import_startup_unsupported"


@pytest.mark.parametrize(
    ("control", "value"),
    (
        ("PYTHONHASHSEED", "+0"),
        ("PYTHONHASHSEED", " 00"),
        ("PYTHONINTMAXSTRDIGITS", "+640"),
        ("PYTHONIOENCODING", ":"),
        ("PYTHONIOENCODING", "utf-8:"),
        ("PYTHONNOUSERSITE", "0"),
        ("PYTHONSAFEPATH", "0"),
        ("PYTHONTRACEMALLOC", "-0"),
        ("PYTHONUNBUFFERED", "00"),
    ),
)
def test_valid_cpython_startup_control_spelling_is_supported(
    control: str,
    value: str,
    clean_doctor_probe_python: Path,
) -> None:
    environment = dict(os.environ)
    environment[control] = value
    script = (
        "import os; "
        "from dpone.readiness.python_import_health import probe_python_import; "
        "os._exit(0 if probe_python_import('json').passed else 1)"
    )

    completed = _run_outer_probe(
        clean_doctor_probe_python,
        script,
        environment=environment,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.parametrize("stdio_mutation", ("reconfigure", "custom_raw"))
def test_noncanonical_unbuffered_stdio_state_is_startup_unsupported(
    stdio_mutation: str,
    tmp_path: Path,
    clean_doctor_probe_python: Path,
) -> None:
    target = f"_dpone_noncanonical_stdio_{stdio_mutation}"
    (tmp_path / f"{target}.py").write_text(
        "import io, os, sys\n"
        "kind = os.environ['DPONE_STDIO_MUTATION']\n"
        "noncanonical = (\n"
        "    sys.stdout.write_through\n"
        "    and (kind == 'reconfigure' or type(sys.stdout.buffer) is not io.FileIO)\n"
        ")\n"
        "if noncanonical: raise ImportError('noncanonical stdio')\n",
        encoding="utf-8",
    )
    script = (
        "import io, os, sys; "
        "sys.path.insert(0, sys.argv[1]); "
        "os.environ['DPONE_STDIO_MUTATION'] = sys.argv[2]; "
        "\nif sys.argv[2] == 'reconfigure':\n"
        "    [stream.reconfigure(write_through=True) for stream in "
        "(sys.__stdin__, sys.__stdout__, sys.__stderr__)]\n"
        "else:\n"
        "    class Sink(io.RawIOBase):\n"
        "        def writable(self): return True\n"
        "        def write(self, value): return len(value)\n"
        "    sys.__stdin__.reconfigure(write_through=True)\n"
        "    sys.stdout = sys.__stdout__ = io.TextIOWrapper(Sink(), write_through=True)\n"
        "    sys.stderr = sys.__stderr__ = io.TextIOWrapper(Sink(), write_through=True)\n"
        "direct_failed = False\n"
        f"try: __import__({target!r})\n"
        "except ImportError: direct_failed = True\n"
        "from dpone.readiness.python_import_health import probe_python_import\n"
        f"result = probe_python_import({target!r})\n"
        f"os._exit(0 if direct_failed and result.reason_code == {_UNSUPPORTED!r} else 1)"
    )

    completed = _run_outer_probe(
        clean_doctor_probe_python,
        script,
        str(tmp_path),
        stdio_mutation,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.parametrize(
    ("control", "value"),
    (
        ("PYTHONMALLOC", "malloc"),
        ("PYTHONMALLOCSTATS", "1"),
        ("PYTHONCASEOK", "1"),
        ("PYTHONCOERCECLOCALE", "1"),
        ("PYTHONLEGACYWINDOWSFSENCODING", "1"),
        ("PYTHONLEGACYWINDOWSSTDIO", "1"),
        ("PYTHONPERFSUPPORT", "1"),
        ("PYTHONPROFILEIMPORTTIME", "1"),
    ),
)
def test_unobservable_env_only_startup_instrumentation_is_unsupported(
    control: str,
    value: str,
    clean_doctor_probe_python: Path,
) -> None:
    script = (
        "import os; "
        f"os.environ[{control!r}] = {value!r}; "
        "from dpone.readiness.python_import_health import probe_python_import; "
        "result = probe_python_import('json'); "
        f"os._exit(0 if result.reason_code == {_UNSUPPORTED!r} else 1)"
    )

    completed = _run_outer_probe(clean_doctor_probe_python, script)

    assert completed.returncode == 0, completed.stderr


@pytest.mark.parametrize(
    ("control", "value"),
    (
        ("PYTHONMALLOC", "malloc"),
        ("PYTHONMALLOCSTATS", "1"),
        ("PYTHONCASEOK", "1"),
        ("PYTHONCOERCECLOCALE", "1"),
        ("PYTHONLEGACYWINDOWSFSENCODING", "1"),
        ("PYTHONLEGACYWINDOWSSTDIO", "1"),
        ("PYTHONPERFSUPPORT", "1"),
        ("PYTHONPROFILEIMPORTTIME", "1"),
    ),
)
def test_ignore_environment_keeps_opaque_startup_controls_supported(
    control: str,
    value: str,
    clean_doctor_probe_python: Path,
) -> None:
    environment = dict(os.environ)
    environment[control] = value
    script = (
        "import os; "
        "from dpone.readiness.python_import_health import probe_python_import; "
        "os._exit(0 if probe_python_import('json').passed else 1)"
    )

    completed = subprocess.run(
        (str(clean_doctor_probe_python), "-E", "-c", script),
        env=_outer_probe_environment(environment),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.parametrize("xoption", ("importtime", "perf"))
def test_unobservable_xoption_instrumentation_is_unsupported(
    xoption: str,
    clean_doctor_probe_python: Path,
) -> None:
    script = (
        "import os; "
        "from dpone.readiness.python_import_health import probe_python_import; "
        "result = probe_python_import('json'); "
        f"os._exit(0 if result.reason_code == {_UNSUPPORTED!r} else 1)"
    )

    completed = subprocess.run(
        (str(clean_doctor_probe_python), "-X", xoption, "-c", script),
        env=_outer_probe_environment(),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.skipif(sys.platform != "darwin", reason="requires macOS launcher semantics")
def test_consumed_macos_venv_launcher_cannot_switch_probe_interpreters(
    tmp_path: Path,
    clean_doctor_probe_python: Path,
) -> None:
    alternate = Path("/usr/bin/python3")
    if not alternate.is_file():
        pytest.skip("no alternate macOS interpreter")
    alternate_version = subprocess.run(
        (str(alternate), "-c", "import sys; print('.'.join(map(str, sys.version_info[:2])))"),
        env=_outer_probe_environment(),
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    ).stdout.strip()
    current_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    if alternate_version == current_version:
        pytest.skip("macOS alternate interpreter has the same runtime version")

    target = "_dpone_macos_pythonexecutable_target"
    (tmp_path / f"{target}.py").write_text(
        "import os, sys\n"
        "if '.'.join(map(str, sys.version_info[:2])) == os.environ['DPONE_PARENT_VERSION']: "
        "raise ImportError('parent runtime')\n",
        encoding="utf-8",
    )
    environment = dict(os.environ)
    environment["DPONE_PARENT_VERSION"] = current_version
    environment["__PYVENV_LAUNCHER__"] = str(alternate)
    discovery = subprocess.run(
        (
            str(clean_doctor_probe_python),
            "-E",
            "-c",
            "import os, sys; print(sys.executable); print('__PYVENV_LAUNCHER__' in os.environ)",
        ),
        check=False,
        env=_outer_probe_environment(environment),
        capture_output=True,
        text=True,
        timeout=30,
    )
    if discovery.returncode != 0:
        pytest.skip("alternate interpreter is not a compatible macOS launcher")
    discovered = discovery.stdout.splitlines()
    if discovered != [str(alternate), "False"]:
        pytest.skip("interpreter does not consume the macOS venv launcher")
    repository_source = Path(__file__).resolve().parents[1] / "src"
    runtime_paths = [
        str(tmp_path),
        str(repository_source),
        *(entry for entry in sys.path if entry and Path(entry).is_dir()),
    ]
    script = (
        "import os, sys; "
        "sys.path[:0] = sys.argv[1:]; "
        "direct_failed = False; "
        f"\ntry: __import__({target!r})\n"
        "except ImportError: direct_failed = True\n"
        "from dpone.readiness.python_import_health import probe_python_import; "
        f"result = probe_python_import({target!r}); "
        "print(direct_failed, result.reason_code, sys.executable, flush=True); "
        f"os._exit(0 if direct_failed and result.reason_code == {_UNSUPPORTED!r} else 1)"
    )

    completed = subprocess.run(
        (
            str(clean_doctor_probe_python),
            "-E",
            "-c",
            script,
            *runtime_paths,
        ),
        check=False,
        env=_outer_probe_environment(environment),
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, (completed.stdout, completed.stderr)
