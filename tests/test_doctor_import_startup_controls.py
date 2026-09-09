from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from tests.doctor_import_test_support import _outer_probe_environment, _run_outer_probe

_UNSUPPORTED = "python_import_startup_unsupported"


@pytest.mark.parametrize("control", ("PYTHONIOENCODING", "PYTHONHASHSEED"))
def test_startup_derived_control_cannot_false_pass(
    control: str,
    tmp_path: Path,
    clean_doctor_probe_python: Path,
) -> None:
    target = f"_dpone_{control.lower()}_target"
    module = tmp_path / f"{target}.py"
    if control == "PYTHONIOENCODING":
        value = "utf-8:strict"
        guard = "import sys\nif sys.stdout.errors == 'strict': raise ImportError('strict startup IO')\n"
    else:
        value = "0"
        guard = (
            "import os, sys\n"
            "if sys.flags.hash_randomization == 0 and hash('dpone-seed') == "
            "int(os.environ['DPONE_PARENT_HASH']): raise ImportError('fixed startup hash')\n"
        )
    module.write_text(guard, encoding="utf-8")
    environment = dict(os.environ)
    environment[control] = value
    script = (
        "import os, sys; "
        "sys.path.insert(0, sys.argv[1]); "
        "os.environ['DPONE_PARENT_HASH'] = str(hash('dpone-seed')); "
        "direct_failed = False; "
        f"\ntry: __import__({target!r})\n"
        "except ImportError: direct_failed = True\n"
        "from dpone.readiness.python_import_health import probe_python_import; "
        f"result = probe_python_import({target!r}); "
        f"raise SystemExit(0 if direct_failed and result.reason_code == {_UNSUPPORTED!r} else 1)"
    )

    completed = _run_outer_probe(
        clean_doctor_probe_python,
        script,
        str(tmp_path),
        environment=environment,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.parametrize("late_seed", ("1", "random", ""))
def test_late_unverifiable_hash_seed_is_startup_unsupported(
    late_seed: str,
    tmp_path: Path,
    clean_doctor_probe_python: Path,
) -> None:
    target = "_dpone_late_hash_seed_target"
    (tmp_path / f"{target}.py").write_text(
        "import os\n"
        "if hash('dpone-late-seed') == int(os.environ['DPONE_PARENT_HASH']): "
        "raise ImportError('parent hash behavior')\n",
        encoding="utf-8",
    )
    script = (
        "import os, sys; "
        "sys.path.insert(0, sys.argv[1]); "
        "os.environ['DPONE_PARENT_HASH'] = str(hash('dpone-late-seed')); "
        "direct_failed = False; "
        f"\ntry: __import__({target!r})\n"
        "except ImportError: direct_failed = True\n"
        "os.environ['PYTHONHASHSEED'] = sys.argv[2]; "
        "from dpone.readiness.python_import_health import probe_python_import; "
        f"result = probe_python_import({target!r}); "
        f"raise SystemExit(0 if direct_failed and result.reason_code == {_UNSUPPORTED!r} else 1)"
    )

    completed = _run_outer_probe(
        clean_doctor_probe_python,
        script,
        str(tmp_path),
        late_seed,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.parametrize(
    ("control", "flag_name"),
    (
        ("PYTHONNOUSERSITE", "no_user_site"),
        ("PYTHONSAFEPATH", "safe_path"),
    ),
)
def test_late_isolation_implied_flag_control_is_startup_unsupported(
    control: str,
    flag_name: str,
    tmp_path: Path,
    clean_doctor_probe_python: Path,
) -> None:
    target = f"_dpone_late_{control.lower()}_target"
    (tmp_path / f"{target}.py").write_text(
        "import os, sys\n"
        "if not bool(getattr(sys.flags, os.environ['DPONE_FLAG_NAME'])): "
        "raise ImportError('parent flag behavior')\n",
        encoding="utf-8",
    )
    script = (
        "import os, sys; "
        "sys.path.insert(0, sys.argv[1]); "
        "os.environ['DPONE_FLAG_NAME'] = sys.argv[2]; "
        "direct_failed = False; "
        f"\ntry: __import__({target!r})\n"
        "except ImportError: direct_failed = True\n"
        "os.environ[sys.argv[3]] = '1'; "
        "from dpone.readiness.python_import_health import probe_python_import; "
        f"result = probe_python_import({target!r}); "
        f"raise SystemExit(0 if direct_failed and result.reason_code == {_UNSUPPORTED!r} else 1)"
    )

    completed = _run_outer_probe(
        clean_doctor_probe_python,
        script,
        str(tmp_path),
        flag_name,
        control,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.parametrize(
    ("control", "value", "guard"),
    (
        (
            "PYTHONFAULTHANDLER",
            "1",
            "import faulthandler\nif faulthandler.is_enabled(): raise ImportError('faulthandler is active')\n",
        ),
        (
            "PYTHONTRACEMALLOC",
            "5",
            "import tracemalloc\nif tracemalloc.is_tracing(): raise ImportError('tracemalloc is active')\n",
        ),
        (
            "PYTHONUNBUFFERED",
            "1",
            "import sys\nif sys.stdout.write_through: raise ImportError('unbuffered IO is active')\n",
        ),
    ),
)
def test_removed_observable_runtime_control_is_replayed(
    control: str,
    value: str,
    guard: str,
    tmp_path: Path,
    clean_doctor_probe_python: Path,
) -> None:
    target = f"_dpone_removed_{control.lower()}_target"
    (tmp_path / f"{target}.py").write_text(guard, encoding="utf-8")
    environment = dict(os.environ)
    environment[control] = value
    script = (
        "import os, sys; "
        "sys.path.insert(0, sys.argv[1]); "
        "os.environ.pop(sys.argv[2]); "
        "direct_failed = False; "
        f"\ntry: __import__({target!r})\n"
        "except ImportError: direct_failed = True\n"
        "from dpone.readiness.python_import_health import probe_python_import; "
        f"result = probe_python_import({target!r}); "
        "os._exit(0 if direct_failed and result.reason_code == 'python_import_failed' else 1)"
    )

    completed = _run_outer_probe(
        clean_doctor_probe_python,
        script,
        str(tmp_path),
        control,
        environment=environment,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.parametrize(
    ("control", "value", "guard"),
    (
        (
            "PYTHONINSPECT",
            "1",
            "import sys\nif sys.flags.inspect: raise ImportError('inspect is active')\n",
        ),
    ),
)
def test_removed_non_reconstructable_startup_state_is_unsupported(
    control: str,
    value: str,
    guard: str,
    tmp_path: Path,
    clean_doctor_probe_python: Path,
) -> None:
    target = "_dpone_removed_non_reconstructable_state"
    (tmp_path / f"{target}.py").write_text(guard, encoding="utf-8")
    child_environment = dict(os.environ)
    child_environment[control] = value
    script = (
        "import os, sys; "
        "sys.path.insert(0, sys.argv[1]); "
        "os.environ.pop(sys.argv[2]); "
        "direct_failed = False; "
        f"\ntry: __import__({target!r})\n"
        "except ImportError: direct_failed = True\n"
        "from dpone.readiness.python_import_health import probe_python_import; "
        f"result = probe_python_import({target!r}); "
        f"os._exit(0 if direct_failed and result.reason_code == {_UNSUPPORTED!r} else 1)"
    )

    completed = subprocess.run(
        (
            str(clean_doctor_probe_python),
            "-c",
            script,
            str(tmp_path),
            control,
        ),
        check=False,
        env=_outer_probe_environment(child_environment),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=30,
    )

    assert completed.returncode == 0


def test_runtime_int_digit_policy_mutation_is_startup_unsupported(
    tmp_path: Path,
    clean_doctor_probe_python: Path,
) -> None:
    target = "_dpone_runtime_int_digit_policy_target"
    (tmp_path / f"{target}.py").write_text(
        "import sys\nif sys.get_int_max_str_digits() == 1000: raise ImportError('runtime digit policy')\n",
        encoding="utf-8",
    )
    script = (
        "import os, sys; "
        "sys.path.insert(0, sys.argv[1]); "
        "sys.set_int_max_str_digits(1000); "
        "direct_failed = False; "
        f"\ntry: __import__({target!r})\n"
        "except ImportError: direct_failed = True\n"
        "from dpone.readiness.python_import_health import probe_python_import; "
        f"result = probe_python_import({target!r}); "
        f"os._exit(0 if direct_failed and result.reason_code == {_UNSUPPORTED!r} else 1)"
    )

    completed = _run_outer_probe(
        clean_doctor_probe_python,
        script,
        str(tmp_path),
    )

    assert completed.returncode == 0, completed.stderr


def test_mutated_frozen_modules_option_is_startup_unsupported(
    tmp_path: Path,
    clean_doctor_probe_python: Path,
) -> None:
    target = "_dpone_mutated_frozen_modules_target"
    (tmp_path / f"{target}.py").write_text(
        "import _imp\nif _imp.is_frozen('os'): raise ImportError('os is effectively frozen')\n",
        encoding="utf-8",
    )
    script = (
        "import os, sys; "
        "sys.path.insert(0, sys.argv[1]); "
        "direct_failed = False; "
        f"\ntry: __import__({target!r})\n"
        "except ImportError: direct_failed = True\n"
        "sys._xoptions['frozen_modules'] = 'off'; "
        "from dpone.readiness.python_import_health import probe_python_import; "
        f"result = probe_python_import({target!r}); "
        f"os._exit(0 if direct_failed and result.reason_code == {_UNSUPPORTED!r} else 1)"
    )

    completed = _run_outer_probe(
        clean_doctor_probe_python,
        script,
        str(tmp_path),
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.parametrize(
    ("option", "value", "guard"),
    (
        (
            "faulthandler",
            "enabled",
            "import faulthandler\nif not faulthandler.is_enabled(): raise ImportError('faulthandler is disabled')\n",
        ),
        (
            "tracemalloc",
            "5",
            "import tracemalloc\nif not tracemalloc.is_tracing(): raise ImportError('tracemalloc is disabled')\n",
        ),
    ),
)
def test_mutated_runtime_xoption_is_startup_unsupported(
    option: str,
    value: str,
    guard: str,
    tmp_path: Path,
    clean_doctor_probe_python: Path,
) -> None:
    target = f"_dpone_mutated_{option}_option_target"
    (tmp_path / f"{target}.py").write_text(guard, encoding="utf-8")
    script = (
        "import os, sys; "
        "sys.path.insert(0, sys.argv[1]); "
        "direct_failed = False; "
        f"\ntry: __import__({target!r})\n"
        "except ImportError: direct_failed = True\n"
        "sys._xoptions[sys.argv[2]] = sys.argv[3]; "
        "from dpone.readiness.python_import_health import probe_python_import; "
        f"result = probe_python_import({target!r}); "
        f"os._exit(0 if direct_failed and result.reason_code == {_UNSUPPORTED!r} else 1)"
    )

    completed = _run_outer_probe(
        clean_doctor_probe_python,
        script,
        str(tmp_path),
        option,
        value,
    )

    assert completed.returncode == 0, completed.stderr


def test_interactive_pythonstartup_state_is_startup_unsupported(
    tmp_path: Path,
    clean_doctor_probe_python: Path,
) -> None:
    target = "_dpone_interactive_startup_target"
    (tmp_path / f"{target}.py").write_text(
        "import builtins\n"
        "if getattr(builtins, 'DPONE_STARTUP_MARKER', False): "
        "raise ImportError('interactive startup ran')\n",
        encoding="utf-8",
    )
    startup = tmp_path / "startup.py"
    startup.write_text("import builtins\nbuiltins.DPONE_STARTUP_MARKER = True\n", encoding="utf-8")
    environment = dict(os.environ)
    environment["PYTHONSTARTUP"] = str(startup)
    script = (
        "import os, sys\n"
        f"sys.path.insert(0, {str(tmp_path)!r})\n"
        "direct_failed = False\n"
        f"try: __import__({target!r})\n"
        "except ImportError: direct_failed = True\n"
        "from dpone.readiness.python_import_health import probe_python_import\n"
        f"result = probe_python_import({target!r})\n"
        f"os._exit(0 if direct_failed and result.reason_code == {_UNSUPPORTED!r} else 1)\n"
    )

    completed = subprocess.run(
        (str(clean_doctor_probe_python), "-i"),
        input=f"exec({script!r})\n",
        check=False,
        env=_outer_probe_environment(environment),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0
