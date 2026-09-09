from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.doctor_import_test_support import (
    _outer_probe_environment,
    _probe_clean_venv_python,
    _probe_venv_python,
    _run_outer_probe,
)

_UNSUPPORTED = "python_import_startup_unsupported"


@pytest.mark.parametrize("mode", ("block", "fake"))
def test_pythonpath_startup_hook_cannot_certify_a_divergent_import(
    mode: str,
    tmp_path: Path,
    clean_doctor_probe_python: Path,
) -> None:
    target = f"_dpone_pythonpath_{mode}_target"
    pythonpath = tmp_path / mode
    pythonpath.mkdir()
    if mode == "block":
        (pythonpath / f"{target}.py").write_text("VALUE = 'present'\n", encoding="utf-8")
    (pythonpath / "sitecustomize.py").write_text(
        "import builtins, os, types\n"
        "real_import = builtins.__import__\n"
        "target = os.environ['DPONE_HOOK_TARGET']\n"
        "mode = os.environ['DPONE_HOOK_MODE']\n"
        "def controlled_import(name, *args, **kwargs):\n"
        "    if name == target and mode == 'block':\n"
        "        raise ImportError('blocked by startup hook')\n"
        "    if name == target and mode == 'fake':\n"
        "        return types.ModuleType(name)\n"
        "    return real_import(name, *args, **kwargs)\n"
        "builtins.__import__ = controlled_import\n",
        encoding="utf-8",
    )
    environment = dict(os.environ)
    environment.update(
        {
            "DPONE_HOOK_MODE": mode,
            "DPONE_HOOK_TARGET": target,
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": str(pythonpath),
        }
    )
    script = (
        "import sys; "
        "target, mode = sys.argv[1:]; "
        "direct_succeeded = True; "
        "\ntry: __import__(target)\n"
        "except ImportError: direct_succeeded = False\n"
        "from dpone.readiness.python_import_health import probe_python_import; "
        "result = probe_python_import(target); "
        "expected_direct = mode == 'fake'; "
        f"raise SystemExit(0 if direct_succeeded is expected_direct and result.reason_code == {_UNSUPPORTED!r} else 1)"
    )

    completed = _run_outer_probe(
        clean_doctor_probe_python,
        script,
        target,
        mode,
        environment=environment,
    )

    assert completed.returncode == 0, completed.stderr


def test_non_idempotent_sitecustomize_is_startup_unsupported(
    tmp_path: Path,
) -> None:
    target = "_dpone_guarded_site_target"
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    (runtime / f"{target}.py").write_text("VALUE = 'present'\n", encoding="utf-8")
    python = _probe_venv_python(
        tmp_path,
        "import builtins, os\n"
        "if os.environ.get('DPONE_SITE_ALREADY_APPLIED') != '1':\n"
        "    os.environ['DPONE_SITE_ALREADY_APPLIED'] = '1'\n"
        "    real_import = builtins.__import__\n"
        "    def guarded_import(name, *args, **kwargs):\n"
        f"        if name == {target!r}:\n"
        "            raise ImportError('blocked once')\n"
        "        return real_import(name, *args, **kwargs)\n"
        "    builtins.__import__ = guarded_import\n",
    )
    script = (
        "import sys; "
        "sys.path.insert(0, sys.argv[1]); "
        "direct_failed = False; "
        f"\ntry: __import__({target!r})\n"
        "except ImportError: direct_failed = True\n"
        "from dpone.readiness.python_import_health import probe_python_import; "
        f"result = probe_python_import({target!r}); "
        f"raise SystemExit(0 if direct_failed and result.reason_code == {_UNSUPPORTED!r} else 1)"
    )

    completed = _run_outer_probe(python, script, str(runtime), cwd=runtime)

    assert completed.returncode == 0, completed.stderr


def test_failed_and_removed_sitecustomize_is_startup_unsupported(
    tmp_path: Path,
) -> None:
    python = _probe_venv_python(
        tmp_path / "failed-customizer",
        "import os\n"
        "count = int(os.environ.get('DPONE_FAILED_SITE_COUNT', '0')) + 1\n"
        "os.environ['DPONE_FAILED_SITE_COUNT'] = str(count)\n"
        "if count == 1: raise ImportError('startup failure', name='sitecustomize')\n",
    )
    target = "_dpone_failed_customizer_target"
    (tmp_path / f"{target}.py").write_text(
        "import os\nif os.environ.get('DPONE_FAILED_SITE_COUNT') == '1': raise ImportError('parent hook ran once')\n",
        encoding="utf-8",
    )
    environment = dict(os.environ)
    environment["DPONE_FAILED_SITE_COUNT"] = "0"
    script = (
        "import os, sys\n"
        "sys.path.insert(0, sys.argv[1])\n"
        "direct_failed = False\n"
        f"try: __import__({target!r})\n"
        "except ImportError: direct_failed = True\n"
        "from dpone.readiness.python_import_health import probe_python_import\n"
        f"result = probe_python_import({target!r})\n"
        f"os._exit(0 if direct_failed and result.reason_code == {_UNSUPPORTED!r} else 1)\n"
    )

    completed = _run_outer_probe(
        python,
        script,
        str(tmp_path),
        environment=environment,
    )

    assert completed.returncode == 0, completed.stderr


def test_failed_and_removed_usercustomize_is_startup_unsupported(
    tmp_path: Path,
    clean_doctor_probe_python: Path,
) -> None:
    base_executable = Path(
        subprocess.run(
            (str(clean_doctor_probe_python), "-c", "import sys; print(sys._base_executable)"),
            env=_outer_probe_environment(),
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout.strip()
    )
    user_base = tmp_path / "user-base"
    environment = dict(os.environ)
    environment["PYTHONUSERBASE"] = str(user_base)
    discovery = subprocess.run(
        (
            str(base_executable),
            "-c",
            "import site; print(site.ENABLE_USER_SITE); print(site.getusersitepackages())",
        ),
        check=True,
        env=_outer_probe_environment(environment),
        capture_output=True,
        text=True,
        timeout=30,
    ).stdout.splitlines()
    if len(discovery) != 2 or discovery[0] != "True":
        pytest.skip("base interpreter does not enable a custom user site")
    user_site = Path(discovery[1])
    user_site.mkdir(parents=True)
    (user_site / "usercustomize.py").write_text(
        "import os\n"
        "count = int(os.environ.get('DPONE_FAILED_USER_COUNT', '0')) + 1\n"
        "os.environ['DPONE_FAILED_USER_COUNT'] = str(count)\n"
        "if count == 1: raise ImportError('startup failure', name='usercustomize')\n",
        encoding="utf-8",
    )
    target = "_dpone_failed_user_customizer_target"
    (tmp_path / f"{target}.py").write_text(
        "import os\n"
        "if os.environ.get('DPONE_FAILED_USER_COUNT') == '1': "
        "raise ImportError('parent user hook ran once')\n",
        encoding="utf-8",
    )
    environment["DPONE_FAILED_USER_COUNT"] = "0"
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
        "import site\n"
        "site.ENABLE_USER_SITE = False\n"
        "from dpone.readiness.python_import_health import probe_python_import\n"
        f"result = probe_python_import({target!r})\n"
        f"os._exit(0 if direct_failed and result.reason_code == {_UNSUPPORTED!r} else 1)\n"
    )

    completed = _run_outer_probe(
        base_executable,
        script,
        *runtime_paths,
        environment=environment,
    )

    assert completed.returncode == 0, completed.stderr


def test_late_pythonpath_early_exit_is_not_executed_by_either_child(
    tmp_path: Path,
    clean_doctor_probe_python: Path,
) -> None:
    injection = tmp_path / "late-pythonpath"
    injection.mkdir()
    (injection / "sitecustomize.py").write_text("import os\nos._exit(0)\n", encoding="utf-8")
    script = (
        "import os, sys; "
        "os.environ['PYTHONPATH'] = sys.argv[1]; "
        "from dpone.readiness.python_import_health import probe_python_import; "
        "result = probe_python_import('_dpone_missing_after_early_exit'); "
        "raise SystemExit(0 if not result.passed and "
        "result.reason_code == 'python_import_not_installed' else 1)"
    )

    completed = _run_outer_probe(clean_doctor_probe_python, script, str(injection))

    assert completed.returncode == 0, completed.stderr


def test_late_pythonpath_hook_cannot_complete_a_disjunctive_dual_pass(
    tmp_path: Path,
    clean_doctor_probe_python: Path,
) -> None:
    injection = tmp_path / "late-disjunctive-hook"
    injection.mkdir()
    (injection / "sitecustomize.py").write_text(
        "import os\nos.environ['DPONE_LATE_HOOK_ACTIVE'] = '1'\n",
        encoding="utf-8",
    )
    target = "_dpone_late_hook_disjunction_target"
    (tmp_path / f"{target}.py").write_text(
        "import os, sys\n"
        "if not (sys.flags.no_site or os.environ.get('DPONE_LATE_HOOK_ACTIVE') == '1'): "
        "raise ImportError('neither proof-only branch is active')\n",
        encoding="utf-8",
    )
    script = (
        "import os, sys\n"
        "sys.path.insert(0, sys.argv[1])\n"
        "os.environ['PYTHONPATH'] = sys.argv[2]\n"
        "direct_failed = False\n"
        f"try: __import__({target!r})\n"
        "except ImportError: direct_failed = True\n"
        "from dpone.readiness.python_import_health import probe_python_import\n"
        f"result = probe_python_import({target!r})\n"
        f"os._exit(0 if direct_failed and result.reason_code == {_UNSUPPORTED!r} else 1)\n"
    )

    completed = _run_outer_probe(
        clean_doctor_probe_python,
        script,
        str(tmp_path),
        str(injection),
    )

    assert completed.returncode == 0, completed.stderr


def test_effective_child_does_not_introduce_a_cwd_sitecustomize_hook(
    tmp_path: Path,
    clean_doctor_probe_python: Path,
) -> None:
    malicious_cwd = tmp_path / "cwd"
    malicious_cwd.mkdir()
    (malicious_cwd / "sitecustomize.py").write_text("import os\nos._exit(0)\n", encoding="utf-8")
    script = (
        "import os, sys; "
        "os.chdir(sys.argv[1]); "
        "sys.path[:] = [entry for entry in sys.path if entry not in {'', sys.argv[1]}]; "
        "from dpone.readiness.python_import_health import probe_python_import; "
        "result = probe_python_import('_dpone_missing_from_console_path'); "
        "raise SystemExit(0 if result.reason_code == 'python_import_not_installed' else 1)"
    )

    completed = _run_outer_probe(clean_doctor_probe_python, script, str(malicious_cwd))

    assert completed.returncode == 0, completed.stderr


def test_no_site_ignores_an_executable_pth_that_never_entered_the_runtime(
    tmp_path: Path,
) -> None:
    python = _probe_clean_venv_python(tmp_path)
    discovered = subprocess.run(
        (str(python), "-c", "import site; print(site.getsitepackages()[0])"),
        env=_outer_probe_environment(),
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    site_packages = Path(discovered.stdout.strip())
    (site_packages / "irrelevant-opaque-hook.pth").write_text(
        "import os; os.environ['DPONE_IRRELEVANT_PTH_RAN'] = '1'\n",
        encoding="utf-8",
    )
    repository_source = Path(__file__).resolve().parents[1] / "src"
    runtime_paths = [
        str(repository_source),
        *(entry for entry in sys.path if entry and Path(entry).is_dir()),
    ]
    script = (
        "import os, sys; "
        "sys.path[:0] = sys.argv[1:]; "
        "from dpone.readiness.python_import_health import probe_python_import; "
        "result = probe_python_import('json'); "
        "raise SystemExit(0 if result.passed and "
        "'DPONE_IRRELEVANT_PTH_RAN' not in os.environ else 1)"
    )

    completed = subprocess.run(
        (str(python), "-S", "-c", script, *runtime_paths),
        env=_outer_probe_environment(),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr
