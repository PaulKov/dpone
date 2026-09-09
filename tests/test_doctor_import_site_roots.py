from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

import dpone.readiness.python_import_startup_surface as startup_surface
from tests.doctor_import_test_support import (
    _outer_probe_environment,
    _probe_clean_venv_python,
    _run_outer_probe,
)

_UNSUPPORTED = "python_import_startup_unsupported"


def test_automatic_site_roots_do_not_trust_mutable_site_prefixes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = startup_surface.site.getsitepackages(prefixes=(sys.prefix, sys.exec_prefix))
    monkeypatch.setattr(startup_surface.site, "PREFIXES", [])

    observed = startup_surface._automatic_site_directories()

    assert set(expected) <= set(observed)


def test_cached_user_site_is_scanned_after_enable_flag_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if bool(sys.flags.no_user_site):
        pytest.skip("parent explicitly disables user site")
    user_site = str(tmp_path / "user-site")
    monkeypatch.setattr(startup_surface.site, "ENABLE_USER_SITE", False)
    monkeypatch.setattr(startup_surface.site, "USER_SITE", user_site)

    assert user_site in startup_surface._automatic_site_directories()


def test_executed_venv_pth_cannot_hide_by_mutating_site_prefixes(
    tmp_path: Path,
) -> None:
    python = _probe_clean_venv_python(tmp_path / "prefix-venv")
    site_packages = Path(
        subprocess.run(
            (str(python), "-c", "import site; print(site.getsitepackages()[0])"),
            env=_outer_probe_environment(),
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout.strip()
    )
    (site_packages / "opaque-prefix-hook.pth").write_text(
        "import builtins; builtins.DPONE_PREFIX_PTH_RAN = True\n",
        encoding="utf-8",
    )
    target = "_dpone_prefix_pth_target"
    (tmp_path / f"{target}.py").write_text(
        "import builtins\nif getattr(builtins, 'DPONE_PREFIX_PTH_RAN', False): raise ImportError('startup pth ran')\n",
        encoding="utf-8",
    )
    script = (
        "import os, site, sys\n"
        "site.PREFIXES = []\n"
        "sys.path.insert(0, sys.argv[1])\n"
        "direct_failed = False\n"
        f"try: __import__({target!r})\n"
        "except ImportError: direct_failed = True\n"
        "from dpone.readiness.python_import_health import probe_python_import\n"
        f"result = probe_python_import({target!r})\n"
        f"os._exit(0 if direct_failed and result.reason_code == {_UNSUPPORTED!r} else 1)\n"
    )

    completed = _run_outer_probe(python, script, str(tmp_path))

    assert completed.returncode == 0, completed.stderr


def test_executed_user_pth_cannot_hide_by_mutating_enable_flag(
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
    (user_site / "opaque-user-hook.pth").write_text(
        "import builtins; builtins.DPONE_USER_PTH_RAN = True\n",
        encoding="utf-8",
    )
    target = "_dpone_user_pth_target"
    (tmp_path / f"{target}.py").write_text(
        "import builtins\n"
        "if getattr(builtins, 'DPONE_USER_PTH_RAN', False): "
        "raise ImportError('startup user pth ran')\n",
        encoding="utf-8",
    )
    environment["DPONE_USER_PTH_TEST"] = "1"
    runtime_paths = [
        str(tmp_path),
        str(Path(__file__).resolve().parents[1] / "src"),
        *(entry for entry in sys.path if entry and Path(entry).is_dir()),
    ]
    script = (
        "import os, site, sys\n"
        "sys.path[:0] = sys.argv[1:]\n"
        "site.ENABLE_USER_SITE = False\n"
        "direct_failed = False\n"
        f"try: __import__({target!r})\n"
        "except ImportError: direct_failed = True\n"
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
