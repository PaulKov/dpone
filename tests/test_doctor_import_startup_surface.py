from __future__ import annotations

import os
import subprocess
import sys
import types
import venv
import zipfile
from pathlib import Path

import pytest

import dpone.readiness.python_import_startup_surface as startup_surface
from tests.doctor_import_test_support import _outer_probe_environment


def test_path_only_pth_files_are_replayable(tmp_path: Path) -> None:
    (tmp_path / "direct-path.pth").write_text("/opt/runtime/src\n# comment\n\n", encoding="utf-8")

    assert startup_surface._site_files_are_replayable((str(tmp_path),)) is True


def test_path_only_symlinked_pth_matches_cpython_addsitedir(tmp_path: Path) -> None:
    site_directory = tmp_path / "site-packages"
    import_directory = tmp_path / "runtime-src"
    site_directory.mkdir()
    import_directory.mkdir()
    target = tmp_path / "path-target.pth"
    target.write_text(f"{import_directory}\n", encoding="utf-8")
    link = site_directory / "linked-path.pth"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symlink creation is unavailable")

    completed = subprocess.run(
        (
            sys.executable,
            "-S",
            "-c",
            "import site,sys; site.addsitedir(sys.argv[1]); raise SystemExit(0 if sys.argv[2] in sys.path else 1)",
            str(site_directory),
            str(import_directory),
        ),
        env=_outer_probe_environment(),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr
    assert startup_surface._site_files_are_replayable((str(site_directory),)) is True


@pytest.mark.skipif(os.name != "posix" or not hasattr(os, "mkfifo"), reason="POSIX FIFO required")
def test_symlinked_pth_fifo_fails_closed_without_blocking(tmp_path: Path) -> None:
    site_directory = tmp_path / "site-packages"
    site_directory.mkdir()
    fifo = tmp_path / "blocking-target"
    os.mkfifo(fifo)
    link = site_directory / "linked-fifo.pth"
    try:
        link.symlink_to(fifo)
    except OSError:
        pytest.skip("symlink creation is unavailable")

    completed = subprocess.run(
        (
            sys.executable,
            "-c",
            "import sys; from dpone.readiness.python_import_startup_surface "
            "import _site_files_are_replayable; "
            "raise SystemExit(1 if _site_files_are_replayable((sys.argv[1],)) else 0)",
            str(site_directory),
        ),
        env=_outer_probe_environment(),
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )

    assert completed.returncode == 0, completed.stderr


def test_executable_pth_files_are_startup_unsupported(tmp_path: Path) -> None:
    (tmp_path / "opaque-hook.pth").write_text(
        "import os; os.environ['DPONE_OPAQUE_HOOK'] = '1'\n",
        encoding="utf-8",
    )

    assert startup_surface._site_files_are_replayable((str(tmp_path),)) is False


def test_exact_virtualenv_scaffold_is_an_executable_pth_exception(
    tmp_path: Path,
) -> None:
    (tmp_path / "_virtualenv.pth").write_text("import _virtualenv\n", encoding="utf-8")

    assert startup_surface._site_files_are_replayable((str(tmp_path),)) is True

    (tmp_path / "_virtualenv.pth").write_text(
        "import _virtualenv\nimport opaque_hook\n",
        encoding="utf-8",
    )
    assert startup_surface._site_files_are_replayable((str(tmp_path),)) is False


def test_exact_setuptools_distutils_precedence_hook_is_supported(
    tmp_path: Path,
) -> None:
    (tmp_path / startup_surface._SETUPTOOLS_PTH_NAME).write_text(
        startup_surface._SETUPTOOLS_PTH_LINE + "\n",
        encoding="utf-8",
    )

    assert startup_surface._site_files_are_replayable((str(tmp_path),)) is True

    (tmp_path / startup_surface._SETUPTOOLS_PTH_NAME).write_text(
        startup_surface._SETUPTOOLS_PTH_LINE + "\nimport opaque_hook\n",
        encoding="utf-8",
    )
    assert startup_surface._site_files_are_replayable((str(tmp_path),)) is False


def test_setuptools_finder_requires_loaded_module_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    finder = object()
    lookalike = type(
        "DistutilsMetaFinder",
        (),
        {"__module__": "_distutils_hack"},
    )()
    module = types.ModuleType("_distutils_hack")
    module.DISTUTILS_FINDER = finder
    monkeypatch.setitem(startup_surface.sys.modules, "_distutils_hack", module)
    monkeypatch.setattr(startup_surface.sys, "meta_path", [finder])

    assert startup_surface._meta_path_entry_is_supported(finder) is False
    assert startup_surface._meta_path_entry_is_supported(lookalike) is False


@pytest.mark.skipif(sys.version_info[:2] != (3, 11), reason="setuptools is bundled by Python 3.11 ensurepip")
def test_stock_python311_setuptools_venv_preserves_probe_compatibility(
    tmp_path: Path,
) -> None:
    environment = tmp_path / "stock-venv"
    venv.EnvBuilder(with_pip=True, symlinks=os.name != "nt").create(environment)
    python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    site_packages = Path(
        subprocess.run(
            (str(python), "-c", "import site; print(site.getsitepackages()[0])"),
            env=_outer_probe_environment(),
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        ).stdout.strip()
    )
    pth = site_packages / startup_surface._SETUPTOOLS_PTH_NAME
    setuptools_hook_present = pth.is_file()
    if setuptools_hook_present:
        assert pth.read_text(encoding="utf-8").splitlines() == [startup_surface._SETUPTOOLS_PTH_LINE]
    runtime_paths = [
        str(Path(__file__).resolve().parents[1] / "src"),
        *(entry for entry in sys.path if entry and Path(entry).is_dir()),
    ]
    script = (
        "import os, sys\n"
        "mode = sys.argv[1]\n"
        "sys.path[:0] = sys.argv[2:]\n"
        "if mode == 'late-disable': os.environ['SETUPTOOLS_USE_DISTUTILS'] = 'stdlib'\n"
        "if mode == 'late-enable': os.environ.pop('SETUPTOOLS_USE_DISTUTILS', None)\n"
        "from dpone.readiness.python_import_health import probe_python_import\n"
        "result = probe_python_import('json')\n"
        "expected = None if mode in {'default', 'disabled'} else 'python_import_startup_unsupported'\n"
        "if result.reason_code != expected:\n"
        "    print(f'observed={result.reason_code!r}; expected={expected!r}; summary={result.summary!r}')\n"
        "    raise SystemExit(1)\n"
    )
    cases = (
        ("default", None),
        ("disabled", "stdlib"),
        ("late-disable", None),
        ("late-enable", "stdlib"),
    )
    for mode, startup_control in cases:
        child_environment = dict(os.environ)
        if startup_control is None:
            child_environment.pop("SETUPTOOLS_USE_DISTUTILS", None)
        else:
            child_environment["SETUPTOOLS_USE_DISTUTILS"] = startup_control
        completed = subprocess.run(
            (str(python), "-c", script, mode, *runtime_paths),
            env=_outer_probe_environment(child_environment),
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert completed.returncode == 0, (mode, completed.stdout, completed.stderr)


def test_loaded_customization_module_is_not_replayable(monkeypatch) -> None:
    monkeypatch.setitem(startup_surface.sys.modules, "sitecustomize", object())

    assert startup_surface.startup_import_surface_replayable() is False


def test_nonstandard_import_machinery_is_not_replayable(monkeypatch) -> None:
    monkeypatch.setattr(startup_surface.sys, "meta_path", [object()])

    assert startup_surface.startup_import_surface_replayable() is False


def test_pth_scan_stops_before_materializing_an_oversized_directory(monkeypatch) -> None:
    class Entry:
        name = "opaque.pth"

    class Scanner:
        observed = 0

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def __iter__(self):
            return self

        def __next__(self):
            self.observed += 1
            if self.observed > startup_surface._MAX_PTH_FILES + 1:
                raise AssertionError("scanner was consumed beyond the bounded limit")
            return Entry()

    scanner = Scanner()
    monkeypatch.setattr(startup_surface.os, "scandir", lambda _directory: scanner)

    assert startup_surface._site_files_are_replayable(("/site-packages",)) is False
    assert scanner.observed == startup_surface._MAX_PTH_FILES + 1


def test_pth_scan_caps_nonmatching_directory_entries(monkeypatch) -> None:
    class Entry:
        name = "ordinary-module.py"

    class Scanner:
        observed = 0

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def __iter__(self):
            return self

        def __next__(self):
            self.observed += 1
            if self.observed > startup_surface._MAX_SITE_SCAN_ENTRIES + 1:
                raise AssertionError("non-pth scan exceeded its total bound")
            return Entry()

    scanner = Scanner()
    monkeypatch.setattr(startup_surface.os, "scandir", lambda _directory: scanner)

    assert startup_surface._site_files_are_replayable(("/site-packages",)) is False
    assert scanner.observed == startup_surface._MAX_SITE_SCAN_ENTRIES + 1


def test_customizer_discovery_supports_bounded_zip_import_roots(tmp_path: Path) -> None:
    archive = tmp_path / "runtime.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr("sitecustomize.py", "VALUE = True\n")

    assert startup_surface._paths_have_no_customizer((str(archive),), ("sitecustomize",)) is False


def test_usercustomize_discovery_is_symmetric_with_sitecustomize(tmp_path: Path) -> None:
    (tmp_path / "usercustomize.py").write_text("VALUE = True\n", encoding="utf-8")

    assert startup_surface._paths_have_no_customizer((str(tmp_path),), ("usercustomize",)) is False


@pytest.mark.parametrize("target_kind", ("directory", "zip"))
def test_customizer_discovery_follows_standard_symlink_import_roots(
    target_kind: str,
    tmp_path: Path,
) -> None:
    target = tmp_path / "target"
    if target_kind == "directory":
        target.mkdir()
        (target / "ordinary.py").write_text("VALUE = True\n", encoding="utf-8")
    else:
        with zipfile.ZipFile(target, "w") as bundle:
            bundle.writestr("ordinary.py", "VALUE = True\n")
    link = tmp_path / "import-root"
    try:
        link.symlink_to(target, target_is_directory=target_kind == "directory")
    except OSError:
        pytest.skip("symlink creation is unavailable")

    assert startup_surface._paths_have_no_customizer((str(link),), ("sitecustomize",)) is True


def test_customizer_discovery_uses_platform_case_semantics(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "SiteCustomize.py").write_text("VALUE = True\n", encoding="utf-8")
    monkeypatch.setenv("PYTHONCASEOK", "1")

    assert startup_surface._paths_have_no_customizer((str(tmp_path),), ("sitecustomize",)) is False


def test_startup_surface_rejects_custom_interpreter_containers_before_iteration(
    monkeypatch,
) -> None:
    class HostileList(list):
        def __iter__(self):
            raise AssertionError("custom import surface was iterated")

    monkeypatch.setattr(startup_surface.sys, "meta_path", HostileList())

    assert startup_surface.startup_import_surface_replayable() is False


def test_setuptools_finder_state_rejects_custom_meta_path_before_iteration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class HostileList(list[object]):
        def __iter__(self):
            raise AssertionError("custom meta path was iterated")

    monkeypatch.setattr(startup_surface.sys, "meta_path", HostileList())

    assert startup_surface._setuptools_distutils_finder_active() is False


def test_startup_surface_requires_exact_builtin_module_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class HostileModules(dict[str, object]):
        def __iter__(self):
            raise AssertionError("custom module registry was iterated")

    hostile_modules = HostileModules(startup_surface.sys.modules)
    with monkeypatch.context() as context:
        context.setattr(startup_surface.sys, "modules", hostile_modules)
        result = startup_surface.startup_import_surface_replayable()

    assert result is False
