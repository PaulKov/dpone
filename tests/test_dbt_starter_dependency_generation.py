"""Synthetic dependency results are not real package-generation evidence."""

import os
import subprocess
import sys

import pytest
import yaml
from tools.dbt_self_service.generate_starter_resources import PackageSource

from dpone.adapters.dbt_starter_resources import _PACKAGE_FILES
from dpone.ports.dbt_publishing import DbtInstalledToolchain
from dpone.runtime.dbt_package_readiness import dbt_package_declaration_sha1


def source():
    files = {name: ("synthetic " + name).encode() for name in _PACKAGE_FILES}
    files["dbt_project.yml"] = b"name: dbt_dpone\nversion: '1.0'\n"
    return PackageSource("a" * 40, tuple(sorted(files.items())))


def pinned():
    return DbtInstalledToolchain(dbt_core_version="1.12.3", adapter_name="sqlserver", adapter_version="1.11.1")


class SyntheticRunner:
    def __init__(self, change=None):
        self.change = change
        self.project = None
        self.lock = None

    def run(self, *, project, environment, timeout_seconds):
        self.project = project
        assert not (project / "package-lock.yml").exists()
        assert timeout_seconds == 300
        assert "HOME" not in environment and "PRIVATE_SENTINEL" not in str(environment)
        declaration = yaml.safe_load((project / "packages.yml").read_bytes())
        dependency = declaration["packages"][0]
        assert dependency["revision"] == source().revision
        lock = {
            "packages": [{**dependency, "name": "dbt_dpone"}],
            "sha1_hash": dbt_package_declaration_sha1(declaration, package_environment={}),
        }
        if self.change == "stale":
            lock["sha1_hash"] = "0" * 40
        elif self.change == "revision":
            lock["packages"][0]["revision"] = "b" * 40
        self.lock = b"# synthetic lock fixture\n" + yaml.safe_dump(lock).encode()
        (project / "package-lock.yml").write_bytes(self.lock)
        for name, content in source().files:
            target = project / "dbt_packages/dbt_dpone" / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
        root = project / "dbt_packages/dbt_dpone"
        if self.change == "bytes":
            (root / "INSTALL.md").write_bytes(b"foreign")
        elif self.change == "extra":
            (root / "extra.sql").write_bytes(b"foreign")
        elif self.change == "error":
            raise RuntimeError("PRIVATE_SENTINEL")
        elif self.change == "lock_symlink":
            (project / "package-lock.yml").unlink()
            (project / "package-lock.yml").symlink_to(project / "packages.yml")
        elif self.change == "file_symlink":
            (root / "INSTALL.md").unlink()
            (root / "INSTALL.md").symlink_to(root / "dbt_project.yml")
        elif self.change == "extra_package":
            (project / "dbt_packages/unknown").mkdir()
        elif self.change == "declaration":
            (project / "packages.yml").write_text("packages: []\n")


def test_verified_lock_bytes_are_returned_unchanged_and_workspace_cleaned():
    from tools.dbt_self_service.starter_dependency_generation import generate_dependencies

    runner = SyntheticRunner()
    result = generate_dependencies(source(), runner=runner, inspect_toolchain=pinned)
    assert result.package_lock_yml == runner.lock
    assert not runner.project.exists()


@pytest.mark.parametrize(
    "change",
    ["stale", "revision", "bytes", "extra", "error", "lock_symlink", "file_symlink", "extra_package", "declaration"],
)
def test_invalid_dependency_outputs_fail_without_retaining_private_messages(change):
    from tools.dbt_self_service.starter_dependency_generation import DependencyGenerationError, generate_dependencies

    runner = SyntheticRunner(change)
    with pytest.raises(DependencyGenerationError) as caught:
        generate_dependencies(source(), runner=runner, inspect_toolchain=pinned)
    assert "PRIVATE_SENTINEL" not in str(caught.value)
    assert not runner.project.exists()


def test_wrong_toolchain_rejects_before_runner():
    from tools.dbt_self_service.starter_dependency_generation import DependencyGenerationError, generate_dependencies

    runner = SyntheticRunner()

    def wrong():
        return DbtInstalledToolchain(dbt_core_version="0.0", adapter_name="sqlserver", adapter_version="1.11.1")

    with pytest.raises(DependencyGenerationError):
        generate_dependencies(source(), runner=runner, inspect_toolchain=wrong)
    assert runner.project is None


def test_ambient_credentials_are_not_forwarded(monkeypatch):
    from tools.dbt_self_service.starter_dependency_generation import generate_dependencies

    monkeypatch.setenv("DBT_PASSWORD", "PRIVATE_SENTINEL")
    monkeypatch.setenv("GIT_CONFIG_VALUE_99", "PRIVATE_SENTINEL")
    generate_dependencies(source(), runner=SyntheticRunner(), inspect_toolchain=pinned)


def test_native_process_output_is_bounded_and_argv_is_current_interpreter(tmp_path, monkeypatch):
    import tools.dbt_self_service.starter_dependency_generation as module

    observed = []
    collector_type = module._BoundedCollector

    def collector(*args):
        result = collector_type(*args)
        observed.append(result)
        return result

    def popen(argv, **kwargs):
        assert argv[:6] == (sys.executable, "-I", "-B", "-m", "dbt.cli.main", "deps")
        assert kwargs["shell"] is False and kwargs["stdin"] == subprocess.DEVNULL
        return subprocess.Popen(
            [sys.executable, "-c", "import os; os.write(1,b'x'*200000); os.write(2,b'y'*200000)"], **kwargs
        )

    monkeypatch.setattr(module, "_BoundedCollector", collector)
    module.PinnedDependencyRunner(popen=popen).run(
        project=tmp_path, environment={"PATH": os.defpath}, timeout_seconds=5
    )
    assert len(observed) == 2
    assert all(item.total_bytes == 200000 and len(item.content) == 65536 for item in observed)


def test_native_timeout_reaps_process(tmp_path):
    from tools.dbt_self_service.starter_dependency_generation import DependencyGenerationError, PinnedDependencyRunner

    processes = []

    def popen(argv, **kwargs):
        process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], **kwargs)
        processes.append(process)
        return process

    with pytest.raises(DependencyGenerationError):
        PinnedDependencyRunner(popen=popen).run(project=tmp_path, environment={"PATH": os.defpath}, timeout_seconds=0.1)
    assert processes[0].poll() is not None


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group probe")
def test_timeout_stops_and_reaps_live_descendant(tmp_path):
    from tools.dbt_self_service.starter_dependency_generation import DependencyGenerationError, PinnedDependencyRunner

    pid_path = tmp_path / "child.pid"
    script = """
import subprocess, sys, signal, time
from pathlib import Path
child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
def stop(*args):
    child.wait(timeout=3)
    sys.exit(0)
signal.signal(signal.SIGTERM, stop)
Path(sys.argv[1]).write_text(str(child.pid))
time.sleep(30)
"""
    processes = []

    def popen(argv, **kwargs):
        process = subprocess.Popen([sys.executable, "-c", script, str(pid_path)], **kwargs)
        processes.append(process)
        return process

    with pytest.raises(DependencyGenerationError):
        PinnedDependencyRunner(popen=popen).run(project=tmp_path, environment={"PATH": os.defpath}, timeout_seconds=1)
    assert processes[0].poll() is not None
    with pytest.raises(ProcessLookupError):
        os.kill(int(pid_path.read_text()), 0)


@pytest.mark.parametrize("method", ["start", "finish"])
def test_collector_failure_still_reaps_process(tmp_path, monkeypatch, method):
    import tools.dbt_self_service.starter_dependency_generation as module

    original = module._BoundedCollector
    processes = []
    fired = False

    def collector(*args):
        nonlocal fired
        result = original(*args)
        action = getattr(result, method)

        def fail_once(*values):
            nonlocal fired
            if not fired:
                fired = True
                raise OSError("PRIVATE_SENTINEL")
            return action(*values)

        setattr(result, method, fail_once)
        return result

    def popen(argv, **kwargs):
        process = subprocess.Popen([sys.executable, "-c", "print('synthetic')"], **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(module, "_BoundedCollector", collector)
    with pytest.raises(module.DependencyGenerationError) as caught:
        module.PinnedDependencyRunner(popen=popen).run(
            project=tmp_path, environment={"PATH": os.defpath}, timeout_seconds=5
        )
    assert "PRIVATE_SENTINEL" not in str(caught.value)
    assert processes[0].poll() is not None


def test_unverified_process_cleanup_preserves_workspace():
    import shutil

    from tools.dbt_self_service.starter_dependency_generation import (
        DependencyGenerationError,
        PinnedDependencyRunner,
        generate_dependencies,
    )

    from dpone.adapters.dbt_process_supervisor import DbtProcessSupervisor, ProcessSupervisionError

    processes = []

    class UnverifiedSupervisor(DbtProcessSupervisor):
        def terminate(self, process):
            raise ProcessSupervisionError("synthetic")

    class ShortRunner(PinnedDependencyRunner):
        def run(self, **kwargs):
            kwargs["timeout_seconds"] = 0.1
            return super().run(**kwargs)

    def popen(argv, **kwargs):
        process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], **kwargs)
        processes.append(process)
        return process

    retained = None
    try:
        with pytest.raises(DependencyGenerationError) as caught:
            generate_dependencies(
                source(), runner=ShortRunner(popen=popen, supervisor=UnverifiedSupervisor()), inspect_toolchain=pinned
            )
        retained = caught.value.retained_workspace
        assert retained is not None and retained.is_dir()
    finally:
        for process in processes:
            DbtProcessSupervisor().terminate(process)
        if retained is not None:
            shutil.rmtree(retained)


def test_toolchain_rejection_never_creates_temporary_workspace(monkeypatch):
    import tools.dbt_self_service.starter_dependency_generation as module

    def forbidden(*args, **kwargs):
        pytest.fail("temporary workspace must not be created")

    def wrong():
        return DbtInstalledToolchain(dbt_core_version="0.0", adapter_name="sqlserver", adapter_version="1.11.1")

    monkeypatch.setattr(module.tempfile, "mkdtemp", forbidden)
    with pytest.raises(module.DependencyGenerationError):
        module.generate_dependencies(source(), runner=SyntheticRunner(), inspect_toolchain=wrong)


def test_keyboard_interrupt_stops_child_before_propagating(tmp_path):
    from tools.dbt_self_service.starter_dependency_generation import PinnedDependencyRunner

    processes = []

    def popen(argv, **kwargs):
        process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], **kwargs)
        wait = process.wait
        interrupted = False

        def interrupt_once(*args, **options):
            nonlocal interrupted
            if not interrupted:
                interrupted = True
                raise KeyboardInterrupt
            return wait(*args, **options)

        process.wait = interrupt_once
        processes.append(process)
        return process

    with pytest.raises(KeyboardInterrupt):
        PinnedDependencyRunner(popen=popen).run(project=tmp_path, environment={"PATH": os.defpath}, timeout_seconds=5)
    assert processes[0].poll() is not None
