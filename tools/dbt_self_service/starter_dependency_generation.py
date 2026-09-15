"""Explicit pinned deps generation; never used during installation or init.

Source authority belongs to capture_package_source and final caller revalidation.
This internal repository tool reuses private bounded collector helpers alongside
the canonical process supervisor; those imports are not a new public API.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from typing import Any, Protocol

from tools.dbt_self_service.generate_starter_resources import PackageSource

from dpone.adapters.dbt_process_supervisor import DbtProcessSupervisor
from dpone.adapters.dbt_starter_resources import _PACKAGE_FILES, _read_inventory
from dpone.adapters.dbt_subprocess import (
    DistributionDbtToolchainInspector,
    _BoundedCollector,
    _close_process_pipes,
    _finish_collectors,
    _stop_collectors,
)
from dpone.contracts.dbt_toolchain import DBT_SQLSERVER_1_12_CERTIFIED
from dpone.contracts.strict_json import canonical_json_bytes
from dpone.manifest.bounded_yaml import BoundedYamlLimits, load_bounded_yaml
from dpone.manifest.confined_files import read_confined_file_snapshot
from dpone.manifest.project_root import inspect_project_root, verify_project_root
from dpone.ports.dbt_publishing import DbtInstalledToolchain
from dpone.readiness.airflow_authoring_directories import open_confined_parent
from dpone.readiness.airflow_pipeline_source import ConfinedAuthoringFileSystem
from dpone.runtime.dbt_package_readiness import require_current_package_lock

_ERROR = "Starter dependency generation failed; no resource output was applied."
_ORIGIN = "https://github.com/PaulKov/dpone.git"
_MAX_BYTES = BoundedYamlLimits().max_bytes
_TIMEOUT_SECONDS = 300


class DependencyGenerationError(RuntimeError):
    def __init__(self, *, retained_workspace: Path | None = None) -> None:
        super().__init__(_ERROR)
        self.retained_workspace = retained_workspace


class _CleanupUnverified(RuntimeError):
    """A live process or output reader may still own temporary files."""


@dataclass(frozen=True, slots=True)
class DependencyResources:
    packages_yml: bytes
    package_lock_yml: bytes


class DependencyRunner(Protocol):
    def run(self, *, project: Path, environment: Mapping[str, str], timeout_seconds: int) -> None: ...


class PinnedDependencyRunner:
    """Same-interpreter module invocation with bounded output and tree cleanup."""

    def __init__(
        self, *, popen: Callable[..., Any] = subprocess.Popen, supervisor: DbtProcessSupervisor | None = None
    ) -> None:
        self._popen = popen
        self._supervisor = supervisor or DbtProcessSupervisor()

    def run(self, *, project: Path, environment: Mapping[str, str], timeout_seconds: int) -> None:
        process = None
        collectors: list[_BoundedCollector] = []
        try:
            process = self._popen(
                (
                    sys.executable,
                    "-I",
                    "-B",
                    "-m",
                    "dbt.cli.main",
                    "deps",
                    "--project-dir",
                    str(project),
                    "--profiles-dir",
                    str(project / "profiles"),
                    "--target-path",
                    str(project / "target"),
                    "--log-path",
                    str(project / "logs"),
                ),
                cwd=project,
                env=dict(environment),
                shell=False,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                close_fds=True,
                start_new_session=self._supervisor.start_new_session,
            )
            for stream in (process.stdout, process.stderr):
                collector = _BoundedCollector(stream, 64 * 1024)
                collectors.append(collector)
                collector.start()
            code = process.wait(timeout=timeout_seconds)
            if not _finish_collectors(tuple(collectors), timeout_seconds=5) or code != 0:
                raise DependencyGenerationError()
        except BaseException as error:
            if process is not None:
                self._cleanup(process, tuple(collectors))
            if not isinstance(error, Exception):
                raise
            raise DependencyGenerationError() from None

    def _cleanup(self, process: Any, collectors: tuple[_BoundedCollector, ...]) -> None:
        failed = False
        try:
            self._supervisor.terminate(process)
        except BaseException:
            failed = True
        try:
            _stop_collectors(collectors)
            _close_process_pipes(process)
            if not _finish_collectors(collectors, timeout_seconds=5):
                failed = True
        except BaseException:
            failed = True
        if failed:
            raise _CleanupUnverified() from None


def generate_dependencies(
    source: PackageSource,
    *,
    runner: DependencyRunner | None = None,
    inspect_toolchain: Callable[[], DbtInstalledToolchain] | None = None,
) -> DependencyResources:
    """Resolve a captured immutable package; return only verified actual lock bytes.

    Injected runners are a test seam, not proof of actual dependency execution.
    Unsafe process cleanup retains the isolated workspace for explicit inspection.
    """
    workspace = None
    preserve = False
    identity = None
    try:
        expected = DBT_SQLSERVER_1_12_CERTIFIED
        installed = (
            inspect_toolchain()
            if inspect_toolchain
            else DistributionDbtToolchainInspector().inspect(expected.adapter_name)
        )
        if (installed.dbt_core_version, installed.adapter_name, installed.adapter_version) != (
            expected.dbt_core_version,
            expected.adapter_name,
            expected.adapter_version,
        ):
            raise DependencyGenerationError()
        files = dict(source.files)
        if (
            re.fullmatch(r"[0-9a-f]{40}", source.revision) is None
            or set(files) != set(_PACKAGE_FILES)
            or len(source.files) != len(_PACKAGE_FILES)
        ):
            raise DependencyGenerationError()
        for content in files.values():
            if not isinstance(content, bytes) or len(content) > _MAX_BYTES:
                raise DependencyGenerationError()
            content.decode("utf-8")
        dependency = {"git": _ORIGIN, "revision": source.revision, "subdirectory": "packages/dbt-dpone"}
        declaration = {"packages": [dependency]}
        declaration_bytes = canonical_json_bytes(declaration)
        workspace = Path(tempfile.mkdtemp(prefix="dpone-starter-deps-")).resolve()
        identity = inspect_project_root(workspace)
        filesystem = ConfinedAuthoringFileSystem(workspace, root_identity=identity)
        filesystem.create(Path("packages.yml"), declaration_bytes)
        filesystem.create(
            Path("dbt_project.yml"),
            canonical_json_bytes(
                {
                    "name": "dpone_starter_dependencies",
                    "version": "1.0",
                    "config-version": 2,
                    "packages-install-path": "dbt_packages",
                    "model-paths": ["models"],
                }
            ),
        )
        (workspace / "profiles").mkdir(mode=0o700)
        (runner or PinnedDependencyRunner()).run(
            project=workspace, environment=_environment(workspace), timeout_seconds=_TIMEOUT_SECONDS
        )
        if (
            read_confined_file_snapshot(workspace, "packages.yml", max_bytes=_MAX_BYTES, root_identity=identity).content
            != declaration_bytes
        ):
            raise DependencyGenerationError()
        raw = read_confined_file_snapshot(
            workspace, "package-lock.yml", max_bytes=_MAX_BYTES, root_identity=identity
        ).content
        lock = load_bounded_yaml(raw)
        if (
            not isinstance(lock, dict)
            or set(lock) != {"packages", "sha1_hash"}
            or lock["packages"] != [{**dependency, "name": "dbt_dpone"}]
        ):
            raise DependencyGenerationError()
        if require_current_package_lock(declaration, lock, package_environment={}) != ("dbt_dpone",):
            raise DependencyGenerationError()
        with open_confined_parent(workspace, ("dbt_packages", "probe"), create=False, root_identity=identity) as parent:
            if parent.descriptor is None:
                raise DependencyGenerationError()
            with os.scandir(parent.descriptor) as entries:
                if [entry.name for entry in islice(entries, 2)] != ["dbt_dpone"]:
                    raise DependencyGenerationError()
        resolved = workspace / "dbt_packages/dbt_dpone"
        _read_inventory(resolved, _PACKAGE_FILES)
        for name, content in source.files:
            observed = read_confined_file_snapshot(
                workspace, "dbt_packages/dbt_dpone/" + name, max_bytes=_MAX_BYTES, root_identity=identity
            )
            if observed.content != content:
                raise DependencyGenerationError()
        _read_inventory(resolved, _PACKAGE_FILES)
        return DependencyResources(declaration_bytes, raw)
    except BaseException as error:
        preserve = isinstance(error, _CleanupUnverified)
        if not isinstance(error, Exception):
            raise
        raise DependencyGenerationError(retained_workspace=workspace if preserve else None) from None
    finally:
        if workspace is not None and not preserve:
            try:
                if identity is None:
                    raise OSError()
                verify_project_root(identity)
                shutil.rmtree(workspace)
            except OSError:
                raise DependencyGenerationError(retained_workspace=workspace) from None


def _environment(project: Path) -> dict[str, str]:
    return {
        "PATH": os.defpath,
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        "DBT_DOWNLOADS_DIR": str(project / "downloads"),
        "DBT_PROFILES_DIR": str(project / "profiles"),
        "DBT_LOG_PATH": str(project / "logs"),
        "PYTHONDONTWRITEBYTECODE": "1",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_COUNT": "2",
        "GIT_CONFIG_KEY_0": "credential.helper",
        "GIT_CONFIG_VALUE_0": "",
        "GIT_CONFIG_KEY_1": "core.hooksPath",
        "GIT_CONFIG_VALUE_1": os.devnull,
    }
