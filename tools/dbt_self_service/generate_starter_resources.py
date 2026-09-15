"""Validate immutable source inputs for explicit starter resource preparation.

This tooling never runs during installation or init. Source capture is read-only
and does not fetch Git objects. A complete source snapshot proves resource bytes,
not macro authority, physical qualification or a dbt-produced dependency lock.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from dpone.adapters.dbt_starter_resources import _PACKAGE_FILES, _STARTER_FILES, _read_inventory
from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.manifest.bounded_yaml import BoundedYamlError, load_bounded_yaml
from dpone.manifest.confined_files import read_confined_file_snapshot
from dpone.manifest.project_root import inspect_project_root, verify_project_root
from dpone.runtime.dbt_package_readiness import require_current_package_lock

_PACKAGE_PREFIX = "packages/dbt-dpone"
_ORIGIN = "https://github.com/PaulKov/dpone.git"
_INVALID_SOURCE = "Starter package source is incomplete, unsafe or differs from the immutable revision."


@dataclass(frozen=True, slots=True)
class PackageSource:
    """Detached immutable bytes from exactly fourteen verified source files."""

    revision: str
    files: tuple[tuple[str, bytes], ...]


def capture_package_source(repo: Path, revision: str) -> PackageSource:
    """Match an existing Git commit, its closed subtree and current source bytes.

    Both ordinary Git blob modes are supported. Unrelated checkout changes do
    not matter. A package change, including an untracked/ignored extra directory,
    invalidates this snapshot. Every file is read through the existing confined
    stable-descriptor reader; content is compared without newline conversion.
    """
    if re.fullmatch(r"[0-9a-f]{40}", revision) is None:
        raise ValueError(_INVALID_SOURCE)
    try:
        root = inspect_project_root(repo)
        actual = _git(repo, "rev-parse", "--verify", revision + "^{commit}").decode().strip()
        if actual != revision:
            raise ValueError(_INVALID_SOURCE)
        _git(repo, "merge-base", "--is-ancestor", revision, "HEAD")
        records = _git(repo, "ls-tree", "-rz", revision, "--", _PACKAGE_PREFIX).split(b"\0")
        blobs: dict[str, str] = {}
        for record in filter(None, records):
            metadata, raw_path = record.split(b"\t", 1)
            mode, kind, oid = metadata.decode("ascii").split()
            path = raw_path.decode("utf-8")
            if mode not in {"100644", "100755"} or kind != "blob" or not path.startswith(_PACKAGE_PREFIX + "/"):
                raise ValueError(_INVALID_SOURCE)
            name = path[len(_PACKAGE_PREFIX) + 1 :]
            if name in blobs:
                raise ValueError(_INVALID_SOURCE)
            blobs[name] = oid
        if set(blobs) != set(_PACKAGE_FILES):
            raise ValueError(_INVALID_SOURCE)
        package_root = repo / _PACKAGE_PREFIX
        # Reuse the installed inventory contract, including empty extra folders.
        _read_inventory(package_root, _PACKAGE_FILES)
        files: list[tuple[str, bytes]] = []
        for name, oid in sorted(blobs.items()):
            expected = _git(repo, "cat-file", "blob", oid)
            expected.decode("utf-8")
            observed = read_confined_file_snapshot(
                repo, _PACKAGE_PREFIX + "/" + name, max_bytes=len(expected), root_identity=root
            )
            if observed.content != expected:
                raise ValueError(_INVALID_SOURCE)
            files.append((name, expected))
        project = load_bounded_yaml(dict(files)["dbt_project.yml"])
        if not isinstance(project, dict) or project.get("name") != "dbt_dpone":
            raise ValueError(_INVALID_SOURCE)
        if any(key in project for key in ("packages", "projects")):
            raise ValueError(_INVALID_SOURCE)
        # Recheck membership after reads; mutation application must revalidate
        # the full snapshot again under its own authoring transaction lock.
        _read_inventory(package_root, _PACKAGE_FILES)
        verify_project_root(root)
        return PackageSource(revision=revision, files=tuple(files))
    except (OSError, UnicodeError, ValueError, BoundedYamlError, subprocess.SubprocessError):
        raise ValueError(_INVALID_SOURCE) from None


def check_starter_resources(repo: Path, revision: str) -> None:
    """Verify source, mirror, templates and dependency metadata without writes.

    No dbt command or dependency download occurs. Lock validation establishes
    consistency, not provenance of an externally supplied lock. Only explicit
    generation with the pinned toolchain can supply real deps execution evidence.
    """
    source = capture_package_source(repo, revision)
    try:
        identity = inspect_project_root(repo)
        assets = "src/dpone/_assets/"
        mirror = assets + "dbt_dpone/"
        starter = assets + "dbt_starter/v4/"
        _read_inventory(repo / mirror, _PACKAGE_FILES)
        _read_inventory(repo / starter, tuple(name for name, _ in _STARTER_FILES))
        for name, content in source.files:
            observed = read_confined_file_snapshot(repo, mirror + name, max_bytes=len(content), root_identity=identity)
            if observed.content != content:
                raise ValueError(_INVALID_SOURCE)
        metadata = {}
        for name in ("packages.yml", "package-lock.yml"):
            snapshot = read_confined_file_snapshot(repo, starter + name, max_bytes=1024 * 1024, root_identity=identity)
            metadata[name] = load_bounded_yaml(snapshot.content)
        dependency = {"git": _ORIGIN, "revision": revision, "subdirectory": _PACKAGE_PREFIX}
        declaration = metadata["packages.yml"]
        lock = metadata["package-lock.yml"]
        if declaration != {"packages": [dependency]} or not isinstance(lock, dict):
            raise ValueError(_INVALID_SOURCE)
        if set(lock) != {"packages", "sha1_hash"} or lock["packages"] != [{**dependency, "name": "dbt_dpone"}]:
            raise ValueError(_INVALID_SOURCE)
        if require_current_package_lock(declaration, lock, package_environment={}) != ("dbt_dpone",):
            raise ValueError(_INVALID_SOURCE)
        verify_project_root(identity)
    except (OSError, UnicodeError, ValueError, BoundedYamlError, DbtPublishingError):
        raise ValueError(_INVALID_SOURCE) from None


def _git(repo: Path, *arguments: str) -> bytes:
    """Run only local object queries, without ambient Git configuration inputs."""
    executable = shutil.which("git")
    if executable is None:
        raise ValueError(_INVALID_SOURCE)
    environment = {
        "PATH": os.defpath,
        "LC_ALL": "C",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_NO_REPLACE_OBJECTS": "1",
        "GIT_NO_LAZY_FETCH": "1",
    }
    completed = subprocess.run(
        [executable, "--no-optional-locks", "-c", "core.fsmonitor=false", "-C", str(repo), *arguments],
        env=environment,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=True,
        timeout=30,
    )
    return completed.stdout
