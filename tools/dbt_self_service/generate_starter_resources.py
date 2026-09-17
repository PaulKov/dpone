"""Validate immutable source inputs for explicit starter resource preparation.

This tooling never runs during installation or init. Source capture is read-only
and does not fetch Git objects. A complete source snapshot proves resource bytes,
not macro authority, physical qualification or a dbt-produced dependency lock.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import stat
import subprocess
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, NoReturn

from dpone.adapters.dbt_starter_resources import _PACKAGE_FILES, _STARTER_FILES, _read_inventory
from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.manifest.bounded_yaml import BoundedYamlError, load_bounded_yaml
from dpone.manifest.confined_files import read_confined_file_snapshot
from dpone.manifest.project_root import inspect_project_root, verify_project_root
from dpone.runtime.dbt_package_readiness import require_current_package_lock

if TYPE_CHECKING:
    from tools.dbt_self_service.starter_dependency_generation import DependencyResources
    from tools.dbt_self_service.starter_resource_transaction import ResourceWriteReceipt

_PACKAGE_PREFIX = "packages/dbt-dpone"
_ORIGIN = "https://github.com/PaulKov/dpone.git"
_INVALID_SOURCE = "Starter package source is incomplete, unsafe or differs from the immutable revision."


@dataclass(frozen=True, slots=True)
class PackageSource:
    """Detached immutable bytes from exactly fifteen verified source files."""

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
        blobs: dict[str, tuple[str, str]] = {}
        for record in filter(None, records):
            metadata, raw_path = record.split(b"\t", 1)
            mode, kind, oid = metadata.decode("ascii").split()
            path = raw_path.decode("utf-8")
            if mode not in {"100644", "100755"} or kind != "blob" or not path.startswith(_PACKAGE_PREFIX + "/"):
                raise ValueError(_INVALID_SOURCE)
            name = path[len(_PACKAGE_PREFIX) + 1 :]
            if name in blobs:
                raise ValueError(_INVALID_SOURCE)
            blobs[name] = (mode, oid)
        if set(blobs) != set(_PACKAGE_FILES):
            raise ValueError(_INVALID_SOURCE)
        indexed: dict[str, tuple[str, str]] = {}
        for record in filter(None, _git(repo, "ls-files", "--stage", "-z", "--", _PACKAGE_PREFIX).split(b"\0")):
            metadata, raw_path = record.split(b"\t", 1)
            mode, oid, stage = metadata.decode("ascii").split()
            path = raw_path.decode("utf-8")
            if stage != "0" or not path.startswith(_PACKAGE_PREFIX + "/"):
                raise ValueError(_INVALID_SOURCE)
            name = path[len(_PACKAGE_PREFIX) + 1 :]
            if name in indexed:
                raise ValueError(_INVALID_SOURCE)
            indexed[name] = (mode, oid)
        if indexed != blobs:
            raise ValueError(_INVALID_SOURCE)
        package_root = repo / _PACKAGE_PREFIX
        # Reuse the installed inventory contract, including empty extra folders.
        _read_inventory(package_root, _PACKAGE_FILES)
        files: list[tuple[str, bytes]] = []
        for name, (mode, oid) in sorted(blobs.items()):
            expected = _git(repo, "cat-file", "blob", oid)
            expected.decode("utf-8")
            observed = read_confined_file_snapshot(
                repo, _PACKAGE_PREFIX + "/" + name, max_bytes=len(expected), root_identity=root
            )
            observed_mode = "100755" if observed.identity.mode & stat.S_IXUSR else "100644"
            if observed.content != expected or observed_mode != mode:
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
        [
            executable,
            "--no-optional-locks",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.filemode=true",
            "-C",
            str(repo),
            *arguments,
        ],
        env=environment,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=True,
        timeout=30,
    )
    return completed.stdout


class _ArgumentError(ValueError):
    pass


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise _ArgumentError()


def main(
    argv: list[str] | None = None, *, generate: Callable[[PackageSource], DependencyResources] | None = None
) -> int:
    """Developer CLI only; dependency adapters are composed after argument checks.

    Emit one JSON result, except explicit help. Exit codes: 0 success, 1 failure,
    2 invalid arguments, 3 recovery required. Captured errors are never printed.
    Injection supports synthetic tests; it does not establish deps provenance.
    """
    parser = _Parser(description="Prepare immutable starter resources; never runs during init.", allow_abbrev=False)
    parser.add_argument("--source-repo", required=True)
    parser.add_argument("--revision")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="Verify offline without writes or dependency downloads.")
    mode.add_argument("--recovery-report", action="store_true", help="Inspect recovery obligations without repair.")
    try:
        args = parser.parse_args(argv)
        if args.recovery_report:
            if args.revision is not None:
                raise _ArgumentError()
        elif args.revision is None or re.fullmatch(r"[0-9a-f]{40}", args.revision) is None:
            raise _ArgumentError()
    except _ArgumentError:
        print(json.dumps({"status": "INVALID_ARGUMENTS"}))
        return 2
    except SystemExit as error:
        return int(error.code or 0)

    # Lazy imports belong to this concrete CLI composition boundary, not init.
    from tools.dbt_self_service.starter_dependency_generation import DependencyGenerationError, generate_dependencies
    from tools.dbt_self_service.starter_resource_recovery import recovery_report

    selected = "recovery-report" if args.recovery_report else "check" if args.check else "generate"
    result: dict[str, object] = {"mode": selected}
    identity = None
    try:
        identity = inspect_project_root(Path(args.source_repo))
        if identity is None:
            raise ValueError()
        root = identity.path
        report = recovery_report(root)
        if args.recovery_report or report.pending:
            result.update(asdict(report))
            code = 3 if report.pending else 0
        elif args.check:
            check_starter_resources(root, str(args.revision))
            verify_project_root(identity)
            report = recovery_report(root)
            result.update(asdict(report) if report.pending else {"status": "PASS", "revision": args.revision})
            code = 3 if report.pending else 0
        else:
            receipt = _generate_resources(root, str(args.revision), generate or generate_dependencies)
            result.update(asdict(receipt))
            result["revision"] = args.revision
            code = 3 if receipt.recovery_required or receipt.unpersisted_recovery_paths else 0 if receipt.passed else 1
    except DependencyGenerationError as error:
        retained = error.retained_workspace
        result.update(status="RECOVERY_REQUIRED" if retained else "FAILED")
        if retained is not None:
            result["retained_workspace"] = str(retained)
        code = 3 if retained else 1
    except Exception:
        result["status"] = "FAILED"
        code = 1
    if code in {1, 3} and identity is not None:
        # Failure before writer entry can race with another interrupted writer.
        # Observe, never repair; preserve any owned dependency-workspace receipt.
        try:
            verify_project_root(identity)
            report = recovery_report(identity.path)
            if report.pending:
                result.update(status="RECOVERY_REQUIRED", recovery=asdict(report))
                code = 3
        except Exception:
            result.update(
                status="RECOVERY_REQUIRED",
                recovery={"pending": True, "status": "ROOT_UNAVAILABLE", "discovery_required": True},
            )
            code = 3
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return code


def _generate_resources(
    root: Path, revision: str, generate: Callable[[PackageSource], DependencyResources]
) -> ResourceWriteReceipt:
    """Preflight before deps, then revalidate captured inputs under the writer lock."""
    from tools.dbt_self_service.starter_resource_files import require_snapshot, snapshot
    from tools.dbt_self_service.starter_resource_journal_schema import MAX_RESOURCE_BYTES, RESOURCE_PATHS
    from tools.dbt_self_service.starter_resource_transaction import apply_resource_plan

    from dpone.adapters.project_authoring_lock import project_authoring_lock

    identity = inspect_project_root(root)
    if identity is None:
        raise ValueError()
    source = capture_package_source(root, revision)
    destinations = {path: snapshot(identity, path) for path in RESOURCE_PATHS}
    starter = "src/dpone/_assets/dbt_starter/v4/"
    templates = {
        starter + name: read_confined_file_snapshot(
            root, starter + name, max_bytes=MAX_RESOURCE_BYTES, root_identity=identity
        )
        for name, _ in _STARTER_FILES
        if name not in {"packages.yml", "package-lock.yml"}
    }
    expected = tuple(
        name
        for name, _ in _STARTER_FILES
        if starter + name in templates or destinations.get(starter + name) is not None
    )
    mirror = "src/dpone/_assets/dbt_dpone/"
    inputs = dict(templates)
    for name, content in source.files:
        path = _PACKAGE_PREFIX + "/" + name
        observed = read_confined_file_snapshot(root, path, max_bytes=MAX_RESOURCE_BYTES, root_identity=identity)
        if observed.content != content:
            raise ValueError()
        inputs[path] = observed

    def revalidate_inputs() -> None:
        verify_project_root(identity)
        if capture_package_source(root, revision) != source:
            raise ValueError()
        for path, previous in inputs.items():
            if (
                read_confined_file_snapshot(root, path, max_bytes=MAX_RESOURCE_BYTES, root_identity=identity)
                != previous
            ):
                raise ValueError()

    def revalidate_destinations() -> None:
        for path, previous in destinations.items():
            require_snapshot(identity, path, previous)
        _read_inventory(root / starter, expected)
        if inspect_project_root(root / mirror, allow_missing=True) is not None:
            _read_inventory(
                root / mirror, tuple(name for name in _PACKAGE_FILES if destinations[mirror + name] is not None)
            )

    @contextmanager
    def locked(path: Path) -> Iterator[None]:
        with project_authoring_lock(path):
            revalidate_inputs()
            revalidate_destinations()
            yield

    revalidate_destinations()
    dependencies = generate(source)
    files = {mirror + name: content for name, content in source.files}
    files.update(
        {
            starter + "packages.yml": dependencies.packages_yml,
            starter + "package-lock.yml": dependencies.package_lock_yml,
        }
    )
    revalidate_inputs()
    revalidate_destinations()

    def validate_result() -> None:
        check_starter_resources(root, revision)
        for path, content in files.items():
            observed = snapshot(identity, path)
            if observed is None or observed.content != content:
                raise ValueError()

    return apply_resource_plan(
        root,
        files,
        source_revision=revision,
        revalidate_inputs=revalidate_inputs,
        validate_result=validate_result,
        authoring_lock=locked,
    )


if __name__ == "__main__":
    raise SystemExit(main())
