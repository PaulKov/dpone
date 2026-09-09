"""Confined, byte-exact acquisition for workflow privilege inputs."""

from __future__ import annotations

import hashlib
import os
import stat
from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path

from tools.agent_policy.workflow_privilege_contracts import (
    ScanLimits,
    Snapshot,
    SnapshotFile,
    SnapshotReference,
    SnapshotResult,
    canonical_sha256,
    finding,
    valid_public_text,
)
from tools.agent_policy.workflow_privilege_mutation_observer import MutationObserver

_POLICY_PATH = ".agents/policy/workflow-security-privileged.yml"
_INVALID_CODE_BY_STAGE = {"policy": "PRIVILEGE_INVALID_POLICY", "workflows": "PRIVILEGE_INVALID_WORKFLOW"}
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
_FILE_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK


@dataclass(slots=True)
class _OpenPath:
    fd: int
    parent_fd: int
    name: str
    metadata: os.stat_result
    track_changes: bool


@dataclass(slots=True)
class _OpenFile(_OpenPath):
    value: SnapshotFile


@dataclass(slots=True)
class _LeaseState:
    observer: MutationObserver
    directory_fds: tuple[int, ...]
    directories: tuple[_OpenPath, ...]
    policy: _OpenFile
    workflows: tuple[_OpenFile, ...]
    workflow_directory: _OpenPath | None
    expected_names: tuple[str, ...]


class _LimitExceeded(ValueError):
    def __init__(self, dimension: str, observed: int | None = None) -> None:
        self.dimension, self.observed = dimension, observed


class _ConcurrentMutation(ValueError): ...


class SnapshotLease:
    """Own open descriptors until a final inventory/identity revalidation."""

    def __init__(self, snapshot: Snapshot, state: _LeaseState | None = None) -> None:
        self.snapshot, self._state, self._closed = snapshot, state, False

    def finalize(self, *, policy_schema_version: int | None) -> SnapshotResult:
        """Revalidate acquired descriptors and return the single report identity."""

        try:
            return self._finalize(policy_schema_version=policy_schema_version)
        finally:
            self.close()

    def _finalize(self, *, policy_schema_version: int | None) -> SnapshotResult:
        snapshot = self.snapshot
        revalidates = self._revalidates() if snapshot.complete or snapshot.policy is not None else True
        if not revalidates:
            policy_only = not snapshot.complete
            mutation = finding(
                "PRIVILEGE_CONCURRENT_MUTATION",
                snapshot.policy.path if policy_only and snapshot.policy is not None else ".github/workflows",
                "policy changed during incomplete workflow acquisition"
                if policy_only
                else "policy or workflow inventory changed during acquisition",
            )
            snapshot = replace(
                snapshot,
                policy=None if policy_only else snapshot.policy,
                complete=False,
                manifest_sha256=None,
                findings=(*snapshot.findings, mutation) if policy_only else (mutation,),
            )
        policy_sha = snapshot.policy.sha256 if snapshot.policy else None
        reference = SnapshotReference(policy_sha, policy_schema_version, snapshot.manifest_sha256, snapshot.complete)
        self.close()
        return SnapshotResult(findings=snapshot.findings, snapshot=snapshot, reference=reference)

    def _revalidates(self) -> bool:
        try:
            state = self._state
            return (
                state is not None
                and _revalidation_phase(state)
                and _tree_revalidates(state)
                and state.observer.unchanged(policy_only=not self.snapshot.complete)
            )
        except (OSError, ValueError):
            return False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._state is not None:
            try:
                self._state.observer.close()
            finally:
                _close_descriptors(
                    [
                        *self._state.directory_fds,
                        *(opened.fd for opened in (self._state.policy, *self._state.workflows)),
                    ]
                )


class SnapshotReader:
    def __init__(self, *, limits: ScanLimits, policy_path: str = _POLICY_PATH) -> None:
        self._limits = limits
        self._policy_name, self._policy_path = policy_path.rsplit("/", 1)[-1], policy_path

    def acquire(self, root: Path) -> SnapshotLease:
        fds: list[int] = []
        bindings: list[_OpenPath] = []
        opened_files: list[_OpenFile] = []
        count, stage = 0, "root"
        policy_state: tuple[int, int, _OpenFile] | None = None
        observer: MutationObserver | None = None
        try:
            observer = MutationObserver()
            root_dir = _open_root(root, fds, bindings, observer)
            stage = "policy"
            policy_dir = _open_components(root_dir, (".agents", "policy"), fds, bindings, observer, "policy")
            policy = _open_file(
                policy_dir.fd,
                self._policy_name,
                self._policy_path,
                self._limits.policy_bytes,
                "policy_bytes",
                observer,
                "policy",
            )
            opened_files.append(policy)
            policy_state = len(fds), len(bindings), policy
            stage = "workflows"
            workflow_dir = _open_components(root_dir, (".github", "workflows"), fds, bindings, observer, "workflows")
            names = _workflow_names(workflow_dir.fd, self._limits.workflow_files)
            count = len(names)
            if not names:
                raise ValueError("workflow inventory is empty")
            workflows: list[_OpenFile] = []
            total = 0
            for name in names:
                remaining = self._limits.total_workflow_bytes - total
                maximum = min(self._limits.workflow_bytes, remaining)
                dimension = "workflow_bytes" if maximum == self._limits.workflow_bytes else "total_workflow_bytes"
                path = f".github/workflows/{name}"
                opened = _open_file(workflow_dir.fd, name, path, maximum, dimension, observer, "workflows")
                workflows.append(opened)
                opened_files.append(opened)
                total += opened.value.byte_length
            entries = (policy.value, *(item.value for item in workflows))
            manifest = canonical_sha256(
                [[entry.path, entry.mode, entry.byte_length, entry.sha256] for entry in entries]
            )
            snapshot = Snapshot(
                policy=policy.value,
                workflows=tuple(item.value for item in workflows),
                complete=True,
                manifest_sha256=manifest,
                workflow_count=count,
            )
            state = _LeaseState(observer, tuple(fds), tuple(bindings), policy, tuple(workflows), workflow_dir, names)
            return SnapshotLease(snapshot, state)
        except _LimitExceeded as exc:
            return _incomplete(
                observer, fds, bindings, opened_files, policy_state, exc.observed or count, exc.dimension
            )
        except (OSError, ValueError) as exc:
            invalid_code = None if isinstance(exc, _ConcurrentMutation) else _INVALID_CODE_BY_STAGE.get(stage)
            subject = self._policy_path if stage == "policy" else ".github/workflows" if stage == "workflows" else "."
            failure = finding(
                invalid_code or "PRIVILEGE_CONCURRENT_MUTATION",
                subject,
                f"fixed input acquisition failed: {type(exc).__name__.lstrip('_')}",
            )
            retained = policy_state if stage == "workflows" else None
            return _incomplete(observer, fds, bindings, opened_files, retained, count, failure=failure)
        except BaseException:
            try:
                if observer is not None:
                    observer.close()
            finally:
                _close_descriptors([*fds, *(opened.fd for opened in opened_files)])
            raise


def _incomplete(
    observer: MutationObserver | None,
    fds: list[int],
    bindings: list[_OpenPath],
    files: list[_OpenFile],
    policy_state: tuple[int, int, _OpenFile] | None,
    count: int,
    dimension: str | None = None,
    failure: object = None,
) -> SnapshotLease:
    if policy_state is None or observer is None:
        try:
            if observer is not None:
                observer.close()
        finally:
            _close_descriptors([*fds, *(opened.fd for opened in files)])
        policy, state = None, None
    else:
        fd_count, binding_count, opened_policy = policy_state
        _close_descriptors([opened.fd for opened in files if opened is not opened_policy])
        _close_descriptors(fds[fd_count:])
        policy = opened_policy.value
        state = _LeaseState(
            observer, tuple(fds[:fd_count]), tuple(bindings[:binding_count]), opened_policy, (), None, ()
        )
    failures = (
        (failure,)
        if failure
        else (finding("PRIVILEGE_RESOURCE_LIMIT", dimension, f"observed value exceeds {dimension}"),)
    )
    if dimension == "policy_bytes":
        failures = (
            finding("PRIVILEGE_INVALID_POLICY", _POLICY_PATH, "policy bytes exceed the closed limit"),
            *failures,
        )
    snapshot = Snapshot(
        policy=policy,
        workflow_count=count,
        overflow_dimensions=() if dimension is None else (dimension,),
        findings=failures,
    )  # type: ignore[arg-type]
    return SnapshotLease(snapshot, state)


def _open_root(path: Path, descriptors: list[int], bindings: list[_OpenPath], observer: MutationObserver) -> _OpenPath:
    components = Path(os.path.abspath(path)).parts[1:]
    if not components:
        raise ValueError("repository root must have a parent binding")
    current_fd = os.open(os.sep, _DIRECTORY_FLAGS)
    descriptors.append(current_fd)
    for component in components:
        current = _open_child_directory(current_fd, component, descriptors, False, observer, "policy")
        bindings.append(current)
        current_fd = current.fd
    return current


def _open_child_directory(
    parent_fd: int, name: str, descriptors: list[int], track_changes: bool, observer: MutationObserver, scope: str
) -> _OpenPath:
    descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
    descriptors.append(descriptor)
    observer.watch(descriptor, scope=scope, contents=track_changes)
    return _OpenPath(descriptor, parent_fd, name, os.fstat(descriptor), track_changes)


def _open_components(
    root: _OpenPath,
    names: tuple[str, ...],
    fds: list[int],
    bindings: list[_OpenPath],
    observer: MutationObserver,
    scope: str,
) -> _OpenPath:
    current = root
    for name in names:
        current = _open_child_directory(current.fd, name, fds, True, observer, scope)
        bindings.append(current)
    return current


def _workflow_names(directory_fd: int, maximum: int) -> tuple[str, ...]:
    workflow_names: list[str] = []
    folded_names: set[str] = set()
    with os.scandir(directory_fd) as entries:
        for entry in entries:
            name = entry.name
            metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                raise ValueError(f"unsafe workflow directory entry: {name}")
            if not name.endswith((".yml", ".yaml")):
                continue
            if not valid_public_text(f".github/workflows/{name}", 1024):
                raise ValueError(f"workflow path is not report-safe: {name!r}")
            folded = name.casefold()
            if folded in folded_names:
                raise ValueError(f"case-colliding workflow name: {name}")
            folded_names.add(folded)
            workflow_names.append(name)
            if len(workflow_names) > maximum:
                raise _LimitExceeded("workflow_files", len(workflow_names))
    return tuple(sorted(workflow_names, key=lambda item: item.encode("utf-8")))


def _open_file(
    directory_fd: int, name: str, path: str, max_bytes: int, dimension: str, observer: MutationObserver, scope: str
) -> _OpenFile:
    descriptor = os.open(name, _FILE_FLAGS, dir_fd=directory_fd)
    try:
        observer.watch(descriptor, scope=scope, contents=True)
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise ValueError(f"not a confined regular file: {path}")
        if metadata.st_size > max_bytes:
            raise _LimitExceeded(dimension)
        content = _read_fd(descriptor, metadata.st_size)
        if len(content) != metadata.st_size:
            raise _ConcurrentMutation(f"short read: {path}")
        if not _metadata_stable(metadata, os.fstat(descriptor)):
            raise _ConcurrentMutation(f"file changed while reading: {path}")
        value = SnapshotFile(
            path=path,
            mode=f"{stat.S_IMODE(metadata.st_mode):04o}",
            byte_length=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
            content=content,
        )
        return _OpenFile(descriptor, directory_fd, name, metadata, True, value)
    except BaseException:
        os.close(descriptor)
        raise


def _metadata_stable(before: os.stat_result, after: os.stat_result) -> bool:
    attributes = ("st_dev", "st_ino", "st_mode", "st_nlink", "st_size", "st_mtime_ns", "st_ctime_ns")
    return all(getattr(before, name) == getattr(after, name) for name in attributes)


def _revalidation_phase(state: _LeaseState) -> bool:
    current = _reopen_directories(state)
    try:
        files = (state.policy, *state.workflows)
        names_match = (
            state.workflow_directory is None
            or _workflow_names(current[state.workflow_directory.fd], len(state.expected_names)) == state.expected_names
        )
        return (
            names_match
            and all(_file_revalidates(opened, current[opened.parent_fd]) for opened in files)
            and all(
                _directory_metadata_revalidates(os.fstat(current[opened.fd]), opened) for opened in state.directories
            )
        )
    finally:
        _close_descriptors(list(current.values()))


_tree_revalidates = _revalidation_phase


def _reopen_directories(state: _LeaseState) -> dict[int, int]:
    current: dict[int, int] = {}
    try:
        for opened in state.directories:
            parent_fd = current.get(opened.parent_fd, opened.parent_fd)
            descriptor = os.open(opened.name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
            if not _directory_metadata_revalidates(os.fstat(descriptor), opened):
                os.close(descriptor)
                raise ValueError("directory binding changed")
            current[opened.fd] = descriptor
        return current
    except BaseException:
        _close_descriptors(list(current.values()))
        raise


def _directory_metadata_revalidates(metadata: os.stat_result, opened: _OpenPath) -> bool:
    same_inode = (metadata.st_dev, metadata.st_ino) == (opened.metadata.st_dev, opened.metadata.st_ino)
    return (
        stat.S_ISDIR(metadata.st_mode)
        and same_inode
        and (not opened.track_changes or _metadata_stable(metadata, opened.metadata))
    )


def _file_metadata_revalidates(metadata: os.stat_result, opened: _OpenFile) -> bool:
    return stat.S_ISREG(metadata.st_mode) and metadata.st_nlink == 1 and _metadata_stable(metadata, opened.metadata)


def _file_revalidates(opened: _OpenFile, parent_fd: int) -> bool:
    return (
        _file_metadata_revalidates(os.fstat(opened.fd), opened)
        and _read_fd(opened.fd, opened.value.byte_length) == opened.value.content
        and _file_metadata_revalidates(os.fstat(opened.fd), opened)
        and _file_path_revalidates(opened, parent_fd)
    )


def _file_path_revalidates(opened: _OpenFile, parent_fd: int) -> bool:
    descriptor = os.open(opened.name, _FILE_FLAGS, dir_fd=parent_fd)
    try:
        return (
            _file_metadata_revalidates(os.fstat(descriptor), opened)
            and _read_fd(descriptor, opened.value.byte_length) == opened.value.content
            and _file_metadata_revalidates(os.fstat(descriptor), opened)
        )
    finally:
        os.close(descriptor)


def _read_fd(descriptor: int, expected_size: int) -> bytes:
    chunks: list[bytes] = []
    offset = 0
    while offset < expected_size:
        chunk = os.pread(descriptor, min(1024 * 1024, expected_size - offset), offset)
        if not chunk:
            break
        chunks.append(chunk)
        offset += len(chunk)
    return b"".join(chunks)


def _close_descriptors(values: list[int]) -> None:
    for descriptor in reversed(values):
        os.close(descriptor)


def snapshot_inventory(
    snapshot: Snapshot, reference: SnapshotReference, overrides: Mapping[str, object] | None = None
) -> dict[str, object]:
    """Project one trusted snapshot/reference identity into report inventory."""
    value: dict[str, object] = {
        "complete": reference.complete,
        "manifest_sha256": reference.manifest_sha256,
        "workflow_count": snapshot.workflow_count,
        **dict.fromkeys("job_count edge_count root_count route_count".split(), 0),
        "overflow_dimensions": list(snapshot.overflow_dimensions),
    }
    value.update(overrides or {})
    value["overflow_dimensions"] = sorted(set(value["overflow_dimensions"]))  # type: ignore[arg-type]
    return value


__all__ = ["SnapshotLease", "SnapshotReader", "snapshot_inventory"]
