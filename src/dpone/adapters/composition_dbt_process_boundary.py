"""Linux root-supervised, exclusive attempt directories and tmpfs profiles.

The configured roots must be provisioned root-owned directories. Allocation
creates only fresh, digest-derived children below the output root and uses the
dedicated UID and GID that the persistent child-identity allocator has already
reserved for exactly this attempt. Keep the supervisor mount persistent across
supervisors; never clear its identity tombstones to retry an uncertain attempt.
OS account provisioning must also exclude other use of the reserved identity.
This adapter never changes permissions of an existing caller path or supplies
SQL credentials.
"""

from __future__ import annotations

import ctypes
import os
import re
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from threading import Lock
from typing import Any

import yaml

from dpone.adapters.composition_child_identity_allocator import (
    CompositionChildIdentity,
    CompositionChildIdentityAllocator,
)
from dpone.adapters.composition_supervisor_filesystem import PROTECTED_FLAGS as _FLAGS
from dpone.adapters.composition_supervisor_filesystem import absolute_supervisor_path as _absolute
from dpone.adapters.composition_supervisor_filesystem import open_protected as _open_protected
from dpone.adapters.composition_supervisor_filesystem import require_supervisor as _require_supervisor
from dpone.contracts.composition_attempt import CompositionAttemptIdentity
from dpone.contracts.composition_dbt_outcome import MAX_ARTIFACT_BYTES, DbtCaptureError

_ENV_REFERENCE = re.compile(r"\{\{\s*env_var\(['\"][A-Za-z_][A-Za-z_0-9]*['\"]\)\s*\}\}")


@dataclass(frozen=True)
class DbtProcessAllocation:
    """Absolute paths consumed unchanged by the existing dbt execution service.

    ``runtime_attempt_id`` must come from canonical ``dbt_attempt_id`` at the
    application root. ``run_output_root`` is passed to the execution service;
    ``output_directory`` is the protected dispatch/capture intent root.
    """

    run_output_root: Path
    output_directory: Path
    preflight_target: Path
    preflight_logs: Path
    target: Path
    logs: Path
    profile_store: ChildReadableDbtProfileStore


class LinuxDbtProcessBoundary:
    """Allocate a fresh layout once, failing closed after partial allocation."""

    def __init__(self, output_root: Path, profile_tmpfs_root: Path, *, identities: CompositionChildIdentityAllocator):
        self.output_root = _absolute(output_root)
        self.profile_tmpfs_root = _absolute(profile_tmpfs_root)
        self._identities = identities

    def allocate(
        self, attempt: CompositionAttemptIdentity, *, runtime_attempt_id: str, target_path: str = "target"
    ) -> DbtProcessAllocation:
        """Reserve one persistent child identity, then precreate runtime paths.

        A rejected request must never consume an identity, so every untrusted
        path input is checked first. The reserved identity outlives this call:
        a failed attempt keeps its tombstones and is never retried under the
        same identity.
        """
        attempt.__post_init__()
        parts = PurePosixPath(target_path).parts
        if (
            re.fullmatch("[0-9a-f]{32}", runtime_attempt_id) is None
            or not parts
            or PurePosixPath(target_path).is_absolute()
            or str(PurePosixPath(target_path)) != target_path
            or any(part in {".", ".."} for part in parts)
            or parts[0] in {"preflight", "logs", "execution-evidence.json", "evidence.json"}
        ):
            raise DbtCaptureError("capture_allocation_path")
        _require_supervisor()
        identity = self._identities.allocate(attempt)
        return self._allocate(
            attempt, identity=identity, runtime_attempt_id=runtime_attempt_id, target_path=target_path
        )

    def _allocate(
        self,
        attempt: CompositionAttemptIdentity,
        *,
        identity: CompositionChildIdentity,
        runtime_attempt_id: str,
        target_path: str,
    ) -> DbtProcessAllocation:
        """Precreate exact runtime paths for one already reserved identity.

        A target cannot overlap root evidence, logs or preflight directories.
        Nested target ancestry stays supervisor-owned; only its leaf is writable
        by the dedicated child. No artifacts from a previous attempt are reused.
        """
        child_uid, child_gid = identity.uid, identity.gid
        if any(type(value) is not int or not 0 < value < 2**31 for value in (child_uid, child_gid)):
            raise DbtCaptureError("capture_child_identity")
        parts = PurePosixPath(target_path).parts
        _require_identity_quiescent(child_uid, child_gid)
        descriptors: list[int] = []
        try:
            root = _open_protected(self.output_root)
            descriptors.append(root)
            profiles = _open_protected(self.profile_tmpfs_root)
            descriptors.append(profiles)
            _require_tmpfs(profiles)
            for name in (f"uid-{child_uid}", f"gid-{child_gid}"):
                descriptor = _mkdir(root, name, 0o700)
                os.close(descriptor)
            name = "attempt-" + attempt.attempt_sha256.removeprefix("sha256:")
            run = _mkdir(root, name, 0o711)
            descriptors.append(run)
            attempts = _mkdir(run, "attempts", 0o711)
            descriptors.append(attempts)
            output = _mkdir(attempts, runtime_attempt_id, 0o711)
            descriptors.append(output)
            preflight = _mkdir(output, "preflight", 0o700)
            descriptors.append(preflight)
            for directory in ("target", "logs"):
                os.close(_mkdir(preflight, directory, 0o700))
            current = output
            for part in parts[:-1]:
                current = _mkdir(current, part, 0o711)
                descriptors.append(current)
            os.close(_mkdir(current, parts[-1], 0o700, uid=child_uid, gid=child_gid))
            os.close(_mkdir(output, "logs", 0o700, uid=child_uid, gid=child_gid))
            profile = _mkdir(profiles, name, 0o710, gid=child_gid)
            os.close(profile)
            run_path = self.output_root / name
            output_path = run_path / "attempts" / runtime_attempt_id
            return DbtProcessAllocation(
                run_path,
                output_path,
                output_path / "preflight" / "target",
                output_path / "preflight" / "logs",
                output_path.joinpath(*parts),
                output_path / "logs",
                ChildReadableDbtProfileStore(self.profile_tmpfs_root / name, child_gid),
            )
        except OSError:
            raise DbtCaptureError("capture_allocation_failed") from None
        finally:
            for descriptor in reversed(descriptors):
                os.close(descriptor)


class ChildReadableDbtProfileStore:
    """One-shot root-owned profile with dedicated GID read permission.

    Only environment references may occupy user/password fields. The root-owned
    directory prevents child replacement; the child has no write permission to
    either file or directory. Profile removal retains the one-shot directory.
    The app renderer must inject per-attempt credentials only into the child's
    environment and must never forward protected controller credentials.
    """

    def __init__(self, directory: Path, child_gid: int):
        self._directory, self._gid = directory, child_gid
        self._used = False
        self._lock = Lock()

    @property
    def profile_path(self) -> Path:
        """Fixed path for app-owned command derivation, before materialization."""
        return self._directory / "profiles.yml"

    @contextmanager
    def materialize(self, content: bytes) -> Iterator[Path]:
        _require_profile_references(content)
        with self._lock:
            if self._used:
                raise DbtCaptureError("capture_profile_replay")
            self._used = True
        _require_supervisor()
        parent = _open_protected(self._directory, traversable=False)
        descriptor = None
        created = False
        try:
            info = os.fstat(parent)
            if info.st_gid != self._gid or stat.S_IMODE(info.st_mode) != 0o710:
                raise DbtCaptureError("capture_profile_permissions")
            _require_tmpfs(parent)
            descriptor = os.open(
                "profiles.yml", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o400, dir_fd=parent
            )
            created = True
            os.fchown(descriptor, 0, self._gid)
            os.fchmod(descriptor, 0o440)
            pending = memoryview(content)
            while pending:
                count = os.write(descriptor, pending)
                if count <= 0:
                    raise OSError
                pending = pending[count:]
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = None
            yield self.profile_path
        except OSError:
            raise DbtCaptureError("capture_profile_unavailable") from None
        finally:
            if descriptor is not None:
                os.close(descriptor)
            if created:
                try:
                    os.unlink("profiles.yml", dir_fd=parent)
                except FileNotFoundError:
                    pass
            os.close(parent)


class _ProfileLoader(yaml.SafeLoader):
    """Reject duplicate resolved keys, including merged mappings, at every depth."""

    def construct_mapping(self, node: yaml.nodes.MappingNode, deep: bool = False) -> dict[Any, Any]:
        self.flatten_mapping(node)
        pairs = self.construct_pairs(node, deep=deep)
        result = dict(pairs)
        if len(result) != len(pairs):
            raise ValueError("duplicate profile mapping key")
        return result


def _require_profile_references(content: bytes) -> None:
    if type(content) is not bytes or not 0 < len(content) <= MAX_ARTIFACT_BYTES:
        raise DbtCaptureError("capture_profile_invalid")
    try:
        document = yaml.load(content, Loader=_ProfileLoader)
        if not isinstance(document, dict):
            raise ValueError
        fields = []
        pending: list[object] = [document]
        visited: set[int] = set()
        while pending:
            item = pending.pop()
            if id(item) in visited:
                continue
            visited.add(id(item))
            if isinstance(item, dict):
                for key, value in item.items():
                    if str(key).lower() in {"user", "username", "password", "pass", "token", "secret"}:
                        if not isinstance(value, str) or _ENV_REFERENCE.fullmatch(value) is None:
                            raise ValueError
                        fields.append(str(key).lower())
                    elif isinstance(value, (dict, list)):
                        pending.append(value)
            elif isinstance(item, list):
                pending.extend(item)
        if "user" not in fields or "password" not in fields:
            raise ValueError
    except (ValueError, TypeError, RecursionError, yaml.YAMLError, UnicodeError):
        raise DbtCaptureError("capture_profile_credentials") from None


def _mkdir(parent: int, name: str, mode: int, *, uid: int = 0, gid: int = 0) -> int:
    os.mkdir(name, mode=0o700, dir_fd=parent)
    descriptor = os.open(name, _FLAGS, dir_fd=parent)
    try:
        os.fchown(descriptor, uid, gid)
        os.fchmod(descriptor, mode)
        os.fsync(parent)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _require_tmpfs(descriptor: int) -> None:
    # Linux fstatfs starts with a native long filesystem magic. An oversized
    # buffer avoids relying on architecture-specific trailing structure fields.
    libc = ctypes.CDLL(None, use_errno=True)
    observation = ctypes.create_string_buffer(256)
    if (
        libc.fstatfs(descriptor, ctypes.byref(observation)) != 0
        or ctypes.c_long.from_buffer(observation).value != 0x01021994
    ):
        raise DbtCaptureError("capture_profile_not_tmpfs")


def _require_identity_quiescent(uid: int, gid: int) -> None:
    try:
        for process in Path("/proc").iterdir():
            if not process.name.isdecimal():
                continue
            try:
                lines = (process / "status").read_text().splitlines()
            except FileNotFoundError:
                continue
            values = {line.split(":", 1)[0]: line.split(":", 1)[1].split() for line in lines if ":" in line}
            uids = tuple(int(value) for value in values["Uid"])
            gids = tuple(int(value) for value in values["Gid"])
            groups = tuple(int(value) for value in values["Groups"])
            if len(uids) != 4 or len(gids) != 4:
                raise ValueError
            if uid in uids or gid in (*gids, *groups):
                raise DbtCaptureError("capture_child_not_quiescent")
    except (OSError, ValueError, KeyError):
        raise DbtCaptureError("capture_process_visibility") from None
