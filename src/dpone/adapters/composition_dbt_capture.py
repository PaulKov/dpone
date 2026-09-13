"""Root-supervised Linux build dispatch and bounded nofollow original capture.

No local file permission is treated as an authority receipt. Store methods must
use protected INSERT-only durable records inaccessible to the child UID. The
child UID is dedicated to this attempt, with no ambient processes or credentials
that can write supervisor/control records. Provisioning is outside this adapter.
"""

from __future__ import annotations

import os
import stat
import subprocess
import sys
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Any

from dpone.adapters.composition_dbt_cleanup import (
    FINAL_GRACE,
    LinuxDbtProcessLifecycle,
    LinuxProcessOperations,
    ProcessLifecycleError,
    read_process,
    require_waitid,
)
from dpone.contracts.composition_attempt import CompositionAttemptIdentity
from dpone.contracts.composition_dbt_outcome import (
    MAX_ARTIFACT_BYTES,
    DbtArtifactOriginal,
    DbtCaptureError,
    DbtCaptureRecord,
    DbtChildExit,
    DbtDispatchIntent,
    DbtExitRecord,
)
from dpone.contracts.strict_json import canonical_json_bytes


class LinuxDbtBuildRunner:
    """Run the exact admitted command as an isolated nonroot UID, without shell.

    Environment is injected just-in-time and never journaled. It must contain
    only this attempt's issued connection references/credentials. Process output
    belongs in the admitted dbt files; stdout/stderr are not trusted receipts.
    """

    def __init__(self, environment: Callable[[CompositionAttemptIdentity], Mapping[str, str]]):
        self._environment = environment

    def __call__(self, intent: DbtDispatchIntent) -> DbtChildExit:
        with _capture_process_errors():
            intent.__post_init__()
            _require_linux_supervisor(intent)
            require_waitid()
            _require_uid_quiescent(intent.child_uid)
            environment = dict(self._environment(intent.attempt))
            process = subprocess.Popen(
                intent.argv,
                cwd=intent.working_directory,
                env=environment,
                user=intent.child_uid,
                group=intent.child_gid,
                extra_groups=[],
                start_new_session=True,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            child = LinuxDbtProcessLifecycle().run(process, intent.child_uid, intent.timeout_seconds)
            return DbtChildExit(child.pid, child.start_ticks, child.exit_code)


class ProtectedDbtCapture:
    """Dispatch once, then separately capture evidence produced after build wait.

    ``load_intent`` resolves an admitted attempt's actual command/paths after
    preflight. ``record_dispatch_once(intent, preflight)`` must reject replay and
    acknowledge durability before runner invocation. An uncertain ACK propagates
    without launch. ``record_exit_once`` and ``capture_once`` persist originals;
    ``read_exit`` must return the exact protected exit, never a caller result.
    No exception after dispatch is classified as FAILED here.
    """

    def __init__(self, store: Any, runner: Callable[[DbtDispatchIntent], DbtChildExit]):
        self._store = store
        self._runner = runner

    def dispatch(self, attempt: CompositionAttemptIdentity) -> DbtChildExit:
        intent = self._store.load_intent(attempt)
        self._require_exact(intent, attempt)
        self._require_boundary(intent)
        _require_uid_quiescent(intent.child_uid)
        preflight = self._read_original(intent, "preflight_manifest")
        if preflight.sha256 != intent.preflight_manifest_sha256:
            raise DbtCaptureError("capture_preflight_changed")
        self._store.record_dispatch_once(intent, preflight)
        child = self._runner(intent)
        if type(child) is not DbtChildExit:
            raise DbtCaptureError("capture_process_identity")
        quiescence = self._observe_quiescence(intent, child)
        self._store.record_exit_once(DbtExitRecord(intent, child, quiescence, preflight))
        return child

    def capture(self, attempt: CompositionAttemptIdentity) -> DbtCaptureRecord:
        """Capture only after outer runtime wrote its final execution evidence."""
        intent = self._store.load_intent(attempt)
        self._require_exact(intent, attempt)
        self._require_boundary(intent)
        exited = self._store.read_exit(attempt)
        if type(exited) is not DbtExitRecord or exited.intent != intent:
            raise DbtCaptureError("capture_exit_missing")
        self._observe_quiescence(intent, exited.child)
        originals = tuple(self._read_original(intent, role) for role, _ in intent.artifact_paths)
        if originals[0] != exited.preflight_original:
            raise DbtCaptureError("capture_preflight_changed")
        result = DbtCaptureRecord(intent, "CAPTURED", exited, originals)
        self._store.capture_once(result)
        return result

    @staticmethod
    def _require_exact(intent: Any, attempt: CompositionAttemptIdentity) -> None:
        if type(intent) is not DbtDispatchIntent or intent.attempt != attempt:
            raise DbtCaptureError("capture_attempt")
        intent.__post_init__()

    @staticmethod
    def _require_boundary(intent: DbtDispatchIntent) -> None:
        _require_linux_supervisor(intent)
        _require_protected_ancestors(intent.output_directory)
        descriptor = _open_directory(intent.output_directory)
        try:
            info = os.fstat(descriptor)
            if info.st_uid != intent.supervisor_uid or info.st_mode & 0o022:
                raise DbtCaptureError("capture_output_owner")
            # Root stays supervisor-owned; dedicated child-owned descendants
            # must be provisioned for dbt outputs and cannot replace this root.
            if not info.st_mode & stat.S_IXOTH:
                raise DbtCaptureError("capture_output_inaccessible")
        finally:
            os.close(descriptor)
        for role, relative in intent.artifact_paths:
            if role not in {"build_manifest", "run_results"}:
                continue
            parent = str(PurePosixPath(intent.output_directory) / PurePosixPath(relative).parent)
            descriptor = _open_directory(parent)
            try:
                info = os.fstat(descriptor)
                if info.st_uid != intent.child_uid or info.st_mode & 0o022 or info.st_mode & 0o700 != 0o700:
                    raise DbtCaptureError("capture_child_output_unusable")
            finally:
                os.close(descriptor)

    @staticmethod
    def _read_original(intent: DbtDispatchIntent, role: str) -> DbtArtifactOriginal:
        relative = dict(intent.artifact_paths)[role]
        current = _open_directory(intent.output_directory)
        descriptor = None
        try:
            for part in PurePosixPath(relative).parts[:-1]:
                following = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=current)
                os.close(current)
                current = following
                info = os.fstat(current)
                if info.st_uid not in {intent.supervisor_uid, intent.child_uid} or info.st_mode & 0o022:
                    raise DbtCaptureError("capture_artifact_owner")
            descriptor = os.open(
                PurePosixPath(relative).name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=current
            )
            before = os.fstat(descriptor)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_nlink != 1
                or before.st_uid not in {intent.supervisor_uid, intent.child_uid}
                or before.st_mode & 0o022
                or not 0 < before.st_size <= MAX_ARTIFACT_BYTES
            ):
                raise DbtCaptureError("capture_artifact_file")
            chunks = []
            remaining = MAX_ARTIFACT_BYTES + 1
            while remaining:
                chunk = os.read(descriptor, min(remaining, 65536))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            content = b"".join(chunks)
            after = os.fstat(descriptor)
            if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
                after.st_ctime_ns,
            ) or len(content) != before.st_size:
                raise DbtCaptureError("capture_artifact_changed")
            return DbtArtifactOriginal(role, relative, content)
        except OSError:
            raise DbtCaptureError("capture_artifact_unavailable") from None
        finally:
            if descriptor is not None:
                os.close(descriptor)
            os.close(current)

    @staticmethod
    def _observe_quiescence(intent: DbtDispatchIntent, child: DbtChildExit) -> bytes:
        _require_linux_supervisor(intent)
        _require_uid_quiescent(intent.child_uid)
        if _read_process(child.pid) is not None:
            raise DbtCaptureError("capture_process_still_present")
        return canonical_json_bytes(
            {
                "schema": "dpone.composition-dbt-quiescence.v1",
                "intent_sha256": intent.intent_sha256,
                "pid": child.pid,
                "start_ticks": child.start_ticks,
                "child_uid": intent.child_uid,
                "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
                "pid_namespace": os.readlink("/proc/self/ns/pid"),
            }
        )


def _require_linux_supervisor(intent: DbtDispatchIntent) -> None:
    if (
        sys.platform != "linux"
        or os.geteuid() != 0
        or intent.supervisor_uid != os.geteuid()
        or intent.child_uid == 0
        or intent.child_gid == 0
    ):
        raise DbtCaptureError("capture_supervisor_boundary")


def _open_directory(path: str) -> int:
    descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in PurePosixPath(path).parts[1:]:
            following = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = following
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _read_process(pid: int) -> tuple[int, int, int] | None:
    with _capture_process_errors():
        observed = read_process(pid)
        if observed is None:
            return None
        return observed.uid, observed.pgid, observed.start_ticks


def _require_uid_quiescent(uid: int) -> None:
    # Independently includes escaped groups and zombies; bounded visibility
    # failures block capture rather than manufacturing an empty process set.
    with _capture_process_errors():
        operations = LinuxProcessOperations()
        if any(item.uid == uid for item in operations.inventory(operations.monotonic() + FINAL_GRACE)):
            raise DbtCaptureError("capture_child_not_quiescent")


@contextmanager
def _capture_process_errors() -> Iterator[None]:
    """Translate the OS boundary while preserving the original cancellation cause."""
    try:
        yield
    except ProcessLifecycleError as error:
        raise DbtCaptureError(str(error)) from (error.__cause__ or error)


def _require_protected_ancestors(path: str) -> None:
    current = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in PurePosixPath(path).parts[1:]:
            following = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=current)
            os.close(current)
            current = following
            info = os.fstat(current)
            if info.st_uid != 0 or (info.st_mode & 0o022 and not info.st_mode & stat.S_ISVTX):
                raise DbtCaptureError("capture_output_ancestry")
    except OSError:
        raise DbtCaptureError("capture_output_ancestry") from None
    finally:
        os.close(current)
