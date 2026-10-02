"""Owner-private, non-reclaimable local attempts for the operator workflow.

Every invocation for one operation must use this same admitted persistent directory.
It is not a distributed lock and is never a substitute for SQL slot CAS or the
deployment freeze. A missing/unavailable directory fails before SQL. Preserve
attempt markers after unknown outcomes; there is deliberately no reset API.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from pathlib import Path
from threading import RLock

from dpone.adapters.publication_plan_file import write_private_plan_at


class PrivateRetirementAttempts:
    def __init__(self, directory: Path) -> None:
        self._directory = directory
        self._identity: str | None = None
        self._lock = RLock()

    def require_ready(self) -> str:
        """Freeze and recheck private directory identity without creating it.

        This local identity is not proof of shared or persistent deployment
        storage. A trusted runner must supply the same admitted journal across
        restarts; a new path is never a recovery/reset mechanism.
        """
        with self._lock:
            descriptor = self._open_admitted()
            try:
                assert self._identity is not None
                return self._identity
            finally:
                os.close(descriptor)

    def _open_admitted(self) -> int:
        path = self._directory
        if os.name != "posix" or not path.is_absolute() or path.resolve(strict=True) != path:
            raise ValueError("retirement journal requires a canonical local directory")
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or metadata.st_uid != os.getuid()
                or stat.S_IMODE(metadata.st_mode) != 0o700
                or not os.path.samestat(metadata, path.stat(follow_symlinks=False))
            ):
                raise ValueError("retirement journal requires an owner-private directory")
            payload = json.dumps(
                [
                    "dpone.publication-retirement-journal.v1",
                    str(path),
                    metadata.st_dev,
                    metadata.st_ino,
                    metadata.st_uid,
                ],
                separators=(",", ":"),
            ).encode()
            identity = hashlib.sha256(payload).hexdigest()
            if self._identity is not None and identity != self._identity:
                raise ValueError("retirement journal identity changed")
            self._identity = identity
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    def claim(self, operation_key: str) -> bool:
        """Durably reserve exactly one attempt; collisions only allow readback."""
        if not isinstance(operation_key, str) or re.fullmatch(r"[0-9a-f]{64}", operation_key) is None:
            raise ValueError("retirement attempt requires a canonical operation key")
        payload = f"dpone.publication-retirement-attempt.v1\n{operation_key}\n".encode()
        with self._lock:
            descriptor = self._open_admitted()
            try:
                try:
                    write_private_plan_at(descriptor, f"{operation_key}.attempt", payload)
                except FileExistsError:
                    self.require_ready()
                    return False
                self.require_ready()
                return True
            finally:
                os.close(descriptor)
