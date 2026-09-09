"""Local lock manager for orchestrated dpone runs."""

from __future__ import annotations

import json
import os
import re
import time
import uuid
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class RunLock:
    key: str
    token: str
    path: Path
    acquired: bool
    stale_replaced: bool = False
    blocker: str | None = None


class LocalRunLockManager:
    """Atomic local-file lock manager for one host/CI worker."""

    def __init__(self, lock_dir: str | Path) -> None:
        self._lock_dir = Path(lock_dir)

    @property
    def lock_dir(self) -> Path:
        return self._lock_dir

    def acquire(self, key: str, *, ttl_seconds: int = 3600) -> RunLock:
        self._lock_dir.mkdir(parents=True, exist_ok=True)
        lock_path = self._path(key)
        stale_replaced = self._remove_stale(lock_path, ttl_seconds)
        token = uuid.uuid4().hex
        payload = {"key": key, "token": token, "created_at_epoch": time.time()}
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            return RunLock(
                key=key,
                token="",
                path=lock_path,
                acquired=False,
                stale_replaced=stale_replaced,
                blocker="lock.concurrent_run",
            )
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True)
            handle.write("\n")
        return RunLock(key=key, token=token, path=lock_path, acquired=True, stale_replaced=stale_replaced)

    def release(self, lock: RunLock) -> None:
        if not lock.acquired or not lock.path.exists():
            return
        payload = self._payload(lock.path)
        if payload.get("token") != lock.token:
            return
        lock.path.unlink(missing_ok=True)

    def _path(self, key: str) -> Path:
        safe_key = re.sub(r"[^A-Za-z0-9_.-]+", "_", key).strip("._") or "default"
        return self._lock_dir / f"{safe_key}.lock.json"

    def _remove_stale(self, path: Path, ttl_seconds: int) -> bool:
        if not path.exists():
            return False
        payload = self._payload(path)
        raw_created_at = payload.get("created_at_epoch", 0.0)
        created_at = raw_created_at if isinstance(raw_created_at, float | int) else 0.0
        if created_at > 0 and time.time() - created_at <= max(1, ttl_seconds):
            return False
        path.unlink(missing_ok=True)
        return True

    @staticmethod
    def _payload(path: Path) -> dict[str, object]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}
        return payload if isinstance(payload, dict) else {}
