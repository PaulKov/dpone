"""Durable commit protocol for one verified Airflow cache activation."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from uuid import uuid4

from dpone.runtime.deployment_cache_audit import append_promotion_audit
from dpone.runtime.deployment_cache_common import (
    DeploymentCacheError,
    atomic_write_json,
    fsync_directory,
    remove_path,
)


class DeploymentCacheCommitter:
    """Commit authorization metadata before atomically switching ``current``."""

    def __init__(self, cache_root: Path) -> None:
        self._cache_root = cache_root

    def commit(self, *, deployment_path: Path, pointer: dict[str, Any]) -> tuple[Path, Path]:
        current = self._cache_root / "current"
        if current.exists() and not current.is_symlink():
            raise DeploymentCacheError(
                "DPONE_CACHE_PROMOTION_WRITE_FAILED",
                "current deployment entry must be an atomic symlink",
                path=current.as_posix(),
            )
        temporary_current = self._prepare_current_symlink(deployment_path)
        pointer_path = self._cache_root / "current-pointer.json"
        audit_path = self._cache_root / "current-pointer-audit.jsonl"
        try:
            atomic_write_json(pointer_path, pointer)
        except OSError as exc:
            _discard_path(temporary_current)
            raise _write_failure(
                "current pointer metadata could not be prepared durably; cache recovery is required",
                path=pointer_path,
                failed_step="prepare_pointer",
            ) from exc
        try:
            append_promotion_audit(audit_path, pointer)
        except OSError as exc:
            _discard_path(temporary_current)
            raise _write_failure(
                "promotion authorization audit could not be written; cache recovery is required",
                path=audit_path,
                failed_step="append_audit",
            ) from exc
        try:
            self._activate_current(temporary_current, current)
        except OSError as exc:
            _discard_path(temporary_current)
            raise _write_failure(
                "current deployment could not be activated; cache recovery is required",
                path=current,
                failed_step="activate_current",
            ) from exc
        return current, pointer_path

    def _activate_current(self, temporary_current: Path, current: Path) -> None:
        os.replace(temporary_current, current)
        fsync_directory(self._cache_root)

    def _prepare_current_symlink(self, deployment_path: Path) -> Path:
        self._cache_root.mkdir(parents=True, exist_ok=True)
        target = os.path.relpath(deployment_path, self._cache_root)
        for _ in range(8):
            temporary = self._cache_root / f".current.{uuid4().hex}.tmp"
            try:
                temporary.symlink_to(target, target_is_directory=True)
            except FileExistsError:
                continue
            except OSError as exc:
                raise _write_failure(
                    "current deployment entry could not be prepared",
                    path=(self._cache_root / "current").as_posix(),
                    failed_step="prepare_current",
                    state_may_have_changed=True,
                    recovery_required=False,
                ) from exc
            return temporary
        raise _write_failure(
            "unique current deployment entry could not be prepared",
            path=(self._cache_root / "current").as_posix(),
            failed_step="prepare_current",
            state_may_have_changed=True,
            recovery_required=False,
        )


def _discard_path(path: Path) -> None:
    try:
        remove_path(path)
    except OSError:
        pass


def _write_failure(
    message: str,
    *,
    path: Path | str,
    failed_step: str,
    state_may_have_changed: bool = True,
    recovery_required: bool = True,
) -> DeploymentCacheError:
    rendered_path = path.as_posix() if isinstance(path, Path) else path
    return DeploymentCacheError(
        "DPONE_CACHE_PROMOTION_WRITE_FAILED",
        message,
        path=rendered_path,
        details={
            "failed_step": failed_step,
            "state_may_have_changed": state_may_have_changed,
            "recovery_required": recovery_required,
        },
    )


__all__ = ["DeploymentCacheCommitter"]
