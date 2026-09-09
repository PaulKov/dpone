"""Recovery of retention-trash entries created before WAL durability."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Protocol

from dpone.runtime.deployment_cache_retention_contracts import DeploymentCacheRetentionApplyError
from dpone.runtime.deployment_cache_retention_deletion import directory_identity
from dpone.runtime.deployment_cache_retention_journal import DeploymentCacheRetentionJournal
from dpone.runtime.deployment_cache_retention_state_codec import bind_retention_transaction


class DetachedRecoveryValidator(Protocol):
    def validate_detached(self, path: Path, *, deployment_id: str, environment: str) -> None: ...


class DeploymentCacheDetachedRecovery:
    """Journal and restore only safe, validated unjournaled detach occurrences."""

    def __init__(
        self,
        cache_root: Path,
        *,
        validator: DetachedRecoveryValidator,
        journal: DeploymentCacheRetentionJournal,
    ) -> None:
        self._root = cache_root
        self._validator = validator
        self._journal = journal

    def recover(
        self,
        *,
        environment: str,
        recover_transaction: Callable[[dict[str, object]], bool],
    ) -> tuple[str, ...]:
        trash_root = self._root / ".retention-trash"
        if not trash_root.exists():
            return ()
        inventory = tuple(sorted(trash_root.iterdir(), key=lambda item: item.name))
        restored: list[str] = []
        for path in inventory:
            deployment_id = _deployment_id_from_detached(path)
            identity = directory_identity(path)
            self._validator.validate_detached(path, deployment_id=deployment_id, environment=environment)
            transaction = bind_retention_transaction(
                {
                    "deployment_id": deployment_id,
                    "environment": environment,
                    "phase": "detached",
                    "original_path": self._deployment_path(environment, deployment_id).as_posix(),
                    "detached_path": path.as_posix(),
                    "activation_path": self._activation_path(environment, deployment_id).as_posix(),
                    "expected_device": identity.device,
                    "expected_inode": identity.inode,
                    "operation_id": None,
                }
            )
            self._journal.commit(transaction)
            if not recover_transaction(transaction):
                raise self._journal.error("unjournaled retention detach could not be restored")
            restored.append(deployment_id)
        return tuple(restored)

    def _activation_path(self, environment: str, deployment_id: str) -> Path:
        return self._root / "activations" / environment / deployment_id.replace(":", "-", 1)

    def _deployment_path(self, environment: str, deployment_id: str) -> Path:
        return self._root / "deployments" / environment / deployment_id.replace(":", "-", 1)


def _deployment_id_from_detached(path: Path) -> str:
    directory_name, separator, nonce = path.name.partition(".")
    deployment_id = directory_name.replace("sha256-", "sha256:", 1)
    if path.is_symlink() or not separator or not nonce or len(deployment_id) != 71:
        raise DeploymentCacheRetentionApplyError(
            "DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_REQUIRED",
            "retention trash contains an unsafe unjournaled entry",
            path=path.as_posix(),
        )
    return deployment_id


__all__ = ["DeploymentCacheDetachedRecovery"]
