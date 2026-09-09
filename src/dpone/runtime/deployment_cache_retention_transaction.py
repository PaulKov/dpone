"""Crash-safe transaction for destructive deployment-cache retention."""

from __future__ import annotations

from pathlib import Path
from typing import NoReturn, cast
from uuid import uuid4

from dpone.runtime.deployment_cache_activation import DeploymentCacheActivationSnapshotter
from dpone.runtime.deployment_cache_activation_restore import DeploymentCacheActivationRestorer
from dpone.runtime.deployment_cache_common import (
    DeploymentCacheError,
    fsync_directory,
    remove_path,
    remove_sealed_directory,
)
from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator
from dpone.runtime.deployment_cache_retention_contracts import (
    DeploymentCacheRetentionApplyError,
    RetentionTransactionOccurrence,
)
from dpone.runtime.deployment_cache_retention_deletion import (
    DetachedDeployment,
    DirectoryIdentity,
    detach_validated_directory,
    restore_detached_directory,
)
from dpone.runtime.deployment_cache_retention_detached_recovery import DeploymentCacheDetachedRecovery
from dpone.runtime.deployment_cache_retention_journal import DeploymentCacheRetentionJournal
from dpone.runtime.deployment_cache_retention_state_codec import bind_retention_transaction
from dpone.runtime.deployment_cache_retention_transaction_validation import (
    DeploymentCacheRetentionTransactionValidator,
    RetentionProjectionValidator,
)


class DeploymentCacheRetentionTransactionCoordinator:
    """Delete one deployment with a durable WAL and an intact activation backup."""

    def __init__(self, cache_root: Path, *, validator: DeploymentCacheProjectionValidator) -> None:
        self._root = cache_root
        self._activation_snapshotter = DeploymentCacheActivationSnapshotter(cache_root, validator=validator)
        self._activation_restorer = DeploymentCacheActivationRestorer(cache_root, validator=validator)
        self._journal = DeploymentCacheRetentionJournal(cache_root)
        self._transaction_validator = DeploymentCacheRetentionTransactionValidator(
            cache_root,
            projection_validator=cast(RetentionProjectionValidator, validator),
            recovery_error=self._journal.error,
            validation_error=_transaction_validation_error,
        )
        self._detached_recovery = DeploymentCacheDetachedRecovery(
            cache_root,
            validator=self._transaction_validator,
            journal=self._journal,
        )

    def delete(
        self,
        path: Path,
        *,
        expected_identity: DirectoryIdentity,
        deployment_id: str,
        environment: str,
        operation_id: str,
    ) -> None:
        trash_root = self._root / ".retention-trash"
        detached_path = trash_root / f"{path.name}.{uuid4().hex}"
        activation_path = self._activation_path(environment, deployment_id)
        if activation_path.exists():
            self._transaction_validator.validate_activation(
                activation_path, deployment_id=deployment_id, environment=environment
            )
        transaction = bind_retention_transaction(
            {
                "deployment_id": deployment_id,
                "environment": environment,
                "phase": "prepared",
                "original_path": path.as_posix(),
                "detached_path": detached_path.as_posix(),
                "activation_path": activation_path.as_posix(),
                "expected_device": expected_identity.device,
                "expected_inode": expected_identity.inode,
                "operation_id": operation_id,
            }
        )
        self._journal.commit(transaction)
        try:
            if not activation_path.exists():
                self._activation_snapshotter.prepare(path, environment=environment)
            self._transaction_validator.validate_activation(
                activation_path, deployment_id=deployment_id, environment=environment
            )
        except (DeploymentCacheError, DeploymentCacheRetentionApplyError, OSError) as exc:
            self._raise_pre_detach_failure(transaction, activation_path=activation_path, cause=exc)
        detached = detach_validated_directory(
            path,
            expected_identity=expected_identity,
            trash_root=trash_root,
            detached_path=detached_path,
        )
        self._journal.advance(transaction, "detached")
        try:
            self._transaction_validator.validate_detached(
                detached.detached_path, deployment_id=deployment_id, environment=environment
            )
            self._journal.advance(transaction, "deletion_started")
            remove_path(detached.detached_path)
            fsync_directory(trash_root)
            self._journal.advance(transaction, "deployment_deleted")
            if activation_path.exists():
                remove_sealed_directory(activation_path)
            fsync_directory(activation_path.parent)
            self._journal.advance(transaction, "activation_deleted")
            self._journal.advance(transaction, "committed")
        except (DeploymentCacheError, DeploymentCacheRetentionApplyError, OSError) as exc:
            failed_phase = str(transaction["phase"])
            recovery_error: DeploymentCacheRetentionApplyError | None = None
            try:
                restored = self._recover_transaction(transaction)
                if transaction["phase"] == "committed":
                    return
            except DeploymentCacheRetentionApplyError as recovery_exc:
                restored = False
                recovery_error = recovery_exc
            quarantined_path = (
                None if restored or not detached.detached_path.exists() else detached.detached_path.as_posix()
            )
            raise DeploymentCacheRetentionApplyError(
                "DPONE_DEPLOYMENT_CACHE_GC_DELETE_FAILED",
                "deployment cache deletion transaction did not commit",
                path=detached.detached_path.as_posix(),
                details={
                    "failed_step": failed_phase,
                    "state_may_have_changed": True,
                    "restored": restored,
                    "restored_deployment_ids": [deployment_id] if restored else [],
                    "quarantined_path": quarantined_path,
                    "quarantined_paths": [quarantined_path] if quarantined_path is not None else [],
                    "recovery_error_code": recovery_error.code if recovery_error is not None else None,
                    "recovery_cause_code": (
                        recovery_error.details.get("cause_code") if recovery_error is not None else None
                    ),
                    "recovery_cause_path": (
                        recovery_error.details.get("cause_path") if recovery_error is not None else None
                    ),
                },
            ) from exc

    def acknowledge_committed_receipt(
        self,
        *,
        operation_id: str,
        deployment_ids: tuple[str, ...],
    ) -> None:
        """Prune success WAL only after the matching receipt commit is durable."""

        self._journal.prune_committed(operation_id=operation_id, deployment_ids=deployment_ids)

    def _raise_pre_detach_failure(
        self,
        transaction: dict[str, object],
        *,
        activation_path: Path,
        cause: BaseException,
    ) -> NoReturn:
        recovery_error: DeploymentCacheRetentionApplyError | None = None
        try:
            restored = self._recover_transaction(transaction)
        except DeploymentCacheRetentionApplyError as exc:
            restored = False
            recovery_error = exc
        details: dict[str, object] = {
            "failed_step": "activation_snapshot",
            "state_may_have_changed": True,
            "restored": restored,
            "restored_deployment_ids": [str(transaction["deployment_id"])] if restored else [],
            "recovery_required": recovery_error is not None,
            "cause_code": getattr(cause, "code", type(cause).__name__),
        }
        if recovery_error is not None:
            details.update(
                recovery_error_code=recovery_error.code,
                pending_deployment_ids=recovery_error.details.get("pending_deployment_ids", []),
                quarantined_paths=recovery_error.details.get("quarantined_paths", []),
            )
        raise DeploymentCacheRetentionApplyError(
            "DPONE_DEPLOYMENT_CACHE_ACTIVATION_SNAPSHOT_FAILED",
            "deployment cache activation snapshot was not ready before destructive retention",
            path=activation_path.as_posix(),
            details=details,
        ) from cause

    def recover(self, *, environment: str) -> tuple[RetentionTransactionOccurrence, ...]:
        transactions = self._journal.read_transactions()
        for transaction in transactions.values():
            if transaction["environment"] != environment:
                raise self._journal.error("recovery journal belongs to another environment")
            phase = str(transaction["phase"])
            if phase in {"committed", "restored"}:
                continue
            self._recover_transaction(transaction)
        self._detached_recovery.recover(
            environment=environment,
            recover_transaction=self._recover_transaction,
        )
        return self._journal.unacknowledged_restored(environment=environment)

    def acknowledge_recovery(
        self,
        *,
        environment: str,
        restored_occurrences: tuple[RetentionTransactionOccurrence, ...],
    ) -> None:
        self._journal.acknowledge_restored(
            environment=environment,
            transaction_ids=tuple(item.transaction_id for item in restored_occurrences),
        )

    def committed_deployment_ids(self, *, environment: str, operation_id: str) -> tuple[str, ...]:
        """Return terminal WAL outcomes still available for receipt replay."""

        return tuple(
            sorted(
                {
                    item.deployment_id
                    for item in self.committed_occurrences(
                        environment=environment,
                        operation_id=operation_id,
                    )
                }
            )
        )

    def committed_occurrences(
        self,
        *,
        environment: str,
        operation_id: str,
    ) -> tuple[RetentionTransactionOccurrence, ...]:
        transactions = self._journal.read_transactions()
        return tuple(
            sorted(
                (
                    RetentionTransactionOccurrence(
                        transaction_id=str(transaction["transaction_id"]),
                        deployment_id=str(transaction["deployment_id"]),
                        operation_id=str(transaction["operation_id"]),
                        original_path=str(transaction["original_path"]),
                    )
                    for transaction in transactions.values()
                    if transaction["environment"] == environment
                    and transaction["phase"] == "committed"
                    and transaction.get("operation_id") == operation_id
                ),
                key=lambda item: (item.deployment_id, item.transaction_id),
            )
        )

    def unbound_committed_deployment_ids(self, *, environment: str) -> tuple[str, ...]:
        """Return migrated v1 commits that cannot prove ownership by one receipt."""

        transactions = self._journal.read_transactions()
        return tuple(
            sorted(
                str(transaction["deployment_id"])
                for transaction in transactions.values()
                if transaction["environment"] == environment
                and transaction["phase"] == "committed"
                and transaction.get("operation_id") is None
            )
        )

    def _recover_transaction(self, transaction: dict[str, object]) -> bool:
        phase = str(transaction["phase"])
        original = Path(str(transaction["original_path"]))
        detached = Path(str(transaction["detached_path"]))
        activation = Path(str(transaction["activation_path"]))
        deployment_id = str(transaction["deployment_id"])
        environment = str(transaction["environment"])
        self._transaction_validator.validate_control_paths(original, detached, activation, environment=environment)
        try:
            if phase == "prepared":
                if original.exists() and not detached.exists():
                    self._journal.advance(transaction, "restored")
                    return True
                phase = "detached"
            if phase == "detached":
                expected_device = transaction["expected_device"]
                expected_inode = transaction["expected_inode"]
                if not isinstance(expected_device, int) or not isinstance(expected_inode, int):
                    raise self._journal.error("retention transaction inode identity is invalid")
                restored = restore_detached_directory(
                    DetachedDeployment(
                        original_path=original,
                        detached_path=detached,
                        identity=DirectoryIdentity(
                            device=expected_device,
                            inode=expected_inode,
                        ),
                    )
                )
                if not restored:
                    return self._journal.block(transaction)
                self._transaction_validator.validate_original(
                    original, deployment_id=deployment_id, environment=environment
                )
                self._journal.advance(transaction, "restored")
                return True
            if phase in {"deletion_started", "blocked"}:
                return self._recover_started_deletion(
                    transaction,
                    original=original,
                    detached=detached,
                    activation=activation,
                    deployment_id=deployment_id,
                    environment=environment,
                )
            if phase == "deployment_deleted":
                if detached.exists():
                    return self._journal.block(transaction)
                fsync_directory(detached.parent)
                if activation.exists():
                    remove_sealed_directory(activation)
                fsync_directory(activation.parent)
                self._journal.advance(transaction, "activation_deleted")
                phase = "activation_deleted"
            if phase == "activation_deleted":
                self._journal.advance(transaction, "committed")
                return False
            if phase == "committed":
                return False
        except (DeploymentCacheError, DeploymentCacheRetentionApplyError, OSError) as exc:
            self._journal.block(transaction)
            error = self._journal.error("retention transaction recovery failed")
            error.details["cause_code"] = getattr(exc, "code", type(exc).__name__)
            if isinstance(exc, OSError) and exc.filename is not None:
                error.details["cause_path"] = str(exc.filename)
            raise error from exc
        self._journal.block(transaction)
        raise self._journal.error("retention transaction has an unsupported recovery phase")

    def _restore_from_activation(
        self,
        original: Path,
        activation: Path,
        *,
        deployment_id: str,
        environment: str,
    ) -> None:
        self._activation_restorer.restore(
            activation,
            original,
            deployment_id=deployment_id,
            environment=environment,
        )

    def _recover_started_deletion(
        self,
        transaction: dict[str, object],
        *,
        original: Path,
        detached: Path,
        activation: Path,
        deployment_id: str,
        environment: str,
    ) -> bool:
        expected_device = transaction.get("expected_device")
        expected_inode = transaction.get("expected_inode")
        if detached.exists() and isinstance(expected_device, int) and isinstance(expected_inode, int):
            try:
                self._transaction_validator.validate_detached(
                    detached, deployment_id=deployment_id, environment=environment
                )
                restored = restore_detached_directory(
                    DetachedDeployment(
                        original_path=original,
                        detached_path=detached,
                        identity=DirectoryIdentity(device=expected_device, inode=expected_inode),
                    )
                )
                if restored:
                    self._transaction_validator.validate_original(
                        original, deployment_id=deployment_id, environment=environment
                    )
                    self._journal.advance(transaction, "restored")
                    return True
            except (DeploymentCacheError, DeploymentCacheRetentionApplyError, OSError):
                pass
        self._restore_from_activation(
            original,
            activation,
            deployment_id=deployment_id,
            environment=environment,
        )
        if detached.exists():
            remove_path(detached)
        fsync_directory(detached.parent)
        self._journal.advance(transaction, "restored")
        return True

    def _activation_path(self, environment: str, deployment_id: str) -> Path:
        return self._root / "activations" / environment / deployment_id.replace(":", "-", 1)


def _transaction_validation_error(code: str, message: str, path: Path) -> Exception:
    return DeploymentCacheRetentionApplyError(code, message, path=path.as_posix())


__all__ = ["DeploymentCacheRetentionTransactionCoordinator"]
