"""Crash-safe restoration from an intact sealed deployment activation."""

from __future__ import annotations

import os
import stat
from pathlib import Path

from dpone.runtime.deployment_cache_activation import (
    DEFAULT_MAX_ACTIVATION_BYTES,
    activation_tree_fingerprint,
    copy_regular_activation_tree,
    remove_sealed_activation,
    seal_activation_tree,
)
from dpone.runtime.deployment_cache_common import DeploymentCacheError, fsync_directory
from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator
from dpone.runtime.deployment_cache_retention_contracts import DeploymentCacheRetentionApplyError


class DeploymentCacheActivationRestorer:
    """Publish a validated read-only copy without mutating the activation."""

    def __init__(
        self,
        cache_root: Path,
        *,
        validator: DeploymentCacheProjectionValidator,
        max_snapshot_bytes: int = DEFAULT_MAX_ACTIVATION_BYTES,
    ) -> None:
        self._root = cache_root
        self._validator = validator
        self._max_snapshot_bytes = max_snapshot_bytes

    def restore(
        self,
        activation: Path,
        original: Path,
        *,
        deployment_id: str,
        environment: str,
    ) -> None:
        if original.exists():
            self._seal_root(original)
            self._validate_original(original, deployment_id=deployment_id, environment=environment)
            return
        self._validate_activation(activation, deployment_id=deployment_id, environment=environment)
        self._require_sealed_root(activation)
        source_fingerprint = self._fingerprint(activation)
        staging = original.parent / f".{original.name}.restore"
        original.parent.mkdir(parents=True, exist_ok=True)
        if staging.exists():
            self._publish_existing_staging(
                staging,
                original,
                expected_fingerprint=source_fingerprint,
                deployment_id=deployment_id,
                environment=environment,
            )
            return
        staging.mkdir(mode=0o700)
        try:
            copy_regular_activation_tree(
                activation,
                staging,
                cache_root=self._root,
                max_bytes=self._max_snapshot_bytes,
            )
            seal_activation_tree(staging, cache_root=self._root)
            if self._fingerprint(staging) != source_fingerprint or self._fingerprint(activation) != source_fingerprint:
                raise self._error("activation changed while its restore staging was prepared", activation)
            self._publish(staging, original)
            self._validate_original(original, deployment_id=deployment_id, environment=environment)
        except BaseException as exc:
            if staging.exists():
                try:
                    remove_sealed_activation(staging)
                except OSError as cleanup_exc:
                    _record_cleanup_failure(exc, staging=staging, cleanup_exc=cleanup_exc)
            raise

    def _publish_existing_staging(
        self,
        staging: Path,
        original: Path,
        *,
        expected_fingerprint: str,
        deployment_id: str,
        environment: str,
    ) -> None:
        self._require_sealed_root(staging)
        if self._fingerprint(staging) != expected_fingerprint:
            raise self._error("existing restore staging differs from the activation", staging)
        self._publish(staging, original)
        self._validate_original(original, deployment_id=deployment_id, environment=environment)

    def _publish(self, staging: Path, original: Path) -> None:
        # macOS requires write permission on a non-empty directory being renamed.
        # The WAL remains in deletion_started until replay reseals and validates it.
        staging.chmod(0o755, follow_symlinks=False)
        fsync_directory(staging)
        os.replace(staging, original)
        fsync_directory(original.parent)
        self._seal_root(original)

    def _fingerprint(self, path: Path) -> str:
        return activation_tree_fingerprint(path, cache_root=self._root, max_bytes=self._max_snapshot_bytes)

    def _validate_activation(self, path: Path, *, deployment_id: str, environment: str) -> None:
        projection = self._validator.validate_activation_details(path, environment=environment)
        self._require_identity(projection.deployment_id, deployment_id, path)

    def _validate_original(self, path: Path, *, deployment_id: str, environment: str) -> None:
        projection = self._validator.validate_details(path, environment=environment)
        self._require_identity(projection.deployment_id, deployment_id, path)

    @staticmethod
    def _require_identity(actual: str, expected: str, path: Path) -> None:
        if actual != expected:
            raise DeploymentCacheRetentionApplyError(
                "DPONE_DEPLOYMENT_ID_MISMATCH",
                "restored activation differs from the reviewed deployment",
                path=path.as_posix(),
            )

    @staticmethod
    def _require_sealed_root(path: Path) -> None:
        if stat.S_IMODE(path.lstat().st_mode) != 0o555:
            raise DeploymentCacheActivationRestorer._error("activation restore tree is not sealed", path)

    @staticmethod
    def _seal_root(path: Path) -> None:
        if stat.S_IMODE(path.lstat().st_mode) == 0o555:
            return
        path.chmod(0o555, follow_symlinks=False)
        fsync_directory(path)

    @staticmethod
    def _error(message: str, path: Path) -> DeploymentCacheError:
        return DeploymentCacheError("DPONE_DEPLOYMENT_ACTIVATION_FAILED", message, path=path.as_posix())


def _record_cleanup_failure(exc: BaseException, *, staging: Path, cleanup_exc: OSError) -> None:
    note = f"restore staging cleanup failed: {cleanup_exc.__class__.__name__}"
    add_note = getattr(exc, "add_note", None)
    if callable(add_note):
        add_note(note)
    if isinstance(exc, (DeploymentCacheError, DeploymentCacheRetentionApplyError)):
        exc.details.update(
            cleanup_failed_paths=[staging.as_posix()],
            cleanup_error_code=cleanup_exc.__class__.__name__,
            recovery_required=True,
            state_may_have_changed=True,
        )


__all__ = ["DeploymentCacheActivationRestorer"]
