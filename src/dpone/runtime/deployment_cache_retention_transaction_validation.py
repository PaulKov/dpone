"""Projection and managed-path validation for retention transactions."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Protocol


class RetentionProjectionDetails(Protocol):
    deployment_id: str


class RetentionProjectionValidator(Protocol):
    def validate_details(self, deployment_dir: str | Path, *, environment: str) -> RetentionProjectionDetails: ...

    def validate_activation_details(
        self,
        deployment_dir: str | Path,
        *,
        environment: str,
    ) -> RetentionProjectionDetails: ...

    def validate_detached_details(
        self,
        deployment_dir: str | Path,
        *,
        environment: str,
        expected_deployment_id: str,
    ) -> RetentionProjectionDetails: ...


class DeploymentCacheRetentionTransactionValidator:
    """Validate transaction projections without owning recovery transitions."""

    def __init__(
        self,
        cache_root: Path,
        *,
        projection_validator: RetentionProjectionValidator,
        recovery_error: Callable[[str], Exception],
        validation_error: Callable[[str, str, Path], Exception],
    ) -> None:
        self._root = cache_root
        self._projection_validator = projection_validator
        self._recovery_error = recovery_error
        self._validation_error = validation_error

    def validate_original(self, path: Path, *, deployment_id: str, environment: str) -> None:
        projection = self._projection_validator.validate_details(path, environment=environment)
        self._require_deployment_id(projection.deployment_id, deployment_id, path)

    def validate_activation(self, path: Path, *, deployment_id: str, environment: str) -> None:
        projection = self._projection_validator.validate_activation_details(path, environment=environment)
        self._require_deployment_id(projection.deployment_id, deployment_id, path)

    def validate_detached(self, path: Path, *, deployment_id: str, environment: str) -> None:
        projection = self._projection_validator.validate_detached_details(
            path,
            environment=environment,
            expected_deployment_id=deployment_id,
        )
        self._require_deployment_id(projection.deployment_id, deployment_id, path)

    def validate_control_paths(
        self,
        original: Path,
        detached: Path,
        activation: Path,
        *,
        environment: str,
    ) -> None:
        expected_roots = (
            (original, self._root / "deployments" / environment),
            (detached, self._root / ".retention-trash"),
            (activation, self._root / "activations" / environment),
        )
        for path, root in expected_roots:
            try:
                path.resolve(strict=False).relative_to(root.resolve(strict=False))
            except ValueError as exc:
                raise self._recovery_error("retention transaction path escapes its managed root") from exc

    def _require_deployment_id(self, actual: str, expected: str, path: Path) -> None:
        if actual != expected:
            raise self._validation_error(
                "DPONE_DEPLOYMENT_ID_MISMATCH",
                "retention transaction projection differs from its reviewed deployment",
                path,
            )


__all__ = ["DeploymentCacheRetentionTransactionValidator"]
