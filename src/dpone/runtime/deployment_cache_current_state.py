"""Consistent reads of the local deployment cache activation state."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from dpone.contracts.airflow_deployment import current_pointer_violation
from dpone.runtime.deployment_cache_common import (
    DeploymentCacheError,
    read_regular_json_object,
    resolve_relative_current_symlink,
)

if TYPE_CHECKING:
    from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator

_DIGEST_PREFIX = "sha256-"
_DIGEST_HEX_LENGTH = 64
_CURRENT_LAYOUTS = {"activations", "deployments"}


class DeploymentCacheCurrentState:
    """Read active identities while treating split control state as recoverable corruption."""

    def __init__(self, cache_root: Path) -> None:
        self._cache_root = cache_root

    def active_deployment_id(self, *, expected_environment: str | None = None) -> str | None:
        current_path = self._cache_root / "current"
        pointer_path = self._cache_root / "current-pointer.json"
        current_present = current_path.exists() or current_path.is_symlink()
        pointer_present = pointer_path.exists() or pointer_path.is_symlink()
        if not current_present and not pointer_present:
            return None
        if not current_present or not pointer_present:
            raise _recovery_required(current_path)
        try:
            current_target = resolve_relative_current_symlink(self._cache_root)
            physical_id = _physical_deployment_id(self._cache_root, current_target)
            pointer = read_regular_json_object(
                pointer_path,
                missing_code="DPONE_CURRENT_POINTER_NOT_FOUND",
                invalid_code="DPONE_CURRENT_POINTER_INVALID",
                label="current pointer",
                root=self._cache_root,
            )
            current = read_regular_json_object(
                current_target / "deployment.json",
                missing_code="DPONE_DEPLOYMENT_NOT_FOUND",
                invalid_code="DPONE_DEPLOYMENT_INVALID",
                label="current deployment",
                root=self._cache_root,
            )
        except (DeploymentCacheError, UnicodeError, OSError) as exc:
            raise _recovery_required(current_path) from exc
        pointer_id = str(pointer.get("deployment_id") or "")
        current_id = str(current.get("deployment_id") or "")
        pointer_environment = str(pointer.get("environment") or "")
        current_environment = str(current.get("environment") or "")
        pointer_release_id = str(pointer.get("release_id") or "")
        current_release_id = str(current.get("release_ref") or "")
        pointer_violation = current_pointer_violation(pointer, expected_environment=expected_environment)
        if (
            pointer_violation is not None
            or not pointer_environment
            or pointer_environment != current_environment
            or expected_environment is not None
            and pointer_environment != expected_environment
            or not pointer_id
            or physical_id is None
            or physical_id != current_id
            or pointer_id != current_id
            or pointer_release_id != current_release_id
        ):
            raise _recovery_required(current_path)
        return current_id

    def actual_deployment_id(self) -> str | None:
        """Return only the current symlink identity for recovery compare-and-swap."""

        current_path = self._cache_root / "current"
        if not (current_path.exists() or current_path.is_symlink()):
            return None
        physical_id: str | None = None
        try:
            current_target = resolve_relative_current_symlink(self._cache_root)
            physical_id = _physical_deployment_id(self._cache_root, current_target)
            current = read_regular_json_object(
                current_target / "deployment.json",
                missing_code="DPONE_DEPLOYMENT_NOT_FOUND",
                invalid_code="DPONE_DEPLOYMENT_INVALID",
                label="current deployment",
                root=self._cache_root,
            )
        except (DeploymentCacheError, UnicodeError, OSError):
            return physical_id
        deployment_id = str(current.get("deployment_id") or "")
        return physical_id or deployment_id or None

    def validated_active_deployment_id(
        self,
        *,
        expected_environment: str,
        projection_validator: DeploymentCacheProjectionValidator,
    ) -> str | None:
        """Return active identity only after full projection and artifact validation."""

        deployment_id = self.active_deployment_id(expected_environment=expected_environment)
        if deployment_id is None:
            return None
        try:
            current_target = resolve_relative_current_symlink(self._cache_root)
            projection = projection_validator.validate_current_details(
                current_target,
                environment=expected_environment,
            )
        except DeploymentCacheError as exc:
            raise _recovery_required(Path(exc.path) if exc.path else self._cache_root / "current") from exc
        if projection.deployment_id != deployment_id:
            raise _recovery_required(current_target)
        return deployment_id


def _physical_deployment_id(cache_root: Path, current_target: Path) -> str | None:
    """Derive the CAS identity from one canonical activation/deployment path."""

    try:
        parts = current_target.relative_to(cache_root).parts
    except ValueError:
        return None
    if len(parts) != 3 or parts[0] not in _CURRENT_LAYOUTS or not parts[1]:
        return None
    digest = parts[2]
    hexadecimal = digest.removeprefix(_DIGEST_PREFIX)
    if not digest.startswith(_DIGEST_PREFIX) or len(hexadecimal) != _DIGEST_HEX_LENGTH:
        return None
    if any(character not in "0123456789abcdef" for character in hexadecimal):
        return None
    return f"sha256:{hexadecimal}"


def _recovery_required(path: Path) -> DeploymentCacheError:
    return DeploymentCacheError(
        "DPONE_DEPLOYMENT_CACHE_RECOVERY_REQUIRED",
        "current deployment is inconsistent or failed integrity validation; run cache recovery",
        path=path.as_posix(),
    )


__all__ = ["DeploymentCacheCurrentState"]
