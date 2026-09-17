"""Fail-closed DEV-only target admission before cache pointer mutation."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime
from pathlib import Path
from typing import Protocol

from dpone.runtime.deployment_cache_common import DeploymentCacheError, read_regular_json_object
from dpone.runtime.deployment_cache_integrity import release_content_id
from dpone.runtime.deployment_cache_models import ValidatedDeploymentProjection

_DEVELOPMENT_RELEASE_SCHEMA = "dpone.dbt-release-set.development.v1"
_DEVELOPMENT_COMPOSITION_PROFILE = "development_workspace_delivery_v1"


class DevelopmentTargetAdmission(Protocol):
    def require(
        self,
        *,
        authority_projection: object,
        operation: str,
        release_id: str,
        deployment_id: str,
        target_environment: str,
        target_trust_tier: str,
        now: datetime,
    ) -> None: ...


class DevelopmentTargetAdmissionVerifier(Protocol):
    def require_current(self, admission: DevelopmentTargetAdmission, *, now: datetime) -> None: ...


class DevelopmentActivationAdmissionGate:
    """Recheck one injected admission against current protected target state."""

    def __init__(
        self,
        cache_root: Path,
        *,
        admission: DevelopmentTargetAdmission | None,
        admission_verifier: DevelopmentTargetAdmissionVerifier | None,
        clock: Callable[[], datetime],
    ) -> None:
        self._cache_root = cache_root
        self._admission = admission
        self._admission_verifier = admission_verifier
        self._clock = clock

    def require(self, projection: ValidatedDeploymentProjection, *, environment: str) -> None:
        require_development_activation_admission(
            self._cache_root,
            projection,
            environment=environment,
            admission=self._admission,
            admission_verifier=self._admission_verifier,
            checked_at=self._clock(),
        )


def require_development_activation_admission(
    cache_root: Path,
    projection: ValidatedDeploymentProjection,
    *,
    environment: str,
    admission: DevelopmentTargetAdmission | None,
    admission_verifier: DevelopmentTargetAdmissionVerifier | None,
    checked_at: datetime,
) -> None:
    """Require a current operation receipt before activation staging or CAS."""

    release_path = cache_root / "releases" / projection.release_id.replace(":", "-", 1) / "release-set.json"
    release = read_regular_json_object(
        release_path,
        missing_code="DPONE_RELEASE_NOT_FOUND",
        invalid_code="DPONE_RELEASE_INVALID",
        label="release-set",
        root=cache_root,
    )
    if release.get("release_id") != projection.release_id or release_content_id(release) != projection.release_id:
        raise DeploymentCacheError(
            "DPONE_RELEASE_FINGERPRINT_MISMATCH",
            "release-set content does not match its content-addressed identity",
            path=release_path.as_posix(),
        )
    try:
        authority_projection = _development_authority_projection(release)
        if authority_projection is None:
            return
        trust_tier = projection.deployment.get("trust_tier")
        if admission is None or admission_verifier is None or not isinstance(trust_tier, str):
            raise ValueError("development target admission is absent")
        admission_verifier.require_current(admission, now=checked_at)
        admission.require(
            authority_projection=authority_projection,
            operation="activate",
            release_id=projection.release_id,
            deployment_id=projection.deployment_id,
            target_environment=environment,
            target_trust_tier=trust_tier,
            now=checked_at,
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise DeploymentCacheError(
            "DPONE_DEVELOPMENT_ACTIVATION_AUTHORITY_REQUIRED",
            "DEV-only activation requires current authority for the exact non-production target",
            path=release_path.as_posix(),
        ) from exc


def _development_authority_projection(release: Mapping[str, object]) -> object | None:
    if release.get("schema") == _DEVELOPMENT_RELEASE_SCHEMA:
        authority = release.get("development_authority")
        if authority is None:
            raise ValueError("development authority is missing")
        return authority
    promotion = release.get("promotion")
    if not isinstance(promotion, dict) or promotion.get("profile") != _DEVELOPMENT_COMPOSITION_PROFILE:
        return None
    constituents = release.get("constituents")
    if not isinstance(constituents, list):
        raise ValueError("development constituents are invalid")
    native = tuple(item for item in constituents if isinstance(item, Mapping) and item.get("id") == "native")
    if len(native) != 1 or not isinstance(native[0].get("release"), Mapping):
        raise ValueError("development native constituent is invalid")
    authority = native[0]["release"].get("development_authority")
    if authority is None:
        raise ValueError("development native authority is missing")
    return authority


__all__ = [
    "DevelopmentTargetAdmission",
    "DevelopmentTargetAdmissionVerifier",
    "DevelopmentActivationAdmissionGate",
    "require_development_activation_admission",
]
