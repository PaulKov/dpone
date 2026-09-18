"""Operation-bound target admission for DEV-only release delivery and activation.

The receipt is returned by an injected platform verifier after it authenticates
the original development grant and protected target policy.  Artifact metadata,
CLI flags, and environment names are inputs to comparison, never authority.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from dpone.contracts.development_delivery_authority import (
    DevelopmentAuthorityError,
    DevelopmentAuthorityReceipt,
)

DevelopmentTargetOperation = Literal["publish", "materialize", "activate"]
_OPERATIONS = frozenset({"publish", "materialize", "activate"})
_ENVIRONMENT = re.compile(r"[a-z0-9][a-z0-9_-]{0,62}\Z")


@dataclass(frozen=True, slots=True)
class DevelopmentTargetAdmission:
    """Current verifier output for one exact operation and protected target."""

    authority: DevelopmentAuthorityReceipt
    operation: DevelopmentTargetOperation
    release_id: str
    deployment_id: str
    target_environment: str
    target_trust_tier: str
    target_policy_sha256: str
    checked_at: datetime
    current_revocation_epoch: int

    def __post_init__(self) -> None:
        if type(self.authority) is not DevelopmentAuthorityReceipt:
            raise DevelopmentAuthorityError("target_authority")
        self.authority.__post_init__()
        if self.operation not in _OPERATIONS:
            raise DevelopmentAuthorityError("target_operation")
        if not _is_canonical_sha256_digest(self.release_id) or not _is_canonical_sha256_digest(self.deployment_id):
            raise DevelopmentAuthorityError("target_identity")
        if _ENVIRONMENT.fullmatch(self.target_environment) is None:
            raise DevelopmentAuthorityError("target_environment")
        if self.target_trust_tier != "non_production":
            raise DevelopmentAuthorityError("production_target_forbidden")
        if not _is_canonical_sha256_digest(self.target_policy_sha256):
            raise DevelopmentAuthorityError("target_policy")
        self.authority.require_current(
            now=self.checked_at,
            current_revocation_epoch=self.current_revocation_epoch,
        )
        if self.authority.environment != self.target_environment:
            raise DevelopmentAuthorityError("target_environment_mismatch")

    @property
    def admission_id(self) -> str:
        """Return a credential-free identity for audit and deterministic retry."""

        return _canonical_fingerprint(
            {
                "authority": self.authority.release_projection(),
                "checked_at": self.checked_at.isoformat(),
                "current_revocation_epoch": self.current_revocation_epoch,
                "deployment_id": self.deployment_id,
                "operation": self.operation,
                "release_id": self.release_id,
                "target_environment": self.target_environment,
                "target_policy_sha256": self.target_policy_sha256,
                "target_trust_tier": self.target_trust_tier,
            }
        )

    def require(
        self,
        *,
        authority_projection: object,
        operation: DevelopmentTargetOperation,
        release_id: str,
        deployment_id: str,
        target_environment: str,
        target_trust_tier: str,
        now: datetime,
    ) -> None:
        """Compare the immutable artifact and independently trusted target."""

        self.__post_init__()
        self.authority.require_current(now=now, current_revocation_epoch=self.current_revocation_epoch)
        if self.authority.release_projection() != authority_projection:
            raise DevelopmentAuthorityError("artifact_scope_mismatch")
        if (
            operation != self.operation
            or release_id != self.release_id
            or deployment_id != self.deployment_id
            or target_environment != self.target_environment
        ):
            raise DevelopmentAuthorityError("target_identity_mismatch")
        if target_trust_tier != self.target_trust_tier or target_trust_tier != "non_production":
            raise DevelopmentAuthorityError("production_target_forbidden")

    def require_current_target(
        self,
        *,
        target_policy_sha256: str,
        current_revocation_epoch: int,
        now: datetime,
    ) -> None:
        """Compare independently reopened target policy and revocation state."""

        self.__post_init__()
        if target_policy_sha256 != self.target_policy_sha256:
            raise DevelopmentAuthorityError("target_policy_changed")
        self.authority.require_current(now=now, current_revocation_epoch=current_revocation_epoch)


def _is_canonical_sha256_digest(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", value) is not None


def _canonical_fingerprint(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


__all__ = ["DevelopmentTargetAdmission", "DevelopmentTargetOperation"]
