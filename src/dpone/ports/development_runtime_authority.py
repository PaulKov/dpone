"""Port for reopening current development execution authority."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from dpone.contracts.development_delivery_authority import DevelopmentAuthorityReceipt


@dataclass(frozen=True, slots=True)
class DevelopmentRuntimeAuthorizationRequest:
    """Exact immutable runtime subject presented to an external verifier."""

    environment: str
    release_id: str
    deployment_id: str
    runtime_image_digest: str
    workload_id: str
    execution_kind: str
    hook_id: str | None


@dataclass(frozen=True, slots=True)
class DevelopmentRuntimeAuthorization:
    """Process-local result returned after external policy verification."""

    authority: DevelopmentAuthorityReceipt
    checked_at: datetime
    current_revocation_epoch: int


class DevelopmentRuntimeAuthority(Protocol):
    """Authenticate and authorize one exact development runtime entry."""

    def authorize(
        self,
        request: DevelopmentRuntimeAuthorizationRequest,
    ) -> DevelopmentRuntimeAuthorization: ...


__all__ = [
    "DevelopmentRuntimeAuthorization",
    "DevelopmentRuntimeAuthorizationRequest",
    "DevelopmentRuntimeAuthority",
]
