"""Canonical target-admission types shared by development runtime boundaries."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol

from dpone.contracts.development_target_admission import (
    DevelopmentTargetAdmission,
    DevelopmentTargetOperation,
)


class DevelopmentTargetAdmissionVerifier(Protocol):
    """Protected adapter that reopens current target policy and revocation."""

    def require_current(self, admission: DevelopmentTargetAdmission, *, now: datetime) -> None: ...


__all__ = [
    "DevelopmentTargetAdmission",
    "DevelopmentTargetAdmissionVerifier",
    "DevelopmentTargetOperation",
]
