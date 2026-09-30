"""Candidate input data and volatile readiness capabilities, never caller flags."""

from __future__ import annotations

import os
import re
import threading
from dataclasses import dataclass

from dpone.contracts.clickhouse_authority import AuthorityError, AuthoritySubject, OperationBinding, require_text
from dpone.contracts.clickhouse_observation import CandidateDesign, MultisetState, ObservationLimits


@dataclass(frozen=True)
class CandidateBatch:
    """Immutable bounded rows and their evidence; the gateway must recompute it."""

    rows: tuple[tuple[object, ...], ...]
    evidence: MultisetState
    payload_digest: str


@dataclass(frozen=True)
class ProtectedPublicationRequest:
    """Fresh enrollment request; existing identities do not authorize re-entry."""

    operation_id: str
    subject: AuthoritySubject
    candidate: str
    design: CandidateDesign
    limits: ObservationLimits

    def __post_init__(self) -> None:
        OperationBinding(self.operation_id, self.subject, self.candidate, 1)
        if type(self.design) is not CandidateDesign or type(self.limits) is not ObservationLimits:
            raise ValueError("protected_publication_requires_typed_design_and_limits")


class VerifiedEnrollment:
    """Same-process/thread, short-lived result of trusted readiness verification.

    Python object privacy is not a hostile-code security boundary. Only the
    explicitly injected trusted readiness implementation may call ``_issue``.
    Serialization, copying, expiration and readback cannot recreate authority.
    No credentials are retained. A digest is identity, not proof of verification.
    """

    __slots__ = ("_request", "_revision", "_inventory_digest", "_profile_digest", "_pid", "_thread", "_active")

    def __init__(self) -> None:
        raise TypeError("VerifiedEnrollment must be issued by trusted readiness")

    @classmethod
    def _issue(
        cls, request: ProtectedPublicationRequest, inventory_revision: str, inventory_digest: str, profile_digest: str
    ) -> VerifiedEnrollment:
        require_text(inventory_revision)
        if any(
            type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None
            for value in (inventory_digest, profile_digest)
        ):
            raise AuthorityError("Invalid readiness identity")
        instance = object.__new__(cls)
        instance._request = request
        instance._revision = inventory_revision
        instance._inventory_digest = inventory_digest
        instance._profile_digest = profile_digest
        instance._pid = os.getpid()
        instance._thread = threading.get_ident()
        instance._active = True
        return instance

    def assert_current(self, request: ProtectedPublicationRequest) -> None:
        """Reject foreign, expired, forked and cross-thread readiness results."""
        if (
            not self._active
            or self._pid != os.getpid()
            or self._thread != threading.get_ident()
            or request != self._request
        ):
            raise AuthorityError("Readiness capability is not current for this request")

    def close(self) -> None:
        """Invalidate volatile permission; this never releases persisted owners."""
        self._active = False

    def __reduce_ex__(self, protocol: int) -> object:
        raise TypeError("Readiness capabilities cannot be copied or serialized")
