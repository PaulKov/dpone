"""Candidate input data and volatile readiness capabilities, never caller flags."""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
from dataclasses import asdict, dataclass
from typing import Literal

from dpone.contracts.clickhouse_authority import (
    AuthorityError,
    AuthorityStorageIdentity,
    AuthoritySubject,
    OperationBinding,
    TransportState,
    require_positive,
    require_text,
)
from dpone.contracts.clickhouse_observation import CandidateDesign, MultisetState, ObservationLimits, TypedTableEvidence
from dpone.contracts.clickhouse_publication import PublicationState


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


class _LocalCapability:
    """Common local lifetime, not a durable or transferable authority object."""

    __slots__ = ("_pid", "_thread", "_active")
    _pid: int
    _thread: threading.Thread
    _active: bool

    def _activate(self) -> None:
        self._pid = os.getpid()
        self._thread = threading.current_thread()
        self._active = True

    def _assert_live(self) -> None:
        if not self._active or self._pid != os.getpid() or self._thread is not threading.current_thread():
            raise AuthorityError("Candidate capability is expired or belongs to another process/thread")

    def close(self) -> None:
        """Invalidate volatile permission; this never releases persisted owners."""
        self._active = False

    def __reduce_ex__(self, protocol: int) -> object:
        raise TypeError("Candidate capabilities cannot be copied or serialized")


class VerifiedEnrollment(_LocalCapability):
    """Same-process/thread, short-lived result of trusted readiness verification.

    Python object privacy is not a hostile-code security boundary. Only the
    explicitly injected trusted readiness implementation may call ``_issue``.
    Serialization, copying, expiration and readback cannot recreate authority.
    No credentials are retained. A digest is identity, not proof of verification.
    """

    __slots__ = ("_request", "_revision", "_inventory_digest", "_profile_digest")
    _request: ProtectedPublicationRequest
    _revision: str
    _inventory_digest: str
    _profile_digest: str

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
        instance._activate()
        return instance

    def assert_current(self, request: ProtectedPublicationRequest) -> None:
        """Reject foreign, expired, forked and cross-thread readiness results."""
        self._assert_live()
        if request != self._request:
            raise AuthorityError("Readiness capability is not current for this request")

    @property
    def inventory_identity(self) -> tuple[str, str, str]:
        """Revision, inventory digest and profile digest; not proof by themselves."""
        self._assert_live()
        return self._revision, self._inventory_digest, self._profile_digest


class CandidateInvocation(_LocalCapability):
    """Issued only after fresh enrollment commit acknowledgement; never reloaded."""

    __slots__ = ("_binding", "_identity", "_secret")
    _binding: OperationBinding
    _identity: AuthorityStorageIdentity
    _secret: str

    def __init__(self) -> None:
        raise TypeError("CandidateInvocation requires acknowledged enrollment")

    @classmethod
    def _issue(cls, binding: OperationBinding, identity: AuthorityStorageIdentity, secret: str) -> CandidateInvocation:
        value = object.__new__(cls)
        value._binding, value._identity, value._secret = binding, identity, secret
        value._activate()
        return value

    @property
    def binding(self) -> OperationBinding:
        return self._binding

    @property
    def authority_identity(self) -> AuthorityStorageIdentity:
        return self._identity

    def assert_current(self) -> None:
        """Persistence consumers must additionally check the original inode/binding."""
        self._assert_live()

    def __enter__(self) -> CandidateInvocation:
        self.assert_current()
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


def _require_digest(value: object) -> None:
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise AuthorityError("Expected a canonical candidate digest")


@dataclass(frozen=True)
class CandidateMutationRequest:
    """Immutable correlation metadata; not permission to execute caller SQL."""

    binding: OperationBinding
    kind: Literal["create", "insert"]
    sequence: int
    design_digest: str
    statement_digest: str
    payload_digest: str
    evidence: MultisetState

    def __post_init__(self) -> None:
        if type(self.sequence) is not int or not 0 <= self.sequence < (1 << 63):
            raise AuthorityError("Invalid candidate sequence")
        if self.kind not in {"create", "insert"} or (self.kind == "create") != (self.sequence == 0):
            raise AuthorityError("Candidate CREATE must be request zero")
        if type(self.binding) is not OperationBinding or type(self.evidence) is not MultisetState:
            raise AuthorityError("Candidate request requires typed binding and evidence")
        if self.kind == "create" and self.evidence.count:
            raise AuthorityError("CREATE cannot contribute rows")
        for value in (self.design_digest, self.statement_digest, self.payload_digest):
            _require_digest(value)

    @property
    def query_id(self) -> str:
        body = json.dumps(
            ["dpone-candidate-v1", self.binding.operation_id, self.kind, self.sequence], separators=(",", ":")
        )
        return "dpone-candidate-" + hashlib.sha256(body.encode()).hexdigest()


class CandidateRequestGrant(_LocalCapability):
    """Acknowledged registered request, bound to the original live invocation."""

    __slots__ = ("_invocation", "_request", "_secret", "_revision")
    _invocation: CandidateInvocation
    _request: CandidateMutationRequest
    _secret: str
    _revision: int

    def __init__(self) -> None:
        raise TypeError("CandidateRequestGrant requires acknowledged registration")

    @classmethod
    def _issue(
        cls, invocation: CandidateInvocation, request: CandidateMutationRequest, secret: str
    ) -> CandidateRequestGrant:
        invocation.assert_current()
        value = object.__new__(cls)
        value._invocation, value._request, value._secret, value._revision = invocation, request, secret, 0
        value._activate()
        return value

    @property
    def request(self) -> CandidateMutationRequest:
        return self._request

    def assert_current(self) -> None:
        self._assert_live()
        self._invocation.assert_current()


@dataclass(frozen=True)
class CandidateCompletion:
    """Trusted transport's positive EOS receipt; DTO construction proves no I/O."""

    operation_id: str
    query_id: str
    kind: Literal["create", "insert"]
    statement_digest: str
    payload_digest: str
    server_id: str
    server_version: tuple[int, int, int]
    server_revision: int
    driver_version: str

    def require_matches(self, request: CandidateMutationRequest) -> None:
        expected = (
            request.binding.operation_id,
            request.query_id,
            request.kind,
            request.statement_digest,
            request.payload_digest,
            request.binding.subject.server_id,
        )
        observed = (
            self.operation_id,
            self.query_id,
            self.kind,
            self.statement_digest,
            self.payload_digest,
            self.server_id,
        )
        if (
            observed != expected
            or type(self.server_version) is not tuple
            or any(type(component) is not int for component in self.server_version)
            or self.server_version != (24, 8, 14)
            or self.driver_version != "0.2.10"
        ):
            raise AuthorityError("Candidate completion does not match the registered request/profile")
        require_positive(self.server_revision)

    @property
    def digest(self) -> str:
        body = {"schema_version": "dpone.clickhouse.candidate-completion.v1", **asdict(self)}
        return hashlib.sha256(
            json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        ).hexdigest()


@dataclass(frozen=True)
class CandidateRequestStatus:
    """Validated durable transport state, never a reconstructable send grant."""

    request: CandidateMutationRequest
    state: TransportState
    revision: int
    completion_digest: str | None

    def __post_init__(self) -> None:
        revisions = {
            TransportState.NOT_STARTED: 0,
            TransportState.MAY_HAVE_SENT: 1,
            TransportState.CLOSED_WITHOUT_SEND: 1,
            TransportState.CLOSED_TERMINAL: 2,
        }
        if (
            type(self.request) is not CandidateMutationRequest
            or type(self.state) is not TransportState
            or type(self.revision) is not int
            or self.revision != revisions[self.state]
        ):
            raise AuthorityError("Invalid candidate request state/revision")
        if self.state is TransportState.CLOSED_TERMINAL:
            _require_digest(self.completion_digest)
        elif self.completion_digest is not None:
            raise AuthorityError("Uncompleted candidate request has a completion digest")


@dataclass(frozen=True)
class CandidateSeal:
    """Immutable join/content evidence; only the protected seal service produces it."""

    binding: OperationBinding
    candidate_uuid: str
    request_frontier: int
    request_history_digest: str
    profile_digest: str
    design_digest: str
    expected: MultisetState
    observed: TypedTableEvidence


@dataclass(frozen=True)
class CandidateStatus:
    """Source-free original metadata; none of these fields grants re-entry."""

    binding: OperationBinding
    request: ProtectedPublicationRequest
    revision: int
    lifecycle: str
    admission_closed: bool
    source_exhausted: bool
    accepted: tuple[str, ...]
    succeeded: tuple[str, ...]
    uncertain: tuple[str, ...]
    expected: MultisetState
    seal: CandidateSeal | None
    publication_state: PublicationState | None
    retained_reason: str | None
    profile_digest: str
