"""Closed coordinator barriers and independent observations, without I/O.

Intent precedes spawn, authenticated process precedes credential intent, and full
session plus independently observed continuous SQL authority precede a one-shot
grant. Result receipt does not establish remote settlement. Original execution
ownership and all evidence survive newer-fence, observation-only recovery.
"""

from dataclasses import asdict, dataclass, replace
from enum import StrEnum
from hashlib import sha256
from uuid import UUID

from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand, TdsCoordinatorDirectory, directory_key
from dpone.contracts.mssql_tds_session import (
    TdsRemoteSessionIdentity,
    TdsRestrictedRemoteSessionIdentity,
    TdsSessionIdentity,
    encode_session_continuity_identity,
)
from dpone.contracts.mssql_tds_worker import (
    TdsAttemptError,
    TdsAttemptIdentity,
    TdsAttemptOwnership,
    TdsProcessIdentity,
    _hash,
    _integer,
)
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object


def _require(condition: bool) -> None:
    if not condition:
        raise ValueError("mssql_native.tds_coordinator_invalid")


class TdsCoordinatorPhase(StrEnum):
    INTENT = "intent"
    PROCESS_REGISTERED = "process_registered"
    CREDENTIAL_INTENT = "credential_intent"
    SESSION_REGISTERED = "session_registered"
    GRANT_INTENT = "grant_intent"
    RESULT_RECEIVED = "result_received"


@dataclass(frozen=True)
class TdsCoordinatorIdentity:
    parent: TdsAttemptIdentity
    slot_index: int
    operation_id: UUID
    command: TdsCoordinatorCommand
    command_sha256: str
    original_fence: int
    implementation_sha256: str

    def __post_init__(self) -> None:
        _require(type(self.parent) is TdsAttemptIdentity and type(self.command) is TdsCoordinatorCommand)
        _require(type(self.operation_id) is UUID and self.operation_id.int != 0)
        _integer(self.slot_index)
        _integer(self.original_fence, 1)
        _hash(self.command_sha256)
        _hash(self.implementation_sha256)


def coordinator_key(identity: TdsCoordinatorIdentity) -> str:
    """Stable locator: changed UUID, digests, owner or build cannot bypass a slot."""
    _require(type(identity) is TdsCoordinatorIdentity)
    return directory_key(identity.parent) + "/operation/" + str(identity.slot_index)


def coordinator_identity_digest(identity: TdsCoordinatorIdentity) -> str:
    _require(type(identity) is TdsCoordinatorIdentity)
    return sha256(canonical_json_bytes(dict(asdict(identity), operation_id=str(identity.operation_id)))).hexdigest()


@dataclass(frozen=True)
class TdsCoordinatorGrant:
    """Exact one-shot execution binding, not a reusable SQL capability.

    The trusted producer must acquire and continuously hold exclusive SQL
    authority; neither a session nor its observation receipt establishes a lock.
    """

    operation_sha256: str
    ownership: TdsAttemptOwnership
    process: TdsProcessIdentity
    session: TdsSessionIdentity
    authority_sha256: str
    grant_id: UUID

    def __post_init__(self) -> None:
        _hash(self.operation_sha256)
        _hash(self.authority_sha256)
        for value, cls in (
            (self.ownership, TdsAttemptOwnership),
            (self.process, TdsProcessIdentity),
        ):
            _require(type(value) is cls)
        _require(type(self.session) in (TdsRemoteSessionIdentity, TdsRestrictedRemoteSessionIdentity))
        _require(type(self.grant_id) is UUID and self.grant_id.int != 0)


def coordinator_grant_digest(grant: TdsCoordinatorGrant) -> str:
    _require(type(grant) is TdsCoordinatorGrant)
    value = dict(
        asdict(grant),
        grant_id=str(grant.grant_id),
        session=strict_json_object(encode_session_continuity_identity(grant.session)),
    )
    return sha256(canonical_json_bytes(value)).hexdigest()


class TdsCoordinatorResultKind(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass(frozen=True)
class TdsCoordinatorResult:
    """Bounded receipt of the command result, never local or remote settlement."""

    operation_sha256: str
    grant_sha256: str
    outcome: TdsCoordinatorResultKind
    proof_sha256: str
    error: TdsAttemptError | None = None

    def __post_init__(self) -> None:
        for digest in (self.operation_sha256, self.grant_sha256, self.proof_sha256):
            _hash(digest)
        _require(type(self.outcome) is TdsCoordinatorResultKind)
        _require(self.error is None or type(self.error) is TdsAttemptError)
        _require((self.outcome is TdsCoordinatorResultKind.FAILED) == (self.error is not None))


class TdsCoordinatorLocalKind(StrEnum):
    NO_PROCESS = "no_process"
    CONTAINED = "contained"


@dataclass(frozen=True)
class TdsCoordinatorLocalObservation:
    """External local proof; no registered process is not itself absence proof."""

    operation_sha256: str
    kind: TdsCoordinatorLocalKind
    process: TdsProcessIdentity | None
    authority_sha256: str
    proof_sha256: str

    def __post_init__(self) -> None:
        for digest in (self.operation_sha256, self.authority_sha256, self.proof_sha256):
            _hash(digest)
        _require(type(self.kind) is TdsCoordinatorLocalKind)
        _require(self.process is None or type(self.process) is TdsProcessIdentity)
        _require((self.kind is TdsCoordinatorLocalKind.NO_PROCESS) == (self.process is None))


class TdsCoordinatorRemoteKind(StrEnum):
    NO_SESSION = "no_session"
    SETTLED = "settled"


@dataclass(frozen=True)
class TdsCoordinatorRemoteObservation:
    """External coordinator-only observation, never bulk-worker quiescence.

    NO_SESSION is not proof that no future mutation is possible. Local containment
    and any prior grant remain independent facts, including after partial login.
    """

    operation_sha256: str
    kind: TdsCoordinatorRemoteKind
    session: TdsSessionIdentity | None
    authority_sha256: str
    proof_sha256: str

    def __post_init__(self) -> None:
        for digest in (self.operation_sha256, self.authority_sha256, self.proof_sha256):
            _hash(digest)
        _require(type(self.kind) is TdsCoordinatorRemoteKind)
        _require(
            self.session is None or type(self.session) in (TdsRemoteSessionIdentity, TdsRestrictedRemoteSessionIdentity)
        )
        _require((self.kind is TdsCoordinatorRemoteKind.NO_SESSION) == (self.session is None))


@dataclass(frozen=True)
class TdsCoordinatorState:
    identity: TdsCoordinatorIdentity
    execution_owner: TdsAttemptOwnership
    ownership: TdsAttemptOwnership
    phase: TdsCoordinatorPhase
    sequence: int
    process: TdsProcessIdentity | None = None
    authentication_sha256: str | None = None
    session: TdsSessionIdentity | None = None
    authority_sha256: str | None = None
    grant: TdsCoordinatorGrant | None = None
    result: TdsCoordinatorResult | None = None
    error: TdsAttemptError | None = None
    local: TdsCoordinatorLocalObservation | None = None
    remote: TdsCoordinatorRemoteObservation | None = None

    def __post_init__(self) -> None:
        for required_value, required_cls in (
            (self.identity, TdsCoordinatorIdentity),
            (self.execution_owner, TdsAttemptOwnership),
            (self.ownership, TdsAttemptOwnership),
            (self.phase, TdsCoordinatorPhase),
        ):
            _require(type(required_value) is required_cls)
        _integer(self.sequence)
        _require(self.execution_owner.fence == self.identity.original_fence)
        _require(self.ownership.fence > self.execution_owner.fence or self.ownership == self.execution_owner)
        for optional_value, optional_cls in (
            (self.process, TdsProcessIdentity),
            (self.grant, TdsCoordinatorGrant),
            (self.result, TdsCoordinatorResult),
            (self.error, TdsAttemptError),
            (self.local, TdsCoordinatorLocalObservation),
            (self.remote, TdsCoordinatorRemoteObservation),
        ):
            _require(optional_value is None or type(optional_value) is optional_cls)
        _require(
            self.session is None or type(self.session) in (TdsRemoteSessionIdentity, TdsRestrictedRemoteSessionIdentity)
        )
        index = list(TdsCoordinatorPhase).index(self.phase)
        for barrier_value, barrier in (
            (self.process, 1),
            (self.authentication_sha256, 1),
            (self.session, 3),
            (self.authority_sha256, 3),
            (self.grant, 4),
            (self.result, 5),
        ):
            _require((barrier_value is not None) == (index >= barrier))
        for digest in (self.authentication_sha256, self.authority_sha256):
            if digest is not None:
                _hash(digest)
        facts = sum(value is not None for value in (self.error, self.local, self.remote))
        takeovers = self.sequence - index - facts
        if self.recovering:
            _require(1 <= takeovers <= self.ownership.fence - self.execution_owner.fence)
            _require(takeovers >= 2 or self.ownership.supervisor_id != self.execution_owner.supervisor_id)
        else:
            _require(takeovers == 0)
        binding = coordinator_identity_digest(self.identity)
        if self.grant is not None:
            _require(
                self.grant.operation_sha256 == binding
                and self.grant.ownership == self.execution_owner
                and self.grant.process == self.process
                and self.grant.session == self.session
                and self.grant.authority_sha256 == self.authority_sha256
            )
        if self.result is not None:
            _require(
                self.grant is not None
                and self.result.operation_sha256 == binding
                and self.result.grant_sha256 == coordinator_grant_digest(self.grant)
            )
        if self.local is not None:
            _require(self.local.operation_sha256 == binding and self.local.process == self.process)
        if self.remote is not None:
            _require(self.remote.operation_sha256 == binding and self.remote.session == self.session)

    @property
    def recovering(self) -> bool:
        return self.ownership.fence > self.execution_owner.fence


def initial_coordinator_state(
    identity: TdsCoordinatorIdentity, directory: TdsCoordinatorDirectory, owner: TdsAttemptOwnership
) -> TdsCoordinatorState:
    """Validate the complete original reservation before recording operation intent."""
    _require(type(identity) is TdsCoordinatorIdentity and type(directory) is TdsCoordinatorDirectory)
    _require(directory.parent == identity.parent and identity.slot_index < len(directory.slots))
    slot = directory.slots[identity.slot_index]
    _require(not directory.admission_closed and identity.slot_index == len(directory.slots) - 1)
    _require(slot.local_containment is None and slot.remote_settlement is None)
    _require(
        (slot.operation_id, slot.command, slot.command_sha256, slot.owner_fence)
        == (identity.operation_id, identity.command, identity.command_sha256, identity.original_fence)
    )
    return TdsCoordinatorState(identity, owner, owner, TdsCoordinatorPhase.INTENT, 0)


@dataclass(frozen=True)
class CoordinatorProcessRegistered:
    process: TdsProcessIdentity
    authentication_sha256: str


@dataclass(frozen=True)
class CoordinatorCredentialIntent:
    pass


@dataclass(frozen=True)
class CoordinatorSessionRegistered:
    session: TdsSessionIdentity
    authority_sha256: str


@dataclass(frozen=True)
class CoordinatorGrantIntent:
    grant: TdsCoordinatorGrant


@dataclass(frozen=True)
class CoordinatorResultReceived:
    result: TdsCoordinatorResult


@dataclass(frozen=True)
class CoordinatorFailed:
    error: TdsAttemptError


@dataclass(frozen=True)
class CoordinatorLocalObserved:
    observation: TdsCoordinatorLocalObservation


@dataclass(frozen=True)
class CoordinatorRemoteObserved:
    observation: TdsCoordinatorRemoteObservation


TdsCoordinatorEvent = (
    CoordinatorProcessRegistered
    | CoordinatorCredentialIntent
    | CoordinatorSessionRegistered
    | CoordinatorGrantIntent
    | CoordinatorResultReceived
    | CoordinatorFailed
    | CoordinatorLocalObserved
    | CoordinatorRemoteObserved
)


def advance_coordinator_state(
    state: TdsCoordinatorState, event: TdsCoordinatorEvent, *, expected_phase: TdsCoordinatorPhase
) -> TdsCoordinatorState:
    """Advance a closed barrier or independent observation; never produce proof."""
    _require(type(state) is TdsCoordinatorState and type(expected_phase) is TdsCoordinatorPhase)
    _require(state.phase is expected_phase)
    facts = {
        CoordinatorFailed: ("error", "error"),
        CoordinatorLocalObserved: ("local", "observation"),
        CoordinatorRemoteObserved: ("remote", "observation"),
    }
    if type(event) in facts:
        field, attr = facts[type(event)]
        value = getattr(event, attr)
        _require(value is not None)
        previous = getattr(state, field)
        _require(previous is None or previous == value)
        return state if previous == value else replace(state, sequence=state.sequence + 1, **{field: value})
    _require(not state.recovering and state.error is None and state.local is None and state.remote is None)
    barriers = (
        CoordinatorProcessRegistered,
        CoordinatorCredentialIntent,
        CoordinatorSessionRegistered,
        CoordinatorGrantIntent,
        CoordinatorResultReceived,
    )
    index = list(TdsCoordinatorPhase).index(state.phase)
    _require(index < len(barriers) and type(event) is barriers[index])
    return replace(
        state,
        phase=list(TdsCoordinatorPhase)[index + 1],
        sequence=state.sequence + 1,
        **{field: getattr(event, field) for field in event.__dataclass_fields__},
    )


def take_over_coordinator_state(state: TdsCoordinatorState, ownership: TdsAttemptOwnership) -> TdsCoordinatorState:
    """Preserve original execution bindings; recovered intent can never replay."""
    _require(type(state) is TdsCoordinatorState and type(ownership) is TdsAttemptOwnership)
    _require(ownership.fence > state.ownership.fence and ownership.supervisor_id != state.ownership.supervisor_id)
    return replace(state, ownership=ownership, sequence=state.sequence + 1)


@dataclass(frozen=True)
class TdsCoordinatorSnapshot:
    """Immutable acknowledged state and its positive signed64 store revision."""

    state: TdsCoordinatorState
    revision: int

    def __post_init__(self) -> None:
        _require(type(self.state) is TdsCoordinatorState)
        _integer(self.revision, 1)
