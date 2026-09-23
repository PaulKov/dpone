"""Closed per-attempt TDS lifecycle state and pure transitions."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from enum import StrEnum as StrEnum
from typing import Any

from dpone.contracts.mssql_sqlclient_attempt import (
    SqlClientAttemptEvidence,
    SqlClientCredentialIntent,
    SqlClientGrantIntent,
    SqlClientWriterObserved,
    advance_sqlclient_evidence,
)
from dpone.contracts.mssql_tds_phase_obligations import validate_phase_obligations
from dpone.contracts.mssql_tds_worker_identity import (
    ParentAuthority,
    TdsAttemptError,
    TdsAttemptIdentity,
    TdsAttemptOwnership,
    TdsAttemptPhase,
    TdsChildExit,
    TdsObjectIdentity,
    TdsProcessIdentity,
    _hash,
    _integer,
)
from dpone.contracts.mssql_tds_worker_identity import _text as _text
from dpone.contracts.strict_record import canonical_uuid


def _uuid(value: Any) -> None:
    try:
        valid = canonical_uuid(value).int != 0
    except (TypeError, ValueError):
        valid = False
    if not valid:
        raise ValueError("mssql_native.tds_invalid_uuid")


# These records predate the responsibility split. Their defining module is a
# compatibility contract used by annotations, serialization and downstream
# reflection, so the facade remains their canonical public home.
for _identity_type in (
    ParentAuthority,
    TdsAttemptError,
    TdsAttemptIdentity,
    TdsAttemptOwnership,
    TdsAttemptPhase,
    TdsChildExit,
    TdsObjectIdentity,
    TdsProcessIdentity,
):
    _identity_type.__module__ = __name__


@dataclass(frozen=True)
class TdsAttemptState:
    identity: TdsAttemptIdentity
    ownership: TdsAttemptOwnership
    phase: TdsAttemptPhase
    sequence: int
    object_identity: TdsObjectIdentity | None = None
    process: TdsProcessIdentity | None = None
    exit_code: int | None = None
    result_sha256: str | None = None
    verification_sha256: str | None = None
    error: TdsAttemptError | None = None
    observation_sha256: str | None = None
    parent_authority: ParentAuthority | None = None
    schema_version: int = 1
    backend: str | None = None
    sqlclient: SqlClientAttemptEvidence | None = None

    def __post_init__(self) -> None:
        _integer(self.schema_version, 1, 2)
        if self.schema_version == 1:
            if self.backend is not None or self.sqlclient is not None:
                raise ValueError("mssql_native.tds_schema1_backend")
        elif type(self.backend) is not str or self.backend != "mssql_sqlclient":
            raise ValueError("mssql_native.tds_schema2_backend")
        if self.sqlclient is not None and type(self.sqlclient) is not SqlClientAttemptEvidence:
            raise ValueError("mssql_native.sqlclient_evidence_type")
        _integer(self.sequence)
        for value, expected in (
            (self.identity, TdsAttemptIdentity),
            (self.ownership, TdsAttemptOwnership),
            (self.phase, TdsAttemptPhase),
        ):
            if type(value) is not expected:
                raise ValueError("mssql_native.tds_invalid_record_type")
        for optional_value, optional_type in (
            (self.object_identity, TdsObjectIdentity),
            (self.process, TdsProcessIdentity),
            (self.error, TdsAttemptError),
            (self.parent_authority, ParentAuthority),
        ):
            if optional_value is not None and type(optional_value) is not optional_type:
                raise ValueError("mssql_native.tds_invalid_record_type")
        for digest in (self.result_sha256, self.verification_sha256, self.observation_sha256):
            if digest is not None:
                _hash(digest)
        if self.exit_code is not None:
            _integer(self.exit_code, -255, 255)
        _validate_phase(self)
        if self.schema_version == 2:
            early = list(TdsAttemptPhase)[:4]
            if (self.phase in early and self.sqlclient is not None) or (
                self.sqlclient is not None and self.process is None
            ):
                raise ValueError("mssql_native.sqlclient_evidence_phase")
            if self.phase in list(TdsAttemptPhase)[4:7] and self.sqlclient is None:
                raise ValueError("mssql_native.sqlclient_missing_intent")
            if self.exit_code == 0 and (
                self.sqlclient is None
                or (not self.sqlclient.input_empty and self.sqlclient.grant_intent_sha256 is None)
            ):
                raise ValueError("mssql_native.sqlclient_success_without_grant")
            if self.sqlclient is not None:
                extra = int(self.sqlclient.writer_observation_sha256 is not None) + int(
                    self.sqlclient.grant_intent_sha256 is not None
                )
                index = list(TdsAttemptPhase).index(self.phase)
                prior = 6 if self.verification_sha256 is not None else 5 if self.exit_code is not None else 4
                minimum = index + extra if index < 7 else prior + extra + index - 6
                if self.sequence < minimum:
                    raise ValueError("mssql_native.sqlclient_evidence_sequence")


def state_payload(state: TdsAttemptState) -> dict[str, Any]:
    """Return the version-aware fields embedded by durable record codecs."""
    if type(state) is not TdsAttemptState:
        raise ValueError("mssql_native.tds_invalid_record_type")
    value = asdict(state)
    if state.schema_version == 1:
        del value["backend"], value["sqlclient"]
    return value


def _validate_phase(state: TdsAttemptState) -> None:
    validate_phase_obligations(
        phase=list(TdsAttemptPhase).index(state.phase),
        sequence=state.sequence,
        object_present=state.object_identity is not None,
        process_present=state.process is not None,
        exit_code=state.exit_code,
        result_present=state.result_sha256 is not None,
        verification_present=state.verification_sha256 is not None,
        error_present=state.error is not None,
        observation_present=state.observation_sha256 is not None,
        parent_present=state.parent_authority is not None,
    )


@dataclass(frozen=True)
class Prepared:
    object_identity: TdsObjectIdentity
    proof_sha256: str


@dataclass(frozen=True)
class LaunchIntent:
    grants_sha256: str


@dataclass(frozen=True)
class ProcessRegistered:
    process: TdsProcessIdentity


@dataclass(frozen=True)
class Running:
    """Durable credential-release intent, persisted before credentials are sent."""


@dataclass(frozen=True)
class Exited:
    exit_code: int
    result_sha256: str | None


@dataclass(frozen=True)
class Verified:
    proof_sha256: str


@dataclass(frozen=True)
class ParentRetirementRequired:
    """Authorize normal post-publication containment without inventing an error."""

    parent_authority: ParentAuthority


@dataclass(frozen=True)
class ContainmentRequired:
    error: TdsAttemptError
    parent_authority: ParentAuthority | None = None


@dataclass(frozen=True)
class Contained:
    proof_sha256: str


@dataclass(frozen=True)
class RetirementRequired:
    pass


@dataclass(frozen=True)
class Retired:
    absence_sha256: str


TdsLifecycleEvent = (
    Prepared
    | SqlClientCredentialIntent
    | SqlClientWriterObserved
    | SqlClientGrantIntent
    | LaunchIntent
    | ProcessRegistered
    | Running
    | Exited
    | Verified
    | ParentRetirementRequired
    | ContainmentRequired
    | Contained
    | RetirementRequired
    | Retired
)


def initial_state(
    identity: TdsAttemptIdentity, ownership: TdsAttemptOwnership, *, backend: str = "mssql_python"
) -> TdsAttemptState:
    """Creation intent must be persisted before CREATE or other external effects."""
    if type(backend) is not str or backend not in {"mssql_python", "mssql_sqlclient"}:
        raise ValueError("mssql_native.tds_backend_invalid")
    return TdsAttemptState(
        identity,
        ownership,
        TdsAttemptPhase.CREATION_INTENT,
        0,
        schema_version=2 if backend == "mssql_sqlclient" else 1,
        backend=backend if backend == "mssql_sqlclient" else None,
    )


def advance_state(
    state: TdsAttemptState, event: TdsLifecycleEvent, *, expected_phase: TdsAttemptPhase
) -> TdsAttemptState:
    """Validate one closed transition; store CAS and proof collection are external."""
    if (
        type(expected_phase) is not TdsAttemptPhase
        or state.phase != expected_phase
        or state.phase == TdsAttemptPhase.RETIRED
    ):
        raise ValueError("mssql_native.tds_stale_or_terminal_phase")
    if type(event) in (SqlClientCredentialIntent, SqlClientWriterObserved, SqlClientGrantIntent):
        required = (
            TdsAttemptPhase.SPAWNED_WAITING if type(event) is SqlClientCredentialIntent else TdsAttemptPhase.RUNNING
        )
        if state.schema_version != 2 or state.phase != required:
            raise ValueError("mssql_native.sqlclient_invalid_transition")
        evidence = advance_sqlclient_evidence(state.sqlclient, event)
        return replace(state, phase=TdsAttemptPhase.RUNNING, sequence=state.sequence + 1, sqlclient=evidence)
    if type(event) is Running and state.schema_version != 1:
        raise ValueError("mssql_native.sqlclient_explicit_intent_required")
    if type(event) is ContainmentRequired:
        if state.phase not in list(TdsAttemptPhase)[:6]:
            raise ValueError("mssql_native.tds_invalid_transition")
        return replace(
            state,
            phase=TdsAttemptPhase.CONTAINMENT_REQUIRED,
            sequence=state.sequence + 1,
            error=event.error,
            parent_authority=event.parent_authority,
        )
    if type(event) is ParentRetirementRequired:
        if state.phase is not TdsAttemptPhase.VERIFIED or type(event.parent_authority) is not ParentAuthority:
            raise ValueError("mssql_native.tds_invalid_transition")
        return replace(
            state,
            phase=TdsAttemptPhase.CONTAINMENT_REQUIRED,
            sequence=state.sequence + 1,
            parent_authority=event.parent_authority,
        )
    transitions = {
        Prepared: (TdsAttemptPhase.CREATION_INTENT, TdsAttemptPhase.PREPARED),
        LaunchIntent: (TdsAttemptPhase.PREPARED, TdsAttemptPhase.LAUNCH_INTENT),
        ProcessRegistered: (TdsAttemptPhase.LAUNCH_INTENT, TdsAttemptPhase.SPAWNED_WAITING),
        Running: (TdsAttemptPhase.SPAWNED_WAITING, TdsAttemptPhase.RUNNING),
        Exited: (TdsAttemptPhase.RUNNING, TdsAttemptPhase.EXITED),
        Verified: (TdsAttemptPhase.EXITED, TdsAttemptPhase.VERIFIED),
        Contained: (TdsAttemptPhase.CONTAINMENT_REQUIRED, TdsAttemptPhase.CONTAINED),
        RetirementRequired: (TdsAttemptPhase.CONTAINED, TdsAttemptPhase.RETIREMENT_REQUIRED),
        Retired: (TdsAttemptPhase.RETIREMENT_REQUIRED, TdsAttemptPhase.RETIRED),
    }
    transition = transitions.get(type(event))
    if transition is None or state.phase != transition[0]:
        raise ValueError("mssql_native.tds_invalid_transition")
    changes: dict[str, Any] = {}
    if isinstance(event, Prepared):
        changes.update(object_identity=event.object_identity, observation_sha256=event.proof_sha256)
    elif isinstance(event, LaunchIntent):
        changes["observation_sha256"] = event.grants_sha256
    elif isinstance(event, ProcessRegistered):
        changes["process"] = event.process
    elif isinstance(event, Exited):
        changes.update(exit_code=event.exit_code, result_sha256=event.result_sha256)
    elif isinstance(event, Verified):
        changes.update(verification_sha256=event.proof_sha256, observation_sha256=event.proof_sha256)
    elif isinstance(event, Contained):
        changes["observation_sha256"] = event.proof_sha256
    elif isinstance(event, Retired):
        changes["observation_sha256"] = event.absence_sha256
    return replace(state, phase=transition[1], sequence=state.sequence + 1, **changes)


def replace_ownership(state: TdsAttemptState, ownership: TdsAttemptOwnership) -> TdsAttemptState:
    """Retain all evidence on takeover; the adapter restricts recovery effects."""
    if (
        type(ownership) is not TdsAttemptOwnership
        or ownership.fence <= state.ownership.fence
        or ownership.supervisor_id == state.ownership.supervisor_id
    ):
        raise ValueError("mssql_native.tds_stale_fence")
    return replace(state, ownership=ownership, sequence=state.sequence + 1)


@dataclass(frozen=True)
class TdsAttemptSnapshot:
    """One immutable state and its opaque positive store-CAS revision."""

    state: TdsAttemptState
    revision: int

    def __post_init__(self) -> None:
        if type(self.state) is not TdsAttemptState:
            raise ValueError("mssql_native.tds_invalid_record_type")
        _integer(self.revision, 1)


@dataclass(frozen=True)
class TdsAttemptObservation:
    """Acknowledged read-only snapshot or absence, without writer authority.

    This in-process envelope does not change the serialized lifecycle format.
    An absent snapshot means the bounded backend read acknowledged absence; an
    unavailable or timed-out read must not produce an observation envelope.
    """

    snapshot: TdsAttemptSnapshot | None

    def __post_init__(self) -> None:
        if self.snapshot is not None and type(self.snapshot) is not TdsAttemptSnapshot:
            raise ValueError("mssql_native.tds_attempt_observation_invalid")


__all__ = [
    "Contained",
    "ContainmentRequired",
    "Exited",
    "LaunchIntent",
    "ParentAuthority",
    "ParentRetirementRequired",
    "Prepared",
    "ProcessRegistered",
    "Retired",
    "RetirementRequired",
    "Running",
    "TdsAttemptError",
    "TdsAttemptIdentity",
    "TdsAttemptObservation",
    "TdsAttemptOwnership",
    "TdsAttemptPhase",
    "TdsAttemptSnapshot",
    "TdsAttemptState",
    "TdsChildExit",
    "TdsObjectIdentity",
    "TdsProcessIdentity",
    "Verified",
    "advance_state",
    "initial_state",
    "replace_ownership",
]
