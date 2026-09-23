"""P10f remote-session settlement, typed stage verification and VERIFIED CAS."""

from __future__ import annotations

import os
from copy import deepcopy
from hashlib import sha256
from inspect import getattr_static
from threading import Lock, get_ident
from typing import Never

from dpone.contracts.mssql_sqlclient_native_chunk import (
    SqlClientDirectoryCoordinate,
    SqlClientNativeChunkProjection,
    bind_sqlclient_native_chunk,
)
from dpone.contracts.mssql_sqlclient_registration import encode_registration
from dpone.contracts.mssql_sqlclient_stage_identity import stage_object_identity
from dpone.services.mssql_tds_writer_contracts import (
    SqlClientEvidenceKind,
    SqlClientEvidenceObservation,
    SqlClientEvidenceReceipt,
    SqlClientEvidenceRecord,
    SqlClientWriterSettlementObservation,
    SqlClientWriterSettlementProvenance,
    SqlClientWriterSettlementRecord,
    TdsAttemptPhase,
    TdsAttemptSnapshot,
    Verified,
    advance_state,
    directory_key,
    encode_bulk_grant,
    encode_writer_observation,
    encode_writer_settlement,
)
from dpone.services.mssql_tds_writer_execution_custody import (
    SqlClientWriterLocallyExited,
    _LocallyExitedOwner,
)

ERROR = "mssql_native.sqlclient_writer_settlement_unknown"
_TERMINAL_TOKEN = object()


def _invalid() -> Never:
    raise ValueError(ERROR)


def _class_call(value: object, name: str, /, *args: object, **kwargs: object):
    descriptor = getattr_static(type(value), name)
    return descriptor.__get__(value, type(value))(*args, **kwargs)


def _class_property(value: object, name: str):
    descriptor = getattr_static(type(value), name)
    if type(descriptor) is not property:
        _invalid()
    return descriptor.__get__(value, type(value))


class SqlClientWriterVerified:
    """Credential-free and resource-free P10f terminal."""

    __slots__ = ()

    def __init__(self, *args: object, **kwargs: object) -> None:
        _invalid()

    def __init_subclass__(cls, *, _token: object = None, **kwargs: object) -> None:
        if _token is not _TERMINAL_TOKEN:
            raise TypeError(ERROR)
        super().__init_subclass__(**kwargs)

    def __repr__(self) -> str:
        return "SqlClientWriterVerified(<opaque>)"

    def _claim_projection_once(self, candidate: SqlClientWriterVerified, token: object) -> object:
        _invalid()

    def _assert_projection_claim(
        self, candidate: SqlClientWriterVerified, claim: object, token: object
    ) -> SqlClientNativeChunkProjection:
        _invalid()


class SqlClientWriterSettlementUnknown(RuntimeError):
    """Sticky ambiguity after the one-shot P10f claim."""

    def __init__(self) -> None:
        super().__init__(ERROR)


class SqlClientTerminalProjectionUnknown(RuntimeError):
    """Sticky rejection of an invalid, substituted or repeated terminal claim."""

    def __init__(self) -> None:
        super().__init__("mssql_native.sqlclient_terminal_projection_unknown")


def _terminal(projection: SqlClientNativeChunkProjection) -> SqlClientWriterVerified:
    """Seal one exact projection in closure custody, without terminal fields."""
    if type(projection) is not SqlClientNativeChunkProjection:
        _invalid()
    projection.__post_init__()
    snapshot = deepcopy(projection)
    lock, pid, thread_id = Lock(), os.getpid(), get_ident()
    exact: SqlClientWriterVerified
    claim: object | None = None

    class _ExactVerified(SqlClientWriterVerified, _token=_TERMINAL_TOKEN):
        __slots__ = ()

        def __init__(self) -> None:
            pass

        def __init_subclass__(cls, **kwargs: object) -> None:
            raise TypeError(ERROR)

        def _claim_projection_once(self, candidate, token):
            nonlocal claim
            if token is not _TERMINAL_TOKEN or candidate is not self or self is not exact:
                _invalid()
            if (os.getpid(), get_ident()) != (pid, thread_id):
                _invalid()
            with lock:
                if claim is not None:
                    _invalid()
                claim = object()
                return claim

        def _assert_projection_claim(self, candidate, candidate_claim, token):
            if token is not _TERMINAL_TOKEN or candidate is not self or self is not exact:
                _invalid()
            if (os.getpid(), get_ident()) != (pid, thread_id):
                _invalid()
            with lock:
                if claim is None or candidate_claim is not claim or projection != snapshot:
                    _invalid()
                projection.__post_init__()
                return projection

    exact = _ExactVerified()
    return exact


def project_sqlclient_native_chunk(terminal: SqlClientWriterVerified) -> SqlClientNativeChunkProjection:
    """Consume the exact opaque P10f terminal once and return its value projection."""
    try:
        if not isinstance(terminal, SqlClientWriterVerified):
            _invalid()
        claim = type(terminal)._claim_projection_once(terminal, terminal, _TERMINAL_TOKEN)
        projection = type(terminal)._assert_projection_claim(terminal, terminal, claim, _TERMINAL_TOKEN)
        if type(projection) is not SqlClientNativeChunkProjection:
            _invalid()
        projection.__post_init__()
        return projection
    except BaseException:
        raise SqlClientTerminalProjectionUnknown() from None


def _projection(owner: _LocallyExitedOwner, record, receipt, state) -> SqlClientNativeChunkProjection:
    identity = owner.registration.binding.identity
    input_receipt = owner.registration.input.expected
    verification_sha256 = state.state.verification_sha256 if type(state) is TdsAttemptSnapshot else None
    if (
        type(receipt) is not SqlClientEvidenceReceipt
        or type(state) is not TdsAttemptSnapshot
        or state.state.phase is not TdsAttemptPhase.VERIFIED
        or state.state.identity != identity
        or state.state.object_identity != owner.registration.binding.object_identity
        or stage_object_identity(owner.stage) != owner.registration.binding.object_identity
        or verification_sha256 != receipt.payload_sha256
        or record.attempt_sha256 != owner.registration.binding.attempt_sha256
        or record.observation.typed_sum is None
    ):
        _invalid()
    return bind_sqlclient_native_chunk(
        attempt=identity,
        attempt_sha256=owner.registration.binding.attempt_sha256,
        stage=owner.stage,
        object_identity=stage_object_identity(owner.stage),
        rows=record.observation.row_count,
        encoded_bytes=input_receipt.encoded_bytes,
        file_sha256=input_receipt.file_sha256,
        typed_digest=record.observation.typed_digest,
        typed_sum=record.observation.typed_sum,
        registration_receipt=SqlClientEvidenceRecord(
            owner.registration.binding.attempt_sha256,
            SqlClientEvidenceKind.REGISTRATION,
            encode_registration(owner.registration),
        ).receipt,
        verification_receipt=receipt,
        lifecycle_verification_sha256=verification_sha256,
        lifecycle_revision=state.revision,
        worker_build_sha256=owner.registration.binding.build_sha256,
        implementation_sha256=identity.implementation_sha256,
        helper_implementation_sha256=record.helper.implementation_sha256,
        directory_key=directory_key(identity),
        directory_coordinate=SqlClientDirectoryCoordinate(
            target_key=identity.target_key,
            run_id=identity.run_id,
            ordinal=identity.ordinal,
            attempt=identity.attempt,
        ),
    )


def _record(
    owner: _LocallyExitedOwner,
    observation: SqlClientWriterSettlementObservation,
    helper: SqlClientWriterSettlementProvenance,
):
    expectation = owner.content_expectation
    record = SqlClientWriterSettlementRecord(
        attempt_sha256=owner.registration.binding.attempt_sha256,
        registration_sha256=sha256(encode_registration(owner.registration)).hexdigest(),
        writer_observation_sha256=sha256(encode_writer_observation(owner.observation)).hexdigest(),
        grant_sha256=sha256(encode_bulk_grant(owner.grant)).hexdigest(),
        result_sha256=owner.result_receipt.payload_sha256,
        local_exit_sha256=owner.local_exit_receipt.payload_sha256,
        p9_settlement_sha256=owner.p9_settlement_sha256,
        helper=helper,
        expectation=expectation,
        observation=observation,
    )
    if (
        observation.departure.original != owner.observation.remote_session
        or observation.stage_before != owner.stage
        or owner.local_exit.exit_code != 0
        or owner.result.result.error is not None
        or owner.result.result.receipt != owner.input_descriptor.expected
        or expectation.rows != owner.input_descriptor.expected.rows
        or expectation.file_sha256 != owner.input_descriptor.expected.file_sha256
    ):
        _invalid()
    return record


def _close(owner: _LocallyExitedOwner, verifier: object, *, close_lifecycle: bool = True) -> None:
    failed = False
    resources = [
        (verifier, "close", {}),
        (owner.evidence, "close", {"deadline": owner.operation_deadline}),
    ]
    if close_lifecycle:
        resources.append((owner.lifecycle, "close", {"deadline": owner.operation_deadline}))
    for resource, name, kwargs in resources:
        try:
            if _class_call(resource, name, **kwargs) is not None:
                failed = True
        except BaseException:
            failed = True
    if failed:
        _invalid()


def settle_sqlclient_writer(
    exited: SqlClientWriterLocallyExited,
    verifier: object,
    *,
    retain_lifecycle: bool = False,
) -> SqlClientWriterVerified:
    """Consume P10e once; evidence ACK precedes EXITED→VERIFIED and all closes."""
    if not isinstance(exited, SqlClientWriterLocallyExited) or type(retain_lifecycle) is not bool:
        _invalid()
    owner: _LocallyExitedOwner | None = None
    cleanup_attempted = False
    try:
        claim = type(exited)._claim_p10f_once(exited, exited)
        owner = type(exited)._assert_p10f_claim(exited, exited, claim)
        if (
            type(owner) is not _LocallyExitedOwner
            or owner.state.state.phase is not TdsAttemptPhase.EXITED
            or _class_property(owner.lifecycle, "snapshot") is not owner.state
        ):
            _invalid()
        observed = _class_call(
            verifier,
            "observe",
            writer_observation=owner.observation,
            writer_admission=owner.writer_admission,
            stage=owner.stage,
            # P10f verifies the descriptor that the writer process actually
            # admitted. Its inherited descriptor number is intentionally
            # distinct from the preparation owner's source descriptor.
            input_descriptor=owner.registration.input,
            expectation=owner.content_expectation,
            operation_deadline=owner.operation_deadline,
        )
        if type(observed) is not SqlClientWriterSettlementObservation:
            _invalid()
        observed.__post_init__()
        helper = _class_property(verifier, "provenance")
        if type(helper) is not SqlClientWriterSettlementProvenance:
            _invalid()
        helper.__post_init__()
        record = _record(owner, observed, helper)
        payload = encode_writer_settlement(record)
        evidence = SqlClientEvidenceRecord(
            owner.registration.binding.attempt_sha256,
            SqlClientEvidenceKind.VERIFICATION,
            payload,
        )
        receipt = _class_call(owner.evidence, "write", evidence, deadline=owner.operation_deadline)
        acknowledgement = _class_property(owner.evidence, "observation")
        if (
            type(receipt) is not SqlClientEvidenceReceipt
            or receipt != evidence.receipt
            or type(acknowledgement) is not SqlClientEvidenceObservation
            or acknowledgement.receipt is not receipt
        ):
            _invalid()
        event = Verified(receipt.payload_sha256)
        predicted = advance_state(owner.state.state, event, expected_phase=TdsAttemptPhase.EXITED)
        state = _class_call(
            owner.lifecycle,
            "advance",
            event,
            expected_phase=TdsAttemptPhase.EXITED,
            deadline=owner.operation_deadline,
        )
        if (
            type(state) is not TdsAttemptSnapshot
            or state.state != predicted
            or state.revision <= owner.state.revision
            or _class_property(owner.lifecycle, "snapshot") is not state
        ):
            _invalid()
        projection = _projection(owner, record, receipt, state)
        cleanup_attempted = True
        _close(owner, verifier, close_lifecycle=not retain_lifecycle)
        return _terminal(projection)
    except BaseException:
        if owner is not None and not cleanup_attempted:
            try:
                _close(owner, verifier)
            except BaseException:
                pass
        raise SqlClientWriterSettlementUnknown() from None


__all__ = (
    "SqlClientTerminalProjectionUnknown",
    "SqlClientWriterSettlementUnknown",
    "SqlClientWriterVerified",
    "project_sqlclient_native_chunk",
    "settle_sqlclient_writer",
)
