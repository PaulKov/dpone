"""One-shot independent observation and durable pre-grant intent ordering."""

from __future__ import annotations

from typing import Never
from uuid import UUID

from dpone.ports.mssql_sqlclient_writer_observer import (
    SqlClientWriterObserverCleanup,
    SqlClientWriterObserverCustody,
)
from dpone.services.mssql_tds_writer_contracts import (
    SqlClientEvidenceKind,
    SqlClientGrantIntent,
    SqlClientWriterObserved,
    encode_bulk_grant,
    encode_writer_observation,
)
from dpone.services.mssql_tds_writer_observation_custody import (
    SqlClientWriterGrantReady,
    SqlClientWriterObservationUnknown,
    SqlClientWriterResultReady,
    _grant_ready,
    _GrantReadyOwner,
    _result_ready,
    _ResultReadyOwner,
)
from dpone.services.mssql_tds_writer_observation_validation import (
    advance_lifecycle,
    bulk_grant_record,
    capture_claimed_refs,
    persist_evidence,
    reassert_claimed_refs,
    reassert_evidence_receipt,
    validate_observer_refs,
    validate_writer_result,
    writer_observation_record,
)
from dpone.services.mssql_tds_writer_pregrant import SqlClientWriterPreGrant

ERROR = "mssql_native.sqlclient_writer_observation_unknown"


def _invalid() -> Never:
    raise ValueError(ERROR)


def prepare_sqlclient_writer_observation(
    pregrant: SqlClientWriterPreGrant,
    *,
    observer: SqlClientWriterObserverCustody | None,
    grant_id: str | None,
    now_ns: int,
) -> SqlClientWriterResultReady | SqlClientWriterGrantReady:
    """Consume P10c once and stop after durable grant intent, before delivery."""
    if not isinstance(pregrant, SqlClientWriterPreGrant) or type(now_ns) is not int or now_ns < 0:
        _invalid()
    if type(pregrant)._p10d_identity(pregrant) is not pregrant:
        _invalid()
    route = type(pregrant)._p10d_route(pregrant, pregrant)
    writer_cleanup = type(pregrant)._prepare_p10d_cleanup(pregrant, pregrant)
    observer_cleanup: SqlClientWriterObserverCleanup | None = None
    observer_claim: object | None = None
    if route == "result":
        if observer is not None or grant_id is not None:
            _invalid()
    elif route == "session":
        if not isinstance(observer, SqlClientWriterObserverCustody) or type(grant_id) is not str:
            _invalid()
        try:
            parsed = UUID(grant_id)
        except (ValueError, TypeError, AttributeError):
            _invalid()
        if not parsed.int or str(parsed) != grant_id or type(observer)._p10d_identity(observer) is not observer:
            _invalid()
        observer_cleanup = type(observer)._prepare_cleanup(observer, observer)
    else:
        _invalid()
    try:
        writer_claim = type(pregrant)._claim_p10d_once(pregrant, pregrant, writer_cleanup)
        if observer is not None:
            assert observer_cleanup is not None
            observer_claim = type(observer)._claim_once(observer, observer, observer_cleanup)
        refs = capture_claimed_refs(pregrant, writer_claim, writer_cleanup)
        if route == "result":
            validate_writer_result(refs)
            return _result_ready(_ResultReadyOwner(refs))
        assert (
            observer is not None
            and observer_cleanup is not None
            and observer_claim is not None
            and grant_id is not None
        )
        if now_ns >= refs.launch.registration.launch.operation_deadline_ns:
            _invalid()
        observer_refs = type(observer)._assert_claim(observer, observer, observer_claim, observer_cleanup)
        validate_observer_refs(refs, observer_refs)
        session = refs.session
        assert session is not None
        observed = type(observer)._observe_once(
            observer,
            observer,
            observer_claim,
            observer_cleanup,
            session_id=session.session_id,
            nonce=bytes.fromhex(session.nonce),
            deadline=observer_refs.operation_deadline,
        )
        observation = writer_observation_record(refs, observed)
        observation_receipt = persist_evidence(
            refs, SqlClientEvidenceKind.WRITER_OBSERVATION, encode_writer_observation(observation)
        )
        observed_state = advance_lifecycle(
            refs, refs.state, SqlClientWriterObserved(observation_receipt.payload_sha256)
        )
        reassert_claimed_refs(refs, observed_state)
        reassert_evidence_receipt(refs, observation_receipt, SqlClientEvidenceKind.WRITER_OBSERVATION)
        if type(observer)._assert_claim(observer, observer, observer_claim, observer_cleanup) is not observer_refs:
            _invalid()
        grant = bulk_grant_record(
            refs, observation, observation_receipt.payload_sha256, grant_id=grant_id, now_ns=now_ns
        )
        grant_bytes = encode_bulk_grant(grant)
        grant_receipt = persist_evidence(refs, SqlClientEvidenceKind.GRANT_INTENT, grant_bytes)
        grant_state = advance_lifecycle(refs, observed_state, SqlClientGrantIntent(grant_receipt.payload_sha256))
        reassert_claimed_refs(refs, grant_state)
        reassert_evidence_receipt(refs, grant_receipt, SqlClientEvidenceKind.GRANT_INTENT)
        if type(observer)._assert_claim(observer, observer, observer_claim, observer_cleanup) is not observer_refs:
            _invalid()
        return _grant_ready(
            _GrantReadyOwner(
                refs,
                observer,
                observer_claim,
                observer_cleanup,
                observer_refs,
                observation,
                observation_receipt,
                grant,
                grant_bytes,
                grant_receipt,
                grant_state,
            )
        )
    except BaseException:
        type(writer_cleanup)._cleanup_once(writer_cleanup)
        if observer_cleanup is not None:
            type(observer_cleanup)._cleanup_once(observer_cleanup)
    raise SqlClientWriterObservationUnknown(writer_cleanup, observer_cleanup) from None


__all__ = (
    "SqlClientWriterGrantReady",
    "SqlClientWriterObservationUnknown",
    "SqlClientWriterResultReady",
    "prepare_sqlclient_writer_observation",
)
