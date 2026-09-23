"""Closed bounded coordinator record codec; no credentials or command payloads.

An operation's locator intentionally omits mutable identity fields. Readers must
supply and compare the complete expected reservation identity before using state.
"""

from dataclasses import asdict
from typing import Any

from dpone.contracts.mssql_tds_coordinator import (
    TdsCoordinatorGrant,
    TdsCoordinatorIdentity,
    TdsCoordinatorLocalKind,
    TdsCoordinatorLocalObservation,
    TdsCoordinatorPhase,
    TdsCoordinatorRemoteKind,
    TdsCoordinatorRemoteObservation,
    TdsCoordinatorResult,
    TdsCoordinatorResultKind,
    TdsCoordinatorState,
)
from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand
from dpone.contracts.mssql_tds_session import (
    TdsSessionIdentity,
    decode_session_continuity_identity,
    encode_session_continuity_identity,
)
from dpone.contracts.mssql_tds_worker import (
    TdsAttemptError,
    TdsAttemptIdentity,
    TdsAttemptOwnership,
    TdsProcessIdentity,
)
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import canonical_uuid as _uuid
from dpone.contracts.strict_record import construct_record as _construct
from dpone.contracts.strict_record import record_shape as _shape
from dpone.contracts.strict_record import string_enum as _enum

MAX_COORDINATOR_RECORD_BYTES = 16384
_SCHEMA = "dpone.tds.coordinator-operation.v1"


def _session_payload(session: TdsSessionIdentity | None) -> dict | None:
    return None if session is None else strict_json_object(encode_session_continuity_identity(session))


def encode_coordinator_state(state: TdsCoordinatorState) -> bytes:
    """Canonical UTF-8, bounded before persistence; history cannot accumulate."""
    if type(state) is not TdsCoordinatorState:
        raise ValueError("mssql_native.tds_coordinator_record_invalid")
    value = asdict(state)
    value["schema"] = _SCHEMA
    value["identity"]["operation_id"] = str(state.identity.operation_id)
    value["session"] = _session_payload(state.session)
    if state.grant is not None:
        value["grant"]["session"] = _session_payload(state.grant.session)
        value["grant"]["grant_id"] = str(state.grant.grant_id)
    if state.remote is not None:
        value["remote"]["session"] = _session_payload(state.remote.session)
    payload = canonical_json_bytes(value)
    if len(payload) > MAX_COORDINATOR_RECORD_BYTES:
        raise ValueError("mssql_native.tds_coordinator_record_too_large")
    return payload


def _session(value: Any) -> TdsSessionIdentity | None:
    if value is None:
        return None
    if type(value) is not dict:
        raise ValueError("session_type")
    return decode_session_continuity_identity(canonical_json_bytes(value))


def decode_coordinator_state(payload: bytes, *, identity: TdsCoordinatorIdentity) -> TdsCoordinatorState:
    """Reject ambiguous scalars, nested shapes and changed original reservations."""
    try:
        if type(payload) is not bytes or len(payload) > MAX_COORDINATOR_RECORD_BYTES:
            raise ValueError("record_bytes")
        if type(identity) is not TdsCoordinatorIdentity:
            raise ValueError("expected_identity")
        value = strict_json_object(payload)
        if value.pop("schema", None) != _SCHEMA:
            raise ValueError("schema")
        _shape(TdsCoordinatorState, value)
        original = _shape(TdsCoordinatorIdentity, value["identity"])
        original["parent"] = _construct(TdsAttemptIdentity, original["parent"])
        original["operation_id"] = _uuid(original["operation_id"])
        original["command"] = _enum(TdsCoordinatorCommand, original["command"])
        value["identity"] = TdsCoordinatorIdentity(**original)
        if value["identity"] != identity:
            raise ValueError("identity_changed")
        for key in ("execution_owner", "ownership"):
            value[key] = _construct(TdsAttemptOwnership, value[key])
        value["phase"] = _enum(TdsCoordinatorPhase, value["phase"])
        value["session"] = _session(value["session"])
        if value["process"] is not None:
            value["process"] = _construct(TdsProcessIdentity, value["process"])
        if value["error"] is not None:
            value["error"] = _enum(TdsAttemptError, value["error"])
        if value["grant"] is not None:
            grant = _shape(TdsCoordinatorGrant, value["grant"])
            grant["grant_id"] = _uuid(grant["grant_id"])
            grant["ownership"] = _construct(TdsAttemptOwnership, grant["ownership"])
            grant["process"] = _construct(TdsProcessIdentity, grant["process"])
            grant["session"] = _session(grant["session"])
            value["grant"] = TdsCoordinatorGrant(**grant)
        if value["result"] is not None:
            result = _shape(TdsCoordinatorResult, value["result"])
            result["outcome"] = _enum(TdsCoordinatorResultKind, result["outcome"])
            if result["error"] is not None:
                result["error"] = _enum(TdsAttemptError, result["error"])
            value["result"] = TdsCoordinatorResult(**result)
        if value["local"] is not None:
            local = _shape(TdsCoordinatorLocalObservation, value["local"])
            local["kind"] = _enum(TdsCoordinatorLocalKind, local["kind"])
            if local["process"] is not None:
                local["process"] = _construct(TdsProcessIdentity, local["process"])
            value["local"] = TdsCoordinatorLocalObservation(**local)
        if value["remote"] is not None:
            remote = _shape(TdsCoordinatorRemoteObservation, value["remote"])
            remote["kind"] = _enum(TdsCoordinatorRemoteKind, remote["kind"])
            remote["session"] = _session(remote["session"])
            value["remote"] = TdsCoordinatorRemoteObservation(**remote)
        return TdsCoordinatorState(**value)
    except (ValueError, TypeError, OverflowError, RecursionError):
        raise ValueError("mssql_native.tds_coordinator_record_invalid") from None


def coordinator_identity_from_body(value: dict) -> TdsCoordinatorIdentity:
    body = dict(_shape(TdsCoordinatorIdentity, value))
    body["parent"] = _construct(TdsAttemptIdentity, body["parent"])
    body["operation_id"] = _uuid(body["operation_id"])
    body["command"] = _enum(TdsCoordinatorCommand, body["command"])
    return TdsCoordinatorIdentity(**body)


def coordinator_identity_body(identity: TdsCoordinatorIdentity) -> dict:
    return dict(asdict(identity), operation_id=str(identity.operation_id))
