"""Closed versioned JSON boundary for durable TDS attempt records.

Type/transition invariants belong to the immutable models. This codec rejects
ambiguous wire records and bounds the serialized envelope before decoding.
"""

from dataclasses import fields
from typing import Any

from dpone.contracts.mssql_sqlclient_attempt import SqlClientAttemptEvidence
from dpone.contracts.mssql_tds_worker import TdsAttemptState, state_payload
from dpone.contracts.mssql_tds_worker_identity import (
    ParentAuthority,
    TdsAttemptError,
    TdsAttemptIdentity,
    TdsAttemptOwnership,
    TdsAttemptPhase,
    TdsObjectIdentity,
    TdsProcessIdentity,
)
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object


def encode_state(state: TdsAttemptState) -> bytes:
    """Canonical closed wire representation, capped before persistence."""
    payload = canonical_json_bytes(state_payload(state))
    if len(payload) > 16384:
        raise ValueError("mssql_native.tds_record_too_large")
    return payload


def _construct(cls: Any, value: Any) -> Any:
    if type(value) is not dict or set(value) != {f.name for f in fields(cls)}:
        raise ValueError("mssql_native.tds_invalid_record_fields")
    return cls(**value)


def decode_state(payload: bytes) -> TdsAttemptState:
    """Reject oversize data before JSON decoding and reject ambiguous structure."""
    if type(payload) is not bytes or len(payload) > 16384:
        raise ValueError("mssql_native.tds_invalid_record_bytes")
    try:
        value = strict_json_object(payload)
    except RecursionError:
        raise ValueError("mssql_native.tds_invalid_record_depth") from None
    version = value.get("schema_version")
    if type(version) is not int or version not in (1, 2):
        raise ValueError("mssql_native.tds_invalid_record_version")
    expected = {f.name for f in fields(TdsAttemptState)}
    if version == 1:
        expected -= {"backend", "sqlclient"}
    if set(value) != expected:
        raise ValueError("mssql_native.tds_invalid_record_fields")
    if version == 2 and value["sqlclient"] is not None:
        value["sqlclient"] = _construct(SqlClientAttemptEvidence, value["sqlclient"])
    for key, cls in (
        ("identity", TdsAttemptIdentity),
        ("ownership", TdsAttemptOwnership),
        ("object_identity", TdsObjectIdentity),
        ("process", TdsProcessIdentity),
        ("parent_authority", ParentAuthority),
    ):
        if value[key] is not None:
            value[key] = _construct(cls, value[key])
    value["phase"] = TdsAttemptPhase(value["phase"])
    if value["error"] is not None:
        value["error"] = TdsAttemptError(value["error"])
    return TdsAttemptState(**value)
