"""Dual-bound handshake, exact legacy identity projections and secret separation."""

from dataclasses import replace

import pytest

from dpone.app import mssql_tds_coordinator_request as legacy
from dpone.contracts import mssql_tds_coordinator_codec as canonical
from dpone.contracts.mssql_sqlclient_observe_handshake import (
    SqlClientObserveCredentials,
    decode_credentials,
    encode_credentials,
    encode_request_accepted,
    validate_request_accepted,
)
from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand
from tests.test_mssql_sqlclient_observe import request
from tests.test_mssql_tds_coordinator_request import values


def credentials():
    req = request()
    old, startup, *_ = values()
    identity = replace(
        old.identity, parent=req.parent, command=TdsCoordinatorCommand.OBSERVE, command_sha256=req.command_sha256
    )
    value = SqlClientObserveCredentials(
        req.command_sha256,
        identity,
        old.execution_owner,
        startup.process,
        startup.launch_nonce,
        old.session_nonce,
        old.connection_material,
        old.driver_profile,
    )
    return req, value, startup


def test_identity_projection_has_one_canonical_producer():
    assert legacy._identity is canonical.coordinator_identity_from_body
    assert legacy._identity_body is canonical.coordinator_identity_body


def test_request_ack_requires_original_digest_and_nonce():
    req, value, startup = credentials()
    payload = encode_request_accepted(req, startup)
    validate_request_accepted(payload, req, startup)
    for bad in (payload + b" ", payload.replace(req.command_sha256.encode(), b"a" * 64)):
        with pytest.raises(ValueError):
            validate_request_accepted(bad, req, startup)
    assert value.connection_material.password.encode() not in payload


def test_secret_envelope_roundtrip_is_bound_to_both_messages():
    req, value, startup = credentials()
    payload = encode_credentials(value, req)
    assert decode_credentials(payload, req, startup, value.driver_profile) == value
    with pytest.raises(ValueError):
        decode_credentials(
            payload, replace(req, operation_deadline_ns=req.operation_deadline_ns + 1), startup, value.driver_profile
        )
    assert value.connection_material.password not in repr(value)
