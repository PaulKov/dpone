"""Private credentials bind to the accepted request without secret diagnostics."""

from dataclasses import replace
from hashlib import sha256

import pytest

from dpone.app.mssql_sqlclient_permission_grant_request import (
    PRIVATE_LIMIT,
    SqlClientPermissionGrantLaunchRequest,
    decode_permission_credentials,
    decode_permission_launch_request,
    encode_permission_credentials,
    encode_permission_launch_request,
)
from dpone.contracts.mssql_sqlclient_permission_grant_wire import PermissionWireBinding
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial, TdsConnectionProfile
from tests.test_mssql_sqlclient_permission_grant_wire import fixture


def launch_request():
    binding, *_ = fixture()
    operation_deadline = 20.0
    binding = PermissionWireBinding(
        binding.request, binding.operation, binding.startup, binding.execution_owner, 20_000_000_000
    )
    value = SqlClientPermissionGrantLaunchRequest(
        request=binding.request,
        operation=binding.operation,
        execution_owner=binding.execution_owner,
        connection_material=TdsConnectionMaterial("synthetic.invalid", 1433, "db", "writer", "PRIVATE_CANARY"),
        profile=TdsConnectionProfile.SYNTHETIC_LOCAL,
        admission_sha256="a" * 64,
        startup_deadline=10.0,
        operation_deadline=operation_deadline,
        termination_timeout=2.0,
        session_nonce=b"s" * 32,
    )
    return value, binding


def test_public_request_is_canonical_and_secret_free():
    value, _ = launch_request()
    payload = encode_permission_launch_request(value)
    decoded = decode_permission_launch_request(payload)
    assert decoded["request"] == value.request
    assert decoded["operation"] == value.operation
    assert decoded["execution_owner"] == value.execution_owner
    assert b"PRIVATE_CANARY" not in payload
    assert "PRIVATE_CANARY" not in repr(value)
    assert encode_permission_launch_request(value) == value.public_payload()


def test_credentials_bind_request_process_nonce_profile_and_material():
    value, binding = launch_request()
    request_payload = b"accepted-public-request"
    payload = encode_permission_credentials(value, binding=binding, request_payload=request_payload)
    material, nonce = decode_permission_credentials(
        payload,
        binding=binding,
        request_payload=request_payload,
        profile=TdsConnectionProfile.SYNTHETIC_LOCAL,
    )
    assert material == value.connection_material
    assert nonce == b"s" * 32
    assert b"PRIVATE_CANARY" in payload


@pytest.mark.parametrize("change", ["request", "process", "nonce", "profile", "trailing", "oversize"])
def test_credentials_fail_closed_on_binding_or_encoding_change(change):
    value, binding = launch_request()
    request_payload = b"accepted-public-request"
    payload = encode_permission_credentials(value, binding=binding, request_payload=request_payload)
    supplied_binding, supplied_request, supplied_profile = binding, request_payload, value.profile
    if change == "request":
        supplied_request += b"x"
    elif change == "process":
        supplied_binding = replace(binding, startup=replace(binding.startup, launch_nonce=b"x" * 32))
    elif change == "nonce":
        with pytest.raises(ValueError):
            replace(value, session_nonce=b"short")
        return
    elif change == "profile":
        supplied_profile = TdsConnectionProfile.VERIFIED_TLS
    elif change == "trailing":
        payload += b" "
    else:
        payload = b"x" * (PRIVATE_LIMIT + 1)
    with pytest.raises(ValueError):
        decode_permission_credentials(
            payload,
            binding=supplied_binding,
            request_payload=supplied_request,
            profile=supplied_profile,
        )


def test_credentials_bind_exact_accepted_digest():
    value, binding = launch_request()
    request_payload = b"accepted"
    payload = encode_permission_credentials(value, binding=binding, request_payload=request_payload)
    assert sha256(request_payload).hexdigest().encode() in payload
