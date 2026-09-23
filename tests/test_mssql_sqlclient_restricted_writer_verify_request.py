import math
from dataclasses import replace

import pytest

from dpone.app.mssql_sqlclient_restricted_writer_verify_request import (
    RestrictedWriterCredentialSupplier,
    RestrictedWriterVerificationOperations,
    RestrictedWriterVerifyLaunchRequest,
    build_origin_request,
    decode_verify_credentials,
    decode_verify_launch_request,
)
from dpone.contracts.mssql_sqlclient_restricted_writer_verify import (
    RestrictedWriterVerifyRegistration,
    RestrictedWriterVerifyReservation,
    verify_request_digest,
)
from dpone.contracts.mssql_sqlclient_restricted_writer_verify_codec import encode_verify_request
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial, TdsConnectionProfile
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from tests.test_mssql_sqlclient_permission_grant import request as grant_request
from tests.test_mssql_sqlclient_restricted_writer_verify import verify_request
from tests.test_mssql_tds_coordinator import PROCESS
from tests.test_mssql_tds_directory_journal import OWNER


def launch_request():
    return RestrictedWriterVerifyLaunchRequest(
        request=verify_request(),
        startup_deadline=100.0,
        operation_deadline=200.0,
        termination_timeout=2.0,
        admission_sha256="a" * 64,
        profile=TdsConnectionProfile.SYNTHETIC_LOCAL,
    )


def registration(launch=None):
    launch = launch_request() if launch is None else launch
    request_sha256 = verify_request_digest(encode_verify_request(launch.request))
    reservation = RestrictedWriterVerifyReservation(launch.request.operation_id, request_sha256, "b" * 64, OWNER)
    return RestrictedWriterVerifyRegistration(reservation, PROCESS, OWNER)


def test_public_payload_excludes_credentials_and_private_frame_is_bound():
    launch = launch_request()
    public = launch.public_payload()
    assert b"secret-value" not in public and b"localhost" not in public and b"writer" in public
    decoded = decode_verify_launch_request(public)
    assert decoded["request"] == launch.request
    reg = registration(launch)
    supplier = RestrictedWriterCredentialSupplier(
        lambda: (TdsConnectionMaterial("localhost", 1433, "db", "writer", "secret-value"), b"n" * 32)
    )
    private = supplier.take(launch, reg, public_payload=public)
    material, nonce = decode_verify_credentials(private, public_payload=public, process=PROCESS)
    assert material.database == "db" and nonce == b"n" * 32
    assert supplier.consumed and not hasattr(launch, "connection_material")


def test_substituted_process_or_registration_owner_is_rejected():
    launch, reg = launch_request(), registration()
    private = RestrictedWriterCredentialSupplier(
        lambda: (TdsConnectionMaterial("localhost", 1433, "db", "writer", "secret-value"), b"n" * 32)
    ).take(launch, reg, public_payload=launch.public_payload())
    with pytest.raises(ValueError):
        decode_verify_credentials(private, public_payload=launch.public_payload(), process=replace(PROCESS, pid=999))
    with pytest.raises(ValueError):
        RestrictedWriterVerifyRegistration(reg.reservation, PROCESS, replace(OWNER))


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("startup_deadline", 201.0),
        ("termination_timeout", 0.0),
        ("termination_timeout", math.inf),
        ("termination_timeout", 2),
        ("admission_sha256", "A" * 64),
        ("admission_sha256", "a" * 63),
    ),
)
def test_public_decoder_rejects_invalid_deadline_timeout_and_digest(field, value):
    body = strict_json_object(launch_request().public_payload())
    body[field] = value
    with pytest.raises(ValueError):
        decode_verify_launch_request(canonical_json_bytes(body))


def test_supplier_is_one_shot_and_launch_never_retains_secrets():
    launch, reg = launch_request(), registration()
    calls = []
    supplier = RestrictedWriterCredentialSupplier(
        lambda: (
            calls.append(True) or TdsConnectionMaterial("localhost", 1433, "db", "writer", "secret-value"),
            b"n" * 32,
        )
    )
    assert b"secret-value" not in launch.public_payload() and b"localhost" not in launch.public_payload()
    supplier.take(launch, reg, public_payload=launch.public_payload())
    assert calls == [True] and supplier.consumed and supplier._factory is None
    with pytest.raises(ValueError):
        supplier.take(launch, reg, public_payload=launch.public_payload())


def _effective(request):
    return {
        "subject": {},
        "login_token": [[row.principal_id, row.sid, row.name, row.type, row.usage] for row in request.login_token],
        "user_token": [[row.principal_id, row.sid, row.name, row.type, row.usage] for row in request.user_token],
        "server_permissions": [
            [row.entity_name, row.subentity_name, row.permission_name] for row in request.server_permissions
        ],
        "database_permissions": [
            [row.entity_name, row.subentity_name, row.permission_name] for row in request.database_permissions
        ],
        "restored_management": {},
    }


def test_origin_builder_is_the_canonical_validation_projection():
    expected = verify_request()
    built = build_origin_request(
        grant_request(),
        _effective(expected),
        operation_id=expected.operation_id,
        implementation_sha256=expected.implementation_sha256,
    )
    assert built == expected
    RestrictedWriterVerificationOperations().validate_origin_request(built, grant_request(), _effective(expected))


def test_origin_builder_canonicalizes_sql_empty_permission_names():
    expected = verify_request()
    effective = _effective(expected)
    effective["server_permissions"] = [["", "", "CONNECT SQL"]]
    effective["database_permissions"] = [["", "", "CONNECT"]]

    built = build_origin_request(
        grant_request(),
        effective,
        operation_id=expected.operation_id,
        implementation_sha256=expected.implementation_sha256,
    )

    assert built.server_permissions[0].entity_name is None
    assert built.server_permissions[0].subentity_name is None
    assert built.database_permissions[0].entity_name is None
    assert built.database_permissions[0].subentity_name is None


def test_origin_builder_rejects_incomplete_effective_facts():
    expected = verify_request()
    effective = _effective(expected)
    del effective["user_token"]
    with pytest.raises(ValueError, match="restricted_writer_verify_invalid"):
        build_origin_request(
            grant_request(),
            effective,
            operation_id=expected.operation_id,
            implementation_sha256=expected.implementation_sha256,
        )
