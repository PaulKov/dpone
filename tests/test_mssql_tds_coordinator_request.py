"""Semantic coordinator envelopes reject wrong bindings before SQL effects."""

from dataclasses import replace
from functools import partial
from uuid import UUID

import pytest

from dpone.adapters.mssql_tds_coordinator_connection import TdsConnectionMaterial, TdsConnectionProfile
from dpone.app.mssql_tds_coordinator_request import (
    TdsCoordinatorCredentials,
    decode_create_response,
    decode_credentials,
    decode_grant,
    encode_create_response,
    encode_credentials,
    encode_grant,
    failed_response,
    successful_response,
)
from dpone.contracts.mssql_tds_coordinator import (
    TdsCoordinatorGrant,
    coordinator_grant_digest,
    coordinator_identity_digest,
)
from dpone.contracts.mssql_tds_coordinator_authority import authority_digest
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup
from dpone.contracts.mssql_tds_create import create_command_digest
from dpone.contracts.mssql_tds_worker import TdsAttemptError
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from tests.test_mssql_tds_coordinator import identity
from tests.test_mssql_tds_coordinator_authority import authority
from tests.test_mssql_tds_create import request
from tests.test_mssql_tds_create_codec import evidence


def values():
    operation = replace(identity(), command_sha256=create_command_digest(request()))
    auth = replace(
        authority(),
        operation_sha256=coordinator_identity_digest(operation),
        implementation_sha256=operation.implementation_sha256,
    )
    startup = TdsCoordinatorStartup(auth.process, operation.implementation_sha256, "/admitted", b"l" * 32)
    credentials = TdsCoordinatorCredentials(
        operation,
        auth.execution_owner,
        auth.process,
        startup.launch_nonce,
        request(),
        TdsConnectionMaterial("localhost", 1433, "db", "user", "privatepassword"),
        b"n" * 32,
        TdsConnectionProfile.SYNTHETIC_LOCAL,
    )
    grant = TdsCoordinatorGrant(
        coordinator_identity_digest(operation),
        auth.execution_owner,
        auth.process,
        auth.session,
        authority_digest(auth),
        UUID(int=88),
    )
    proof = replace(
        evidence(),
        operation_sha256=grant.operation_sha256,
        grant_sha256=coordinator_grant_digest(grant),
        authority_sha256=grant.authority_sha256,
    )
    return credentials, startup, auth, grant, proof


def test_roundtrip_and_no_credentials_in_repr():
    credentials, startup, auth, grant, proof = values()
    assert (
        decode_credentials(encode_credentials(credentials), startup=startup, profile=credentials.driver_profile)
        == credentials
    )
    assert "privatepassword" not in repr(credentials)
    assert (
        decode_grant(encode_grant(credentials.identity, grant), identity=credentials.identity, authority=auth) == grant
    )
    for response in (
        successful_response(proof),
        failed_response(credentials.identity, grant, auth, TdsAttemptError.DRIVER),
    ):
        assert (
            decode_create_response(
                encode_create_response(response),
                request=credentials.request,
                identity=credentials.identity,
                grant=grant,
                authority=auth,
            )
            == response
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("login_timeout_seconds", 5),
        ("query_timeout_seconds", 3),
        ("connection_string", "secret"),
        ("driver_profile", "arbitrary"),
    ],
)
def test_obsolete_or_arbitrary_credential_settings_rejected(field, value):
    credentials, startup, *_ = values()
    body = strict_json_object(encode_credentials(credentials))
    body[field] = value
    with pytest.raises(ValueError):
        decode_credentials(canonical_json_bytes(body), startup=startup, profile=credentials.driver_profile)


@pytest.mark.parametrize("binding", ["source", "process", "nonce", "profile", "request"])
def test_credentials_require_exact_startup_and_admission(binding):
    credentials, startup, *_ = values()
    profile = credentials.driver_profile
    if binding == "source":
        startup = replace(startup, implementation_sha256="f" * 64)
    elif binding == "process":
        startup = replace(startup, process=replace(startup.process, pid=999))
    elif binding == "nonce":
        startup = replace(startup, launch_nonce=b"x" * 32)
    elif binding == "profile":
        profile = TdsConnectionProfile.VERIFIED_TLS
    else:
        body = strict_json_object(encode_credentials(credentials))
        body["request"]["columns"][0]["nullable"] = False
        with pytest.raises(ValueError):
            decode_credentials(canonical_json_bytes(body), startup=startup, profile=profile)
        return
    with pytest.raises(ValueError):
        decode_credentials(encode_credentials(credentials), startup=startup, profile=profile)


@pytest.mark.parametrize("binding", ["ownership", "process", "session", "authority"])
def test_grant_cannot_change_original_authority(binding):
    credentials, _, auth, grant, _ = values()
    if binding == "ownership":
        grant = replace(grant, ownership=replace(grant.ownership, owner="other"))
    elif binding == "process":
        grant = replace(grant, process=replace(grant.process, pid=999))
    elif binding == "session":
        grant = replace(grant, session=replace(grant.session, session_id=999))
    else:
        grant = replace(grant, authority_sha256="f" * 64)
    with pytest.raises(ValueError):
        decode_grant(encode_grant(credentials.identity, grant), identity=credentials.identity, authority=auth)


@pytest.mark.parametrize(
    "binding", ["owner_binding", "object_nonce", "columns", "session", "database", "authority_sha256"]
)
def test_well_hashed_wrong_evidence_is_not_accepted(binding):
    credentials, _, auth, grant, proof = values()
    if binding == "owner_binding":
        proof = replace(proof, owner_binding="f" * 64)
    elif binding == "object_nonce":
        proof = replace(proof, object_nonce=UUID(int=777))
    elif binding == "columns":
        proof = replace(proof, columns=(replace(proof.columns[0], nullable=False),))
    elif binding == "session":
        proof = replace(proof, session=replace(proof.session, session_id=999))
    elif binding == "database":
        proof = replace(proof, database=replace(proof.database, database_id=99))
    else:
        proof = replace(proof, authority_sha256="f" * 64)
    with pytest.raises(ValueError):
        decode_create_response(
            encode_create_response(successful_response(proof)),
            request=credentials.request,
            identity=credentials.identity,
            grant=grant,
            authority=auth,
        )


@pytest.mark.parametrize("mode", ["credentials", "grant", "result"])
@pytest.mark.parametrize("mutation", ["duplicate", "extra", "oversize", "noncanonical"])
def test_closed_envelope_shapes(mode, mutation):
    credentials, startup, auth, grant, proof = values()
    if mode == "credentials":
        payload = encode_credentials(credentials)
        limit = 1048576
        decode = partial(decode_credentials, startup=startup, profile=credentials.driver_profile)
    elif mode == "grant":
        payload = encode_grant(credentials.identity, grant)
        limit = 16384
        decode = partial(decode_grant, identity=credentials.identity, authority=auth)
    else:
        payload = encode_create_response(successful_response(proof))
        limit = 262144
        decode = partial(
            decode_create_response,
            request=credentials.request,
            identity=credentials.identity,
            grant=grant,
            authority=auth,
        )
    body = strict_json_object(payload)
    if mutation == "duplicate":
        payload = payload[:-1] + b',"schema":"duplicate"}'
    elif mutation == "extra":
        body["unexpected"] = 1
        payload = canonical_json_bytes(body)
    elif mutation == "oversize":
        payload = b" " * (limit + 1)
    else:
        if mode == "credentials":
            body["launch_nonce"] = body["launch_nonce"].upper()
        elif mode == "grant":
            body["grant"]["grant_id"] = "{" + body["grant"]["grant_id"] + "}"
        else:
            body["evidence"]["create_date"] = "2026-01-01T00:00:00"
        payload = canonical_json_bytes(body)
    with pytest.raises(ValueError):
        decode(payload)


def test_credential_type_is_not_raw_sql_or_connection_string():
    assert "connection_material" in TdsCoordinatorCredentials.__dataclass_fields__
    assert "connection_string" not in TdsCoordinatorCredentials.__dataclass_fields__


def test_failure_must_be_a_closed_object_not_pairs_list():
    credentials, _, auth, grant, _ = values()
    response = failed_response(credentials.identity, grant, auth, TdsAttemptError.DRIVER)
    body = strict_json_object(encode_create_response(response))
    body["failure"] = list(body["failure"].items())
    with pytest.raises(ValueError):
        decode_create_response(
            canonical_json_bytes(body),
            request=credentials.request,
            identity=credentials.identity,
            grant=grant,
            authority=auth,
        )
