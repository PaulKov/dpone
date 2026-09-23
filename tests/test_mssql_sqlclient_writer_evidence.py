"""Full writer authority preserved without inferring effective token or ACKs."""

import json
from dataclasses import replace

import pytest

from dpone.contracts.mssql_sqlclient_writer_evidence import (
    decode_writer_observation,
    encode_writer_observation,
    validate_writer_observation,
)
from dpone.contracts.strict_json import canonical_json_bytes
from tests.mssql_sqlclient_evidence_fixtures import intent, registration, writer


def test_roundtrip_full_authority_and_originals():
    record = writer()
    assert decode_writer_observation(encode_writer_observation(record)) == record
    validate_writer_observation(record, registration(), intent())


@pytest.mark.parametrize("group", ["server", "database", "login", "transport", "principal_resolution"])
def test_nested_authority_unknown_rejected(group):
    data = json.loads(encode_writer_observation(writer()))
    data["authority"][group]["secret-canary"] = "secret-canary"
    with pytest.raises(ValueError, match=r"^mssql_native.sqlclient_evidence_invalid$"):
        decode_writer_observation(canonical_json_bytes(data))


@pytest.mark.parametrize("field", ["registration_sha256", "credential_intent_sha256", "capability_evidence_sha256"])
def test_ack_reference_drift(field):
    record = replace(writer(), **{field: "1" * 64})
    with pytest.raises(ValueError):
        validate_writer_observation(record, registration(), intent())


def test_announcement_principal_and_authority_drift():
    r = writer()
    with pytest.raises(ValueError):
        replace(r, announcement=replace(r.announcement, session_id=73))
    with pytest.raises(ValueError):
        replace(r, resolved_database_principal=replace(r.resolved_database_principal, name="other"))
    object.__setattr__(r.authority.login, "is_sysadmin", True)
    with pytest.raises(ValueError):
        encode_writer_observation(r)
