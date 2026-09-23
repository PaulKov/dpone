"""CREATE codecs reject ambiguous structures, identities and metadata."""

from dataclasses import replace
from datetime import datetime
from uuid import UUID

import pytest

from dpone.contracts.mssql_tds_create import (
    TdsCreateEvidence,
    TdsCreateObservedColumn,
    TdsCreateType,
    create_command_digest,
    create_evidence_digest,
    decode_create_evidence,
    decode_create_request,
    encode_create_evidence,
    encode_create_request,
)
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from tests.test_mssql_tds_coordinator_authority import authority
from tests.test_mssql_tds_create import request


def evidence():
    auth = authority()
    return TdsCreateEvidence(
        "a" * 64,
        create_command_digest(request()),
        "c" * 64,
        "d" * 64,
        auth.session,
        auth.database,
        auth.schema_observation,
        "table",
        10,
        datetime(2026, 1, 1),
        request().parent.owner_binding,
        request().object_nonce,
        (TdsCreateObservedColumn(1, "value", TdsCreateType.BIGINT, True, 8, 19, 0, None),),
    )


def test_request_and_evidence_roundtrip():
    assert decode_create_request(encode_create_request(request())) == request()
    assert decode_create_evidence(encode_create_evidence(evidence())) == evidence()
    assert len(create_evidence_digest(evidence())) == 64


@pytest.mark.parametrize(
    "change",
    [
        {"object_nonce": UUID(int=10)},
        {"parent": replace(request().parent, table="other")},
        {"columns": (replace(request().columns[0], nullable=False),)},
    ],
)
def test_every_effect_input_changes_command_digest(change):
    assert create_command_digest(replace(request(), **change)) != create_command_digest(request())


@pytest.mark.parametrize("is_evidence", [False, True])
@pytest.mark.parametrize("mutation", ["extra", "missing", "duplicate", "large", "uuid_alias", "nested", "bool"])
def test_closed_codecs(is_evidence, mutation):
    encode, decode, value = (
        (encode_create_evidence, decode_create_evidence, evidence())
        if is_evidence
        else (encode_create_request, decode_create_request, request())
    )
    body = strict_json_object(encode(value))
    if mutation == "extra":
        body["unknown"] = 1
    elif mutation == "missing":
        del body["schema"]
    elif mutation == "uuid_alias":
        body["object_nonce"] = "{" + body["object_nonce"] + "}"
    elif mutation == "nested":
        body["columns"][0]["extra"] = 1
    elif mutation == "bool":
        body["columns"][0]["nullable"] = 1
    payload = canonical_json_bytes(body)
    if mutation == "duplicate":
        payload = payload[:-1] + b',"schema":"duplicate"}'
    if mutation == "large":
        payload = b" " * 131073
    with pytest.raises(ValueError):
        decode(payload)


@pytest.mark.parametrize(
    "change",
    [
        {"empty": False},
        {"committed": False},
        {"object_id": True},
        {"create_date": datetime(1899, 1, 1)},
        {"columns": (replace(evidence().columns[0], ordinal=2),)},
    ],
)
def test_evidence_cannot_claim_incomplete_creation(change):
    with pytest.raises(ValueError):
        replace(evidence(), **change)


@pytest.mark.parametrize("control", ["\u0085", "\u009b"])
def test_catalog_control_characters_rejected_by_both_codecs(control):
    for encode, decode, value in (
        (encode_create_request, decode_create_request, request()),
        (encode_create_evidence, decode_create_evidence, evidence()),
    ):
        body = strict_json_object(encode(value))
        body["columns"][0]["name"] = "x" + control
        with pytest.raises(ValueError):
            decode(canonical_json_bytes(body))
