"""Pure stage fingerprints, not observed SQL ownership or admission authority."""

import json
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha256
from uuid import UUID

import pytest

from dpone.contracts.mssql_sqlclient_stage_identity import (
    SqlClientStageIdentity,
    decode_stage_identity,
    encode_stage_identity,
    stage_fingerprint,
    stage_identity_from_create,
    stage_object_identity,
)
from dpone.contracts.mssql_tds_create import TdsCreateObservedColumn, TdsCreateType
from tests.test_mssql_tds_create_codec import evidence as create_evidence_fixture

WIRE = (
    b'{"columns":[{"collation":null,"max_length":8,"name":"value","nullable":true,'
    b'"ordinal":1,"precision":19,"scale":0,"type":"bigint"}],'
    b'"create_date":"2026-01-01T00:00:00.000000",'
    b'"database_guid":"00000000-0000-0000-0000-000000000001",'
    b'"database_id":1,"database_name":"db","object_id":10,'
    b'"object_nonce":"00000000-0000-0000-0000-000000000002",'
    b'"owner_binding":"' + b"a" * 64 + b'","schema":"dpone.sqlclient.stage-identity.v1",'
    b'"schema_id":2,"schema_name":"test","table_name":"stage"}'
)


def evidence():
    # Legacy CREATE fixtures share SESSION. Forgery cases must never mutate it.
    return deepcopy(create_evidence_fixture())


def stage():
    return SqlClientStageIdentity(
        database_guid=UUID(int=1),
        database_id=1,
        database_name="db",
        schema_id=2,
        schema_name="test",
        table_name="stage",
        object_id=10,
        create_date=datetime(2026, 1, 1),
        owner_binding="a" * 64,
        object_nonce=UUID(int=2),
        columns=(TdsCreateObservedColumn(1, "value", TdsCreateType.BIGINT, True, 8, 19, 0, None),),
    )


def test_frozen_canonical_wire_and_domain_hash():
    expected = sha256(b"dpone.sqlclient.stage-identity.v1\0" + WIRE).hexdigest()
    assert encode_stage_identity(stage()) == WIRE
    assert decode_stage_identity(WIRE) == stage()
    assert stage_fingerprint(stage()) == expected
    assert stage_object_identity(stage()).object_id == 10
    assert stage_object_identity(stage()).fingerprint == expected
    assert expected != sha256(WIRE).hexdigest()


@pytest.mark.parametrize(
    "change",
    [
        {"database_guid": UUID(int=3)},
        {"database_id": 2},
        {"database_name": "DB"},
        {"schema_id": 3},
        {"schema_name": "other"},
        {"table_name": "other"},
        {"object_id": 11},
        {"create_date": datetime(2026, 1, 1, 0, 0, 0, 1)},
        {"owner_binding": "b" * 64},
        {"object_nonce": UUID(int=3)},
        {"columns": (TdsCreateObservedColumn(1, "value", TdsCreateType.FLOAT53, True, 8, 53, 0, None),)},
    ],
)
def test_stable_catalog_fields_participate(change):
    assert stage_fingerprint(replace(stage(), **change)) != stage_fingerprint(stage())


def test_create_projection_excludes_transient_authority_fields():
    original = evidence()
    changed = replace(
        original,
        operation_sha256="1" * 64,
        grant_sha256="2" * 64,
        command_sha256="3" * 64,
        authority_sha256="4" * 64,
        session=replace(original.session, session_id=original.session.session_id + 1),
    )
    projected = stage_identity_from_create(original)
    assert stage_identity_from_create(changed) == projected
    assert projected.database_guid == original.database.database_guid
    assert projected.schema_id == original.schema_observation.schema_id
    assert projected.columns == original.columns
    assert projected.owner_binding == original.owner_binding
    assert stage_fingerprint(projected) == stage_fingerprint(stage_identity_from_create(changed))


@pytest.mark.parametrize(
    "kind", ["extra", "missing", "duplicate", "alias", "nested", "unknown_type", "noncanonical", "large"]
)
def test_closed_decoder(kind):
    body = json.loads(WIRE)
    if kind == "extra":
        body["PRIVATE_CANARY"] = "hidden"
    elif kind == "missing":
        del body["object_id"]
    elif kind == "alias":
        body["database_id"] = True
    elif kind == "nested":
        body["columns"][0]["nullable"] = 1
    elif kind == "unknown_type":
        body["columns"][0]["type"] = "decimal"
    payload = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    if kind == "duplicate":
        payload = payload[:-1] + b',"object_id":10}'
    elif kind == "noncanonical":
        payload += b" "
    elif kind == "large":
        payload = b" " * 131073
    with pytest.raises(ValueError) as caught:
        decode_stage_identity(payload)
    assert "PRIVATE_CANARY" not in str(caught.value)


@pytest.mark.parametrize(
    "field,value",
    [
        ("database_id", True),
        ("object_id", 10.0),
        ("database_guid", UUID(int=0)),
        ("create_date", datetime(2026, 1, 1, tzinfo=UTC)),
        ("create_date", datetime(1899, 1, 1)),
        ("table_name", "#temp"),
        ("database_name", "\ud800"),
        ("owner_binding", "PRIVATE_CANARY"),
        ("schema", "unknown"),
        ("columns", ()),
    ],
)
def test_invalid_typed_values_fail_before_hash(field, value):
    with pytest.raises(ValueError):
        replace(stage(), **{field: value})


@pytest.mark.parametrize("field,value", [("ordinal", True), ("nullable", 1), ("max_length", 8.0), ("precision", 19.0)])
def test_forged_nested_scalars_rejected_by_producer(field, value):
    record = stage()
    object.__setattr__(record.columns[0], field, value)
    with pytest.raises(ValueError):
        stage_fingerprint(record)


@pytest.mark.parametrize("text", ["\u0800" * 128, "😀" * 64, '\\"' * 64])
def test_maximum_valid_width_unicode_and_escaping(text):
    suffix = text.encode("utf-16le")[:248].decode("utf-16le")
    columns = tuple(
        TdsCreateObservedColumn(i + 1, f"{i:04d}" + suffix, TdsCreateType.NVARCHARMAX, True, -1, 0, 0, text)
        for i in range(100)
    )
    assert all(len(column.name.encode("utf-16le")) == 256 for column in columns)
    record = replace(stage(), database_name=text, schema_name=text, table_name=text, columns=columns)
    payload = encode_stage_identity(record)
    assert len(payload) <= 131072
    assert decode_stage_identity(payload) == record
    with pytest.raises(ValueError):
        replace(record, columns=columns + (columns[-1],))


def test_order_duplicate_names_and_collation_are_not_normalized():
    a = TdsCreateObservedColumn(1, "first", TdsCreateType.NVARCHARMAX, True, -1, 0, 0, "Latin1_General_BIN2")
    b = replace(a, ordinal=2, name="second")
    record = replace(stage(), columns=(a, b))
    assert stage_fingerprint(record) != stage_fingerprint(
        replace(record, columns=(replace(b, ordinal=1), replace(a, ordinal=2)))
    )
    assert stage_fingerprint(record) != stage_fingerprint(
        replace(record, columns=(replace(a, collation="Latin1_General_CI_AS"), b))
    )
    with pytest.raises(ValueError):
        replace(record, columns=(b, a))
    with pytest.raises(ValueError):
        replace(record, columns=(a, replace(b, name="first")))


@pytest.mark.parametrize("field", ["database", "schema_observation", "session"])
def test_create_projection_revalidates_nested_types(field):
    original = evidence()
    object.__setattr__(original, field, {"PRIVATE_CANARY": "hidden"})
    with pytest.raises(ValueError) as caught:
        stage_identity_from_create(original)
    assert "PRIVATE_CANARY" not in str(caught.value)


@pytest.mark.parametrize("kind", ["column_enum", "database_uuid", "connection_uuid", "nonce", "authority", "timestamp"])
def test_create_projection_rejects_aliases_before_codec_normalization(kind):
    original = evidence()
    if kind == "column_enum":
        object.__setattr__(original.columns[0], "type", "bigint")
    elif kind == "database_uuid":
        object.__setattr__(original.database, "database_guid", str(original.database.database_guid))
    elif kind == "connection_uuid":
        object.__setattr__(original.session, "connection_id", str(original.session.connection_id))
    elif kind == "nonce":
        object.__setattr__(original.session, "nonce", bytearray(original.session.nonce))
    elif kind == "authority":
        object.__setattr__(original.session, "authority_sha256", bytearray(original.session.authority_sha256))
    else:
        object.__setattr__(original.session, "connect_time", "PRIVATE_TIMESTAMP_CANARY")
    with pytest.raises(ValueError) as caught:
        stage_identity_from_create(original)
    assert "PRIVATE_TIMESTAMP_CANARY" not in str(caught.value)
