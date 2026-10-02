"""Schema plans bind reviewed SQL, not caller-provided executable statements."""

import hashlib
import json
from dataclasses import asdict, replace
from importlib import import_module

import pytest

from dpone.contracts.publication_authority_binding import PublicationAuthorityBinding

BINDING = PublicationAuthorityBinding("mssql", "metadata", "Example_System", "dbo", "sample", "test")
ENDPOINT = "a" * 64
DDL = "b" * 64


def api():
    return import_module("dpone.contracts.publication_schema")


def plan():
    return api().PublicationSchemaPlan(binding=BINDING, endpoint_identity=ENDPOINT, ddl_sha256=DDL)


def test_plan_round_trip_binds_exact_closed_identity():
    value = plan()
    payload = value.payload.encode()
    expected = {
        "contract": "dpone.publication-schema-plan.v1",
        "catalog_version": 1,
        "binding": asdict(BINDING),
        "endpoint_identity": ENDPOINT,
        "ddl_sha256": DDL,
    }
    assert json.loads(payload) == expected
    assert value.digest == hashlib.sha256(payload).hexdigest()
    assert api().decode_schema_plan(payload) == value


@pytest.mark.parametrize(
    "change",
    [
        {"binding": replace(BINDING, database="Other_System")},
        {"binding": replace(BINDING, schema="metadata")},
        {"binding": replace(BINDING, connection_ref="other")},
        {"binding": replace(BINDING, service_id="other")},
        {"binding": replace(BINDING, environment="other")},
        {"endpoint_identity": "c" * 64},
        {"ddl_sha256": "c" * 64},
    ],
)
def test_any_operator_scope_or_sql_change_invalidates_confirmation(change):
    value = plan()
    assert replace(value, **change).digest != value.digest


@pytest.mark.parametrize(
    "change",
    [
        {"binding": asdict(BINDING)},
        {"endpoint_identity": "A" * 64},
        {"endpoint_identity": "a" * 63},
        {"endpoint_identity": None},
        {"ddl_sha256": "a" * 65},
        {"ddl_sha256": "SELECT secret"},
        {"ddl_sha256": 1},
        {"catalog_version": True},
        {"catalog_version": 2},
        {"contract": "unrecognized"},
    ],
)
def test_constructor_rejects_noncanonical_or_unsupported_plan(change):
    with pytest.raises(ValueError):
        replace(plan(), **change)


@pytest.mark.parametrize("change", ["extra", "missing", "duplicate", "whitespace", "wrong_version", "wrong_binding"])
def test_decode_rejects_unknown_fields_or_noncanonical_bytes(change):
    value = plan()
    document = json.loads(value.payload)
    if change == "extra":
        document["sql"] = "SELECT synthetic_private_value"
    elif change == "missing":
        del document["ddl_sha256"]
    elif change == "wrong_version":
        document["catalog_version"] = True
    elif change == "wrong_binding":
        document["binding"]["database"] = "x]; SELECT synthetic_private_value"
    raw = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    if change == "duplicate":
        raw = raw[:-1] + b',"ddl_sha256":"' + DDL.encode() + b'"}'
    elif change == "whitespace":
        raw += b"\n"
    with pytest.raises(ValueError) as error:
        api().decode_schema_plan(raw)
    assert "synthetic_private_value" not in str(error.value)


@pytest.mark.parametrize(
    "raw",
    [b"", b"x" * (16 * 1024 + 1), b"null", b"[]", b"\xff", "{}"],
    ids=["empty", "oversize", "null", "array", "bad_utf8", "not_bytes"],
)
def test_decode_rejects_unbounded_or_non_object_payload(raw):
    with pytest.raises(ValueError):
        api().decode_schema_plan(raw)
