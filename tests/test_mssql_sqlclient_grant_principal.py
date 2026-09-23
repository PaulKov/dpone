"""Independent principal expectation is required before a SqlClient grant is usable."""

import json
from dataclasses import replace
from pathlib import Path

import pytest

from dpone.contracts.mssql_sqlclient_session_control import (
    SqlClientDatabasePrincipal,
    decode_bulk_grant,
    encode_bulk_grant,
)
from tests.test_mssql_sqlclient_session_control import grant, validate


def test_original_resolved_principal_must_match():
    value = grant()
    for change in [dict(principal_id=8), dict(name="other"), dict(sid="ab" * 16)]:
        with pytest.raises(ValueError):
            validate(replace(value, resolved_database_principal=replace(value.resolved_database_principal, **change)))
    assert decode_bulk_grant(encode_bulk_grant(value)) == value


@pytest.mark.parametrize(
    "field,value",
    [
        ("principal_id", True),
        ("principal_id", 0),
        ("principal_id", 2**31),
        ("name", "😀" * 65),
        ("name", "bad\nname"),
        ("sid", ""),
        ("sid", "AB"),
        ("sid", "a"),
        ("sid", "aa "),
        ("sid", "aa" * 86),
    ],
)
def test_principal_exact_bounds(field, value):
    data = dict(principal_id=7, name="writer", sid="ab" * 16)
    data[field] = value
    with pytest.raises(ValueError):
        SqlClientDatabasePrincipal(**data)


def test_unresolved_old_grant_and_nested_ambiguity_reject():
    old = (Path(__file__).parent / "fixtures/mssql_sqlclient/bulk-grant-v1.json").read_bytes()
    with pytest.raises(ValueError):
        decode_bulk_grant(old)
    data = json.loads(encode_bulk_grant(grant()))
    for key in ["extra", "missing"]:
        changed = json.loads(json.dumps(data))
        if key == "extra":
            changed["resolved_database_principal"]["unexpected"] = 1
        else:
            del changed["resolved_database_principal"]["sid"]
        with pytest.raises(ValueError):
            decode_bulk_grant(json.dumps(changed).encode())
