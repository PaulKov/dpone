"""Frozen request canonicalization happens before any retained operation effects."""

from dataclasses import replace
from hashlib import sha256

import pytest

from dpone.contracts.mssql_sqlclient_grant_inventory import SqlClientGrantInventoryLimits, SqlClientGrantPrincipal
from dpone.contracts.mssql_sqlclient_observe import SqlClientObserveRequest, decode_request, encode_request
from tests.test_mssql_sqlclient_grant_catalog import Cursor
from tests.test_mssql_sqlclient_stage_locator import REQUEST


def request():
    cursor = Cursor()
    return SqlClientObserveRequest(
        parent=REQUEST.parent,
        selected_stage=cursor.stage,
        management_admission=cursor.management,
        writer_admission=cursor.writer,
        writer_principal=SqlClientGrantPrincipal(5, "writer_user", "aa", "SQL_USER", "INSTANCE"),
        limits=SqlClientGrantInventoryLimits(),
        operation_deadline_ns=10**15,
    )


def test_exact_nonsecret_canonical_request():
    value = request()
    encoded = encode_request(value)
    assert decode_request(encoded) == value
    assert value.command_sha256 == sha256(encoded).hexdigest()
    with pytest.raises(ValueError):
        decode_request(encoded + b" ")
    with pytest.raises(ValueError):
        decode_request(encoded[:-1] + b',"extra":1}')


def test_scalar_alias_and_changed_independent_binding_rejected():
    with pytest.raises(ValueError):
        replace(request(), operation_deadline_ns=True)
    with pytest.raises(ValueError):
        replace(request(), writer_principal=SqlClientGrantPrincipal(5, "writer_user", "bb", "SQL_USER", "INSTANCE"))
