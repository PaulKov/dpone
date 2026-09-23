"""Only matching reaped local exit; context is deeply revalidated."""

from dataclasses import replace

import pytest

from dpone.contracts.mssql_sqlclient_execution_evidence import (
    SqlClientLocalExit,
    SqlClientResultContext,
    decode_worker_local_exit,
    encode_worker_local_exit,
)
from dpone.contracts.mssql_tds_worker import TdsChildExit
from tests.mssql_sqlclient_evidence_fixtures import registration
from tests.test_mssql_sqlclient_result import EMPTY, GRANT


def local_exit():
    r = registration()
    return SqlClientLocalExit(
        binding=r.binding,
        registration_sha256="1" * 64,
        result_sha256=None,
        exit=TdsChildExit(r.launch.process, 0, True),
    )


@pytest.mark.parametrize("code", [-255, 0, 255])
def test_roundtrip_reaped_exit(code):
    r = local_exit()
    r = replace(r, exit=replace(r.exit, exit_code=code))
    assert decode_worker_local_exit(encode_worker_local_exit(r)) == r


def test_exit_identity_and_false_reaping():
    r = local_exit()
    with pytest.raises(ValueError):
        replace(r, exit=replace(r.exit, reaped=False))
    with pytest.raises(ValueError):
        replace(r, exit=replace(r.exit, identity=replace(r.exit.identity, pid=999)))
    object.__setattr__(r.exit.identity, "pid", True)
    with pytest.raises(ValueError):
        encode_worker_local_exit(r)


def test_context_revalidates_frozen_nested_and_empty_grant():
    r = registration()
    with pytest.raises(ValueError):
        SqlClientResultContext(r.launch, EMPTY, GRANT)
    object.__setattr__(r.input.expected, "rows", True)
    with pytest.raises(ValueError):
        SqlClientResultContext(r.launch, r.input.expected, None)
