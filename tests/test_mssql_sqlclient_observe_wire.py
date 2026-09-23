"""Response END, physical framing, and sequence binding precede exposure."""

import pytest

from dpone.contracts.mssql_sqlclient_observe_wire import ResponseReader, command, response_frames
from tests.test_mssql_sqlclient_observe import request


def test_rows_not_exposed_without_exact_end_and_binding():
    req = request()
    cmd = command(req, b"x" * 32, 1, "PRINCIPALS", {"name": "writer_user", "sid": "aa"})
    frames = list(response_frames(cmd, [[5, "writer_user", "aa", "SQL_USER", "INSTANCE"]]))
    reader = ResponseReader(cmd)
    assert reader.accept(frames[0]) is None
    assert reader.accept(frames[1]) == [[5, "writer_user", "aa", "SQL_USER", "INSTANCE"]]
    with pytest.raises(ValueError):
        reader.accept(frames[1])


def test_closed_arguments_and_exact_scalars():
    req = request()
    for args in ({"object_id": True}, {"object_id": 1, "sql": "SELECT 1"}):
        with pytest.raises(ValueError):
            command(req, b"x" * 32, 1, "MEMBER", args)


def test_batch_candidate_bound_accepts_1024_canonical_ids_and_rejects_aliases_or_overflow():
    req = request()
    ids = list(range(2**31 - 1024, 2**31))
    assert command(req, b"x" * 32, 1, "BATCH_MEMBERS", {"object_ids": ids})["arguments"] == {"object_ids": ids}
    for values in (ids + [2**31], [2, 1], [1, 1], [True]):
        with pytest.raises(ValueError):
            command(req, b"x" * 32, 1, "BATCH_MEMBERS", {"object_ids": values})


def test_column_batch_bound_is_81_objects():
    req = request()
    assert command(req, b"x" * 32, 1, "BATCH_COLUMNS", {"object_ids": list(range(1, 82))})
    with pytest.raises(ValueError):
        command(req, b"x" * 32, 1, "BATCH_COLUMNS", {"object_ids": list(range(1, 83))})
