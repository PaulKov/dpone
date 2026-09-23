"""P10 writer departure has its own canonical session-authority domain."""

import json

import pytest

from dpone.contracts.mssql_sqlclient_writer_session_departure_codec import (
    decode_writer_session_departure,
    encode_writer_session_departure,
)
from tests.test_mssql_tds_writer_launch import setup as setup
from tests.test_mssql_tds_writer_settlement import _observation


def test_codec_roundtrip_uses_writer_departure(setup, monkeypatch):
    from dpone.services.mssql_tds_writer_execution import execute_sqlclient_writer
    from tests.test_mssql_tds_writer_execution import _install_process, _ready, _result

    ready, _ = _ready(setup, monkeypatch)
    _install_process(monkeypatch, setup, _result(setup))
    exited = execute_sqlclient_writer(ready, clock_ns=lambda: 1)
    owner = type(exited)._assert_p10f_claim(exited, exited, type(exited)._claim_p10f_once(exited, exited))
    departure = _observation(owner).departure
    payload = encode_writer_session_departure(departure)
    assert decode_writer_session_departure(payload) == departure


@pytest.mark.parametrize("mutation", ["extra", "missing", "trailing"])
def test_codec_is_closed(setup, monkeypatch, mutation):
    from dpone.services.mssql_tds_writer_execution import execute_sqlclient_writer
    from tests.test_mssql_tds_writer_execution import _install_process, _ready, _result

    ready, _ = _ready(setup, monkeypatch)
    _install_process(monkeypatch, setup, _result(setup))
    exited = execute_sqlclient_writer(ready, clock_ns=lambda: 1)
    owner = type(exited)._assert_p10f_claim(exited, exited, type(exited)._claim_p10f_once(exited, exited))
    raw = encode_writer_session_departure(_observation(owner).departure)
    body = json.loads(raw)
    if mutation == "extra":
        body["extra"] = True
    elif mutation == "missing":
        del body["original"]
    raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    if mutation == "trailing":
        raw += b" "
    with pytest.raises(ValueError):
        decode_writer_session_departure(raw)
