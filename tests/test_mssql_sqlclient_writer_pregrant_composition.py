import pytest

import dpone.app.mssql_sqlclient_writer_pregrant_composition as composition
from dpone.app.mssql_sqlclient_writer_pregrant_composition import prepare_mssql_sqlclient_writer
from tests.test_mssql_tds_writer_launch import setup as setup
from tests.test_mssql_tds_writer_pregrant import _bind_p9, _credentials, _profile, _registered, _writer_admission


def test_pregrant_composition_exists():
    assert prepare_mssql_sqlclient_writer is not None


def test_composition_preloads_value_and_reaches_pregrant(setup, monkeypatch):
    writer = _writer_admission(setup.plan.attempt.state.identity.database)
    _bind_p9(setup, writer)
    profile = _profile(writer)
    nonce = b"n" * 32
    registered = _registered(setup, monkeypatch, nonce)

    pregrant = prepare_mssql_sqlclient_writer(
        registered,
        profile=profile,
        credentials=_credentials(database=profile.database),
        session_nonce=nonce,
        clock_ns=lambda: 1,
    )

    assert repr(pregrant) == "SqlClientWriterPreGrant(<opaque>)"


def _composition_frames(error):
    trace = error.__traceback__
    frames = []
    while trace is not None:
        if trace.tb_frame.f_code.co_name == "prepare_mssql_sqlclient_writer":
            frames.append(trace.tb_frame.f_locals)
        trace = trace.tb_next
    return frames


def test_composition_clears_credentials_when_preload_fails(monkeypatch):
    credentials = _credentials()

    def fail_preload(profile, value):
        assert value is credentials
        raise RuntimeError("synthetic_preload_failure")

    monkeypatch.setattr(composition, "preload_sqlclient_credentials", fail_preload)
    with pytest.raises(RuntimeError, match="synthetic_preload_failure") as caught:
        prepare_mssql_sqlclient_writer(
            object(),
            profile=_profile(),
            credentials=credentials,
            session_nonce=b"n" * 32,
        )

    frames = _composition_frames(caught.value)
    assert frames and all(frame.get("credentials") is None for frame in frames)
    assert all("secret-canary" not in repr(frame) for frame in frames)


def test_composition_clears_supplier_and_credentials_when_service_fails(monkeypatch):
    credentials = _credentials()

    def fail_service(*args, **kwargs):
        raise RuntimeError("synthetic_service_failure")

    monkeypatch.setattr(composition, "prepare_sqlclient_writer_pregrant", fail_service)
    with pytest.raises(RuntimeError, match="synthetic_service_failure") as caught:
        prepare_mssql_sqlclient_writer(
            object(),
            profile=_profile(),
            credentials=credentials,
            session_nonce=b"n" * 32,
        )

    frames = _composition_frames(caught.value)
    assert frames and all(frame.get("credentials") is None and frame.get("supplier") is None for frame in frames)
    assert all("secret-canary" not in repr(frame) for frame in frames)
