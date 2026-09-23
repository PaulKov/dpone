import pytest

import dpone.app.mssql_sqlclient_writer_observation_composition as observation_composition
from dpone.app import mssql_sqlclient_dependency_bundle
from dpone.app.mssql_sqlclient_writer_observation_composition import (
    SqlClientWriterObservationDependencies,
    SqlClientWriterObservationInputs,
    compose_sqlclient_writer_observation,
)
from dpone.ports.mssql_sqlclient_credentials import preload_sqlclient_credentials
from dpone.services.mssql_tds_writer_observation import (
    SqlClientWriterGrantReady,
    SqlClientWriterObservationUnknown,
    SqlClientWriterResultReady,
)
from dpone.services.mssql_tds_writer_pregrant import prepare_sqlclient_writer_pregrant
from tests.test_mssql_tds_writer_launch import setup as setup
from tests.test_mssql_tds_writer_observation import GRANT_ID, NONCE, _observer
from tests.test_mssql_tds_writer_pregrant import (
    _bind_p9,
    _credentials,
    _profile,
    _registered,
    _writer_admission,
)


def _pregrant(setup, monkeypatch, *, response="session"):
    target = _writer_admission(setup.plan.attempt.state.identity.database)
    _bind_p9(setup, target)
    profile = _profile(target)
    registered = _registered(setup, monkeypatch, NONCE, response=response)
    pregrant = prepare_sqlclient_writer_pregrant(
        registered,
        profile=profile,
        supplier=preload_sqlclient_credentials(profile, _credentials(database=profile.database)),
        session_nonce=NONCE,
        clock_ns=lambda: 1,
    )
    return pregrant, target


def test_result_route_never_calls_session_input_provider(setup, monkeypatch):
    pregrant, _ = _pregrant(setup, monkeypatch, response="failure")

    result = compose_sqlclient_writer_observation(
        pregrant,
        session_inputs=lambda: pytest.fail("result route acquired observer inputs"),
    )

    assert isinstance(result, SqlClientWriterResultReady)


def test_observation_uses_one_frozen_prepare_dependency(setup, monkeypatch):
    pregrant, _ = _pregrant(setup, monkeypatch, response="failure")
    calls = []

    def prepare(candidate, **kwargs):
        calls.append((candidate, kwargs))
        return "prepared"

    result = compose_sqlclient_writer_observation(
        pregrant,
        session_inputs=lambda: pytest.fail("result route acquired observer inputs"),
        dependencies=SqlClientWriterObservationDependencies(prepare=prepare),
    )

    assert result == "prepared"
    assert calls == [(pregrant, {"observer": None, "grant_id": None, "now_ns": 0})]


def test_default_prepare_identity_cannot_drift_after_composition(setup, monkeypatch):
    pregrant, target = _pregrant(setup, monkeypatch)
    observer, _backend = _observer(setup, target, None)
    calls = []

    def original(candidate, **kwargs):
        calls.append((candidate, kwargs))
        return "original"

    original_bundle = SqlClientWriterObservationDependencies(prepare=original)
    replacement_bundle = SqlClientWriterObservationDependencies(
        prepare=lambda *_args, **_kwargs: pytest.fail("factory drift changed the selected dependency")
    )
    monkeypatch.setattr(
        observation_composition,
        "sqlclient_writer_observation_dependencies",
        lambda: original_bundle,
    )

    def inputs():
        mssql_sqlclient_dependency_bundle.sqlclient_writer_observation_dependencies.cache_clear()
        monkeypatch.setattr(
            observation_composition,
            "sqlclient_writer_observation_dependencies",
            lambda: replacement_bundle,
        )
        return SqlClientWriterObservationInputs(observer, GRANT_ID, 1)

    result = compose_sqlclient_writer_observation(pregrant, session_inputs=inputs)

    assert result == "original"
    assert calls == [(pregrant, {"observer": observer, "grant_id": GRANT_ID, "now_ns": 1})]


def test_legacy_module_global_patch_seam_remains_authoritative(setup, monkeypatch):
    pregrant, _ = _pregrant(setup, monkeypatch, response="failure")
    monkeypatch.setattr(observation_composition, "prepare_sqlclient_writer_observation", lambda *_args, **_kwargs: 7)

    assert (
        compose_sqlclient_writer_observation(
            pregrant,
            session_inputs=lambda: pytest.fail("result route acquired observer inputs"),
        )
        == 7
    )


def test_session_inputs_are_materialized_before_core_claim(setup, monkeypatch):
    pregrant, target = _pregrant(setup, monkeypatch)
    observer, backend = _observer(setup, target, None)
    events = []

    def inputs():
        assert type(pregrant)._p10d_route(pregrant, pregrant) == "session"
        events.append("inputs")
        return SqlClientWriterObservationInputs(observer, GRANT_ID, 1)

    result = compose_sqlclient_writer_observation(pregrant, session_inputs=inputs)

    assert isinstance(result, SqlClientWriterGrantReady)
    assert events == ["inputs"]
    assert backend.calls[0][0] == "observe_once"


def test_input_provider_failure_leaves_pregrant_unclaimed(setup, monkeypatch):
    pregrant, _ = _pregrant(setup, monkeypatch)

    def failed():
        raise OSError("preclaim")

    with pytest.raises(OSError, match="preclaim"):
        compose_sqlclient_writer_observation(pregrant, session_inputs=failed)

    assert type(pregrant)._p10d_route(pregrant, pregrant) == "session"


@pytest.mark.parametrize("field,value", [("grant_id", "not-a-uuid"), ("now_ns", -1)])
def test_mutated_returned_inputs_are_cleaned_before_core_claim(setup, monkeypatch, field, value):
    pregrant, target = _pregrant(setup, monkeypatch)
    observer, backend = _observer(setup, target, None)
    inputs = SqlClientWriterObservationInputs(observer, GRANT_ID, 1)
    object.__setattr__(inputs, field, value)

    with pytest.raises(ValueError, match="observation_composition_invalid"):
        compose_sqlclient_writer_observation(pregrant, session_inputs=lambda: inputs)

    assert type(pregrant)._p10d_route(pregrant, pregrant) == "session"
    assert backend.calls[-2:] == [("contain", 6.0), ("close", 6.0)]


@pytest.mark.parametrize("subject", ["process", "incarnation", "observer_admission", "target_admission"])
def test_identity_drift_before_composition_is_cleaned_without_claiming_pregrant(setup, monkeypatch, subject):
    pregrant, target = _pregrant(setup, monkeypatch)
    observer, backend = _observer(setup, target, None)
    if subject == "process":
        object.__setattr__(backend._identity, "pid", 0)
    elif subject == "incarnation":
        object.__setattr__(backend.incarnation, "session_id", 0)
    elif subject == "observer_admission":
        object.__setattr__(backend.observer_admission.login, "name", "")
    else:
        object.__setattr__(backend.target_admission.database, "database_id", 0)

    with pytest.raises(SqlClientWriterObservationUnknown, match="writer_observation_unknown"):
        compose_sqlclient_writer_observation(
            pregrant,
            session_inputs=lambda: SqlClientWriterObservationInputs(observer, GRANT_ID, 1),
        )

    with pytest.raises(ValueError):
        type(pregrant)._p10d_route(pregrant, pregrant)
    assert backend.calls[-2:] == [("contain", 6.0), ("close", 6.0)]
