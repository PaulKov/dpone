"""Composition roots must not substitute endpoint names for protected authority."""

from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest

from dpone.app.composition_store_factory import CompositionMssqlStoreFactory
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.runtime_connection import ResolvedConnectionDescriptor
from tests.test_composition_activation_contract import request


class Inputs:
    def __init__(self, descriptor):
        self.context = request().context
        self.descriptor = descriptor
        self.events = []

    def load_context(self, **kwargs):
        self.events.append(("reopen", kwargs))
        return self.context

    def resolve_connection(self, context, connection_ref):
        self.events.append(("resolve", context, connection_ref))
        return SimpleNamespace(descriptor=self.descriptor)


@pytest.mark.parametrize(
    "descriptor",
    [
        None,
        ResolvedConnectionDescriptor("postgres", {}),
        ResolvedConnectionDescriptor("mssql", {}),
        ResolvedConnectionDescriptor("mssql", {"composition_service_id": "not-a-uuid"}),
    ],
)
def test_invalid_authority_never_opens_connection(tmp_path, monkeypatch, descriptor):
    def forbidden(*args, **kwargs):
        pytest.fail("invalid authority attempted credential use")

    monkeypatch.setattr("dpone.app.composition_store_factory.ResolvedConnectorFactory.create", forbidden)
    inputs = Inputs(descriptor)
    factory = CompositionMssqlStoreFactory(inputs=inputs, authority_connection_ref="authority")
    with pytest.raises(CompositionAdmissionError):
        factory.build(projection_root=tmp_path, context=inputs.context)


def test_reopened_context_must_match_before_resolving_credentials(tmp_path):
    inputs = Inputs(ResolvedConnectionDescriptor("mssql", {"composition_service_id": str(uuid4())}))
    original = inputs.context
    inputs.context = replace(original, activation_id=str(uuid4()))
    factory = CompositionMssqlStoreFactory(inputs=inputs, authority_connection_ref="authority")
    with pytest.raises(CompositionAdmissionError):
        factory.build(projection_root=tmp_path, context=original)
    assert [event[0] for event in inputs.events] == ["reopen"]


def test_each_phase_reopens_and_each_store_connection_is_independent(tmp_path, monkeypatch):
    service_id = str(uuid4())
    inputs = Inputs(ResolvedConnectionDescriptor("mssql", {"composition_service_id": service_id}))
    factories = []
    connections = []

    def store(connection_factory, **kwargs):
        assert kwargs == {"expected_service_id": service_id, "control_schema": "dpone_control"}
        factories.append(connection_factory)
        return object()

    def connect(resolved, **kwargs):
        assert kwargs == {"autocommit": False}
        connection = object()
        connections.append(connection)
        return SimpleNamespace(connection=connection)

    monkeypatch.setattr("dpone.app.composition_store_factory.MssqlCompositionActivationStore", store)
    monkeypatch.setattr("dpone.app.composition_store_factory.ResolvedConnectorFactory.create", connect)
    factory = CompositionMssqlStoreFactory(inputs=inputs, authority_connection_ref="authority")
    factory.build(projection_root=tmp_path, context=inputs.context)
    factory.build(projection_root=tmp_path, context=inputs.context)
    assert [event[0] for event in inputs.events] == ["reopen", "resolve", "reopen", "resolve"]
    assert not connections
    assert factories[0]() is not factories[0]()
    assert factories[1]() is not connections[0]
