"""Retirement has endpoint-admitted SQL sessions and no native-create capability."""

from dataclasses import replace
from importlib import import_module

import pytest

from dpone.contracts.publication_authority_binding import publication_binding_digest
from tests.test_publication_authority_composition import _BINDING, _OBSERVED, _PIN, Connector, connection
from tests.test_publication_retirement import observation, plan


def build(factory, **kwargs):
    return import_module("dpone.runtime.publication_authority_composition").build_publication_retirement(
        connection=connection(),
        binding=_BINDING,
        environment="test",
        clock=lambda: 220,
        connector_factory=factory,
        **kwargs,
    )


def test_catalog_is_admitted_and_closed_without_exposing_native_mutation():
    connector = Connector()
    store = build(lambda _: connector)
    assert connector.closed
    assert not hasattr(store, "create_if_absent") and not hasattr(store, "compare_and_swap")
    assert not any("INSERT" in query or "UPDATE" in query for query in connector.calls)


def test_each_new_session_rejects_changed_endpoint_before_transaction():
    sessions = [Connector(), Connector((_OBSERVED[0], _OBSERVED[1], "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"))]
    remaining = iter(sessions)
    store = build(lambda _: next(remaining))
    value = observation()
    digest = publication_binding_digest(_BINDING, endpoint_identity=_PIN)
    value = replace(value, binding_digest=digest, freeze=replace(value.freeze, binding_digest=digest))
    assert store.inspect(plan(value)) == "unknown"
    assert all(item.closed for item in sessions)
    assert len(sessions[1].calls) == 1


def test_failed_catalog_disposal_cannot_return_usable_store():
    class BrokenClose(Connector):
        def close(self):
            self.closed = True
            raise RuntimeError("synthetic-private close failure")

    connector = BrokenClose()
    with pytest.raises(ValueError, match="^publication_authority: catalog session close failed$"):
        build(lambda _: connector)
    assert connector.closed


def test_missing_catalog_does_not_provision_or_leave_session_open():
    class Absent(Connector):
        def get_records(self, query, params=()):
            return super().get_records(query, params) if "SERVERPROPERTY" in query else []

    connector = Absent()
    with pytest.raises(Exception):
        build(lambda _: connector)
    assert connector.closed
    assert not any("CREATE" in query for query in connector.calls)
