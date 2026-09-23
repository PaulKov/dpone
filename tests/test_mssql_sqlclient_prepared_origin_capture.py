"""Actual original actors retain nonsecret producer bytes after cleanup."""

from copy import copy

import pytest

from tests.test_mssql_sqlclient_observe_composition import observe_parent as observe_parent
from tests.test_mssql_sqlclient_preparation_composition import composed_preparation as composed_preparation
from tests.test_mssql_sqlclient_preparation_composition import (
    test_actual_original_join_acknowledges_file_before_prepared as prepare,
)


def test_actual_original_bytes_survive_closed_actors(composed_preparation, tmp_path, monkeypatch):
    h = composed_preparation
    prepare(h, tmp_path)
    origin = h.attempt._prepared_origin
    assert origin.preparation_payload == (h.evidence_root / origin.evidence_receipt.relative_name).read_bytes()
    assert origin.contained_exit.exit_code == -9 and origin.contained_exit.reaped is True
    for record, receipt, observation in origin.original_evidence:
        assert record.payload == (h.evidence_root / receipt.relative_name).read_bytes()
        assert receipt == observation.receipt

    def forbidden(*args, **kwargs):
        pytest.fail("terminal validation reread a closed actor")

    monkeypatch.setattr(type(h.handle.writer), "execute", forbidden)
    monkeypatch.setattr(type(h.handle.evidence), "observation", property(forbidden))
    origin.validate_origin()


def test_writer_inputs_ignores_instance_behavior_shadow(composed_preparation, tmp_path):
    from dpone.services.mssql_tds_preparation_origin import PreparationOrigin

    h = composed_preparation
    prepare(h, tmp_path)
    origin = h.attempt._prepared_origin._origin
    callbacks = []
    object.__setattr__(origin, "assert_references", lambda: callbacks.append("called"))

    captured = PreparationOrigin.writer_inputs(origin)

    assert captured is origin._writer_inputs
    assert callbacks == []


@pytest.mark.parametrize(
    "field",
    [
        "factory",
        "pool",
        "handle",
        "lifecycle",
        "directory",
        "payload",
        "receipt",
        "missing",
        "deadline",
        "exit",
        "capture",
        "transition",
        "factory_revision",
    ],
)
def test_substituted_or_missing_origin_rejects(composed_preparation, tmp_path, field):
    h = composed_preparation
    prepare(h, tmp_path)
    o = h.attempt._prepared_origin
    if field == "capture":
        o._origin = copy(o._origin)
    elif field == "factory_revision":
        object.__setattr__(o.factory._record, "revision", float(o.factory._record.revision))
    elif field == "transition":
        o = copy(o)
    elif field in ("factory", "pool", "handle"):
        setattr(o, field, copy(getattr(o, field)))
    elif field in ("lifecycle", "directory"):
        setattr(h.attempt, "_" + field, copy(getattr(h.attempt, "_" + field)))
    elif field == "payload":
        o.preparation_payload += b" "
    elif field == "receipt":
        object.__setattr__(o.evidence_receipt, "byte_count", True)
    elif field == "deadline":
        o.deadline += 100
    elif field == "exit":
        object.__setattr__(o.contained_exit, "exit_code", 0)
    else:
        o.original_evidence = ()
    with pytest.raises(Exception):
        o.validate_origin()


@pytest.mark.parametrize("size", [262145, 8388608, 8388609])
def test_producer_byte_bound_with_actual_preparation_actor(observe_parent, tmp_path, size):
    from dpone.contracts.mssql_sqlclient_preparation import MAX_PREPARATION_BYTES
    from dpone.services.mssql_tds_original_continuation import PreparationTransition
    from tests.test_mssql_sqlclient_observe_composition import retained_parent
    from tests.test_mssql_sqlclient_preparation_composition import acknowledged_preparation

    h = retained_parent(observe_parent)
    o = PreparationTransition(h.attempt, h)
    try:
        if size > MAX_PREPARATION_BYTES:
            with pytest.raises(ValueError):
                acknowledged_preparation(o, tmp_path, payload_size=size)
            assert o.preparation_payload is None
        else:
            receipt = acknowledged_preparation(o, tmp_path, payload_size=size)
            assert receipt.byte_count == size == len(o.preparation_payload)
            assert o.preparation_payload is o._preparation_capture[0]
            assert o.preparation_payload == (tmp_path / receipt.relative_name).read_bytes()
    finally:
        if o.evidence is not None:
            o.evidence.close(deadline=o.deadline)


@pytest.mark.parametrize("field", ["factory_revision", "process_ticks"])
def test_original_values_captured_before_prepared(composed_preparation, field):
    from dpone.services.mssql_tds_original_continuation import PreparationTransition

    h = composed_preparation
    transition = PreparationTransition(h.attempt, h.handle)
    target, name = (
        (h.factory._record, "revision")
        if field == "factory_revision"
        else (h.handle.startup_receipt.process, "start_ticks")
    )
    original = getattr(target, name)
    object.__setattr__(target, name, original + 1)
    try:
        with pytest.raises(Exception):
            transition._origin.assert_references()
    finally:
        object.__setattr__(target, name, original)  # restore only synthetic fixture teardown state


@pytest.mark.parametrize("mutation", ["replacement", "scalar", "equal_copy"])
def test_original_management_incarnation_is_pinned_before_terminal(
    composed_preparation, tmp_path, monkeypatch, mutation
):
    from dataclasses import replace
    from uuid import UUID

    from dpone.services.mssql_tds_preparation_origin import PreparationOrigin

    original = PreparationOrigin.capture_terminal

    def changed(origin):
        transition = origin._transition
        value = transition.management_incarnation
        if mutation == "replacement":
            transition.management_incarnation = replace(value, connection_id=UUID(int=17))
        elif mutation == "equal_copy":
            transition.management_incarnation = replace(value)
        else:
            object.__setattr__(value, "connection_id", UUID(int=17))
        return original(origin)

    monkeypatch.setattr(PreparationOrigin, "capture_terminal", changed)
    with pytest.raises(Exception):
        prepare(composed_preparation, tmp_path)
    assert composed_preparation.attempt._prepared_origin is None
