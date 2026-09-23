"""Canonical column ownership preserves historical imports and pickle locators."""

import pickle
from dataclasses import FrozenInstanceError, fields

import pytest

from dpone.contracts.native_wire_layout import NativeWireColumnLayout
from dpone.runtime.native_wire_models import NativeWireColumnLayout as HistoricalLayout


def test_same_class_fields_and_historical_pickle_locator():
    assert NativeWireColumnLayout is HistoricalLayout
    assert NativeWireColumnLayout.__module__ == "dpone.runtime.native_wire_models"
    assert NativeWireColumnLayout.to_dict.__module__ == "dpone.runtime.native_wire_models"
    assert [f.name for f in fields(NativeWireColumnLayout)] == [
        "name",
        "source_type",
        "target_type",
        "nullable",
        "storage_type",
        "prefix_width",
        "fixed_length",
        "precision",
        "scale",
        "encoding",
    ]
    value = NativeWireColumnLayout("x", "int64", "bigint", False, "bigint", fixed_length=8)
    restored = pickle.loads(pickle.dumps(value))
    assert restored == value and hash(restored) == hash(value)
    assert not hasattr(value, "__dict__")
    with pytest.raises(FrozenInstanceError):
        value.name = "other"


def test_shared_layout_is_not_narrowed_to_sqlclient_types():
    value = NativeWireColumnLayout("legacy", "decimal", "decimal(20,4)", True, "decimal", 1, 13, 20, 4)
    assert value.to_dict()["precision"] == 20
