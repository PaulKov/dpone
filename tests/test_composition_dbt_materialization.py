"""Source-derived declared obligations are not full physical schema evidence."""

import pytest

from dpone.contracts.composition_dbt_materialization import derive_dbt_materializations
from dpone.contracts.composition_dbt_outcome import DbtCaptureError
from tests.test_dbt_runtime_execution import _pack, _preflight_manifest


def derive(columns=None):
    manifest = _preflight_manifest()
    manifest["nodes"]["model.analytics.orders"]["columns"] = {} if columns is None else columns
    return derive_dbt_materializations(project_path="project", pack=_pack(), manifest=manifest)


def test_empty_columns_stay_explicit_absence_of_obligations():
    contracts = derive()
    assert len(contracts) == 1 and contracts[0].columns == ()
    assert contracts[0].write.resource_id == "model.analytics.orders"
    assert contracts[0].expectation == (contracts[0].write.resource_id, "table", contracts[0].schema_sha256)


def test_declared_types_normalize_and_untyped_column_stays_explicit():
    contract = derive({"id": {"name": "id", "data_type": " INT "}, "label": {"name": "label"}})[0]
    assert [(c.name, c.data_type) for c in contract.columns] == [("id", "int"), ("label", None)]
    assert contract.schema_sha256 != derive()[0].schema_sha256


@pytest.mark.parametrize("dtype", ["not_a_type", "varchar", "decimal(100,2)", "int;drop table x", "xml"])
def test_unsupported_declared_type_fails(dtype):
    with pytest.raises(DbtCaptureError):
        derive({"id": {"name": "id", "data_type": dtype}})


@pytest.mark.parametrize("materialized,kind", [("view", "view"), ("incremental", "table")])
def test_kind_comes_from_selected_materialization(materialized, kind):
    manifest = _preflight_manifest()
    manifest["nodes"]["model.analytics.orders"]["config"]["materialized"] = materialized
    values = derive_dbt_materializations(project_path="project", pack=_pack(), manifest=manifest)
    assert len(values) == 1 and values[0].kind == kind and values[0].write.role == "target"


@pytest.mark.parametrize(
    "columns",
    [
        {"a": {"name": "a"}, "A": {"name": "A"}},
        {"a": {"name": "other"}},
        {"a": {"name": "a", "constraints": [{"type": "not_null"}]}},
    ],
)
def test_ambiguous_or_unsupported_declarations_reject(columns):
    with pytest.raises(DbtCaptureError):
        derive(columns)


def test_source_column_order_does_not_change_obligations_digest():
    first = {"z": {"name": "z", "data_type": "decimal(10,2)"}, "a": {"name": "a"}}
    second = {"a": {"name": "a"}, "z": {"name": "z", "data_type": "NUMERIC(10, 2)"}}
    assert derive(first)[0].schema_sha256 == derive(second)[0].schema_sha256
