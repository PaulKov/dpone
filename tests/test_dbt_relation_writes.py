"""Literal write collisions are blocked without pretending to resolve databases."""

from dataclasses import fields, replace

import pytest

from dpone.contracts.dbt_contract_validation import DbtPublishingError, canonical_fingerprint
from dpone.contracts.dbt_execution_pack import DbtExecutionPack
from dpone.contracts.dbt_relation_writes import (
    DbtRelationWrite,
    require_distinct_logical_writes,
    selected_relation_writes,
    transfer_relation_write,
)
from dpone.contracts.dbt_selection_lock import DbtSelectionLock
from dpone.contracts.dbt_sqlserver_graph_policy_contract import dbt_sqlserver_graph_contract_sha256
from tests.test_dbt_runtime_execution import _pack, _preflight_manifest
from tests.test_dbt_sqlserver_graph_policy import _unit_test


def _write(**overrides):
    values = dict(
        project_path="a",
        workflow_id="daily",
        resource_id="model.a.orders",
        kind="model",
        connector="mssql",
        connection_ref="warehouse",
        database="DWH",
        schema="mart",
        relation="orders",
    )
    return DbtRelationWrite(**(values | overrides))


def _selected_pack(manifest, ids):
    pack = _pack()
    values = {
        field.name: getattr(pack.selection_lock, field.name)
        for field in fields(pack.selection_lock)
        if field.name not in {"schema", "selection_sha256"}
    }
    values.update(
        selected_graph_unique_ids=ids,
        expected_run_result_unique_ids=ids,
        graph_contract_sha256=dbt_sqlserver_graph_contract_sha256(manifest, ids),
    )
    raw = pack.to_dict()
    raw["selection_lock"] = DbtSelectionLock.build(**values).to_dict()
    raw["pack_sha256"] = canonical_fingerprint({key: value for key, value in raw.items() if key != "pack_sha256"})
    return DbtExecutionPack.from_mapping(raw)


@pytest.mark.parametrize(
    "materialized,expected",
    [
        ("table", {"", "__dbt_tmp", "__dbt_backup", "__dbt_tmp__dbt_tmp_vw"}),
        ("view", {"", "__dbt_tmp", "__dbt_backup"}),
        ("incremental", {"", "__dbt_tmp", "__dbt_backup", "__dbt_tmp_vw", "__dbt_tmp__dbt_tmp_vw"}),
    ],
)
def test_exact_materialization_footprint_has_no_duplicate_or_speculative_slots(materialized, expected):
    manifest = _preflight_manifest()
    manifest["nodes"]["model.analytics.orders"]["config"]["materialized"] = materialized
    writes = selected_relation_writes(project_path="a", execution=_pack(), manifest=manifest)
    assert {row.relation for row in writes} == {"orders" + suffix for suffix in expected}
    assert len(writes) == len(expected)
    require_distinct_logical_writes(writes)


def test_same_unit_test_name_on_different_models_is_a_relation_collision():
    manifest = _preflight_manifest()
    second = dict(manifest["nodes"]["model.analytics.orders"])
    second.update(name="other", alias="other", unique_id="model.analytics.other")
    manifest["nodes"][second["unique_id"]] = second
    first_unit = _unit_test()
    first_unit["name"] = "shared"
    second_unit = {
        **first_unit,
        "unique_id": "unit_test.analytics.other.shared",
        "model": "other",
        "depends_on": {"nodes": [second["unique_id"]], "macros": []},
    }
    manifest["unit_tests"] = {row["unique_id"]: row for row in (first_unit, second_unit)}
    ids = ("model.analytics.orders", second["unique_id"], first_unit["unique_id"], second_unit["unique_id"])
    writes = selected_relation_writes(project_path="a", execution=_selected_pack(manifest, ids), manifest=manifest)
    with pytest.raises(DbtPublishingError, match="collision") as failure:
        require_distinct_logical_writes(writes)
    assert first_unit["unique_id"] in str(failure.value) and second_unit["unique_id"] in str(failure.value)


def test_selected_manifest_not_just_publish_models_defines_write_inventory():
    pack, manifest = _pack(), _preflight_manifest()
    # Publish membership is a strict subset of the materialized selection.
    node = dict(manifest["nodes"]["model.analytics.orders"])
    node.update(unique_id="model.analytics.helper", alias="helper")
    manifest["nodes"]["model.analytics.helper"] = node
    writes = selected_relation_writes(project_path="a", execution=pack, manifest=manifest)
    assert {row.resource_id for row in writes} == {"model.analytics.orders"}
    assert {row.role for row in writes} == {"target", "intermediate", "backup", "helper"}
    values = {
        field.name: getattr(pack.selection_lock, field.name)
        for field in fields(pack.selection_lock)
        if field.name not in {"schema", "selection_sha256"}
    }
    ids = ("model.analytics.helper", *pack.selection_lock.selected_graph_unique_ids)
    values.update(
        selected_graph_unique_ids=ids,
        expected_run_result_unique_ids=ids,
        graph_contract_sha256=dbt_sqlserver_graph_contract_sha256(manifest, ids),
    )
    raw = pack.to_dict()
    raw["selection_lock"] = DbtSelectionLock.build(**values).to_dict()
    raw["pack_sha256"] = canonical_fingerprint({key: value for key, value in raw.items() if key != "pack_sha256"})
    writes = selected_relation_writes(project_path="a", execution=DbtExecutionPack.from_mapping(raw), manifest=manifest)
    assert {row.relation for row in writes if row.role == "target"} == {"orders", "helper"}
    assert len(writes) == 8


@pytest.mark.parametrize("kind", ["model", "transfer"])
def test_duplicate_coordinates_name_both_owners(kind):
    first, second = _write(), _write(project_path="b", workflow_id="other", resource_id="other", kind=kind)
    with pytest.raises(DbtPublishingError) as raised:
        require_distinct_logical_writes((first, second))
    assert raised.value.code == "DPONE_DBT_WORKSPACE_TARGET_COLLISION"
    assert "a:daily/model.a.orders" in str(raised.value)
    assert "b:other/other" in str(raised.value)


@pytest.mark.parametrize("changes", [{"connection_ref": "alias"}, {"database": None}, {"relation": "Orders"}])
def test_unresolved_alias_or_collation_is_not_a_literal_collision(changes):
    # This is not proof of physical separation; deployment must still resolve it.
    require_distinct_logical_writes((_write(), _write(project_path="b", **changes)))


def test_transfer_uses_generated_sink_coordinates_and_canonical_connector_family():
    row = transfer_relation_write(
        project_path="b",
        workflow_id="other",
        workload_id="publish",
        manifest={
            "sink": {
                "type": "sqlserver",
                "connection_ref": "warehouse",
                "table": {"database": "DWH", "schema": "mart", "name": "orders"},
            }
        },
    )
    with pytest.raises(DbtPublishingError, match="collision"):
        require_distinct_logical_writes((_write(), row))


@pytest.mark.parametrize("field", ["schema", "relation", "connection_ref"])
def test_missing_coordinates_fail_closed(field):
    with pytest.raises(DbtPublishingError) as raised:
        _write(**{field: ""})
    assert raised.value.code == "DPONE_DBT_WORKSPACE_TARGET_INVALID"


@pytest.mark.parametrize("materialized", ["table", "view", "incremental"])
@pytest.mark.parametrize("suffix", ["__dbt_tmp", "__dbt_backup"])
@pytest.mark.parametrize("kind", ["model", "transfer"])
def test_another_writer_cannot_occupy_pinned_adapter_intermediate_names(materialized, suffix, kind):
    manifest = _preflight_manifest()
    manifest["nodes"]["model.analytics.orders"]["config"]["materialized"] = materialized
    writes = selected_relation_writes(project_path="a", execution=_pack(), manifest=manifest)
    target = writes[0]
    other = replace(
        target, project_path="b", workflow_id="other", resource_id="other", kind=kind, relation=target.relation + suffix
    )
    with pytest.raises(DbtPublishingError, match="collision"):
        require_distinct_logical_writes((*writes, other))


@pytest.mark.parametrize(
    "materialized,suffix",
    [("table", "__dbt_tmp__dbt_tmp_vw"), ("incremental", "__dbt_tmp__dbt_tmp_vw"), ("incremental", "__dbt_tmp_vw")],
)
@pytest.mark.parametrize("kind", ["model", "transfer"])
def test_model_helper_view_cannot_destroy_another_writer(materialized, suffix, kind):
    manifest = _preflight_manifest()
    manifest["nodes"]["model.analytics.orders"]["config"]["materialized"] = materialized
    writes = selected_relation_writes(project_path="a", execution=_pack(), manifest=manifest)
    other = replace(writes[0], project_path="b", resource_id="other", kind=kind, relation="orders" + suffix)
    with pytest.raises(DbtPublishingError, match="collision"):
        require_distinct_logical_writes((*writes, other))


@pytest.mark.parametrize("version", [None, 0, 1, "1_2"])
def test_unit_test_names_use_test_name_and_tested_model_version(version):
    manifest = _preflight_manifest()
    manifest["nodes"]["model.analytics.orders"]["version"] = version
    unit = _unit_test()
    unit["name"] = "orders_contract"
    manifest["unit_tests"] = {unit["unique_id"]: unit}
    ids = ("model.analytics.orders", unit["unique_id"])
    writes = selected_relation_writes(project_path="a", execution=_selected_pack(manifest, ids), manifest=manifest)
    alias = "orders_contract" + (f"_v{version}" if version is not None else "")
    unit_writes = [row for row in writes if row.resource_id == unit["unique_id"]]
    assert {row.relation for row in unit_writes} == {alias + "__dbt_tmp", alias + "__dbt_tmp__dbt_tmp_vw"}
    assert all(row.kind == "unit_test" for row in unit_writes)
    assert all(
        (row.database, row.schema)
        == (
            manifest["nodes"]["model.analytics.orders"]["database"],
            manifest["nodes"]["model.analytics.orders"]["schema"],
        )
        for row in unit_writes
    )
    for row in unit_writes:
        with pytest.raises(DbtPublishingError, match="collision"):
            require_distinct_logical_writes(
                (*writes, replace(row, kind="transfer", role="target", project_path="b", resource_id="transfer"))
            )
