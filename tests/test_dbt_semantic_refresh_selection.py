from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest

from dpone.adapters.dbt_semantic_refresh_selection import (
    build_semantic_refresh_selection_arguments,
)
from dpone.contracts.dbt_semantic_refresh_selection import (
    V2_DBT_INDIRECT_SELECTION,
    exact_fqn_selectors,
    prove_mutation_closure,
)

MODEL_ID = "model.analytics.orders"
TEST_ID = "test.analytics.orders_not_null"
EPHEMERAL_ID = "model.analytics.orders_base"
UNMANAGED_ID = "model.analytics.unmanaged"


def _model(
    unique_id: str,
    *,
    materialized: str = "incremental",
    depends_on: tuple[str, ...] = (),
    publish: bool = False,
) -> dict[str, Any]:
    name = unique_id.rsplit(".", 1)[-1]
    config: dict[str, Any] = {
        "enabled": True,
        "materialized": materialized,
        "pre-hook": [],
        "post-hook": [],
    }
    if materialized == "incremental":
        config.update(
            {
                "contract": {"enforced": True},
                "incremental_strategy": "dpone_scope_merge",
                "on_schema_change": "fail",
            }
        )
    return {
        "unique_id": unique_id,
        "name": name,
        "resource_type": "model",
        "language": "sql",
        "fqn": ["analytics", name],
        "config": config,
        "contract": {"enforced": materialized == "incremental"},
        "depends_on": {"nodes": list(depends_on), "macros": []},
        "meta": {"dpone": {"publish": {"enabled": publish}}},
    }


def _test() -> dict[str, Any]:
    return {
        "unique_id": TEST_ID,
        "name": "orders_not_null",
        "resource_type": "test",
        "language": "sql",
        "fqn": ["analytics", "orders_not_null"],
        "config": {
            "enabled": True,
            "materialized": "test",
            "store_failures": False,
            "pre-hook": [],
            "post-hook": [],
        },
        "depends_on": {"nodes": [MODEL_ID], "macros": []},
    }


def _manifest(*nodes: dict[str, Any]) -> dict[str, Any]:
    return {
        "nodes": {node["unique_id"]: deepcopy(node) for node in nodes},
        "unit_tests": {},
    }


def test_v2_builds_exact_fqn_selectors_and_empty_indirect_selection() -> None:
    manifest = _manifest(_model(MODEL_ID, publish=True), _test())

    selectors = exact_fqn_selectors(manifest, (MODEL_ID, TEST_ID))
    arguments = build_semantic_refresh_selection_arguments(selectors)

    assert selectors == ("fqn:analytics.orders", "fqn:analytics.orders_not_null")
    assert all(not selector.startswith("+") for selector in selectors)
    assert V2_DBT_INDIRECT_SELECTION == "empty"
    assert arguments == (
        "--indirect-selection",
        "empty",
        "--select",
        "fqn:analytics.orders",
        "fqn:analytics.orders_not_null",
    )


@pytest.mark.parametrize(
    "fqn",
    [
        [],
        ["analytics", ""],
        ["analytics", "orders value"],
        ["analytics", "orders*"],
        "+analytics.orders",
    ],
)
def test_exact_selector_rejects_ambiguous_or_unsafe_fqn(fqn: object) -> None:
    manifest = _manifest(_model(MODEL_ID, publish=True))
    manifest["nodes"][MODEL_ID]["fqn"] = fqn

    with pytest.raises(ValueError, match="exact FQN"):
        exact_fqn_selectors(manifest, (MODEL_ID,))


def test_mutation_closure_proves_only_the_managed_incremental_model_and_read_only_test() -> None:
    report = prove_mutation_closure(
        _manifest(_model(MODEL_ID, publish=True), _test()),
        selected_graph_unique_ids=(MODEL_ID, TEST_ID),
        selected_mutating_node_ids=(MODEL_ID,),
    )

    assert report.status == "PROVEN"
    assert report.issues == ()
    assert report.selected_mutating_node_ids == (MODEL_ID,)
    assert report.transitive_node_ids == ()
    assert report.proof_sha256.startswith("sha256:")


def test_selected_or_transitive_ephemeral_model_fails_closed() -> None:
    manifest = _manifest(
        _model(MODEL_ID, depends_on=(EPHEMERAL_ID,), publish=True),
        _model(EPHEMERAL_ID, materialized="ephemeral"),
    )

    report = prove_mutation_closure(
        manifest,
        selected_graph_unique_ids=(MODEL_ID,),
        selected_mutating_node_ids=(MODEL_ID,),
    )

    assert report.status == "NONCONFORMANT"
    assert [(issue.unique_id, issue.code) for issue in report.issues] == [
        (EPHEMERAL_ID, "DPONE_DBT_V2_EPHEMERAL_UNSUPPORTED")
    ]


def test_selected_ephemeral_model_uses_the_approved_v2_failure_code() -> None:
    report = prove_mutation_closure(
        _manifest(_model(EPHEMERAL_ID, materialized="ephemeral")),
        selected_graph_unique_ids=(EPHEMERAL_ID,),
        selected_mutating_node_ids=(EPHEMERAL_ID,),
    )

    assert report.status == "NONCONFORMANT"
    assert [issue.code for issue in report.issues] == ["DPONE_DBT_V2_EPHEMERAL_UNSUPPORTED"]


def test_unselected_non_ephemeral_ancestor_is_a_read_boundary_not_an_execution() -> None:
    report = prove_mutation_closure(
        _manifest(
            _model(MODEL_ID, depends_on=(UNMANAGED_ID,), publish=True),
            _model(UNMANAGED_ID, materialized="view"),
        ),
        selected_graph_unique_ids=(MODEL_ID,),
        selected_mutating_node_ids=(MODEL_ID,),
    )

    assert report.status == "PROVEN"
    assert report.transitive_node_ids == (UNMANAGED_ID,)


def test_selected_unmanaged_model_and_test_storage_fail_closed() -> None:
    unmanaged = _model(UNMANAGED_ID, materialized="view")
    test = _test()
    test["config"]["store_failures"] = True

    report = prove_mutation_closure(
        _manifest(_model(MODEL_ID, publish=True), unmanaged, test),
        selected_graph_unique_ids=(MODEL_ID, UNMANAGED_ID, TEST_ID),
        selected_mutating_node_ids=(MODEL_ID,),
    )

    assert report.status == "NONCONFORMANT"
    assert {issue.code for issue in report.issues} == {
        "DPONE_DBT_V2_MUTATION_UNCLASSIFIED",
        "DPONE_DBT_V2_TEST_MUTATION_UNSUPPORTED",
    }


def test_missing_transitive_manifest_metadata_is_unverified_not_empty() -> None:
    report = prove_mutation_closure(
        _manifest(_model(MODEL_ID, depends_on=("model.analytics.missing",), publish=True)),
        selected_graph_unique_ids=(MODEL_ID,),
        selected_mutating_node_ids=(MODEL_ID,),
    )

    assert report.status == "UNVERIFIED"
    assert report.issues[0].code == "DPONE_DBT_V2_GRAPH_UNVERIFIED"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("incremental_strategy", "merge"),
        ("on_schema_change", "ignore"),
        ("contract", {"enforced": False}),
        ("pre-hook", ["select 1"]),
        ("post-hook", ["select 1"]),
    ],
)
def test_managed_model_contract_is_closed(field: str, value: object) -> None:
    model = _model(MODEL_ID, publish=True)
    model["config"][field] = value

    report = prove_mutation_closure(
        _manifest(model),
        selected_graph_unique_ids=(MODEL_ID,),
        selected_mutating_node_ids=(MODEL_ID,),
    )

    assert report.status == "NONCONFORMANT"
    assert report.issues[0].field == f"config.{field}"
