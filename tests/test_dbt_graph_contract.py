from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from dpone.contracts.dbt_graph_contract import dbt_graph_contract_sha256
from dpone.contracts.dbt_sqlserver_graph_policy_contract import (
    dbt_sqlserver_graph_contract_sha256,
)

MODEL_ID = "model.analytics.orders"
TEST_ID = "test.analytics.orders_not_null"


def _manifest() -> dict[str, Any]:
    return {
        "nodes": {
            MODEL_ID: {
                "unique_id": MODEL_ID,
                "name": "orders",
                "resource_type": "model",
                "language": "sql",
                "fqn": ["analytics", "orders"],
                "database": "DWH",
                "schema": "mart",
                "alias": "orders",
                "relation_name": "[DWH].[mart].[orders]",
                "depends_on": {
                    "nodes": [],
                    "macros": ["macro.dbt.is_incremental"],
                },
                "config": {
                    "enabled": True,
                    "materialized": "table",
                    "as_columnstore": False,
                    "indexes": [],
                    "contract": {
                        "enforced": True,
                        "alias_types": True,
                    },
                },
                "contract": {
                    "enforced": True,
                    "alias_types": True,
                },
                "constraints": [],
                "columns": {
                    "order_id": {
                        "name": "order_id",
                        "data_type": "bigint",
                        "constraints": [{"type": "not_null"}],
                    }
                },
            },
            TEST_ID: {
                "unique_id": TEST_ID,
                "name": "orders_not_null",
                "resource_type": "test",
                "language": "sql",
                "fqn": ["analytics", "orders_not_null"],
                "database": None,
                "schema": None,
                "alias": "orders_not_null",
                "relation_name": None,
                "depends_on": {
                    "nodes": [MODEL_ID],
                    "macros": [],
                },
                "config": {
                    "enabled": True,
                    "materialized": "test",
                },
                "columns": {},
            },
        },
        "unit_tests": {},
    }


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("config", "query_options"), {"RECOMPILE": True}),
        (("config", "query_options_raw"), ["RECOMPILE"]),
        (("config", "query_tag"), "dpone"),
        (("config", "sql_header"), "set nocount on"),
        (("config", "persist_docs"), {"relation": True}),
        (("config", "column_types"), {"order_id": "decimal(38,0)"}),
        (("config", "incremental_predicates"), ["order_id > 0"]),
        (("config", "predicates"), ["order_id > 0"]),
        (("config", "auto_provision_aad_principals"), True),
        (("config", "column_type_expansion_max_rows"), 10),
        (
            ("constraints",),
            [{"type": "primary_key", "columns": ["order_id"]}],
        ),
        (
            ("columns", "order_id", "constraints"),
            [{"type": "unique"}],
        ),
        (("config", "contract", "enforced"), False),
        (("depends_on", "macros"), ["macro.analytics.runtime_sql"]),
    ],
)
def test_graph_contract_binds_adapter_constraint_and_macro_semantics(
    path: tuple[str, ...],
    value: object,
) -> None:
    baseline = _manifest()
    changed = deepcopy(baseline)
    target: dict[str, Any] = changed["nodes"][MODEL_ID]
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = value

    assert dbt_graph_contract_sha256(
        baseline,
        (MODEL_ID,),
    ) != dbt_graph_contract_sha256(
        changed,
        (MODEL_ID,),
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("store_failures", True),
        ("store_failures_as", "table"),
        ("severity", "warn"),
        ("where", "order_id > 0"),
        ("limit", 10),
        ("fail_calc", "count(*)"),
        ("warn_if", ">0"),
        ("error_if", ">1"),
    ],
)
def test_graph_contract_binds_selected_test_behavior(
    field: str,
    value: object,
) -> None:
    baseline = _manifest()
    changed = deepcopy(baseline)
    changed["nodes"][TEST_ID]["config"][field] = value
    selected = (MODEL_ID, TEST_ID)

    assert dbt_graph_contract_sha256(
        baseline,
        selected,
    ) != dbt_graph_contract_sha256(
        changed,
        selected,
    )


def test_graph_contract_binds_foreign_selected_test_model_dependencies() -> None:
    baseline = _manifest()
    changed = deepcopy(baseline)
    changed["nodes"][TEST_ID]["depends_on"]["nodes"].append("model.analytics.foreign_orders")
    selected = (MODEL_ID, TEST_ID)

    assert dbt_graph_contract_sha256(
        baseline,
        selected,
    ) != dbt_graph_contract_sha256(
        changed,
        selected,
    )


def test_sqlserver_graph_contract_binds_exact_macro_authority_projection() -> None:
    manifest = json.loads(
        (Path(__file__).parents[1] / "examples" / "dbt-inline-publishing" / "fixtures" / "manifest.v12.json").read_text(
            encoding="utf-8"
        )
    )
    unique_id = "model.dpone_dbt_demo.competitive_pricing"
    baseline = dbt_sqlserver_graph_contract_sha256(manifest, (unique_id,))
    changed = deepcopy(manifest)
    changed["nodes"][unique_id]["depends_on"]["macros"].append("macro.dbt.test_not_null")

    assert (
        dbt_sqlserver_graph_contract_sha256(
            changed,
            (unique_id,),
        )
        != baseline
    )


def test_sqlserver_graph_contract_rejects_macro_body_drift() -> None:
    manifest = json.loads(
        (Path(__file__).parents[1] / "examples" / "dbt-inline-publishing" / "fixtures" / "manifest.v12.json").read_text(
            encoding="utf-8"
        )
    )
    manifest["macros"]["macro.dbt.is_incremental"]["macro_sql"] += " "

    with pytest.raises(
        ValueError,
        match="macro authority is unavailable or invalid",
    ):
        dbt_sqlserver_graph_contract_sha256(
            manifest,
            ("model.dpone_dbt_demo.competitive_pricing",),
        )
