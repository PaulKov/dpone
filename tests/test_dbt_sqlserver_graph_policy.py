from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from dpone.contracts.dbt_sqlserver_graph_policy import (
    DBT_SQLSERVER_GRAPH_POLICY_ID,
    DBT_SQLSERVER_GRAPH_POLICY_SHA256,
    DBT_SQLSERVER_PHYSICAL_CONSTRAINT_UNSUPPORTED,
    DbtSqlServerGraphPolicyReport,
    evaluate_dbt_sqlserver_selected_graph,
)

MODEL_ID = "model.analytics.orders"
VIEW_ID = "model.analytics.customers"
APPEND_ID = "model.analytics.events"
MERGE_ID = "model.analytics.accounts"
TEST_ID = "test.analytics.orders_not_null"
UNIT_TEST_ID = "unit_test.analytics.orders.orders_contract"
FOREIGN_MODEL_ID = "model.analytics.foreign_accounts"
ROOT = Path(__file__).parents[1]
_AUTHORITY_MANIFEST = json.loads(
    (ROOT / "examples" / "dbt-inline-publishing" / "fixtures" / "manifest.v12.json").read_text(encoding="utf-8")
)


def _model(
    unique_id: str = MODEL_ID,
    *,
    materialized: str = "table",
    language: str = "sql",
    **config_overrides: Any,
) -> dict[str, Any]:
    config: dict[str, Any] = {
        "enabled": True,
        "materialized": materialized,
        "as_columnstore": False,
        "indexes": [],
        "drop_unmanaged_indexes": False,
        "prefer_single_alter_column": False,
        "pre-hook": [],
        "post-hook": [],
        "grants": {},
        "full_refresh": None,
        "on_schema_change": "ignore",
    }
    config.update(config_overrides)
    return {
        "unique_id": unique_id,
        "name": unique_id.rsplit(".", 1)[-1],
        "resource_type": "model",
        "language": language,
        "database": "DWH",
        "schema": "mart",
        "original_file_path": f"models/{unique_id.rsplit('.', 1)[-1]}.sql",
        "config": config,
        "depends_on": {"nodes": [], "macros": []},
    }


def _data_test(**config_overrides: Any) -> dict[str, Any]:
    config: dict[str, Any] = {
        "enabled": True,
        "materialized": "test",
        "store_failures": None,
    }
    config.update(config_overrides)
    return {
        "unique_id": TEST_ID,
        "resource_type": "test",
        "language": "sql",
        "original_file_path": "models/schema.yml",
        "config": config,
        "depends_on": {"nodes": [MODEL_ID], "macros": []},
    }


def _unit_test(*, model_id: str = MODEL_ID) -> dict[str, Any]:
    return {
        "unique_id": UNIT_TEST_ID,
        "name": "orders_contract",
        "model": model_id.rsplit(".", 1)[-1],
        "given": [],
        "expect": {"rows": [], "format": "dict", "fixture": None},
        "package_name": "analytics",
        "path": "orders_contract.yml",
        "fqn": ["analytics", "orders", "orders_contract"],
        "resource_type": "unit_test",
        "original_file_path": "models/schema.yml",
        "config": {"enabled": True},
        "depends_on": {"nodes": [model_id], "macros": []},
    }


def _contracted(
    model: dict[str, Any],
    *columns: str,
    nullable: tuple[str, ...] = (),
) -> dict[str, Any]:
    model["config"]["contract"] = {"enforced": True}
    model["contract"] = {"enforced": True}
    model["columns"] = {
        column: {
            "name": column,
            "data_type": "bigint",
            "constraints": [] if column in nullable else [{"type": "not_null"}],
        }
        for column in columns
    }
    return model


def _manifest(
    *nodes: dict[str, Any],
    unit_tests: tuple[dict[str, Any], ...] = (),
) -> dict[str, Any]:
    return {
        "metadata": deepcopy(_AUTHORITY_MANIFEST["metadata"]),
        "macros": deepcopy(_AUTHORITY_MANIFEST["macros"]),
        "nodes": {node["unique_id"]: node for node in nodes},
        "unit_tests": {node["unique_id"]: node for node in unit_tests},
    }


def _issue_fields(report: DbtSqlServerGraphPolicyReport) -> set[str]:
    return {issue.field for issue in report.issues}


def test_policy_accepts_the_complete_approved_model_test_and_unit_test_matrix() -> None:
    table = _model(table_refresh_method="rename", as_columnstore=False)
    view = _model(VIEW_ID, materialized="view")
    append = _model(
        APPEND_ID,
        materialized="incremental",
        incremental_strategy="append",
    )
    merge = _contracted(
        _model(
            MERGE_ID,
            materialized="incremental",
            incremental_strategy="merge",
            unique_key=["account_id", "valid_at"],
            on_schema_change="fail",
        ),
        "account_id",
        "valid_at",
    )
    data_test = _data_test(store_failures=False)
    unit_test = _unit_test()
    selected = (MODEL_ID, VIEW_ID, APPEND_ID, MERGE_ID, TEST_ID, UNIT_TEST_ID)

    report = evaluate_dbt_sqlserver_selected_graph(
        _manifest(table, view, append, merge, data_test, unit_tests=(unit_test,)),
        selected,
    )

    assert report.passed is True
    assert report.issues == ()
    assert report.policy_id == DBT_SQLSERVER_GRAPH_POLICY_ID
    assert report.policy_sha256 == DBT_SQLSERVER_GRAPH_POLICY_SHA256
    assert report.to_jsonable() == {
        "policy_id": "dpone.dbt-sqlserver-selected-graph-policy.v1",
        "policy_sha256": DBT_SQLSERVER_GRAPH_POLICY_SHA256,
        "passed": True,
        "issues": [],
    }
    assert DBT_SQLSERVER_GRAPH_POLICY_SHA256 == (
        "sha256:433e19b5637e06a10c0ec3add271fd4e5182a0e278a132a875ccc97fe9ab4b1f"
    )


def test_table_policy_requires_explicit_safe_adapter_options() -> None:
    report = evaluate_dbt_sqlserver_selected_graph(
        _manifest(_model()),
        (MODEL_ID,),
    )

    assert report.passed


@pytest.mark.parametrize(
    ("node", "field"),
    [
        (_model(language="python"), "language"),
        (_model(materialized="ephemeral"), "config.materialized"),
        (_model(materialized="materialized_view"), "config.materialized"),
        (_model(materialized="clone"), "config.materialized"),
        (_model(materialized="custom_materialization"), "config.materialized"),
        (_model(materialized=[]), "config.materialized"),
        (_model(table_refresh_method="dml"), "config.table_refresh_method"),
        (_model(as_columnstore=True), "config.as_columnstore"),
        (_model(as_columnstore=None), "config.as_columnstore"),
        (_model(indexes=[{"columns": ["order_id"]}]), "config.indexes"),
        (_model(drop_unmanaged_indexes=True), "config.drop_unmanaged_indexes"),
        (
            _model(prefer_single_alter_column=True),
            "config.prefer_single_alter_column",
        ),
        (
            _model(materialized="incremental", incremental_strategy=None),
            "config.incremental_strategy",
        ),
        (
            _model(materialized="incremental", incremental_strategy=[]),
            "config.incremental_strategy",
        ),
        (
            _model(
                materialized="incremental",
                incremental_strategy="delete+insert",
            ),
            "config.incremental_strategy",
        ),
        (
            _model(materialized="incremental", incremental_strategy="microbatch"),
            "config.incremental_strategy",
        ),
        (
            _model(
                materialized="incremental",
                incremental_strategy="append",
                on_schema_change="sync_all_columns",
            ),
            "config.on_schema_change",
        ),
        (
            _model(
                materialized="incremental",
                incremental_strategy="append",
                on_schema_change=[],
            ),
            "config.on_schema_change",
        ),
        (_model(**{"pre-hook": [{"sql": "select 1"}]}), "config.pre-hook"),
        (_model(**{"post-hook": [{"sql": "select 1"}]}), "config.post-hook"),
        (_model(grants={"select": ["analyst"]}), "config.grants"),
        (_model(full_refresh=True), "config.full_refresh"),
        (_model(full_refresh=0), "config.full_refresh"),
        (_model(enabled=False), "config.enabled"),
    ],
)
def test_model_policy_fails_closed_for_unsupported_behavior(
    node: dict[str, Any],
    field: str,
) -> None:
    report = evaluate_dbt_sqlserver_selected_graph(
        _manifest(node),
        (MODEL_ID,),
    )

    assert report.passed is False
    assert field in _issue_fields(report)
    assert {issue.code for issue in report.issues} == {"DPONE_DBT_SQLSERVER_GRAPH_CAPABILITY_UNSUPPORTED"}
    assert all(issue.remediation for issue in report.issues)


def test_merge_unique_key_is_required() -> None:
    merge = _model(
        MERGE_ID,
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=[],
    )

    report = evaluate_dbt_sqlserver_selected_graph(
        _manifest(merge),
        (MERGE_ID,),
    )

    assert [issue.code for issue in report.issues] == [
        "DPONE_DBT_UNIQUE_KEY_MISSING",
    ]
    assert report.issues[0].field == "config.unique_key"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("query_options", {"maxdop": 1}),
        ("query_options_raw", ["RECOMPILE"]),
        ("query_tag", "runtime-dependent"),
        ("sql_header", "set deadlock_priority high"),
        ("persist_docs", {"relation": True}),
        ("column_types", {"order_id": "varchar(128)"}),
        ("incremental_predicates", ["order_id > 0"]),
        ("predicates", ["order_id > 0"]),
        ("auto_provision_aad_principals", True),
        ("column_type_expansion_max_rows", 100),
        ("latest_version_pointer", {"enabled": True, "alias": "orders"}),
        ("on_error", "continue"),
        ("static_analysis", {"enabled": True}),
        ("future_adapter_option", True),
    ],
)
def test_model_policy_closes_adapter_config_surface(
    field: str,
    value: object,
) -> None:
    report = evaluate_dbt_sqlserver_selected_graph(
        _manifest(_model(**{field: value})),
        (MODEL_ID,),
    )

    assert not report.passed
    assert f"config.{field}" in _issue_fields(report)
    assert {issue.code for issue in report.issues} == {
        "DPONE_DBT_SQLSERVER_GRAPH_CAPABILITY_UNSUPPORTED",
    }


def test_model_policy_accepts_canonical_empty_adapter_config_defaults() -> None:
    report = evaluate_dbt_sqlserver_selected_graph(
        _manifest(
            _model(
                query_options={},
                query_options_raw=[],
                query_tag=None,
                sql_header=None,
                persist_docs={},
                column_types={},
                latest_version_pointer={"enabled": None, "alias": None},
                on_error=None,
                static_analysis=None,
            )
        ),
        (MODEL_ID,),
    )

    assert report.passed


@pytest.mark.parametrize(
    "unique_key",
    [
        "lower(account_id)",
        "account_id,valid_at",
        ["account_id", "cast(valid_at as date)"],
    ],
)
def test_merge_unique_key_rejects_sql_expressions(unique_key: object) -> None:
    merge = _contracted(
        _model(
            MERGE_ID,
            materialized="incremental",
            incremental_strategy="merge",
            unique_key=unique_key,
        ),
        "account_id",
        "valid_at",
    )

    report = evaluate_dbt_sqlserver_selected_graph(
        _manifest(merge),
        (MERGE_ID,),
    )

    assert [issue.code for issue in report.issues] == [
        "DPONE_DBT_UNIQUE_KEY_EXPRESSION_UNSUPPORTED",
    ]
    assert report.issues[0].field == "config.unique_key"


def test_merge_unique_key_requires_exact_contracted_columns() -> None:
    merge = _contracted(
        _model(
            MERGE_ID,
            materialized="incremental",
            incremental_strategy="merge",
            unique_key=["account_id", "missing_key"],
        ),
        "account_id",
        "valid_at",
    )

    report = evaluate_dbt_sqlserver_selected_graph(
        _manifest(merge),
        (MERGE_ID,),
    )

    assert [issue.code for issue in report.issues] == [
        "DPONE_DBT_UNIQUE_KEY_NOT_IN_CONTRACT",
    ]


@pytest.mark.parametrize(
    "unique_key",
    [
        ["account_id", "account_id"],
        ["Account_ID", "account_id"],
    ],
)
def test_merge_unique_key_rejects_exact_and_casefold_duplicates(
    unique_key: list[str],
) -> None:
    merge = _contracted(
        _model(
            MERGE_ID,
            materialized="incremental",
            incremental_strategy="merge",
            unique_key=unique_key,
        ),
        "account_id",
        "Account_ID",
    )

    report = evaluate_dbt_sqlserver_selected_graph(
        _manifest(merge),
        (MERGE_ID,),
    )

    assert [issue.code for issue in report.issues] == [
        "DPONE_DBT_UNIQUE_KEY_INVALID",
    ]


def test_merge_unique_key_requires_structural_not_null_constraint() -> None:
    merge = _contracted(
        _model(
            MERGE_ID,
            materialized="incremental",
            incremental_strategy="merge",
            unique_key=["account_id", "valid_at"],
        ),
        "account_id",
        "valid_at",
        nullable=("valid_at",),
    )

    report = evaluate_dbt_sqlserver_selected_graph(
        _manifest(merge),
        (MERGE_ID,),
    )

    assert [issue.code for issue in report.issues] == [
        "DPONE_DBT_UNIQUE_KEY_NULLABLE",
    ]


@pytest.mark.parametrize(
    "macro_name",
    ["get_query_options", "sqlserver__create_table_as"],
)
def test_project_or_package_cannot_shadow_pinned_adapter_macro(
    macro_name: str,
) -> None:
    manifest = _manifest(_model())
    manifest["macros"][f"macro.analytics.{macro_name}"] = {
        "unique_id": f"macro.analytics.{macro_name}",
        "name": macro_name,
        "resource_type": "macro",
        "package_name": "analytics",
        "original_file_path": f"macros/{macro_name}.sql",
        "macro_sql": "{% macro shadow() %}{% endmacro %}",
        "depends_on": {"macros": []},
    }

    report = evaluate_dbt_sqlserver_selected_graph(manifest, (MODEL_ID,))

    assert not report.passed
    assert [(issue.unique_id, issue.field) for issue in report.issues] == [
        (f"macro.analytics.{macro_name}", "name"),
    ]


def test_pinned_adapter_macro_without_shadow_is_accepted() -> None:
    manifest = _manifest(_model())

    assert evaluate_dbt_sqlserver_selected_graph(
        manifest,
        (MODEL_ID,),
    ).passed


def test_pinned_adapter_macro_set_drift_has_safe_regeneration_guidance() -> None:
    manifest = _manifest(_model())
    macro_id = "macro.dbt_sqlserver.get_query_options"
    manifest["macros"][macro_id]["macro_sql"] += " "

    report = evaluate_dbt_sqlserver_selected_graph(manifest, (MODEL_ID,))

    assert not report.passed
    issue = report.issues[0]
    assert issue.field == "macro_sql_sha256"
    assert "dbt deps" in issue.remediation
    assert "dbt parse" in issue.remediation
    assert "do not edit manifest.json" in issue.remediation


def test_only_column_not_null_is_admitted_as_a_physical_constraint() -> None:
    model = _model()
    model["columns"] = {
        "order_id": {
            "name": "order_id",
            "data_type": "bigint",
            "constraints": [{"type": "not_null"}],
        }
    }

    report = evaluate_dbt_sqlserver_selected_graph(
        _manifest(model),
        (MODEL_ID,),
    )

    assert report.passed


@pytest.mark.parametrize(
    "constraint",
    ["unique", "primary_key", "foreign_key", "check", "custom"],
)
def test_unsupported_column_constraints_fail_before_mutation(
    constraint: str,
) -> None:
    model = _model()
    model["columns"] = {
        "order_id": {
            "name": "order_id",
            "data_type": "bigint",
            "constraints": [{"type": constraint}],
        }
    }

    report = evaluate_dbt_sqlserver_selected_graph(
        _manifest(model),
        (MODEL_ID,),
    )

    assert not report.passed
    assert [issue.code for issue in report.issues] == [
        DBT_SQLSERVER_PHYSICAL_CONSTRAINT_UNSUPPORTED,
    ]
    assert report.issues[0].field == "columns.order_id.constraints"


def test_model_constraints_fail_before_mutation() -> None:
    model = _model()
    model["constraints"] = [
        {
            "type": "primary_key",
            "columns": ["order_id"],
        }
    ]

    report = evaluate_dbt_sqlserver_selected_graph(
        _manifest(model),
        (MODEL_ID,),
    )

    assert not report.passed
    assert [issue.code for issue in report.issues] == [
        DBT_SQLSERVER_PHYSICAL_CONSTRAINT_UNSUPPORTED,
    ]
    assert report.issues[0].field == "constraints"


@pytest.mark.parametrize(
    ("overrides", "field"),
    [
        ({"language": "python"}, "language"),
        ({"config": {"enabled": True, "materialized": "custom"}}, "config.materialized"),
        ({"config": {"enabled": True, "materialized": []}}, "config.materialized"),
        (
            {
                "config": {
                    "enabled": True,
                    "materialized": "test",
                    "store_failures": True,
                }
            },
            "config.store_failures",
        ),
        (
            {
                "config": {
                    "enabled": True,
                    "materialized": "test",
                    "store_failures": 0,
                }
            },
            "config.store_failures",
        ),
    ],
)
def test_data_test_policy_requires_standard_non_persisting_sql_tests(
    overrides: dict[str, Any],
    field: str,
) -> None:
    node = _data_test()
    node.update(overrides)

    report = evaluate_dbt_sqlserver_selected_graph(
        _manifest(_model(), node),
        (MODEL_ID, TEST_ID),
    )

    assert report.passed is False
    assert field in _issue_fields(report)


def test_eager_data_test_rejects_model_dependency_outside_workflow_closure() -> None:
    data_test = _data_test()
    data_test["depends_on"]["nodes"] = [MODEL_ID, FOREIGN_MODEL_ID]
    data_test["attached_node"] = MODEL_ID

    report = evaluate_dbt_sqlserver_selected_graph(
        _manifest(_model(), _model(FOREIGN_MODEL_ID), data_test),
        (MODEL_ID, TEST_ID),
    )

    assert [(issue.code, issue.field) for issue in report.issues] == [
        (
            "DPONE_DBT_TEST_DEPENDENCY_OUTSIDE_WORKFLOW",
            "depends_on.nodes",
        ),
    ]


def test_eager_data_test_requires_one_local_model_dependency() -> None:
    data_test = _data_test()
    data_test["depends_on"]["nodes"] = ["source.analytics.orders"]

    report = evaluate_dbt_sqlserver_selected_graph(
        _manifest(_model(), data_test),
        (MODEL_ID, TEST_ID),
    )

    assert [issue.code for issue in report.issues] == [
        "DPONE_DBT_TEST_DEPENDENCY_OUTSIDE_WORKFLOW",
    ]


def test_eager_data_test_attached_node_must_be_a_local_dependency() -> None:
    data_test = _data_test()
    data_test["attached_node"] = FOREIGN_MODEL_ID

    report = evaluate_dbt_sqlserver_selected_graph(
        _manifest(_model(), data_test),
        (MODEL_ID, TEST_ID),
    )

    assert [issue.code for issue in report.issues] == [
        "DPONE_DBT_TEST_DEPENDENCY_OUTSIDE_WORKFLOW",
    ]


def test_unit_test_requires_one_selected_admitted_sql_model() -> None:
    invalid_model = _model(language="python")
    report = evaluate_dbt_sqlserver_selected_graph(
        _manifest(invalid_model, unit_tests=(_unit_test(),)),
        (MODEL_ID, UNIT_TEST_ID),
    )

    assert report.passed is False
    assert [(issue.unique_id, issue.field) for issue in report.issues] == [
        (MODEL_ID, "language"),
        (UNIT_TEST_ID, "depends_on.nodes"),
    ]


def test_unit_test_rejects_custom_language_and_materialization() -> None:
    unit_test = _unit_test()
    unit_test["language"] = "python"
    unit_test["config"]["materialized"] = "custom"

    report = evaluate_dbt_sqlserver_selected_graph(
        _manifest(_model(), unit_tests=(unit_test,)),
        (MODEL_ID, UNIT_TEST_ID),
    )

    assert _issue_fields(report) == {"language", "config.materialized"}


def test_unit_test_model_field_must_match_its_selected_model_dependency() -> None:
    unit_test = _unit_test()
    unit_test["model"] = "different_model"

    report = evaluate_dbt_sqlserver_selected_graph(
        _manifest(_model(), unit_tests=(unit_test,)),
        (MODEL_ID, UNIT_TEST_ID),
    )

    assert [(issue.unique_id, issue.field) for issue in report.issues] == [(UNIT_TEST_ID, "depends_on.nodes")]


@pytest.mark.parametrize("resource_type", ["seed", "snapshot"])
def test_seed_and_snapshot_resources_fail_closed(resource_type: str) -> None:
    unique_id = f"{resource_type}.analytics.reference"
    node = _model(unique_id)
    node["resource_type"] = resource_type
    node["config"]["materialized"] = resource_type

    report = evaluate_dbt_sqlserver_selected_graph(
        _manifest(node),
        (unique_id,),
    )

    assert report.passed is False
    assert report.issues[0].field == "resource_type"


def test_selected_upstream_model_must_share_publish_model_logical_target() -> None:
    upstream = _model(VIEW_ID, materialized="view")
    upstream["database"] = "UNRELATED_DB"
    upstream["schema"] = "foreign_schema"

    report = evaluate_dbt_sqlserver_selected_graph(
        _manifest(_model(), upstream),
        (MODEL_ID, VIEW_ID),
        expected_logical_target=("DWH", "mart"),
    )

    assert [(issue.unique_id, issue.field) for issue in report.issues] == [
        (VIEW_ID, "database/schema"),
    ]


def test_missing_unknown_and_prefix_mismatched_selected_ids_are_all_reported() -> None:
    mismatched = _model()
    mismatched["resource_type"] = "test"
    unknown_id = "exposure.analytics.dashboard"

    report = evaluate_dbt_sqlserver_selected_graph(
        _manifest(mismatched),
        (MODEL_ID, "model.analytics.missing", unknown_id),
    )

    assert report.passed is False
    assert [(issue.unique_id, issue.field) for issue in report.issues] == [
        (unknown_id, "unique_id"),
        ("model.analytics.missing", "manifest.nodes"),
        (MODEL_ID, "resource_type"),
    ]


@pytest.mark.parametrize("unique_id", ["model", "model."])
def test_malformed_selected_identity_fails_before_node_lookup(unique_id: str) -> None:
    report = evaluate_dbt_sqlserver_selected_graph(
        _manifest(_model()),
        (unique_id,),
    )

    assert [(issue.unique_id, issue.field) for issue in report.issues] == [(unique_id, "unique_id")]


def test_project_level_operation_and_selected_node_hooks_are_both_blocked() -> None:
    model = _model(**{"pre-hook": [{"sql": "select 1"}]})
    operation_id = "operation.analytics.on-run-start.0"
    operation = {
        "unique_id": operation_id,
        "resource_type": "operation",
        "original_file_path": "dbt_project.yml",
        "config": {"enabled": True},
    }

    report = evaluate_dbt_sqlserver_selected_graph(
        _manifest(model, operation),
        (MODEL_ID,),
    )

    assert [(issue.unique_id, issue.field) for issue in report.issues] == [
        (MODEL_ID, "config.pre-hook"),
        (operation_id, "resource_type"),
    ]


def test_report_is_deterministic_and_does_not_mutate_the_manifest() -> None:
    table = _model(table_refresh_method="dml", grants={"select": ["analyst"]})
    manifest = _manifest(table)
    original = deepcopy(manifest)

    first = evaluate_dbt_sqlserver_selected_graph(manifest, (MODEL_ID,))
    second = evaluate_dbt_sqlserver_selected_graph(manifest, (MODEL_ID,))

    assert first == second
    assert manifest == original
    assert [issue.field for issue in first.issues] == [
        "config.grants",
        "config.table_refresh_method",
    ]
    assert first.issues[0].path == "models/orders.sql"
    assert first.issues[0].to_jsonable()["code"] == ("DPONE_DBT_SQLSERVER_GRAPH_CAPABILITY_UNSUPPORTED")
