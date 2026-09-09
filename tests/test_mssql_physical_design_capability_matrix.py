from __future__ import annotations

import json
from itertools import product
from pathlib import Path
from typing import Any

import pytest

from dpone.readiness.physical_apply import PhysicalDdlApplyService
from dpone.readiness.physical_design import PhysicalDesignOptions, PhysicalDesignPlanner
from dpone.readiness.physical_design_models import PhysicalReconciliationApproval, PhysicalReconciliationOptions
from dpone.readiness.physical_reconciliation import PhysicalDesignReconciler, PhysicalReconciliationApplyService
from dpone.readiness.physical_state import PhysicalTableState
from dpone.runtime.sinks.mssql_physical_introspection import MssqlPhysicalMigrationDialect
from dpone.runtime.sinks.mssql_table_ddl import render_load_strategy_create_table


class _Executor:
    def __init__(self) -> None:
        self.sql: list[str] = []

    def execute(self, request: Any) -> None:
        self.sql.append(request.sql)


def _plan(physical: dict[str, object]):
    return PhysicalDesignPlanner().plan(
        sink_type="mssql",
        table="landing.orders",
        source_schema=[("id", "bigint"), ("tenant_id", "int"), ("payload", "text")],
        options=PhysicalDesignOptions.from_config(physical),
    )


@pytest.mark.parametrize(
    ("compression", "primary_key", "fillfactor"),
    [
        (compression, primary_key, fillfactor)
        for compression, primary_key in product(("none", "row", "page"), (False, True))
        for fillfactor in ((None, 1, 80, 100) if primary_key else (None,))
    ],
)
def test_all_supported_rowstore_capabilities_have_exact_ddl(
    compression: str,
    primary_key: bool,
    fillfactor: int | None,
) -> None:
    storage: dict[str, object] = {"compression": compression, "clustered_columnstore": False}
    if fillfactor is not None:
        storage["index_fillfactor"] = fillfactor
    physical: dict[str, object] = {"storage": {"mssql": storage}}
    if primary_key:
        physical["indexes"] = {"primary_key": ["id", "tenant_id"]}

    plan = _plan(physical)
    joined = "\n".join(plan.ddl)

    assert (f"DATA_COMPRESSION = {compression.upper()}" in joined) is (primary_key or compression != "none")
    assert ("PRIMARY KEY CLUSTERED ([id], [tenant_id])" in joined) is primary_key
    assert (f"FILLFACTOR = {fillfactor}" in joined) is (fillfactor is not None)
    assert plan.columns["id"].nullable is (not primary_key)
    assert plan.resolved_target_design.to_dict()["primary_key"] == (["id", "tenant_id"] if primary_key else [])


def test_supported_filegroup_lob_and_columnstore_capabilities_are_not_discarded() -> None:
    placement = _plan(
        {
            "indexes": {"primary_key": ["id"]},
            "storage": {
                "mssql": {
                    "compression": "row",
                    "filegroup": "DATA",
                    "textimage_filegroup": "LOB",
                    "index_fillfactor": 80,
                }
            },
        }
    )
    columnstore = _plan(
        {"storage": {"mssql": {"compression": "none", "clustered_columnstore": True, "filegroup": "DATA"}}}
    )

    placement_sql = "\n".join(placement.ddl)
    assert "ON [DATA] TEXTIMAGE_ON [LOB]" in placement_sql
    assert "PRIMARY KEY CLUSTERED" in placement_sql
    assert placement_sql.count("ON [DATA]") == 2
    assert "CREATE CLUSTERED COLUMNSTORE INDEX" in "\n".join(columnstore.ddl)
    assert "ON [DATA]" in "\n".join(columnstore.ddl)


@pytest.mark.parametrize(
    "physical",
    [
        {"storage": {"mssql": {"compression": "invalid"}}},
        {"storage": {"mssql": {"compression": "row", "clustered_columnstore": True}}},
        {"storage": {"mssql": {"compression": "page", "clustered_columnstore": True}}},
        {"indexes": {"primary_key": ["id"]}, "storage": {"mssql": {"clustered_columnstore": True}}},
        {"storage": {"mssql": {"textimage_filegroup": "PRIMARY"}}},
        {"storage": {"mssql": {"index_fillfactor": 80}}},
        {"storage": {"mssql": {"index_fillfactor": True}}, "indexes": {"primary_key": ["id"]}},
        {"storage": {"mssql": {"clustered_columnstore": "auto"}}},
        {"storage": {"mssql": {"filegroup": "PRIMARY]; DROP TABLE x;--"}}},
        {"storage": {"mssql": {"unsupported_setting": True}}},
        {"partitioning": {"column": "id"}},
        {"indexes": {"unsupported_index": ["id"]}},
        {"indexes": {"primary_key": []}},
        {"indexes": {"primary_key": ["id", "ID"]}},
        {"indexes": {"primary_key": ["missing"]}},
        {"columns": {"payload": {"target_type": {"mssql": "nvarchar(10)); DROP TABLE x;--"}}}},
    ],
)
def test_unsupported_or_ambiguous_capabilities_fail_before_ddl(physical: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        _plan(physical)


@pytest.mark.parametrize(
    "target_type",
    ["decimal(39,2)", "float(54)", "datetime2(8)", "nvarchar(4001)", "binary(max)", "typo"],
)
def test_invalid_mssql_column_override_fails_during_typed_option_parsing(target_type: str) -> None:
    with pytest.raises(ValueError):
        PhysicalDesignOptions.from_config({"columns": {"payload": {"target_type": {"mssql": target_type}}}})


def test_textimage_placement_requires_a_lob_capable_resolved_column() -> None:
    with pytest.raises(ValueError, match="LOB-capable"):
        PhysicalDesignPlanner().plan(
            sink_type="mssql",
            table="landing.orders",
            source_schema=[("id", "bigint")],
            options=PhysicalDesignOptions.from_config(
                {"storage": {"mssql": {"filegroup": "PRIMARY", "textimage_filegroup": "LOB"}}}
            ),
        )


@pytest.mark.parametrize(
    ("mode", "apply", "should_apply"),
    [
        (mode, apply, mode in {"auto", "explicit"} and apply in {"online", "safe_window"})
        for mode, apply in product(
            ("auto", "explicit", "off"),
            ("online", "safe_window", "plan_only", "manual_approval"),
        )
    ],
)
def test_policy_cross_product_never_mutates_disabled_or_non_execution_modes(
    mode: str,
    apply: str,
    should_apply: bool,
) -> None:
    plan = _plan({"mode": mode, "apply": apply, "storage": {"mssql": {"compression": "page"}}})
    executor = _Executor()
    report = PhysicalDdlApplyService(executor=executor).apply(plan, table_exists=False)
    assert report.applied is should_apply
    assert bool(executor.sql) is should_apply
    if mode == "off":
        assert plan.ddl == []
        assert report.blockers == ("physical_design.mode_off",)


def test_load_strategy_uses_same_contract_and_mode_off_means_plain_table() -> None:
    common = {
        "qualified_table": "[landing].[orders]",
        "columns": [("id", "bigint"), ("payload", "text")],
        "quote_identifier": lambda value: f"[{value}]",
        "to_mssql_type": lambda value: {"bigint": "bigint", "text": "nvarchar(max)"}[value],
    }
    active = render_load_strategy_create_table(
        options={
            "physical_design": {
                "indexes": {"primary_key": ["id"]},
                "storage": {"mssql": {"compression": "page"}},
            }
        },
        **common,
    )
    off = render_load_strategy_create_table(
        options={
            "physical_design": {
                "mode": "off",
                "indexes": {"primary_key": ["id"]},
                "storage": {"mssql": {"compression": "page"}},
            }
        },
        **common,
    )
    external = render_load_strategy_create_table(
        options={
            "physical_design": {
                "apply_runtime": False,
                "indexes": {"primary_key": ["id"]},
                "storage": {"mssql": {"compression": "page"}},
            }
        },
        **common,
    )
    non_execution = [
        render_load_strategy_create_table(
            options={
                "physical_design": {
                    "apply": apply_mode,
                    "indexes": {"primary_key": ["id"]},
                    "storage": {"mssql": {"compression": "page"}},
                }
            },
            **common,
        )
        for apply_mode in ("plan_only", "manual_approval")
    ]

    assert "[id] bigint NOT NULL" in active
    assert "PRIMARY KEY CLUSTERED" in active
    assert "DATA_COMPRESSION = PAGE" in active
    assert "PRIMARY KEY" not in off
    assert "DATA_COMPRESSION" not in off
    assert "PRIMARY KEY" not in external
    assert "DATA_COMPRESSION" not in external
    assert all("PRIMARY KEY" not in sql and "DATA_COMPRESSION" not in sql for sql in non_execution)


def test_columnstore_transition_suppresses_invalid_rowstore_rebuild_preview() -> None:
    desired = PhysicalTableState(
        sink_type="mssql",
        table="landing.orders",
        table_settings={"compression": "ROW", "clustered_columnstore": False},
    )
    actual = PhysicalTableState(
        sink_type="mssql",
        table="landing.orders",
        table_settings={"compression": "NONE", "clustered_columnstore": True},
    )
    plan = PhysicalDesignReconciler().reconcile(
        desired=desired,
        actual=actual,
        options=PhysicalDesignOptions().reconciliation,
        dialect=MssqlPhysicalMigrationDialect(),
    )

    assert plan.ddl == ()
    assert "physical_design.blocking:table_settings.clustered_columnstore" in plan.blockers


@pytest.mark.parametrize("apply_mode", ["plan_only", "manual_approval"])
def test_existing_table_reconciliation_respects_non_execution_apply_modes(apply_mode: str) -> None:
    executor = _Executor()
    plan = PhysicalDesignReconciler().reconcile(
        desired=PhysicalTableState(
            sink_type="mssql",
            table="landing.orders",
            table_settings={"compression": "PAGE"},
        ),
        actual=PhysicalTableState(
            sink_type="mssql",
            table="landing.orders",
            table_settings={"compression": "NONE"},
        ),
        options=PhysicalReconciliationOptions(
            mode="safe_window",
            approval=PhysicalReconciliationApproval(
                approved_by="test",
                approved_risks=("table_settings.compression",),
                table="landing.orders",
            ),
        ),
        dialect=MssqlPhysicalMigrationDialect(),
        execution_apply_mode=apply_mode,
    )
    report = PhysicalReconciliationApplyService(executor=executor).apply(plan)

    assert report.applied is False
    assert executor.sql == []
    assert report.blockers == (
        "physical_design.plan_only" if apply_mode == "plan_only" else "physical_design.manual_approval_required",
    )


def test_runtime_config_model_keeps_policy_types_strict() -> None:
    with pytest.raises(ValueError, match="enabled must be boolean"):
        PhysicalDesignOptions.from_config({"enabled": "false"})


@pytest.mark.parametrize(
    "schema_path",
    [
        Path("src/dpone/schema/etl-config.schema.json"),
        Path("src/dpone/schema/etl-batch-manifest.schema.json"),
    ],
)
def test_authoring_schema_exposes_the_same_finite_mssql_storage_surface(schema_path: Path) -> None:
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    if schema_path.name == "etl-config.schema.json":
        options = schema["properties"]["sink"]["properties"]["options"]["properties"]
    else:
        options = schema["definitions"]["process_fragment"]["properties"]["sink"]["properties"]["options"]["properties"]
    mssql = options["physical_design"]["properties"]["storage"]["properties"]["mssql"]

    assert mssql["additionalProperties"] is False
    assert set(mssql["properties"]) == {
        "compression",
        "clustered_columnstore",
        "filegroup",
        "textimage_filegroup",
        "index_fillfactor",
        "fillfactor",
    }
    assert mssql["properties"]["compression"]["enum"] == ["none", "row", "page"]
