from __future__ import annotations

import pytest

from dpone.readiness.physical_design_models import PhysicalReconciliationApproval, PhysicalReconciliationOptions
from dpone.readiness.physical_reconciliation import PhysicalDesignReconciler
from dpone.readiness.physical_reconciliation_approval import approval_blockers
from dpone.readiness.physical_state import PhysicalTableState
from dpone.runtime.sinks.mssql_physical_introspection import MssqlPhysicalMigrationDialect


def _mssql_desired(*, compression: str = "PAGE") -> PhysicalTableState:
    return PhysicalTableState(
        sink_type="mssql",
        table="dwh_example.ch.marketing__sample_web_sync",
        table_settings={"compression": compression, "clustered_columnstore": False},
    )


def test_mssql_compression_drift_blocks_by_default() -> None:
    plan = PhysicalDesignReconciler().reconcile(
        desired=_mssql_desired(compression="PAGE"),
        actual=_mssql_desired(compression="NONE"),
        options=PhysicalReconciliationOptions(mode="block"),
        dialect=MssqlPhysicalMigrationDialect(),
    )

    assert "physical_design.blocking:table_settings.compression" in plan.blockers
    assert plan.ddl == ()


def test_mssql_safe_window_requires_approval_for_compression_rebuild() -> None:
    plan = PhysicalDesignReconciler().reconcile(
        desired=_mssql_desired(compression="PAGE"),
        actual=_mssql_desired(compression="NONE"),
        options=PhysicalReconciliationOptions(mode="safe_window"),
        dialect=MssqlPhysicalMigrationDialect(),
        execution_apply_mode="safe_window",
    )

    assert plan.blockers == ("physical_design.approval_required:table_settings.compression",)
    assert "REBUILD WITH (DATA_COMPRESSION = PAGE)" in plan.ddl[0]


def test_mssql_safe_window_executes_rebuild_when_approval_matches() -> None:
    approval = PhysicalReconciliationApproval(
        approved_by="dba-team",
        approved_risks=("table_settings.compression",),
        table="dwh_example.ch.marketing__sample_web_sync",
    )
    plan = PhysicalDesignReconciler().reconcile(
        desired=_mssql_desired(compression="ROW"),
        actual=_mssql_desired(compression="NONE"),
        options=PhysicalReconciliationOptions(mode="safe_window", approval=approval),
        dialect=MssqlPhysicalMigrationDialect(),
        execution_apply_mode="safe_window",
    )

    assert plan.blockers == ()
    assert plan.ddl == (
        "ALTER TABLE [dwh_example].[ch].[marketing__sample_web_sync] REBUILD WITH (DATA_COMPRESSION = ROW);",
    )
    assert plan.execution_apply_mode == "safe_window"


def test_approval_blockers_reject_table_mismatch_and_expiry() -> None:
    mismatch = approval_blockers(
        PhysicalReconciliationApproval(
            approved_risks=("table_settings.compression",),
            table="DWH_Stage.ch.other_table",
        ),
        risk="table_settings.compression",
        table="dwh_example.ch.marketing__sample_web_sync",
    )
    expired = approval_blockers(
        PhysicalReconciliationApproval(
            approved_risks=("table_settings.compression",),
            expires_at="2020-01-01T00:00:00Z",
        ),
        risk="table_settings.compression",
        table="dwh_example.ch.marketing__sample_web_sync",
    )

    assert mismatch == ("physical_design.approval_table_mismatch:table_settings.compression",)
    assert expired == ("physical_design.approval_expired:table_settings.compression",)


def test_physical_reconciliation_options_parse_safe_window_approval() -> None:
    options = PhysicalReconciliationOptions.from_config(
        {
            "mode": "safe_window",
            "approval": {
                "approved_by": "dba",
                "approved_risks": ["table_settings.compression"],
                "table": "dwh_example.ch.orders",
            },
        }
    )

    assert options.mode == "safe_window"
    assert options.approval is not None
    assert options.approval.approved_by == "dba"
    assert options.approval.approved_risks == ("table_settings.compression",)


def test_invalid_reconciliation_mode_is_rejected() -> None:
    with pytest.raises(ValueError, match="must be one of"):
        PhysicalReconciliationOptions.from_config({"mode": "invalid"})
