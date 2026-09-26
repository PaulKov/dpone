"""ClickHouse engine identity and post-load checks for opaque native files."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from dpone.config import LoadConfig
from dpone.readiness.physical_design import PhysicalDesignOptions, PhysicalDesignPlanner
from dpone.readiness.physical_design_models import PhysicalReconciliationOptions
from dpone.readiness.physical_reconciliation import PhysicalDesignReconciler
from dpone.readiness.physical_state import PhysicalTableState
from dpone.runtime.etl.contract_artifacts import ContractValidatedFileArtifact
from dpone.runtime.etl.lifecycle import RuntimeLifecycleService
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.governance.acceptance_snapshot import AcceptanceMetricSnapshot
from dpone.runtime.sinks.clickhouse_loaded_contract import (
    ClickHouseLoadedContractError,
    loaded_contract_blocker,
    require_loaded_contract,
)
from dpone.runtime.sinks.clickhouse_physical_reconciliation import (
    ClickHousePhysicalIntrospector,
    ClickHousePhysicalMigrationDialect,
)
from dpone.runtime.sinks.clickhouse_table_ddl import clickhouse_engines_equivalent
from dpone.runtime.sinks.load_payload import LoadPayload

_ENGINE = "ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')"


def test_bare_engine_family_matches_a_parameterized_live_table() -> None:
    assert clickhouse_engines_equivalent("ReplicatedMergeTree", "ReplicatedMergeTree", _ENGINE + " ORDER BY id")
    assert clickhouse_engines_equivalent("MergeTree", "MergeTree", None)


def test_parameterized_engine_must_match_the_live_clause() -> None:
    full = _ENGINE + " ORDER BY (id) SETTINGS index_granularity = 8192"

    assert clickhouse_engines_equivalent(_ENGINE, "ReplicatedMergeTree", full)
    assert not clickhouse_engines_equivalent(
        "ReplicatedMergeTree('/clickhouse/tables/{uuid}/other', '{replica}')",
        "ReplicatedMergeTree",
        full,
    )
    assert not clickhouse_engines_equivalent("MergeTree", "ReplacingMergeTree", None)


def test_matching_replication_clause_is_not_engine_drift() -> None:
    desired = _plan_state(_ENGINE)
    actual = replace(
        _plan_state("ReplicatedMergeTree"),
        engine_full=_ENGINE + " ORDER BY (id) SETTINGS index_granularity = 8192",
    )

    plan = _reconcile(desired, actual)

    assert "physical_design.shadow_required:engine" not in plan.blockers


def test_bare_family_is_not_false_engine_drift() -> None:
    desired = _plan_state("ReplicatedMergeTree")
    actual = replace(desired, engine_full=_ENGINE + " ORDER BY id")

    plan = _reconcile(desired, actual)

    assert "physical_design.shadow_required:engine" not in plan.blockers


def test_different_replication_path_is_engine_drift() -> None:
    desired = _plan_state(_ENGINE)
    actual = replace(
        desired,
        engine="ReplicatedMergeTree",
        engine_full="ReplicatedMergeTree('/clickhouse/tables/{uuid}/other', '{replica}') ORDER BY id",
    )

    plan = _reconcile(desired, actual)

    assert "physical_design.shadow_required:engine" in plan.blockers


def test_introspector_keeps_family_and_full_clause() -> None:
    full = _ENGINE + " ORDER BY id SETTINGS index_granularity = 8192"

    class _Connector:
        def get_records(self, query: str):
            if "system.columns" in query:
                return [("id", "Int64", 1)]
            return [("ReplicatedMergeTree", full, "", "id", "id", f"ENGINE = {full}")]

    actual = ClickHousePhysicalIntrospector(_Connector()).inspect(_load_config(_ENGINE))

    assert actual.engine == "ReplicatedMergeTree"
    assert actual.engine_full == full


def test_loaded_contract_accepts_matching_rows_and_rejects_nulls() -> None:
    ok = AcceptanceMetricSnapshot(side="staged", row_count=2, null_counts={"id": 0})
    assert loaded_contract_blocker(ok, rows_exported=2, required_columns=("id",)) is None

    mismatch = AcceptanceMetricSnapshot(side="staged", row_count=1, null_counts={"id": 0})
    assert loaded_contract_blocker(mismatch, rows_exported=2, required_columns=("id",)) == "row_count_mismatch"

    nulls = AcceptanceMetricSnapshot(side="staged", row_count=2, null_counts={"id": 1})
    assert loaded_contract_blocker(nulls, rows_exported=2, required_columns=("id",)) == "not_null_violation:id"


def test_require_loaded_contract_queries_the_inserted_table() -> None:
    seen: list[str] = []

    class _Connector:
        def get_records(self, query: str, as_dict: bool = False):
            del as_dict
            seen.append(query)
            return [{"row_count": 2, "null__id": 0}]

    require_loaded_contract(
        _Connector(),
        database="landing",
        table="orders",
        contract=_contract(nullable=False),
        rows_exported=2,
    )

    assert "landing" in seen[0] and "orders" in seen[0] and "isNull" in seen[0]


def test_require_loaded_contract_fails_closed_without_an_export_count() -> None:
    try:
        require_loaded_contract(
            object(),
            database="landing",
            table="orders",
            contract=_contract(nullable=True),
            rows_exported=None,
        )
    except ClickHouseLoadedContractError as exc:
        assert exc.blocker == "rows_exported_required"
    else:
        raise AssertionError("missing exporter row count must fail closed")


def test_lifecycle_does_not_wrap_opaque_native_files(tmp_path: Path) -> None:
    path = tmp_path / "extract.bcp"
    path.write_bytes(b"\x00")
    native = FileExportArtifact(str(path), ("id",), format="mssql-bcp-native")
    character = FileExportArtifact(str(path), ("id",), format="csv")
    service = RuntimeLifecycleService()
    config = _load_config("MergeTree")
    config.options["schema_contract"] = {"columns": {"id": {"type": "bigint", "nullable": True}}}

    native_context = service.prepare_before_schema_evolution(
        load_config=config,
        payload=LoadPayload(artifact=native, schema=[("id", "bigint")]),
        run_id="run",
        load_id="load",
    )
    character_context = service.prepare_before_schema_evolution(
        load_config=config,
        payload=LoadPayload(artifact=character, schema=[("id", "bigint")]),
        run_id="run",
        load_id="load",
    )

    assert native_context.payload.artifact is native
    assert isinstance(character_context.payload.artifact, ContractValidatedFileArtifact)


def _reconcile(desired: PhysicalTableState, actual: PhysicalTableState):
    return PhysicalDesignReconciler().reconcile(
        desired=desired,
        actual=actual,
        options=PhysicalReconciliationOptions(mode="auto_safe"),
        dialect=ClickHousePhysicalMigrationDialect(),
    )


def _plan_state(engine: str) -> PhysicalTableState:
    plan = PhysicalDesignPlanner().plan(
        sink_type="clickhouse",
        table="landing.orders",
        source_schema=[("id", "bigint")],
        options=PhysicalDesignOptions.from_config(_load_config(engine).options["physical_design"]),
    )
    return PhysicalTableState.from_physical_plan(plan)


def _load_config(engine: str) -> LoadConfig:
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        options={
            "sink_type": "clickhouse",
            "physical_design": {
                "storage": {"clickhouse": {"engine": engine, "order_by": ["id"]}},
            },
        },
    )


def _contract(*, nullable: bool):
    from dpone.readiness.schema_contracts import SchemaContract

    return SchemaContract.from_config({"columns": {"id": {"type": "bigint", "nullable": nullable}}})
