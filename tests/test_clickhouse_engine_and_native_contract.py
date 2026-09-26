"""ClickHouse engine identity and opaque MSSQL native contract admission."""

from __future__ import annotations

import hashlib
from pathlib import Path

from dpone.config import LoadConfig
from dpone.readiness.physical_design import PhysicalDesignOptions, PhysicalDesignPlanner
from dpone.readiness.physical_design_models import PhysicalReconciliationOptions
from dpone.readiness.physical_reconciliation import PhysicalDesignReconciler
from dpone.readiness.physical_state import PhysicalTableState
from dpone.readiness.schema_contracts import SchemaContract
from dpone.runtime.artifact_integrity import CompletedFileWrite
from dpone.runtime.etl.contract_artifacts import ContractValidatedFileArtifact
from dpone.runtime.etl.file_contract_validation import (
    OPAQUE_NATIVE_VALIDATOR_VERSION,
    FileContractValidationError,
    admit_mssql_native_file_contract,
)
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sinks.clickhouse_payload_ingestion import ClickHousePayloadIngestionService
from dpone.runtime.sinks.clickhouse_physical_reconciliation import ClickHousePhysicalIntrospector
from dpone.runtime.sinks.clickhouse_table_ddl import clickhouse_engine_identity
from dpone.runtime.sinks.load_payload import LoadPayload

_ENGINE = "ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')"


def test_engine_identity_drops_order_by_and_settings() -> None:
    full = _ENGINE + " ORDER BY (id, ValidFrom) SETTINGS index_granularity = 8192"

    assert clickhouse_engine_identity(full) == _ENGINE
    assert clickhouse_engine_identity(_ENGINE) == _ENGINE
    assert clickhouse_engine_identity("MergeTree") == "MergeTree"
    assert clickhouse_engine_identity(full) != clickhouse_engine_identity(
        "ReplicatedMergeTree('/clickhouse/tables/{uuid}/other', '{replica}')"
    )


def test_matching_replication_clause_is_not_engine_drift() -> None:
    desired = _plan_state(_ENGINE)
    actual = _plan_state(_ENGINE)

    plan = PhysicalDesignReconciler().reconcile(
        desired=desired,
        actual=actual,
        options=PhysicalReconciliationOptions(mode="auto_safe"),
        dialect=_dialect(),
    )

    assert "physical_design.shadow_required:engine" not in plan.blockers


def test_different_replication_path_is_engine_drift() -> None:
    desired = _plan_state(_ENGINE)
    actual = _plan_state("ReplicatedMergeTree('/clickhouse/tables/{uuid}/other', '{replica}')")

    plan = PhysicalDesignReconciler().reconcile(
        desired=desired,
        actual=actual,
        options=PhysicalReconciliationOptions(mode="auto_safe"),
        dialect=_dialect(),
    )

    assert "physical_design.shadow_required:engine" in plan.blockers


def test_introspector_compares_engine_full_clause_not_family_name() -> None:
    full = _ENGINE + " ORDER BY id SETTINGS index_granularity = 8192"

    class _Connector:
        def get_records(self, query: str):
            if "system.columns" in query:
                return [("id", "Int64", 1)]
            assert "system.tables" in query
            return [("ReplicatedMergeTree", full, "", "id", "id", f"ENGINE = {full}")]

    actual = ClickHousePhysicalIntrospector(_Connector()).inspect(_load_config(_ENGINE))

    assert actual.engine == _ENGINE
    assert actual.engine_full == full


def test_native_export_receipt_admits_opaque_bytes_without_a_row_scan(tmp_path: Path) -> None:
    payload = b"\x00native-bytes"
    path = tmp_path / "extract.bcp"
    path.write_bytes(payload)
    artifact = FileExportArtifact(
        str(path),
        ("id",),
        format="mssql-bcp-native",
        _completed_write=CompletedFileWrite(
            sha256=hashlib.sha256(payload).hexdigest(),
            size_bytes=len(payload),
            rows_exported=1,
        ),
    )
    contract = SchemaContract.from_config(
        {"enforcement": "strict", "columns": {"id": {"type": "bigint", "nullable": True}}}
    )

    receipt = admit_mssql_native_file_contract(artifact, schema=(("id", "bigint"),), contract=contract)

    assert receipt.validator_version == OPAQUE_NATIVE_VALIDATOR_VERSION
    assert receipt.rows_validated == 1
    wrapper = ContractValidatedFileArtifact(
        artifact,
        contract=contract,
        schema=(("id", "bigint"),),
        run_id="run",
        load_id="load",
    )
    assert wrapper.validated_file_contract_artifact is artifact


def test_native_admission_rejects_character_wire(tmp_path: Path) -> None:
    payload = b"1\n"
    path = tmp_path / "extract.bcp"
    path.write_bytes(payload)
    artifact = FileExportArtifact(
        str(path),
        ("id",),
        format="mssql-delimited",
        _completed_write=CompletedFileWrite(
            sha256=hashlib.sha256(payload).hexdigest(),
            size_bytes=len(payload),
            rows_exported=1,
        ),
    )
    contract = SchemaContract.from_config({"columns": {"id": {"type": "bigint"}}})

    try:
        admit_mssql_native_file_contract(artifact, schema=(("id", "bigint"),), contract=contract)
    except FileContractValidationError as exc:
        assert exc.blocker == "file_contract_receipt.unsupported_wire"
    else:
        raise AssertionError("character wire must not use opaque admission")


def test_clickhouse_insert_dispatches_validated_native_file(tmp_path: Path) -> None:
    payload = b"\x01"
    path = tmp_path / "extract.bcp"
    path.write_bytes(payload)
    artifact = FileExportArtifact(
        str(path),
        ("id",),
        format="mssql-bcp-native",
        _completed_write=CompletedFileWrite(
            sha256=hashlib.sha256(payload).hexdigest(),
            size_bytes=len(payload),
            rows_exported=1,
        ),
    )
    contract = SchemaContract.from_config({"columns": {"id": {"type": "bigint"}}})
    admit_mssql_native_file_contract(artifact, schema=(("id", "bigint"),), contract=contract)
    wrapper = ContractValidatedFileArtifact(
        artifact,
        contract=contract,
        schema=(("id", "bigint"),),
        run_id="run",
        load_id="load",
    )
    seen: list[str] = []

    class _Service(ClickHousePayloadIngestionService):
        def insert_file(self, load_config: LoadConfig, file_artifact: FileExportArtifact, schema: object) -> int:
            del load_config, schema
            seen.append(file_artifact.format)
            return 1

    loaded = _Service(sink=object(), sink_factory=lambda connector: connector).insert_payload(
        _load_config("MergeTree"),
        LoadPayload(artifact=wrapper, schema=[("id", "bigint")]),
    )

    assert loaded == 1
    assert seen == ["mssql-bcp-native"]


def _dialect():
    from dpone.runtime.sinks.clickhouse_physical_reconciliation import ClickHousePhysicalMigrationDialect

    return ClickHousePhysicalMigrationDialect()


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
