"""ClickHouse engine identity and staging proof for opaque native files."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.readiness.physical_design import PhysicalDesignOptions, PhysicalDesignPlanner
from dpone.readiness.physical_design_models import PhysicalReconciliationOptions
from dpone.readiness.physical_reconciliation import PhysicalDesignReconciler
from dpone.readiness.physical_state import PhysicalTableState
from dpone.readiness.schema_contracts import SchemaContract
from dpone.runtime.artifact_integrity import CompletedFileWrite
from dpone.runtime.etl.contract_artifacts import ContractValidatedFileArtifact
from dpone.runtime.etl.file_contract_validation import FileContractValidationError
from dpone.runtime.etl.lifecycle import RuntimeLifecycleService
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sinks.clickhouse_loaded_contract import (
    OBSERVED_VALIDATION_MODE,
    ClickHouseLoadedContractError,
    pending_native_observation,
)
from dpone.runtime.sinks.clickhouse_payload_ingestion import ClickHousePayloadIngestionService
from dpone.runtime.sinks.clickhouse_physical_reconciliation import ClickHousePhysicalMigrationDialect
from dpone.runtime.sinks.clickhouse_staged_load import ClickHouseStagedLoadService
from dpone.runtime.sinks.clickhouse_table_ddl import clickhouse_engines_equivalent
from dpone.runtime.sinks.load_payload import LoadPayload

_ENGINE = "ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')"
_OTHER_PATH = "ReplicatedMergeTree('/clickhouse/tables/{uuid}/other', '{replica}')"


# --- engine identity -------------------------------------------------------


def test_bare_engine_family_matches_a_parameterized_live_table() -> None:
    assert clickhouse_engines_equivalent("ReplicatedMergeTree", "ReplicatedMergeTree", _ENGINE + " ORDER BY id")
    assert clickhouse_engines_equivalent("MergeTree", "MergeTree", None)
    assert not clickhouse_engines_equivalent("MergeTree", "ReplacingMergeTree", None)


def test_parameterized_engine_must_match_the_live_clause() -> None:
    full = _ENGINE + " ORDER BY (id) SETTINGS index_granularity = 8192"

    assert clickhouse_engines_equivalent(_ENGINE, "ReplicatedMergeTree", full)
    assert not clickhouse_engines_equivalent(_OTHER_PATH, "ReplicatedMergeTree", full)


@pytest.mark.parametrize(
    ("desired", "actual_full", "drift"),
    [
        (_ENGINE, _ENGINE + " ORDER BY id SETTINGS index_granularity = 8192", False),
        ("ReplicatedMergeTree", _ENGINE + " ORDER BY id", False),
        (_ENGINE, _OTHER_PATH + " ORDER BY id", True),
    ],
)
def test_engine_drift_follows_the_replication_clause(desired: str, actual_full: str, drift: bool) -> None:
    desired_state = _plan_state(desired)
    actual = replace(desired_state, engine="ReplicatedMergeTree", engine_full=actual_full)

    plan = PhysicalDesignReconciler().reconcile(
        desired=desired_state,
        actual=actual,
        options=PhysicalReconciliationOptions(mode="auto_safe"),
        dialect=ClickHousePhysicalMigrationDialect(),
    )

    assert ("physical_design.shadow_required:engine" in plan.blockers) is drift


# --- opaque native contract -----------------------------------------------


def test_lifecycle_still_wraps_native_files_for_every_sink(tmp_path: Path) -> None:
    config = _load_config("MergeTree", contract=_contract(nullable=True))

    context = RuntimeLifecycleService().prepare_before_schema_evolution(
        load_config=config,
        payload=LoadPayload(artifact=_native(tmp_path, rows=1), schema=[("id", "bigint")]),
        run_id="run",
        load_id="load",
    )

    assert isinstance(context.payload.artifact, ContractValidatedFileArtifact)


def test_direct_clickhouse_insert_without_receipt_fails_closed(tmp_path: Path) -> None:
    payload = _wrapped(tmp_path, rows=1)
    service = ClickHousePayloadIngestionService(sink=object(), sink_factory=lambda connector: connector)

    with pytest.raises(FileContractValidationError) as raised:
        service.insert_payload(_load_config("MergeTree", contract=_contract(nullable=True)), payload)

    assert raised.value.blocker == "file_contract_receipt.required"


def test_only_unreceipted_native_wrappers_owe_an_observation(tmp_path: Path) -> None:
    config = _load_config("MergeTree", contract=_contract(nullable=True))

    assert pending_native_observation(config, _wrapped(tmp_path, rows=1)) is not None
    assert pending_native_observation(config, LoadPayload(artifact=_native(tmp_path, rows=1), schema=[])) is None
    assert pending_native_observation(config, _wrapped(tmp_path, rows=1, wire="csv")) is None


def test_staged_load_observes_the_whole_staging_table_once(tmp_path: Path) -> None:
    sink = _Sink(inserted=3, table_rows=3, nulls=0)
    payload = _wrapped(tmp_path, rows=3)

    handle = ClickHouseStagedLoadService(sink).stage(
        _load_config("MergeTree", contract=_contract(nullable=False)), payload
    )

    assert handle.staged_rows == 3
    assert [type(artifact).__name__ for artifact in sink.inserted_artifacts] == ["FileExportArtifact"]
    assert len(sink.connector.queries) == 1
    assert "technical" in sink.connector.queries[0] and "stage" in sink.connector.queries[0]
    assert payload.artifact.validation_summary.validation_mode == OBSERVED_VALIDATION_MODE


@pytest.mark.parametrize(
    ("inserted", "table_rows", "nulls", "blocker"),
    [
        (2, 3, 0, "staged_row_count_mismatch"),
        (3, 2, 0, "row_count_mismatch"),
        (3, 3, 1, "not_null_violation:id"),
    ],
)
def test_staged_load_rejects_and_drops_a_table_that_breaks_the_contract(
    tmp_path: Path, inserted: int, table_rows: int, nulls: int, blocker: str
) -> None:
    sink = _Sink(inserted=inserted, table_rows=table_rows, nulls=nulls)

    with pytest.raises(ClickHouseLoadedContractError) as raised:
        ClickHouseStagedLoadService(sink).stage(
            _load_config("MergeTree", contract=_contract(nullable=False)), _wrapped(tmp_path, rows=3)
        )

    assert raised.value.blocker == blocker
    assert sink.dropped == ["technical.stage"]


# --- helpers ----------------------------------------------------------------


class _Connector:
    def __init__(self, *, table_rows: int, nulls: int) -> None:
        self.table_rows = table_rows
        self.nulls = nulls
        self.queries: list[str] = []

    def get_records(self, query: str, as_dict: bool = False) -> list[dict[str, int]]:
        del as_dict
        self.queries.append(query)
        return [{"row_count": self.table_rows, "null__id": self.nulls}]


class _Decoder:
    def prepare(self, load_config: Any, staging_config: Any, payload: Any) -> tuple[Any, None]:
        del load_config, payload
        return staging_config, None


class _Sink:
    def __init__(self, *, inserted: int, table_rows: int, nulls: int) -> None:
        self.connector = _Connector(table_rows=table_rows, nulls=nulls)
        self._staging_decoder = _Decoder()
        self.inserted = inserted
        self.inserted_artifacts: list[Any] = []
        self.dropped: list[str] = []

    def _create_payload_staging_table(self, load_config: Any, payload: Any) -> Any:
        del payload
        return SimpleNamespace(target_schema="technical", target_table="stage", options=load_config.options)

    def _insert_payload(self, staging_config: Any, payload: Any) -> int:
        del staging_config
        self.inserted_artifacts.append(payload.artifact)
        return self.inserted

    def _drop_table(self, table: str, config: Any) -> None:
        del config
        self.dropped.append(table)

    @staticmethod
    def _table(config: Any) -> str:
        return f"{config.target_schema}.{config.target_table}"


def _native(tmp_path: Path, *, rows: int, wire: str = "mssql-bcp-native") -> FileExportArtifact:
    payload = b"\x00" * (rows + 1)
    path = tmp_path / f"extract-{wire}-{rows}.bcp"
    path.write_bytes(payload)
    return FileExportArtifact(
        str(path),
        ("id",),
        format=wire,
        _completed_write=CompletedFileWrite(
            sha256=hashlib.sha256(payload).hexdigest(),
            size_bytes=len(payload),
            rows_exported=rows,
        ),
    )


def _wrapped(tmp_path: Path, *, rows: int, wire: str = "mssql-bcp-native") -> LoadPayload:
    contract = _contract(nullable=False)
    wrapper = ContractValidatedFileArtifact(
        _native(tmp_path, rows=rows, wire=wire),
        contract=contract,
        schema=(("id", "bigint"),),
        run_id="run",
        load_id="load",
    )
    return LoadPayload(artifact=wrapper, schema=[("id", "bigint")])


def _contract(*, nullable: bool) -> SchemaContract:
    return SchemaContract.from_config({"columns": {"id": {"type": "bigint", "nullable": nullable}}})


def _plan_state(engine: str) -> PhysicalTableState:
    plan = PhysicalDesignPlanner().plan(
        sink_type="clickhouse",
        table="landing.orders",
        source_schema=[("id", "bigint")],
        options=PhysicalDesignOptions.from_config(_load_config(engine).options["physical_design"]),
    )
    return PhysicalTableState.from_physical_plan(plan)


def _load_config(engine: str, *, contract: SchemaContract | None = None) -> LoadConfig:
    options: dict[str, Any] = {
        "sink_type": "clickhouse",
        "physical_design": {"storage": {"clickhouse": {"engine": engine, "order_by": ["id"]}}},
    }
    if contract is not None:
        options["schema_contract"] = {
            "columns": {
                name: {"type": column.logical_type, "nullable": column.nullable}
                for name, column in (contract.columns or {}).items()
            }
        }
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options=options,
    )
