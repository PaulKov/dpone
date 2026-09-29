from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.ops.evidence_pack import UnifiedRunEvidencePackWriter
from dpone.ops.quarantine import QuarantineService
from dpone.readiness.ddl_executors import ClickHouseDdlExecutor, MSSQLDdlExecutor, PostgresDdlExecutor
from dpone.readiness.physical_apply import DdlExecutionRequest
from dpone.readiness.profiles import ProductionProfileService
from dpone.readiness.schema_contracts import SchemaContract
from dpone.runtime.artifacts import FileExportArtifact, StreamingRowsArtifact
from dpone.runtime.etl.contract_artifacts import ContractEnforcedStreamingArtifact, ContractValidatedFileArtifact
from dpone.runtime.etl.load_config_runtime import LoadConfigRuntimeService
from dpone.runtime.sinks.clickhouse_staged_evidence import SourceByteBudgetError, enforce_source_byte_budget


class _StagingManager:
    def __init__(self) -> None:
        self.rows: list[dict] = []
        self.insert_sizes: list[int] = []

    def create(self, load_config, schema):
        del schema
        return SimpleNamespace(
            schema=load_config.staging_schema,
            table=load_config.staging_table or "orders__stg",
            columns=[],
            staging_manager=self,
            row_count=0,
            qualified_name=lambda: "staging.orders__stg",
        )

    def insert_rows(self, handle, rows):
        chunk = [dict(row) for row in rows]
        self.insert_sizes.append(len(chunk))
        self.rows.extend(chunk)
        handle.row_count += len(chunk)
        return len(chunk)


def _contract(enforcement: str = "quarantine") -> SchemaContract:
    return SchemaContract.from_config(
        {
            "enforcement": enforcement,
            "columns": {"amount": {"type": "decimal", "precision": 18, "scale": 2, "nullable": False}},
        }
    )


def _load_config():
    return SimpleNamespace(staging_schema="staging", staging_table="orders__stg")


def test_streaming_contract_artifact_validates_chunks_without_full_materialization(tmp_path: Path) -> None:
    source = StreamingRowsArtifact(
        iter([{"id": 1, "amount": "10.00"}, {"id": 2, "amount": "bad"}, {"id": 3, "amount": "20.00"}]),
        batch_size=1,
        estimated_rows=3,
    )
    quarantine = QuarantineService(tmp_path / "quarantine")
    artifact = ContractEnforcedStreamingArtifact(
        source,
        contract=_contract("quarantine"),
        run_id="01JSTREAMRUN000000000000",
        load_id="01JSTREAMLOAD00000000000",
        quarantine=quarantine,
    )
    manager = _StagingManager()

    handle = artifact.materialize(manager, _load_config(), [("id", "bigint"), ("amount", "decimal(18,2)")])

    assert handle.row_count == 2
    assert manager.rows == [{"id": 1, "amount": "10.00"}, {"id": 3, "amount": "20.00"}]
    assert artifact.validation_summary.accepted_rows == 2
    assert artifact.validation_summary.quarantined_rows == 1
    assert quarantine.export(run_id="01JSTREAMRUN000000000000").total_rows == 1
    raw_rows = [
        {"id": 1, "amount": "10.00"},
        {"id": 2, "amount": "bad"},
        {"id": 3, "amount": "20.00"},
    ]
    raw_bytes = sum(len(json.dumps(row, sort_keys=True, ensure_ascii=False).encode()) for row in raw_rows)
    evidence = enforce_source_byte_budget(
        SimpleNamespace(artifact=artifact), maximum_bytes=raw_bytes, full_refresh=True
    )
    assert evidence is not None and evidence.observed_bytes == raw_bytes
    with pytest.raises(SourceByteBudgetError, match="DPONE_SOURCE_BYTE_BUDGET_EXCEEDED"):
        enforce_source_byte_budget(SimpleNamespace(artifact=artifact), maximum_bytes=raw_bytes - 1, full_refresh=True)


def test_empty_contract_stream_materializes_explicit_zero_row_boundary() -> None:
    source = StreamingRowsArtifact(iter(()), batch_size=2, estimated_rows=0)
    artifact = ContractEnforcedStreamingArtifact(
        source,
        contract=_contract("strict"),
        run_id="01JEMPTYRUN0000000000000",
        load_id="01JEMPTYLOAD000000000000",
    )
    manager = _StagingManager()

    handle = artifact.materialize(
        manager,
        _load_config(),
        [("id", "bigint"), ("amount", "decimal(18,2)")],
    )

    assert handle.row_count == 0
    assert manager.insert_sizes == [0]
    assert artifact.validation_summary.accepted_rows == 0


def test_strict_stream_rejects_late_bad_row_without_completion_evidence() -> None:
    source = StreamingRowsArtifact(iter(({"amount": "10.00"}, {"amount": "bad"})), batch_size=1)
    artifact = ContractEnforcedStreamingArtifact(
        source,
        contract=_contract("strict"),
        run_id="01JSTRICTRUN00000000000",
        load_id="01JSTRICTLOAD0000000000",
    )
    inserted: list[dict] = []

    with pytest.raises(RuntimeError, match="data contract enforcement failed"):
        artifact.load_with_row_inserter(lambda rows: inserted.extend(rows) or len(rows))

    assert inserted == [{"amount": "10.00"}]
    assert artifact.enforcement_result is None
    assert not hasattr(source, "rows_exported")


def test_validated_stream_rejects_unconfirmed_sink_row_count() -> None:
    source = StreamingRowsArtifact(iter(({"amount": "10.00"},)), batch_size=1)
    artifact = ContractEnforcedStreamingArtifact(
        source,
        contract=_contract("strict"),
        run_id="01JCOUNTMISMATCHRUN00000",
        load_id="01JCOUNTMISMATCHLOAD0000",
    )

    with pytest.raises(RuntimeError, match="streaming_rows.insert_count_mismatch"):
        artifact.load_with_row_inserter(lambda rows: len(rows) - 1)

    assert artifact.enforcement_result is None
    assert not hasattr(source, "rows_exported")


@pytest.mark.parametrize("batch_size", (0, -1))
def test_streaming_rows_rejects_nonpositive_batch_size(batch_size: int) -> None:
    with pytest.raises(ValueError, match="streaming_rows.batch_size_positive"):
        StreamingRowsArtifact(iter(()), batch_size=batch_size)


def test_strict_contract_file_artifact_fails_closed_when_file_is_opaque(tmp_path: Path) -> None:
    file_path = tmp_path / "orders.tsv"
    file_path.write_text("1\tbad\n", encoding="utf-8")
    artifact = ContractValidatedFileArtifact(
        FileExportArtifact(str(file_path), ["id", "amount"], format="tsv"),
        contract=_contract("strict"),
        run_id="01JFILECONTRACT0000000000",
        load_id="01JFILELOAD000000000000",
    )

    with pytest.raises(RuntimeError, match="file_contract_receipt.required"):
        artifact.materialize(_StagingManager(), _load_config(), [("id", "bigint"), ("amount", "decimal(18,2)")])


def test_dialect_ddl_executors_render_session_safety_before_sql() -> None:
    request = DdlExecutionRequest(
        sink_type="mssql",
        table="landing.orders",
        sql="ALTER TABLE [landing].[orders] ADD [amount] decimal(18,2) NULL;",
        apply_mode="online",
    )
    mssql = MSSQLDdlExecutor()
    postgres = PostgresDdlExecutor()
    clickhouse = ClickHouseDdlExecutor()

    assert mssql.plan(request)[0].startswith("SET LOCK_TIMEOUT")
    assert postgres.plan(request)[0].startswith("SET lock_timeout")
    assert clickhouse.plan(request)[0].startswith("SET mutations_sync")


def test_unified_evidence_pack_collects_runtime_artifacts(tmp_path: Path) -> None:
    run = tmp_path / "run.json"
    contract = tmp_path / "data_contract_evidence.json"
    ddl = tmp_path / "physical_ddl.json"
    for path in (run, contract, ddl):
        path.write_text(json.dumps({"passed": True}), encoding="utf-8")

    artifact = UnifiedRunEvidencePackWriter(tmp_path / "pack").write(
        run_id="01JEVIDENCEPACK000000000",
        artifacts={"run": run, "data_contract": contract, "physical_ddl": ddl},
    )

    payload = json.loads(artifact.json_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "dpone.unified_run_evidence.v1"
    assert payload["passed"] is True
    assert {item["name"] for item in payload["items"]} == {"run", "data_contract", "physical_ddl"}
    assert "# dpone unified run evidence" in artifact.markdown_path.read_text(encoding="utf-8")


def test_production_safe_profile_merges_fail_closed_defaults() -> None:
    resolved = ProductionProfileService().apply(
        {
            "profile": "production_safe",
            "sink": {"options": {"type_inference": {"sample_rows": 5000}}},
        }
    )

    assert resolved["schema_contract"]["enforcement"] == "strict"
    assert resolved["sink"]["options"]["type_inference"]["conflict_policy"] == "fail"
    assert resolved["sink"]["options"]["type_inference"]["sample_rows"] == 5000
    assert resolved["sink"]["options"]["physical_design"]["apply"] == "online"
    assert resolved["sink"]["options"]["runtime_evidence"]["enabled"] is True


def test_production_safe_profile_is_applied_to_runtime_load_config_options() -> None:
    cfg = LoadConfig(
        source_conn_id="src",
        target_conn_id="sink",
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={"profile": "production_safe", "type_inference": {"sample_rows": 1234}},
    )

    prepared = LoadConfigRuntimeService().prepare(cfg)

    assert prepared.options["schema_contract"]["enforcement"] == "strict"
    assert prepared.options["type_inference"]["conflict_policy"] == "fail"
    assert prepared.options["type_inference"]["sample_rows"] == 1234
    assert prepared.options["physical_design"]["apply"] == "online"
