from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifacts import InMemoryRowsArtifact, StreamingRowsArtifact
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.sinks.base import LoadResult
from dpone.runtime.sources.base import ExtractResult


class _Logger:
    def log_etl_start(self, payload):
        del payload

    def log_etl_progress(self, event, payload):
        del event, payload

    def log_etl_error(self, message, payload):
        del message, payload

    def log_etl_end(self, payload):
        del payload


class _Source:
    def __init__(self, rows):
        self.saved = False
        self._rows = rows

    def get_incremental_state(self, load_config):
        del load_config
        return None

    def extract(self, load_config, last_state):
        del load_config, last_state
        return ExtractResult(
            artifact=InMemoryRowsArtifact(self._rows),
            schema=[("id", "bigint"), ("amount", "text")],
            state=SimpleNamespace(cursor="after"),
        )

    def save_state(self, load_config, saved_state):
        del load_config, saved_state
        self.saved = True


class _Sink:
    def __init__(self):
        self.rows: list[dict] | None = None
        self.ddl_requests = []

    def get_target_schema(self, load_config):
        del load_config
        return [("id", "bigint"), ("amount", "decimal(18,2)")]

    def target_table_exists(self, load_config):
        del load_config
        return False

    def execute_ddl(self, request):
        self.ddl_requests.append(request)

    def load(self, load_config, payload):
        del load_config
        self.rows = list(payload.artifact._rows)
        return LoadResult(
            inserted_rows=len(self.rows),
            updated_rows=0,
            total_rows=len(self.rows),
            staging_rows=len(self.rows),
        )


class _MaterializingSink(_Sink):
    def __init__(self):
        super().__init__()
        self.manager = _RuntimeStagingManager()

    def load(self, load_config, payload):
        handle = payload.artifact.materialize(self.manager, load_config, payload.schema)
        self.rows = self.manager.rows
        return LoadResult(
            inserted_rows=handle.row_count,
            updated_rows=0,
            total_rows=handle.row_count,
            staging_rows=handle.row_count,
        )


class _RuntimeStagingManager:
    def __init__(self) -> None:
        self.rows: list[dict] = []

    def create(self, load_config, schema):
        del schema
        return SimpleNamespace(
            schema=load_config.staging_schema,
            table=load_config.staging_table or "orders__stg",
            columns=[],
            staging_manager=self,
            row_count=0,
        )

    def insert_rows(self, handle, rows):
        chunk = [dict(row) for row in rows]
        self.rows.extend(chunk)
        handle.row_count += len(chunk)
        return len(chunk)


def _cfg(tmp_path: Path, *, enforcement: str, canonical_dlq: bool = False) -> LoadConfig:
    quarantine_options = (
        {"dlq": {"directory": str(tmp_path / "dlq"), "pii_policy": "reference_only"}}
        if canonical_dlq
        else {"quarantine": {"dir": str(tmp_path / "quarantine")}}
    )
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="sink",
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        options={
            "lineage": False,
            "schema_contract": {
                "enforcement": enforcement,
                "columns": {"amount": {"type": "decimal", "precision": 18, "scale": 2, "nullable": False}},
            },
            "type_inference": {"conflict_policy": "fail"},
            "physical_design": {
                "apply_runtime": True,
                "storage": {"mssql": {"compression": "page"}},
            },
            "runtime_evidence": {"output_dir": str(tmp_path / "evidence")},
            "sink_type": "mssql",
            **quarantine_options,
        },
    )


def test_etl_processor_blocks_state_and_sink_load_when_strict_contract_fails(tmp_path: Path) -> None:
    source = _Source([{"id": 1, "amount": "bad"}])
    sink = _Sink()

    with pytest.raises(RuntimeError, match="data contract enforcement failed"):
        ETLProcessor(source, sink, etl_logger=_Logger()).run(_cfg(tmp_path, enforcement="strict"))

    assert source.saved is False
    assert sink.rows is None


def test_etl_processor_quarantines_bad_rows_applies_runtime_ddl_and_writes_evidence(tmp_path: Path) -> None:
    source = _Source([{"id": 1, "amount": "10.00"}, {"id": 2, "amount": "bad"}])
    sink = _Sink()

    result = ETLProcessor(source, sink, etl_logger=_Logger()).run(_cfg(tmp_path, enforcement="quarantine"))

    assert result["status"] == "success"
    assert sink.rows == [{"id": 1, "amount": "10.00"}]
    # Fresh MSSQL target DDL is owned by the load strategy transaction; the
    # lifecycle records the delegation instead of executing DDL out of band.
    assert sink.ddl_requests == []
    quarantine_files = list((tmp_path / "quarantine").glob("*.jsonl"))
    assert len(quarantine_files) == 1
    assert '"amount": "bad"' in quarantine_files[0].read_text(encoding="utf-8")
    evidence = json.loads((tmp_path / "evidence" / "data_contract_evidence.json").read_text(encoding="utf-8"))
    assert evidence["passed"] is True
    assert evidence["ddl_apply"]["delegated_to_strategy_target_creation"] is True
    assert evidence["summary"]["quarantined_rows"] == 1
    assert evidence["openlineage_facets"]["dpone_data_contract"]["passed"] is True
    assert result["reconciliation_metrics"]["data_contract_evidence"].endswith("data_contract_evidence.json")


def test_etl_processor_wraps_streaming_artifacts_for_contract_validation(tmp_path: Path) -> None:
    class StreamingSource(_Source):
        def extract(self, load_config, last_state):
            del load_config, last_state
            return ExtractResult(
                artifact=StreamingRowsArtifact(
                    iter([{"id": 1, "amount": "10.00"}, {"id": 2, "amount": "bad"}]),
                    batch_size=1,
                    estimated_rows=2,
                ),
                schema=[("id", "bigint"), ("amount", "text")],
                state=None,
            )

    sink = _MaterializingSink()

    result = ETLProcessor(StreamingSource([]), sink, etl_logger=_Logger()).run(
        _cfg(tmp_path, enforcement="quarantine", canonical_dlq=True)
    )

    assert result["status"] == "success"
    assert sink.rows == [{"id": 1, "amount": "10.00"}]
    assert result["reconciliation_metrics"]["contract_validation"]["accepted_rows"] == 1
    assert result["reconciliation_metrics"]["contract_validation"]["quarantined_rows"] == 1
    evidence = json.loads((tmp_path / "evidence" / "data_contract_evidence.json").read_text(encoding="utf-8"))
    assert evidence["data_outcome"] == "passed_with_quarantine"
    assert evidence["summary"]["accepted_rows"] == 1


def test_etl_processor_uses_safe_canonical_dlq_without_persisting_bad_row(tmp_path: Path) -> None:
    source = _Source([{"id": 1, "amount": "10.00"}, {"id": 2, "amount": "pii-canary"}])
    sink = _Sink()

    result = ETLProcessor(source, sink, etl_logger=_Logger()).run(
        _cfg(tmp_path, enforcement="quarantine", canonical_dlq=True)
    )

    artifacts = "\n".join(path.read_text(encoding="utf-8") for path in (tmp_path / "dlq").rglob("*.json"))
    evidence = (tmp_path / "evidence" / "data_contract_evidence.json").read_text(encoding="utf-8")
    assert result["status"] == "success"
    assert sink.rows == [{"id": 1, "amount": "10.00"}]
    assert "pii-canary" not in artifacts
    assert "pii-canary" not in evidence
    assert json.loads(evidence)["data_outcome"] == "passed_with_quarantine"


def test_etl_processor_blocks_sink_and_state_when_dlq_cannot_be_created(tmp_path: Path) -> None:
    source = _Source([{"id": 1, "amount": "pii-canary"}])
    sink = _Sink()
    blocked = tmp_path / "blocked-dlq"
    blocked.write_text("not a directory", encoding="utf-8")
    config = _cfg(tmp_path, enforcement="quarantine", canonical_dlq=True)
    config.options["dlq"]["directory"] = str(blocked)

    with pytest.raises(FileExistsError):
        ETLProcessor(source, sink, etl_logger=_Logger()).run(config)

    assert source.saved is False
    assert sink.rows is None
