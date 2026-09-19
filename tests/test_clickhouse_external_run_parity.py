"""Executable CLI/Python parity contracts for the documented external route."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from dpone.config import LoadConfig
from dpone.contracts.clickhouse_external_replication import ExternalPublicationError
from dpone.contracts.process_types import ProcessResult
from dpone.ports.runtime_hydrator import RuntimeBindings
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.etl.source_state import SourceStateService
from dpone.runtime.lineage.audit import LoadIdentityService
from dpone.runtime.sinks.base import LoadResult
from dpone.runtime.sinks.load_result import AtomicCommitOutcome
from dpone.services.run_manifest import RunManifestService

ROOT = Path(__file__).resolve().parents[1]
DOCUMENTED_MANIFEST = ROOT / "examples/batch/clickhouse-external-replication-full-refresh.batch.yaml"
RECEIPT = {
    "phase": "COMMITTED",
    "operation_id": "external-operation",
    "evidence_scope": "runtime",
    "evidence_status": "UNVERIFIED",
}


class StubLogger:
    def log_etl_start(self, payload: Any) -> None:
        del payload

    def log_etl_progress(self, event: str, payload: Any) -> None:
        del event, payload

    def info(self, message: str) -> None:
        del message

    def warning(self, message: str) -> None:
        del message

    def log_etl_error(self, message: str, payload: Any) -> None:
        del message, payload

    def log_etl_end(self, payload: Any) -> None:
        del payload


class StubSource:
    def __init__(self) -> None:
        self.connector = SimpleNamespace()

    def get_incremental_state(self, load_config: LoadConfig) -> None:
        del load_config
        return None

    def extract(self, load_config: LoadConfig, state: Any) -> Any:
        del load_config, state
        return SimpleNamespace(
            artifact=SimpleNamespace(column_timezone=None),
            schema=[],
            state={"cursor": 1},
            force_full_refresh=False,
        )


class ExternalSink:
    def __init__(self, events: list[str], *, fail: bool) -> None:
        self.connector = SimpleNamespace()
        self.events = events
        self._fail = fail

    def load(self, load_config: LoadConfig, payload: Any) -> LoadResult:
        del load_config, payload
        if self._fail:
            self.events.append("publication_partial")
            raise ExternalPublicationError(
                "PUBLICATION_PARTIAL_TERMINAL",
                evidence={
                    "phase": "PUBLICATION_DISPATCHING",
                    "recovery": "reconcile the operation receipt before retry",
                },
            )
        self.events.append("publication_receipt_durable")
        return LoadResult(
            inserted_rows=2,
            updated_rows=0,
            total_rows=2,
            staging_rows=2,
            commit_receipt_id="external-operation",
            commit_outcome=AtomicCommitOutcome.COMMITTED,
            reconciliation_metrics={"clickhouse_cluster_external_full_refresh": dict(RECEIPT)},
        )

    def save_state(self, load_config: LoadConfig, state: Any) -> None:
        del load_config, state


class ObservedIdentity(LoadIdentityService):
    def __init__(self, events: list[str]) -> None:
        super().__init__()
        self._events = events

    def mark_committed(self, record: Any, load_result: LoadResult) -> Any:
        metrics = load_result.reconciliation_metrics or {}
        receipt = metrics["clickhouse_cluster_external_full_refresh"]
        assert all(receipt[field] == value for field, value in RECEIPT.items())
        self._events.append("target_commit_callback")
        return super().mark_committed(record, load_result)


class ObservedSourceState(SourceStateService):
    def __init__(self, events: list[str]) -> None:
        self._events = events

    def persist_after_load(self, **kwargs: Any) -> None:
        load_result = kwargs["load_result"]
        assert load_result.commit_outcome is AtomicCommitOutcome.COMMITTED
        self._events.append("source_checkpoint")


class ProcessorBackedProcess:
    def __init__(self, *, config: Any, config_path: str) -> None:
        del config_path
        self._config = config

    def run(self, *, context: Any, dag_id: str | None, execution_date: Any) -> ProcessResult:
        events = self._config.sink_obj.events
        result = ETLProcessor(
            source=self._config.source_obj,
            sink=self._config.sink_obj,
            etl_logger=self._config.etl_logger,
            load_identity_service=ObservedIdentity(events),
            source_state_service=ObservedSourceState(events),
        ).run(
            self._config.load_config,
            run_context=context,
            dag_id=dag_id,
            execution_date=execution_date,
        )
        details = {
            "reconciliation_metrics": result["reconciliation_metrics"],
            "commit_receipt_id": result["commit_receipt_id"],
            "events": list(events),
        }
        return ProcessResult(
            status=result["status"],
            inserted_rows=result["inserted_rows"],
            updated_rows=result["updated_rows"],
            final_rows=result["final_rows"],
            extracted_rows=result["extracted_rows"],
            duration_seconds=result["duration_seconds"],
            errors=result["errors"],
            details=details,
        )


class Hydrator:
    def __init__(self, *, fail: bool) -> None:
        self._fail = fail

    def build(self, *, config: dict[str, Any], load_config: LoadConfig) -> RuntimeBindings:
        del config, load_config
        events: list[str] = []
        source = StubSource()
        sink = ExternalSink(events, fail=self._fail)
        return RuntimeBindings(source_obj=source, sink_obj=sink, etl_logger=StubLogger())


def test_documented_external_route_has_processor_backed_cli_python_parity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from dpone import api
    from dpone.commands import run_cmd
    from dpone.ports import runtime_hydrator

    manifest = _runtime_manifest_without_quality_gate(tmp_path)
    monkeypatch.setattr(runtime_hydrator, "_RUNTIME_HYDRATOR", Hydrator(fail=False))
    service = lambda: RunManifestService(process_factory=ProcessorBackedProcess)  # noqa: E731
    monkeypatch.setattr(run_cmd, "RunManifestService", service)
    monkeypatch.setattr(api, "RunManifestService", service)
    args = _run_args(manifest, run_id="external-cluster-full-refresh-parity")

    assert run_cmd.cmd_run(args, ctx=object(), logger=logging.getLogger("test")) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    python_payload = api.run(manifest, run_id=args.run_id).to_dict()

    assert cli_payload["result"]["status"] == python_payload["result"]["status"] == "success"
    for field in ("inserted_rows", "updated_rows", "final_rows", "extracted_rows", "errors", "details"):
        assert cli_payload["result"][field] == python_payload["result"][field]
    details = python_payload["result"]["details"]
    assert details["commit_receipt_id"] == "external-operation"
    assert details["events"] == ["publication_receipt_durable", "target_commit_callback", "source_checkpoint"]
    receipt = details["reconciliation_metrics"]["clickhouse_cluster_external_full_refresh"]
    assert all(receipt[field] == value for field, value in RECEIPT.items())


def test_external_route_processor_failure_preserves_typed_recovery_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from dpone import api
    from dpone.commands import run_cmd
    from dpone.ports import runtime_hydrator

    manifest = _runtime_manifest_without_quality_gate(tmp_path)
    monkeypatch.setattr(runtime_hydrator, "_RUNTIME_HYDRATOR", Hydrator(fail=True))
    service = lambda: RunManifestService(process_factory=ProcessorBackedProcess)  # noqa: E731
    monkeypatch.setattr(run_cmd, "RunManifestService", service)
    monkeypatch.setattr(api, "RunManifestService", service)
    args = _run_args(manifest, run_id="external-cluster-full-refresh-failure")

    assert run_cmd.cmd_run(args, ctx=object(), logger=logging.getLogger("test")) == 1
    cli_error = json.loads(capsys.readouterr().err)
    with pytest.raises(ExternalPublicationError) as raised:
        api.run(manifest, run_id=args.run_id)

    assert raised.value.code == "DPONE_CLICKHOUSE_CLUSTER_EXTERNAL_PUBLICATION_PARTIAL_TERMINAL"
    assert cli_error["result"]["error_code"] == raised.value.code
    assert cli_error["result"]["evidence"] == raised.value.evidence


def _runtime_manifest_without_quality_gate(tmp_path: Path) -> Path:
    payload = yaml.safe_load(DOCUMENTED_MANIFEST.read_text(encoding="utf-8"))
    payload.pop("quality")
    target = tmp_path / DOCUMENTED_MANIFEST.name
    target.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return target


def _run_args(path: Path, *, run_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        path=path,
        selector=None,
        run_id=run_id,
        dag_id=None,
        execution_date=None,
        interval_start=None,
        interval_end=None,
        retry_attempts=0,
        retry_backoff_seconds=0.0,
        format="json",
        registry=[],
        sample=None,
        target=None,
        select=[],
        exclude=[],
        state=None,
        selectors="selectors.yaml",
        max_selected=10,
    )
